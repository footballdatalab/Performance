"""
Data quality engine for outlier detection and anomaly flagging.

Supports multiple detection methods:
- Modified Z-Score (median + MAD, robust to outliers)
- IQR-based detection
- Absolute range checks
- Asymmetry detection (left/right imbalance)
- Negative value detection for non-negative metrics

Baselines are computed from historical bronze data and cached in
silver.data_quality_baseline. Thresholds are configurable per metric
via silver.data_quality_threshold.
"""

import json
import math
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

import psycopg2.extras

from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger

logger = get_logger(__name__)

DEFAULT_ZSCORE_WARNING = 2.5
DEFAULT_ZSCORE_CRITICAL = 3.5
DEFAULT_IQR_MULTIPLIER = 2.0
DEFAULT_ASYMMETRY_WARNING_PCT = 15.0
DEFAULT_ASYMMETRY_CRITICAL_PCT = 25.0
MIN_SAMPLE_SIZE = 10  # Need at least N records to compute meaningful stats


@dataclass
class QualityFlag:
    """A single data quality flag to be persisted."""
    source_table: str
    record_id: str
    metric_name: str
    metric_value: Optional[float]
    flag_type: str
    severity: str  # 'info', 'warning', 'critical'
    details: dict = field(default_factory=dict)
    profile_id: Optional[str] = None
    tenant_id: Optional[str] = None
    test_date: Optional[str] = None
    batch_id: Optional[str] = None
    # Phase 1 (2026-05-09): per-team scoping. NULL means "global / not
    # team-specific" (legacy flags from before the migration; or
    # checks that don't have a team dimension like duplicate_suspect).
    team_group_id: Optional[str] = None


@dataclass
class MetricRule:
    """Configuration for how to check a single metric."""
    metric_name: str
    abs_min: Optional[float] = None
    abs_max: Optional[float] = None
    non_negative: bool = True
    zscore_warning: float = DEFAULT_ZSCORE_WARNING
    zscore_critical: float = DEFAULT_ZSCORE_CRITICAL
    iqr_multiplier: float = DEFAULT_IQR_MULTIPLIER
    check_zscore: bool = True
    check_iqr: bool = True
    check_range: bool = True


@dataclass
class AsymmetryRule:
    """Configuration for checking left/right asymmetry.

    Args:
        min_absolute: Skip this check when either value is below this
            threshold.  Useful for inner/outer comparisons where some
            test types don't use one channel (value ≈ 0).
    """
    left_metric: str
    right_metric: str
    label: str  # descriptive name for the asymmetry check
    warning_pct: float = DEFAULT_ASYMMETRY_WARNING_PCT
    critical_pct: float = DEFAULT_ASYMMETRY_CRITICAL_PCT
    min_absolute: float = 0.0


@dataclass
class QualityRuleSet:
    """Complete set of rules for a bronze/silver table."""
    source_table: str
    test_type_column: Optional[str] = None  # column to group baselines by
    record_id_column: str = "test_id"
    profile_id_column: str = "profile_id"
    tenant_id_column: str = "tenant_id"
    test_date_column: Optional[str] = None
    metric_rules: list[MetricRule] = field(default_factory=list)
    asymmetry_rules: list[AsymmetryRule] = field(default_factory=list)
    # Phase 1 (2026-05-09): per-team baseline scoping. When set, the
    # baseline is computed per (tenant_id, team_group_id, test_type,
    # metric_name) — so an outlier is "outlier within this team" rather
    # than "outlier across the whole organization". The user's locked
    # decision #5 says baseline window = all-time; locked decision is
    # per-team because a 70cm jump-height is normal for the senior
    # squad and an outlier for the U17 group.
    team_group_column: Optional[str] = None
    # Optional WHERE clause appended to baseline + audit reads.
    # Example: ``family_filter="source_module = 'forcedecks'"``
    family_filter: Optional[str] = None
    # Free-form labels for telemetry / API filters.
    provider: Optional[str] = None    # 'vald' | 'catapult'
    family: Optional[str] = None       # 'forcedecks', 'nordics', etc.


class QualityEngine:
    """Runs quality checks against bronze data and writes flags to silver."""

    def __init__(self, db: DatabaseManager):
        self.db = db
        self._baseline_cache: dict[str, dict] = {}
        self._threshold_cache: dict[str, dict] = {}
        # Maps source_table -> test_type_column name (populated during audit)
        self._test_type_columns: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Baseline computation
    # ------------------------------------------------------------------

    def compute_baseline(self, source_table: str, metric_name: str,
                         test_type: Optional[str] = None,
                         tenant_id: Optional[str] = None,
                         team_group_id: Optional[str] = None,
                         team_group_column: Optional[str] = None,
                         family_filter: Optional[str] = None) -> Optional[dict]:
        """Compute population statistics for a metric from bronze data.

        Runs SQL aggregation directly against the bronze table to compute
        mean, std, median, MAD, percentiles. Upserts into
        silver.data_quality_baseline.

        Phase 1 (2026-05-09) extensions:
          - ``team_group_id`` + ``team_group_column``: when both set,
            the baseline is filtered to rows in that team group. This
            is the "per-team baseline" the user requested.
          - ``family_filter``: optional WHERE clause appended verbatim,
            used by silver-layer rule sets to filter to a single
            assessment family (e.g. ``source_module = 'forcedecks'``).
        """
        where_clauses = [f"{metric_name} IS NOT NULL"]
        params: list = []

        # Filter by test_type if provided (for per-test-type baselines)
        if test_type is not None:
            # Discover the test_type column from the rule set registry
            tt_col = self._resolve_test_type_column(source_table)
            if tt_col:
                where_clauses.append(f"{tt_col} = %s")
                params.append(test_type)

        # Phase 1: filter by team_group_id when scoping per-team.
        if team_group_id is not None and team_group_column:
            where_clauses.append(f"{team_group_column} = %s::UUID")
            params.append(team_group_id)

        # Phase 1: family_filter is a literal SQL fragment (no params).
        # Caller is trusted (only set internally from the rule_set).
        if family_filter:
            where_clauses.append(f"({family_filter})")

        where_sql = ' AND '.join(where_clauses)

        sql = f"""
            SELECT
                COUNT(*)                                        AS sample_count,
                AVG({metric_name})                              AS mean_value,
                STDDEV_POP({metric_name})                       AS std_value,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {metric_name})  AS median_value,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {metric_name}) AS p25_value,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {metric_name}) AS p75_value,
                MIN({metric_name})                              AS min_value,
                MAX({metric_name})                              AS max_value
            FROM {source_table}
            WHERE {where_sql}
        """

        row = self.db.fetch_one(sql, tuple(params))
        if not row or row[0] < MIN_SAMPLE_SIZE:
            return None

        sample_count, mean_val, std_val, median_val, p25_val, p75_val, min_val, max_val = row

        # Compute MAD (Median Absolute Deviation)
        mad_sql = f"""
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY ABS({metric_name} - {float(median_val)})
            )
            FROM {source_table}
            WHERE {where_sql}
        """
        mad_row = self.db.fetch_one(mad_sql, tuple(params))
        mad_val = mad_row[0] if mad_row else None

        baseline = {
            'source_table': source_table,
            'metric_name': metric_name,
            'test_type': test_type,
            'tenant_id': tenant_id,
            'team_group_id': team_group_id,  # Phase 1
            'sample_count': sample_count,
            'mean_value': float(mean_val) if mean_val is not None else None,
            'std_value': float(std_val) if std_val is not None else None,
            'median_value': float(median_val) if median_val is not None else None,
            'mad_value': float(mad_val) if mad_val is not None else None,
            'p25_value': float(p25_val) if p25_val is not None else None,
            'p75_value': float(p75_val) if p75_val is not None else None,
            'min_value': float(min_val) if min_val is not None else None,
            'max_value': float(max_val) if max_val is not None else None,
        }

        # Upsert into silver.data_quality_baseline
        self._upsert_baseline(baseline)
        return baseline

    def _upsert_baseline(self, baseline: dict) -> None:
        """Upsert a baseline record.

        Phase 1 (2026-05-09): the unique key now includes
        ``team_group_id`` so per-team baselines coexist with the legacy
        global / per-tenant rows. The COALESCE fenceposts in the index
        keep NULL keys collation-safe.
        """
        sql = """
            INSERT INTO silver.data_quality_baseline
                (source_table, metric_name, test_type, tenant_id,
                 team_group_id, sample_count, mean_value, std_value,
                 median_value, mad_value, p25_value, p75_value,
                 min_value, max_value, last_computed_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            ON CONFLICT (source_table, metric_name,
                         COALESCE(test_type, '__all__'),
                         COALESCE(tenant_id, '00000000-0000-0000-0000-000000000000'::UUID),
                         COALESCE(team_group_id, '00000000-0000-0000-0000-000000000000'::UUID))
            DO UPDATE SET
                sample_count = EXCLUDED.sample_count,
                mean_value = EXCLUDED.mean_value,
                std_value = EXCLUDED.std_value,
                median_value = EXCLUDED.median_value,
                mad_value = EXCLUDED.mad_value,
                p25_value = EXCLUDED.p25_value,
                p75_value = EXCLUDED.p75_value,
                min_value = EXCLUDED.min_value,
                max_value = EXCLUDED.max_value,
                last_computed_at = now(),
                updated_at = now()
        """
        self.db.execute(sql, (
            baseline['source_table'], baseline['metric_name'],
            baseline.get('test_type'), baseline.get('tenant_id'),
            baseline.get('team_group_id'),  # Phase 1
            baseline['sample_count'], baseline['mean_value'], baseline['std_value'],
            baseline['median_value'], baseline['mad_value'],
            baseline['p25_value'], baseline['p75_value'],
            baseline['min_value'], baseline['max_value'],
        ))

    def _resolve_test_type_column(self, source_table: str) -> Optional[str]:
        """Return the test_type column name for a source table, if known."""
        return self._test_type_columns.get(source_table)

    def get_baseline(self, source_table: str, metric_name: str,
                     test_type: Optional[str] = None,
                     tenant_id: Optional[str] = None,
                     team_group_id: Optional[str] = None) -> Optional[dict]:
        """Retrieve cached or stored baseline for a metric.

        Phase 1 (2026-05-09): cache key + WHERE clause include
        ``team_group_id`` so per-team baselines and legacy global
        baselines are addressed independently.
        """
        cache_key = f"{source_table}|{metric_name}|{test_type}|{tenant_id}|{team_group_id}"
        if cache_key in self._baseline_cache:
            return self._baseline_cache[cache_key]

        sql = """
            SELECT sample_count, mean_value, std_value, median_value, mad_value,
                   p25_value, p75_value, min_value, max_value
            FROM silver.data_quality_baseline
            WHERE source_table = %s AND metric_name = %s
              AND COALESCE(test_type, '__all__') = COALESCE(%s, '__all__')
              AND COALESCE(tenant_id, '00000000-0000-0000-0000-000000000000'::UUID)
                = COALESCE(%s::UUID, '00000000-0000-0000-0000-000000000000'::UUID)
              AND COALESCE(team_group_id, '00000000-0000-0000-0000-000000000000'::UUID)
                = COALESCE(%s::UUID, '00000000-0000-0000-0000-000000000000'::UUID)
        """
        row = self.db.fetch_one(
            sql,
            (source_table, metric_name, test_type, tenant_id, team_group_id),
        )
        if not row:
            return None

        baseline = {
            'sample_count': row[0], 'mean_value': row[1], 'std_value': row[2],
            'median_value': row[3], 'mad_value': row[4],
            'p25_value': row[5], 'p75_value': row[6],
            'min_value': row[7], 'max_value': row[8],
        }
        self._baseline_cache[cache_key] = baseline
        return baseline

    # ------------------------------------------------------------------
    # Outlier detection methods
    # ------------------------------------------------------------------

    @staticmethod
    def modified_zscore(value: float, median: float, mad: float,
                        min_mad: float = 1.0) -> Optional[float]:
        """Compute modified z-score using median and MAD.

        More robust than standard z-score for non-normal distributions.
        Uses the consistency constant 0.6745 for normal distribution equivalence.

        When MAD is below *min_mad* (default 1.0), the data is too
        concentrated for the z-score to be meaningful and ``None`` is
        returned.  This prevents hypersensitivity in near-constant metrics
        (e.g. force channels reading ~0 for unused test types).
        """
        if mad is None or mad < min_mad:
            return None
        return 0.6745 * (value - median) / mad

    @staticmethod
    def standard_zscore(value: float, mean: float, std: float) -> Optional[float]:
        """Compute standard z-score."""
        if std is None or std == 0:
            return None
        return (value - mean) / std

    @staticmethod
    def iqr_bounds(p25: float, p75: float, multiplier: float = 2.0) -> tuple[float, float]:
        """Compute IQR-based outlier bounds."""
        iqr = p75 - p25
        return (p25 - multiplier * iqr, p75 + multiplier * iqr)

    @staticmethod
    def asymmetry_pct(left: float, right: float) -> Optional[float]:
        """Compute asymmetry percentage between left and right values.

        Returns absolute percentage difference relative to the maximum.
        """
        max_val = max(abs(left), abs(right))
        if max_val == 0:
            return 0.0
        return abs(left - right) / max_val * 100.0

    # ------------------------------------------------------------------
    # Check a single record against a rule set
    # ------------------------------------------------------------------

    def check_record(self, record: dict, rule_set: QualityRuleSet,
                     batch_id: Optional[str] = None) -> list[QualityFlag]:
        """Run all quality checks on a single record. Returns list of flags."""
        flags = []
        record_id = str(record.get(rule_set.record_id_column, ''))
        profile_id = record.get(rule_set.profile_id_column)
        tenant_id = record.get(rule_set.tenant_id_column)
        test_date = record.get(rule_set.test_date_column) if rule_set.test_date_column else None
        test_type = record.get(rule_set.test_type_column) if rule_set.test_type_column else None
        # Phase 1: per-team scoping for baselines + flag attribution.
        team_group_id = (
            record.get(rule_set.team_group_column)
            if rule_set.team_group_column
            else None
        )
        team_group_id_str = str(team_group_id) if team_group_id else None

        # Check each metric rule
        for rule in rule_set.metric_rules:
            value = record.get(rule.metric_name)
            if value is None:
                continue

            try:
                value = float(value)
            except (ValueError, TypeError):
                continue

            # Non-negative check
            if rule.non_negative and value < 0:
                flags.append(QualityFlag(
                    source_table=rule_set.source_table,
                    record_id=record_id,
                    metric_name=rule.metric_name,
                    metric_value=value,
                    flag_type='negative_value',
                    severity='critical',
                    details={'expected': '>= 0', 'actual': value},
                    profile_id=str(profile_id) if profile_id else None,
                    tenant_id=str(tenant_id) if tenant_id else None,
                    team_group_id=team_group_id_str,
                    test_date=str(test_date) if test_date else None,
                    batch_id=batch_id,
                ))

            # Absolute range check
            if rule.check_range:
                if rule.abs_min is not None and value < rule.abs_min:
                    flags.append(QualityFlag(
                        source_table=rule_set.source_table,
                        record_id=record_id,
                        metric_name=rule.metric_name,
                        metric_value=value,
                        flag_type='range_violation',
                        severity='critical',
                        details={'abs_min': rule.abs_min, 'abs_max': rule.abs_max, 'actual': value},
                        profile_id=str(profile_id) if profile_id else None,
                        tenant_id=str(tenant_id) if tenant_id else None,
                        team_group_id=team_group_id_str,
                        test_date=str(test_date) if test_date else None,
                        batch_id=batch_id,
                    ))
                if rule.abs_max is not None and value > rule.abs_max:
                    flags.append(QualityFlag(
                        source_table=rule_set.source_table,
                        record_id=record_id,
                        metric_name=rule.metric_name,
                        metric_value=value,
                        flag_type='range_violation',
                        severity='critical',
                        details={'abs_min': rule.abs_min, 'abs_max': rule.abs_max, 'actual': value},
                        profile_id=str(profile_id) if profile_id else None,
                        tenant_id=str(tenant_id) if tenant_id else None,
                        team_group_id=team_group_id_str,
                        test_date=str(test_date) if test_date else None,
                        batch_id=batch_id,
                    ))

            # Statistical checks: prefer per-team + per-test-type baseline,
            # then per-test-type, then global. Phase 1: per-team is the
            # primary key for outlier detection; the cascade is the
            # backstop for thin samples.
            baseline = None
            if team_group_id_str and test_type:
                baseline = self.get_baseline(
                    rule_set.source_table, rule.metric_name,
                    test_type=test_type, team_group_id=team_group_id_str,
                )
            if (not baseline or baseline['sample_count'] < MIN_SAMPLE_SIZE) and team_group_id_str:
                baseline = self.get_baseline(
                    rule_set.source_table, rule.metric_name,
                    team_group_id=team_group_id_str,
                )
            if (not baseline or baseline['sample_count'] < MIN_SAMPLE_SIZE) and test_type:
                baseline = self.get_baseline(
                    rule_set.source_table, rule.metric_name, test_type=test_type,
                )
            if not baseline or baseline['sample_count'] < MIN_SAMPLE_SIZE:
                baseline = self.get_baseline(rule_set.source_table, rule.metric_name)
            if not baseline or baseline['sample_count'] < MIN_SAMPLE_SIZE:
                continue

            # Modified Z-Score (robust)
            if rule.check_zscore and baseline.get('median_value') is not None and baseline.get('mad_value'):
                mz = self.modified_zscore(value, baseline['median_value'], baseline['mad_value'])
                if mz is not None:
                    abs_mz = abs(mz)
                    if abs_mz >= rule.zscore_critical:
                        flags.append(QualityFlag(
                            source_table=rule_set.source_table,
                            record_id=record_id,
                            metric_name=rule.metric_name,
                            metric_value=value,
                            flag_type='outlier_modified_zscore',
                            severity='critical',
                            details={
                                'modified_zscore': round(mz, 3),
                                'threshold': rule.zscore_critical,
                                'median': baseline['median_value'],
                                'mad': baseline['mad_value'],
                                'population_min': baseline['min_value'],
                                'population_max': baseline['max_value'],
                                'sample_count': baseline['sample_count'],
                            },
                            profile_id=str(profile_id) if profile_id else None,
                            tenant_id=str(tenant_id) if tenant_id else None,
                            team_group_id=team_group_id_str,
                            test_date=str(test_date) if test_date else None,
                            batch_id=batch_id,
                        ))
                    elif abs_mz >= rule.zscore_warning:
                        flags.append(QualityFlag(
                            source_table=rule_set.source_table,
                            record_id=record_id,
                            metric_name=rule.metric_name,
                            metric_value=value,
                            flag_type='outlier_modified_zscore',
                            severity='warning',
                            details={
                                'modified_zscore': round(mz, 3),
                                'threshold': rule.zscore_warning,
                                'median': baseline['median_value'],
                                'mad': baseline['mad_value'],
                                'population_min': baseline['min_value'],
                                'population_max': baseline['max_value'],
                                'sample_count': baseline['sample_count'],
                            },
                            profile_id=str(profile_id) if profile_id else None,
                            tenant_id=str(tenant_id) if tenant_id else None,
                            team_group_id=team_group_id_str,
                            test_date=str(test_date) if test_date else None,
                            batch_id=batch_id,
                        ))

            # IQR-based check
            if rule.check_iqr and baseline.get('p25_value') is not None and baseline.get('p75_value') is not None:
                lower, upper = self.iqr_bounds(baseline['p25_value'], baseline['p75_value'], rule.iqr_multiplier)
                if value < lower or value > upper:
                    severity = 'critical' if (value < lower - (upper - lower) or value > upper + (upper - lower)) else 'warning'
                    flags.append(QualityFlag(
                        source_table=rule_set.source_table,
                        record_id=record_id,
                        metric_name=rule.metric_name,
                        metric_value=value,
                        flag_type='outlier_iqr',
                        severity=severity,
                        details={
                            'iqr_lower': round(lower, 3),
                            'iqr_upper': round(upper, 3),
                            'iqr_multiplier': rule.iqr_multiplier,
                            'p25': baseline['p25_value'],
                            'p75': baseline['p75_value'],
                            'sample_count': baseline['sample_count'],
                        },
                        profile_id=str(profile_id) if profile_id else None,
                        tenant_id=str(tenant_id) if tenant_id else None,
                        team_group_id=team_group_id_str,
                        test_date=str(test_date) if test_date else None,
                        batch_id=batch_id,
                    ))

        # Asymmetry checks
        for asym in rule_set.asymmetry_rules:
            left_val = record.get(asym.left_metric)
            right_val = record.get(asym.right_metric)
            if left_val is None or right_val is None:
                continue
            try:
                left_val = float(left_val)
                right_val = float(right_val)
            except (ValueError, TypeError):
                continue

            # Skip when either value is below min_absolute (e.g., channel not
            # used for this test type — inner/outer on non-rotational tests)
            if asym.min_absolute and (abs(left_val) < asym.min_absolute or abs(right_val) < asym.min_absolute):
                continue

            pct = self.asymmetry_pct(left_val, right_val)
            if pct is not None and pct >= asym.critical_pct:
                flags.append(QualityFlag(
                    source_table=rule_set.source_table,
                    record_id=record_id,
                    metric_name=asym.label,
                    metric_value=round(pct, 2),
                    flag_type='asymmetry_extreme',
                    severity='critical',
                    details={
                        'left_metric': asym.left_metric,
                        'right_metric': asym.right_metric,
                        'left_value': left_val,
                        'right_value': right_val,
                        'asymmetry_pct': round(pct, 2),
                        'threshold_pct': asym.critical_pct,
                    },
                    profile_id=str(profile_id) if profile_id else None,
                    tenant_id=str(tenant_id) if tenant_id else None,
                    team_group_id=team_group_id_str,
                    test_date=str(test_date) if test_date else None,
                    batch_id=batch_id,
                ))
            elif pct is not None and pct >= asym.warning_pct:
                flags.append(QualityFlag(
                    source_table=rule_set.source_table,
                    record_id=record_id,
                    metric_name=asym.label,
                    metric_value=round(pct, 2),
                    flag_type='asymmetry_extreme',
                    severity='warning',
                    details={
                        'left_metric': asym.left_metric,
                        'right_metric': asym.right_metric,
                        'left_value': left_val,
                        'right_value': right_val,
                        'asymmetry_pct': round(pct, 2),
                        'threshold_pct': asym.warning_pct,
                    },
                    profile_id=str(profile_id) if profile_id else None,
                    tenant_id=str(tenant_id) if tenant_id else None,
                    team_group_id=team_group_id_str,
                    test_date=str(test_date) if test_date else None,
                    batch_id=batch_id,
                ))

        return flags

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def check_batch(self, records: list[dict], rule_set: QualityRuleSet,
                    batch_id: Optional[str] = None) -> list[QualityFlag]:
        """Check a batch of records and return all flags."""
        all_flags = []
        for record in records:
            all_flags.extend(self.check_record(record, rule_set, batch_id))
        return all_flags

    def refresh_baselines(self, rule_set: QualityRuleSet) -> int:
        """Recompute all baselines for a rule set from current bronze/silver data.

        Computes baselines at every grouping level the rule set asks for:
          1. Global (across the whole table).
          2. Per-test-type (when ``test_type_column`` is set).
          3. Per-team-group (Phase 1 — when ``team_group_column`` is set).
          4. Per-team-group + per-test-type (Phase 1 — both set).

        All baselines are cached in memory so ``check_record`` never hits
        the DB for lookups.
        """
        count = 0

        # --- Global baselines (across all test types AND teams) ---
        for rule in rule_set.metric_rules:
            baseline = self.compute_baseline(
                rule_set.source_table, rule.metric_name,
                family_filter=rule_set.family_filter,
            )
            if baseline:
                count += 1
                cache_key = f"{rule_set.source_table}|{rule.metric_name}|None|None|None"
                self._baseline_cache[cache_key] = baseline
                logger.info(
                    "Baseline (global): %s.%s — n=%d, median=%.2f, MAD=%.4f",
                    rule_set.source_table, rule.metric_name,
                    baseline['sample_count'],
                    baseline['median_value'] or 0,
                    baseline['mad_value'] or 0,
                )

        # --- Per-test-type baselines ---
        if rule_set.test_type_column:
            test_types = self._get_distinct_test_types(
                rule_set.source_table, rule_set.test_type_column,
                family_filter=rule_set.family_filter,
            )
            logger.info(
                "Computing per-test-type baselines for %d test types in %s",
                len(test_types), rule_set.source_table,
            )
            for tt in test_types:
                for rule in rule_set.metric_rules:
                    baseline = self.compute_baseline(
                        rule_set.source_table, rule.metric_name,
                        test_type=tt,
                        family_filter=rule_set.family_filter,
                    )
                    if baseline:
                        count += 1
                        cache_key = f"{rule_set.source_table}|{rule.metric_name}|{tt}|None|None"
                        self._baseline_cache[cache_key] = baseline

        # --- Per-team-group baselines (Phase 1) ---
        if rule_set.team_group_column:
            team_group_ids = self._get_distinct_team_group_ids(
                rule_set.source_table, rule_set.team_group_column,
                family_filter=rule_set.family_filter,
            )
            logger.info(
                "Computing per-team-group baselines for %d team groups in %s",
                len(team_group_ids), rule_set.source_table,
            )
            for tgid in team_group_ids:
                # team-group only
                for rule in rule_set.metric_rules:
                    baseline = self.compute_baseline(
                        rule_set.source_table, rule.metric_name,
                        team_group_id=tgid,
                        team_group_column=rule_set.team_group_column,
                        family_filter=rule_set.family_filter,
                    )
                    if baseline:
                        count += 1
                        cache_key = f"{rule_set.source_table}|{rule.metric_name}|None|None|{tgid}"
                        self._baseline_cache[cache_key] = baseline

                # team-group × test_type
                if rule_set.test_type_column:
                    test_types = self._get_distinct_test_types(
                        rule_set.source_table, rule_set.test_type_column,
                        team_group_id=tgid,
                        team_group_column=rule_set.team_group_column,
                        family_filter=rule_set.family_filter,
                    )
                    for tt in test_types:
                        for rule in rule_set.metric_rules:
                            baseline = self.compute_baseline(
                                rule_set.source_table, rule.metric_name,
                                test_type=tt,
                                team_group_id=tgid,
                                team_group_column=rule_set.team_group_column,
                                family_filter=rule_set.family_filter,
                            )
                            if baseline:
                                count += 1
                                cache_key = (
                                    f"{rule_set.source_table}|{rule.metric_name}|"
                                    f"{tt}|None|{tgid}"
                                )
                                self._baseline_cache[cache_key] = baseline

        return count

    def _get_distinct_test_types(
        self,
        source_table: str,
        test_type_column: str,
        *,
        team_group_id: Optional[str] = None,
        team_group_column: Optional[str] = None,
        family_filter: Optional[str] = None,
    ) -> list[str]:
        """Return distinct non-null test type values from a source table.

        Phase 1: optional team_group / family filters so per-team
        baselines only see the test types that actually appear for
        that team.
        """
        where_clauses = [f"{test_type_column} IS NOT NULL"]
        params: list = []
        if team_group_id is not None and team_group_column:
            where_clauses.append(f"{team_group_column} = %s::UUID")
            params.append(team_group_id)
        if family_filter:
            where_clauses.append(f"({family_filter})")
        where_sql = " AND ".join(where_clauses)
        sql = (
            f"SELECT DISTINCT {test_type_column} FROM {source_table} "
            f"WHERE {where_sql}"
        )
        rows = self.db.fetch_all(sql, tuple(params) if params else None)
        return [str(r[0]) for r in rows] if rows else []

    def _get_distinct_team_group_ids(
        self,
        source_table: str,
        team_group_column: str,
        *,
        family_filter: Optional[str] = None,
    ) -> list[str]:
        """Return distinct non-null team_group_id values (Phase 1)."""
        where_clauses = [f"{team_group_column} IS NOT NULL"]
        if family_filter:
            where_clauses.append(f"({family_filter})")
        sql = (
            f"SELECT DISTINCT {team_group_column} FROM {source_table} "
            f"WHERE {' AND '.join(where_clauses)}"
        )
        rows = self.db.fetch_all(sql)
        return [str(r[0]) for r in rows] if rows else []

    def _get_last_audit_at(self, source_table: str) -> Optional[str]:
        """Get the timestamp of the last completed audit for a source table."""
        row = self.db.fetch_one(
            "SELECT MAX(last_computed_at) FROM silver.data_quality_baseline "
            "WHERE source_table = %s",
            (source_table,),
        )
        return str(row[0]) if row and row[0] else None

    def audit_table(self, rule_set: QualityRuleSet,
                    batch_id: Optional[str] = None,
                    limit: Optional[int] = None,
                    incremental: bool = True) -> dict:
        """Run quality audit on records in a bronze table.

        When *incremental* is True (default), only records ingested or
        updated since the last audit are checked.  Set
        ``incremental=False`` to re-audit everything.

        Computes baselines first, then scans records and flags outliers.
        Returns summary dict.
        """
        logger.info("Starting quality audit for %s (incremental=%s)",
                     rule_set.source_table, incremental)

        # Register test_type column so compute_baseline can filter by it
        if rule_set.test_type_column:
            self._test_type_columns[rule_set.source_table] = rule_set.test_type_column

        # Determine cutoff for incremental scan BEFORE refreshing baselines
        last_audit_at = None
        if incremental:
            last_audit_at = self._get_last_audit_at(rule_set.source_table)
            if last_audit_at:
                logger.info("Incremental audit — only records after %s", last_audit_at)

        # Step 1: Refresh baselines (populates in-memory cache)
        baselines_computed = self.refresh_baselines(rule_set)
        logger.info("Computed %d baselines", baselines_computed)

        # Step 2: Build column list
        columns = set()
        columns.add(rule_set.record_id_column)
        columns.add(rule_set.profile_id_column)
        columns.add(rule_set.tenant_id_column)
        if rule_set.test_date_column:
            columns.add(rule_set.test_date_column)
        if rule_set.test_type_column:
            columns.add(rule_set.test_type_column)
        # Phase 1: pull team_group_id so check_record can scope the
        # baseline lookup correctly.
        if rule_set.team_group_column:
            columns.add(rule_set.team_group_column)
        for r in rule_set.metric_rules:
            columns.add(r.metric_name)
        for a in rule_set.asymmetry_rules:
            columns.add(a.left_metric)
            columns.add(a.right_metric)

        col_names = sorted(columns)
        col_list = ', '.join(col_names)

        # Step 3: Fetch records — incremental uses updated_at cutoff,
        # and family_filter (when set) restricts the audit to a single
        # assessment family from a multi-family table like
        # silver.vald_assessment_metric.
        where_clauses: list[str] = []
        params_list: list = []
        if incremental and last_audit_at:
            where_clauses.append("updated_at > %s")
            params_list.append(last_audit_at)
        if rule_set.family_filter:
            where_clauses.append(f"({rule_set.family_filter})")
        where_sql = (
            f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        )
        sql = f"SELECT {col_list} FROM {rule_set.source_table}{where_sql}"
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = self.db.fetch_all(sql, tuple(params_list) if params_list else None)

        if not rows:
            logger.info("No new records to audit in %s", rule_set.source_table)
            return {'table': rule_set.source_table, 'records_checked': 0,
                    'flags': 0, 'skipped_already_audited': True}

        records = [dict(zip(col_names, row)) for row in rows]
        logger.info("Checking %d records in %s", len(records), rule_set.source_table)

        # Step 4: Check records in chunks with progress logging
        all_flags: list[QualityFlag] = []
        chunk_size = 1000
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            all_flags.extend(self.check_batch(chunk, rule_set, batch_id))
            if len(records) > chunk_size:
                logger.info(
                    "  checked %d / %d records (%d flags so far)",
                    min(i + chunk_size, len(records)), len(records), len(all_flags),
                )

        # Step 5: Persist flags (batch upsert — no duplicates)
        flags_written = self.persist_flags(all_flags)

        summary = {
            'table': rule_set.source_table,
            'records_checked': len(records),
            'baselines_computed': baselines_computed,
            'flags': flags_written,
            'by_severity': {},
            'by_type': {},
        }
        for f in all_flags:
            summary['by_severity'][f.severity] = summary['by_severity'].get(f.severity, 0) + 1
            summary['by_type'][f.flag_type] = summary['by_type'].get(f.flag_type, 0) + 1

        logger.info(
            "Quality audit complete: %s — %d records, %d flags (%s)",
            rule_set.source_table, len(records), flags_written,
            ', '.join(f"{k}={v}" for k, v in summary['by_severity'].items()),
        )
        return summary

    # ------------------------------------------------------------------
    # Phase 1 (2026-05-09): long-form audit for silver.vald_assessment_metric
    # ------------------------------------------------------------------

    def audit_long_form_table(
        self,
        rule_set: QualityRuleSet,
        *,
        metric_name_column: str = "metric_name",
        metric_value_column: str = "metric_value",
        batch_id: Optional[str] = None,
        limit: Optional[int] = None,
        incremental: bool = True,
    ) -> dict:
        """Audit a long-form fact table (one row per test x metric).

        Phase 1 — used for ``silver.vald_assessment_metric``. The
        baseline is computed per
        ``(family_filter, team_group_id, test_type, metric_name)`` and
        each row is checked against the most-specific baseline available.

        Differs from :meth:`audit_table` in that
        ``rule_set.metric_rules[*].metric_name`` is matched against the
        VALUE of ``metric_name_column``, not against a column name.
        """
        logger.info(
            "Long-form quality audit for %s (incremental=%s family=%s)",
            rule_set.source_table, incremental, rule_set.family,
        )

        if rule_set.test_type_column:
            self._test_type_columns[rule_set.source_table] = rule_set.test_type_column

        # Refresh baselines per (team_group_id, test_type, metric_name).
        baselines_computed = self._refresh_long_form_baselines(
            rule_set,
            metric_name_column=metric_name_column,
            metric_value_column=metric_value_column,
        )
        logger.info("Computed %d long-form baselines", baselines_computed)

        # Determine cutoff for incremental audits — use updated_at on
        # the source table, restricted to the family filter.
        last_audit_at = None
        if incremental:
            last_audit_at = self._get_last_audit_at_long_form(rule_set)

        # Build the row-fetch SQL. We pull only the columns we need to
        # check + emit a flag.
        wanted_metric_names = [r.metric_name for r in rule_set.metric_rules]
        if not wanted_metric_names:
            return {
                'table': rule_set.source_table,
                'family': rule_set.family,
                'records_checked': 0,
                'flags': 0,
                'reason': 'no_metric_rules',
            }

        cols = [
            rule_set.record_id_column,
            rule_set.profile_id_column,
            rule_set.tenant_id_column,
            metric_name_column,
            metric_value_column,
        ]
        if rule_set.test_type_column:
            cols.append(rule_set.test_type_column)
        if rule_set.test_date_column:
            cols.append(rule_set.test_date_column)
        if rule_set.team_group_column:
            cols.append(rule_set.team_group_column)
        col_list = ", ".join(cols)

        where_clauses: list[str] = [f"{metric_name_column} = ANY(%s)"]
        params: list = [wanted_metric_names]
        if rule_set.family_filter:
            where_clauses.append(f"({rule_set.family_filter})")
        if incremental and last_audit_at:
            where_clauses.append("updated_at > %s")
            params.append(last_audit_at)
        where_sql = " AND ".join(where_clauses)
        sql = f"SELECT {col_list} FROM {rule_set.source_table} WHERE {where_sql}"
        if limit:
            sql += f" LIMIT {int(limit)}"

        rows = self.db.fetch_all(sql, tuple(params))
        if not rows:
            return {
                'table': rule_set.source_table,
                'family': rule_set.family,
                'records_checked': 0,
                'flags': 0,
                'skipped_already_audited': True,
            }

        # Build a lookup so check_record can find the right MetricRule
        # for each row's metric_name value.
        rule_by_name = {r.metric_name: r for r in rule_set.metric_rules}

        all_flags: list[QualityFlag] = []
        for row in rows:
            record = dict(zip(cols, row))
            metric_name_value = record.get(metric_name_column)
            metric_value = record.get(metric_value_column)
            if metric_name_value is None or metric_value is None:
                continue
            rule = rule_by_name.get(metric_name_value)
            if rule is None:
                continue
            all_flags.extend(
                self._check_long_form_record(
                    record,
                    rule_set=rule_set,
                    rule=rule,
                    metric_name=metric_name_value,
                    metric_value_column=metric_value_column,
                    batch_id=batch_id,
                )
            )

        flags_written = self.persist_flags(all_flags)
        summary = {
            'table': rule_set.source_table,
            'family': rule_set.family,
            'records_checked': len(rows),
            'baselines_computed': baselines_computed,
            'flags': flags_written,
            'by_severity': {},
            'by_type': {},
        }
        for f in all_flags:
            summary['by_severity'][f.severity] = summary['by_severity'].get(f.severity, 0) + 1
            summary['by_type'][f.flag_type] = summary['by_type'].get(f.flag_type, 0) + 1
        logger.info(
            "Long-form audit complete: %s/%s — %d records, %d flags",
            rule_set.source_table, rule_set.family,
            len(rows), flags_written,
        )
        return summary

    def _refresh_long_form_baselines(
        self,
        rule_set: QualityRuleSet,
        *,
        metric_name_column: str,
        metric_value_column: str,
    ) -> int:
        """Compute baselines for a long-form fact (Phase 1).

        Iterates all (team_group_id?, test_type?, metric_name) tuples
        present in the source table after applying ``family_filter``,
        and writes one baseline row per tuple.
        """
        count = 0
        wanted_metric_names = [r.metric_name for r in rule_set.metric_rules]
        if not wanted_metric_names:
            return 0

        # Determine which tuples are present.
        select_cols = [metric_name_column]
        if rule_set.test_type_column:
            select_cols.append(rule_set.test_type_column)
        if rule_set.team_group_column:
            select_cols.append(rule_set.team_group_column)
        select_list = ", ".join(select_cols)

        where_clauses = [f"{metric_name_column} = ANY(%s)",
                         f"{metric_value_column} IS NOT NULL"]
        params: list = [wanted_metric_names]
        if rule_set.family_filter:
            where_clauses.append(f"({rule_set.family_filter})")
        sql = (
            f"SELECT DISTINCT {select_list} "
            f"FROM {rule_set.source_table} "
            f"WHERE {' AND '.join(where_clauses)}"
        )
        tuples = self.db.fetch_all(sql, tuple(params))

        for tup in tuples:
            tup_dict = dict(zip(select_cols, tup))
            metric_name_value = tup_dict[metric_name_column]
            test_type_value = (
                tup_dict.get(rule_set.test_type_column)
                if rule_set.test_type_column else None
            )
            team_group_id_value = (
                tup_dict.get(rule_set.team_group_column)
                if rule_set.team_group_column else None
            )
            baseline = self._compute_long_form_baseline(
                rule_set=rule_set,
                metric_name_column=metric_name_column,
                metric_name_value=metric_name_value,
                metric_value_column=metric_value_column,
                test_type_value=str(test_type_value) if test_type_value else None,
                team_group_id_value=str(team_group_id_value) if team_group_id_value else None,
            )
            if baseline:
                count += 1
                cache_key = (
                    f"{rule_set.source_table}|{metric_name_value}|"
                    f"{test_type_value or 'None'}|None|{team_group_id_value or 'None'}"
                )
                self._baseline_cache[cache_key] = baseline
        return count

    def _compute_long_form_baseline(
        self,
        *,
        rule_set: QualityRuleSet,
        metric_name_column: str,
        metric_name_value: str,
        metric_value_column: str,
        test_type_value: Optional[str],
        team_group_id_value: Optional[str],
    ) -> Optional[dict]:
        """Compute one (team, test_type, metric_name) baseline row."""
        where_clauses = [
            f"{metric_name_column} = %s",
            f"{metric_value_column} IS NOT NULL",
        ]
        params: list = [metric_name_value]
        if rule_set.family_filter:
            where_clauses.append(f"({rule_set.family_filter})")
        if test_type_value is not None and rule_set.test_type_column:
            where_clauses.append(f"{rule_set.test_type_column} = %s")
            params.append(test_type_value)
        if team_group_id_value is not None and rule_set.team_group_column:
            where_clauses.append(f"{rule_set.team_group_column} = %s::UUID")
            params.append(team_group_id_value)
        where_sql = " AND ".join(where_clauses)

        sql = f"""
            SELECT
                COUNT(*)                                                          AS sample_count,
                AVG({metric_value_column})                                        AS mean_value,
                STDDEV_POP({metric_value_column})                                 AS std_value,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {metric_value_column})  AS median_value,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {metric_value_column}) AS p25_value,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {metric_value_column}) AS p75_value,
                MIN({metric_value_column})                                        AS min_value,
                MAX({metric_value_column})                                        AS max_value
            FROM {rule_set.source_table}
            WHERE {where_sql}
        """
        row = self.db.fetch_one(sql, tuple(params))
        if not row or row[0] < MIN_SAMPLE_SIZE:
            return None

        sample_count, mean_val, std_val, median_val, p25_val, p75_val, min_val, max_val = row
        mad_val = None
        if median_val is not None:
            mad_sql = f"""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY ABS({metric_value_column} - {float(median_val)})
                )
                FROM {rule_set.source_table}
                WHERE {where_sql}
            """
            mad_row = self.db.fetch_one(mad_sql, tuple(params))
            mad_val = mad_row[0] if mad_row else None

        baseline = {
            'source_table': rule_set.source_table,
            'metric_name': metric_name_value,
            'test_type': test_type_value,
            'tenant_id': None,  # tenant scoping not used in long-form mode
            'team_group_id': team_group_id_value,
            'sample_count': sample_count,
            'mean_value': float(mean_val) if mean_val is not None else None,
            'std_value': float(std_val) if std_val is not None else None,
            'median_value': float(median_val) if median_val is not None else None,
            'mad_value': float(mad_val) if mad_val is not None else None,
            'p25_value': float(p25_val) if p25_val is not None else None,
            'p75_value': float(p75_val) if p75_val is not None else None,
            'min_value': float(min_val) if min_val is not None else None,
            'max_value': float(max_val) if max_val is not None else None,
        }
        self._upsert_baseline(baseline)
        return baseline

    def _check_long_form_record(
        self,
        record: dict,
        *,
        rule_set: QualityRuleSet,
        rule: MetricRule,
        metric_name: str,
        metric_value_column: str,
        batch_id: Optional[str],
    ) -> list[QualityFlag]:
        """Apply one MetricRule against one long-form row."""
        flags: list[QualityFlag] = []
        try:
            value = float(record[metric_value_column])
        except (KeyError, TypeError, ValueError):
            return flags

        record_id = str(record.get(rule_set.record_id_column, ''))
        profile_id = record.get(rule_set.profile_id_column)
        tenant_id = record.get(rule_set.tenant_id_column)
        test_type = (
            record.get(rule_set.test_type_column) if rule_set.test_type_column else None
        )
        test_date = (
            record.get(rule_set.test_date_column) if rule_set.test_date_column else None
        )
        team_group_id = (
            record.get(rule_set.team_group_column)
            if rule_set.team_group_column else None
        )
        team_group_id_str = str(team_group_id) if team_group_id else None

        # Range / non-negative / IQR / zscore checks — same logic as
        # check_record but the metric_name is the row's value, not the
        # column name.
        if rule.non_negative and value < 0:
            flags.append(QualityFlag(
                source_table=rule_set.source_table,
                record_id=record_id,
                metric_name=metric_name,
                metric_value=value,
                flag_type='negative_value',
                severity='critical',
                details={'expected': '>= 0', 'actual': value},
                profile_id=str(profile_id) if profile_id else None,
                tenant_id=str(tenant_id) if tenant_id else None,
                team_group_id=team_group_id_str,
                test_date=str(test_date) if test_date else None,
                batch_id=batch_id,
            ))

        if rule.check_range:
            if rule.abs_min is not None and value < rule.abs_min:
                flags.append(QualityFlag(
                    source_table=rule_set.source_table,
                    record_id=record_id,
                    metric_name=metric_name,
                    metric_value=value,
                    flag_type='range_violation',
                    severity='critical',
                    details={'abs_min': rule.abs_min, 'abs_max': rule.abs_max, 'actual': value},
                    profile_id=str(profile_id) if profile_id else None,
                    tenant_id=str(tenant_id) if tenant_id else None,
                    team_group_id=team_group_id_str,
                    test_date=str(test_date) if test_date else None,
                    batch_id=batch_id,
                ))
            if rule.abs_max is not None and value > rule.abs_max:
                flags.append(QualityFlag(
                    source_table=rule_set.source_table,
                    record_id=record_id,
                    metric_name=metric_name,
                    metric_value=value,
                    flag_type='range_violation',
                    severity='critical',
                    details={'abs_min': rule.abs_min, 'abs_max': rule.abs_max, 'actual': value},
                    profile_id=str(profile_id) if profile_id else None,
                    tenant_id=str(tenant_id) if tenant_id else None,
                    team_group_id=team_group_id_str,
                    test_date=str(test_date) if test_date else None,
                    batch_id=batch_id,
                ))

        # Pick the most-specific baseline available.
        baseline = None
        if team_group_id_str and test_type:
            baseline = self.get_baseline(
                rule_set.source_table, metric_name,
                test_type=test_type, team_group_id=team_group_id_str,
            )
        if (not baseline or baseline['sample_count'] < MIN_SAMPLE_SIZE) and team_group_id_str:
            baseline = self.get_baseline(
                rule_set.source_table, metric_name,
                team_group_id=team_group_id_str,
            )
        if (not baseline or baseline['sample_count'] < MIN_SAMPLE_SIZE) and test_type:
            baseline = self.get_baseline(
                rule_set.source_table, metric_name, test_type=test_type,
            )
        if not baseline or baseline['sample_count'] < MIN_SAMPLE_SIZE:
            baseline = self.get_baseline(rule_set.source_table, metric_name)
        if not baseline or baseline['sample_count'] < MIN_SAMPLE_SIZE:
            return flags

        # IQR check
        if rule.check_iqr and baseline.get('p25_value') is not None and baseline.get('p75_value') is not None:
            lower, upper = self.iqr_bounds(
                baseline['p25_value'], baseline['p75_value'], rule.iqr_multiplier,
            )
            if value < lower or value > upper:
                # 2× the IQR span beyond the fence = critical.
                severity = (
                    'critical'
                    if (value < lower - (upper - lower) or value > upper + (upper - lower))
                    else 'warning'
                )
                flags.append(QualityFlag(
                    source_table=rule_set.source_table,
                    record_id=record_id,
                    metric_name=metric_name,
                    metric_value=value,
                    flag_type='outlier_iqr',
                    severity=severity,
                    details={
                        'iqr_lower': round(lower, 3),
                        'iqr_upper': round(upper, 3),
                        'iqr_multiplier': rule.iqr_multiplier,
                        'p25': baseline['p25_value'],
                        'p75': baseline['p75_value'],
                        'sample_count': baseline['sample_count'],
                    },
                    profile_id=str(profile_id) if profile_id else None,
                    tenant_id=str(tenant_id) if tenant_id else None,
                    team_group_id=team_group_id_str,
                    test_date=str(test_date) if test_date else None,
                    batch_id=batch_id,
                ))

        # Modified Z-score check (median + MAD)
        if rule.check_zscore and baseline.get('median_value') is not None and baseline.get('mad_value'):
            mz = self.modified_zscore(
                value, baseline['median_value'], baseline['mad_value'],
            )
            if mz is not None and abs(mz) >= rule.zscore_warning:
                severity = 'critical' if abs(mz) >= rule.zscore_critical else 'warning'
                flags.append(QualityFlag(
                    source_table=rule_set.source_table,
                    record_id=record_id,
                    metric_name=metric_name,
                    metric_value=value,
                    flag_type='outlier_modified_zscore',
                    severity=severity,
                    details={
                        'modified_zscore': round(mz, 3),
                        'threshold': (
                            rule.zscore_critical
                            if severity == 'critical' else rule.zscore_warning
                        ),
                        'median': baseline['median_value'],
                        'mad': baseline['mad_value'],
                        'sample_count': baseline['sample_count'],
                    },
                    profile_id=str(profile_id) if profile_id else None,
                    tenant_id=str(tenant_id) if tenant_id else None,
                    team_group_id=team_group_id_str,
                    test_date=str(test_date) if test_date else None,
                    batch_id=batch_id,
                ))

        return flags

    def _get_last_audit_at_long_form(
        self, rule_set: QualityRuleSet,
    ) -> Optional[str]:
        """Last successful audit timestamp scoped to family_filter (Phase 1)."""
        if not rule_set.family:
            return self._get_last_audit_at(rule_set.source_table)
        row = self.db.fetch_one(
            """
            SELECT MAX(finished_at)
              FROM silver.data_quality_audit_run
             WHERE family = %s AND status = 'success'
            """,
            (rule_set.family,),
        )
        return str(row[0]) if row and row[0] else None

    def persist_flags(self, flags: list[QualityFlag]) -> int:
        """Batch-upsert quality flags into silver.data_quality_flag.

        Uses ``execute_values`` for performance and ``ON CONFLICT`` on the
        unique index ``(source_table, record_id, metric_name, flag_type)``
        to update existing flags without creating duplicates.  Flags that
        have been manually reviewed (resolution_status != 'open') are NOT
        overwritten.
        """
        if not flags:
            return 0

        # Phase 1 (2026-05-09): include team_group_id in the persisted
        # row so /v1/quality/flags can filter by team. ON CONFLICT
        # updates it too (best-effort refresh; reviewed flags are
        # untouched per the WHERE clause).
        sql = """
            INSERT INTO silver.data_quality_flag
                (source_table, record_id, profile_id, tenant_id, test_date,
                 metric_name, metric_value, flag_type, severity, details,
                 batch_id, team_group_id)
            VALUES %s
            ON CONFLICT (source_table, record_id, metric_name, flag_type)
            DO UPDATE SET
                metric_value  = EXCLUDED.metric_value,
                severity      = EXCLUDED.severity,
                details       = EXCLUDED.details,
                batch_id      = EXCLUDED.batch_id,
                profile_id    = EXCLUDED.profile_id,
                tenant_id     = EXCLUDED.tenant_id,
                test_date     = EXCLUDED.test_date,
                team_group_id = EXCLUDED.team_group_id
            WHERE silver.data_quality_flag.resolution_status = 'open'
        """

        template = (
            "(%(source_table)s, %(record_id)s, %(profile_id)s, %(tenant_id)s,"
            " %(test_date)s, %(metric_name)s, %(metric_value)s, %(flag_type)s,"
            " %(severity)s, %(details)s, %(batch_id)s, %(team_group_id)s)"
        )

        records = [
            {
                'source_table': f.source_table,
                'record_id': f.record_id,
                'profile_id': f.profile_id,
                'tenant_id': f.tenant_id,
                'test_date': f.test_date,
                'metric_name': f.metric_name,
                'metric_value': f.metric_value,
                'flag_type': f.flag_type,
                'severity': f.severity,
                'details': json.dumps(f.details),
                'batch_id': f.batch_id,
                'team_group_id': f.team_group_id,
            }
            for f in flags
        ]

        # Batch in chunks of 500 for memory safety
        batch_size = 500
        total = 0
        with self.db.connection() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(records), batch_size):
                    chunk = records[i : i + batch_size]
                    psycopg2.extras.execute_values(
                        cur, sql, chunk, template=template,
                    )
                    total += len(chunk)

        logger.info("Persisted %d quality flags (upsert, no duplicates)", total)
        return total
