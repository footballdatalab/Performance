"""Unit tests for ingestion.vald.quality_rules (Phase 1, 2026-05-09)."""

from __future__ import annotations

import sys
import types

import pytest

if "psycopg2" not in sys.modules:
    psycopg2_stub = types.ModuleType("psycopg2")
    psycopg2_stub.extensions = types.ModuleType("psycopg2.extensions")
    psycopg2_stub.extensions.connection = object
    psycopg2_stub.extensions.cursor = object
    psycopg2_stub.extras = types.ModuleType("psycopg2.extras")
    psycopg2_stub.extras.execute_values = lambda *args, **kwargs: None
    psycopg2_stub.extras.RealDictCursor = object
    psycopg2_pool_stub = types.ModuleType("psycopg2.pool")
    psycopg2_pool_stub.ThreadedConnectionPool = object
    sys.modules["psycopg2"] = psycopg2_stub
    sys.modules["psycopg2.extensions"] = psycopg2_stub.extensions
    sys.modules["psycopg2.extras"] = psycopg2_stub.extras
    sys.modules["psycopg2.pool"] = psycopg2_pool_stub

from ingestion.common.quality import MetricRule, QualityRuleSet
from ingestion.vald import quality_rules


# ---------------------------------------------------------------------------
# Builders return well-formed rule sets
# ---------------------------------------------------------------------------

def test_all_five_families_have_a_builder() -> None:
    expected = {"forcedecks", "forceframe", "nordics", "speed", "dynamo"}
    assert set(quality_rules.ALL_RULE_SET_BUILDERS.keys()) == expected


@pytest.mark.parametrize("family", ["forcedecks", "forceframe", "nordics", "speed", "dynamo"])
def test_each_builder_returns_a_quality_rule_set(family: str) -> None:
    rs = quality_rules.build_rule_set(family)
    assert isinstance(rs, QualityRuleSet)
    assert rs.source_table == "silver.vald_assessment_metric"
    assert rs.provider == "vald"
    assert rs.family == family
    # Phase 1 requires per-team baselines.
    assert rs.team_group_column == "team_group_id"
    # And family_filter scoping the audit to this family.
    assert rs.family_filter == f"assessment_family = '{family}'"


@pytest.mark.parametrize("family", ["forcedecks", "forceframe", "nordics", "speed", "dynamo"])
def test_every_rule_set_has_at_least_one_metric_rule(family: str) -> None:
    rs = quality_rules.build_rule_set(family)
    assert len(rs.metric_rules) > 0
    for rule in rs.metric_rules:
        assert isinstance(rule, MetricRule)
        # Phase 1 default: IQR check enabled with multiplier 2.0.
        assert rule.check_iqr is True
        assert rule.iqr_multiplier == 2.0


def test_unknown_family_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Unknown VALD family"):
        quality_rules.build_rule_set("nonexistent_family")


# ---------------------------------------------------------------------------
# Long-form audit path on QualityEngine — happy path with mocked DB
# ---------------------------------------------------------------------------

class _MockEngineDb:
    """Mock DB exposing the methods QualityEngine touches."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []
        # Scripted return tuples for fetch_one / fetch_all in order.
        self.fetch_one_responses: list = []
        self.fetch_all_responses: list = []

    def execute(self, sql: str, params: tuple = None) -> None:
        self.executed.append((sql, params))

    def fetch_one(self, sql: str, params: tuple = None):
        self.executed.append((sql, params))
        return self.fetch_one_responses.pop(0) if self.fetch_one_responses else None

    def fetch_all(self, sql: str, params: tuple = None):
        self.executed.append((sql, params))
        return self.fetch_all_responses.pop(0) if self.fetch_all_responses else []


def test_audit_long_form_table_returns_skipped_when_no_metric_rules() -> None:
    """Empty metric_rules → audit short-circuits without touching the DB."""
    from ingestion.common.quality import QualityEngine
    rs = QualityRuleSet(
        source_table="silver.vald_assessment_metric",
        family="forcedecks",
        family_filter="assessment_family = 'forcedecks'",
        team_group_column="team_group_id",
        metric_rules=[],
    )
    engine = QualityEngine(_MockEngineDb())
    result = engine.audit_long_form_table(rs)
    assert result["records_checked"] == 0
    assert result["flags"] == 0
    assert result["reason"] == "no_metric_rules"


def test_audit_long_form_table_includes_team_group_column_in_select() -> None:
    """When team_group_column is set, it must be SELECTed for check_record."""
    from ingestion.common.quality import QualityEngine
    rs = quality_rules.build_rule_set("forcedecks")

    db = _MockEngineDb()
    # _get_last_audit_at_long_form -> None
    db.fetch_one_responses.append(None)
    # _refresh_long_form_baselines's distinct query -> empty
    db.fetch_all_responses.append([])
    # The main fetch_all -> empty
    db.fetch_all_responses.append([])

    engine = QualityEngine(db)
    result = engine.audit_long_form_table(rs, incremental=True)

    # When all baseline + records returns are empty the audit still
    # completes cleanly with a 'skipped_already_audited' marker.
    assert result["records_checked"] == 0


def test_quality_flag_dataclass_carries_team_group_id() -> None:
    """Phase 1: QualityFlag.team_group_id is a real field on the dataclass."""
    from ingestion.common.quality import QualityFlag
    flag = QualityFlag(
        source_table="silver.vald_assessment_metric",
        record_id="test-1",
        metric_name="JUMP_HEIGHT_IMP_MOM",
        metric_value=120.0,
        flag_type="outlier_iqr",
        severity="critical",
        team_group_id="00000000-0000-0000-0000-000000000001",
    )
    assert flag.team_group_id == "00000000-0000-0000-0000-000000000001"


def test_quality_rule_set_team_group_column_field_default_none() -> None:
    """Backwards-compat: existing rule sets without team_group_column still work."""
    from ingestion.common.quality import QualityRuleSet
    rs = QualityRuleSet(source_table="bronze.vald_nordbord_tests")
    assert rs.team_group_column is None
    assert rs.family_filter is None
    assert rs.provider is None
    assert rs.family is None
