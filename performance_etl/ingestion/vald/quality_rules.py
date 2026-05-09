"""
Per-family quality rule sets for the VALD outlier audit (Phase 1, 2026-05-09).

Each builder returns a :class:`QualityRuleSet` configured for one
assessment family (forcedecks, forceframe, nordics, smartspeed, dynamo)
against the canonical long-form fact ``silver.vald_assessment_metric``.

Baselines are computed per
``(team_group_id, test_type, metric_name)`` per locked decision #5
(all-time window) and locked decision #6 (per-team scope).

The metric lists are deliberately narrow: each family enumerates the
metrics whose outlier behaviour is meaningful for coaching staff. The
list can be expanded over time without DDL changes — just add a
:class:`MetricRule` to the appropriate builder and the next audit run
picks it up.

Design notes
------------
* ``check_iqr=True`` with ``iqr_multiplier=2.0`` is the user's primary
  ask. The IQR-based outlier check is robust to non-normal
  distributions and easy for coaches to reason about ("more than 2×
  the IQR away from the median tail").
* ``check_zscore=True`` adds the modified-Z-score (median + MAD) check
  for redundant signal — flags fire on either rule independently.
* ``check_range`` + ``abs_min`` / ``abs_max`` catch obvious data
  errors (negative jump heights, sub-zero forces) that would
  otherwise pollute the IQR baseline.
* ``non_negative=True`` is on by default so the typical force /
  duration / impulse metrics never go negative without flagging.
* Asymmetry rules apply only to ForceDecks + ForceFrame where the
  long-form table carries L/R variants of the same metric. The other
  three families don't have natural left/right pairs.
"""

from __future__ import annotations

from typing import Callable

from ingestion.common.quality import (
    AsymmetryRule,
    MetricRule,
    QualityRuleSet,
)


_SOURCE_TABLE = "silver.vald_assessment_metric"


# ---------------------------------------------------------------------------
# ForceDecks — jump and squat tests on the dual-force-plate hardware.
# ---------------------------------------------------------------------------

def build_forcedecks_rule_set() -> QualityRuleSet:
    return QualityRuleSet(
        source_table=_SOURCE_TABLE,
        provider="vald",
        family="forcedecks",
        family_filter="assessment_family = 'forcedecks'",
        record_id_column="test_id",
        profile_id_column="provider_profile_id",
        tenant_id_column="provider_profile_id",  # no tenant in long-form silver
        test_type_column="test_type",
        test_date_column="test_date",
        team_group_column="team_group_id",
        metric_rules=[
            # Concentric-mean-power-related metrics
            MetricRule(metric_name="CONCENTRIC_MEAN_POWER",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="PEAK_POWER",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            # Jump-height-style metrics (cm, range 0..100)
            MetricRule(metric_name="JUMP_HEIGHT_IMP_MOM",
                       non_negative=True, check_iqr=True,
                       check_range=True, abs_min=0, abs_max=100,
                       iqr_multiplier=2.0),
            MetricRule(metric_name="JUMP_HEIGHT_FLIGHT_TIME",
                       non_negative=True, check_iqr=True,
                       check_range=True, abs_min=0, abs_max=100,
                       iqr_multiplier=2.0),
            # Time-to-peak / contraction metrics
            MetricRule(metric_name="TIME_TO_PEAK_FORCE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="CONTRACTION_TIME",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="ECCENTRIC_DURATION",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            # RFD / impulse
            MetricRule(metric_name="RFD_AT_100MS",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="CONCENTRIC_IMPULSE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="ECCENTRIC_IMPULSE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
        ],
        # No asymmetry rules at the long-form level — left/right
        # variants land in distinct rows with side='left'/'right'. A
        # follow-up phase can add a JOIN-based asymmetry rule when the
        # need arises.
        asymmetry_rules=[],
    )


# ---------------------------------------------------------------------------
# ForceFrame — isometric strength testing.
# ---------------------------------------------------------------------------

def build_forceframe_rule_set() -> QualityRuleSet:
    return QualityRuleSet(
        source_table=_SOURCE_TABLE,
        provider="vald",
        family="forceframe",
        family_filter="assessment_family = 'forceframe'",
        record_id_column="test_id",
        profile_id_column="provider_profile_id",
        tenant_id_column="provider_profile_id",
        test_type_column="test_type",
        test_date_column="test_date",
        team_group_column="team_group_id",
        metric_rules=[
            MetricRule(metric_name="MAX_FORCE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="AVG_FORCE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="IMPULSE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="REPETITIONS",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0,
                       check_range=True, abs_min=0, abs_max=100),
        ],
        asymmetry_rules=[],
    )


# ---------------------------------------------------------------------------
# NordBord — eccentric hamstring tester (the "nordics" family in silver).
# ---------------------------------------------------------------------------

def build_nordics_rule_set() -> QualityRuleSet:
    return QualityRuleSet(
        source_table=_SOURCE_TABLE,
        provider="vald",
        family="nordics",
        family_filter="assessment_family = 'nordics'",
        record_id_column="test_id",
        profile_id_column="provider_profile_id",
        tenant_id_column="provider_profile_id",
        test_type_column="test_type",
        test_date_column="test_date",
        team_group_column="team_group_id",
        metric_rules=[
            MetricRule(metric_name="MAX_FORCE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="AVG_FORCE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="IMPULSE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="TORQUE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="REPETITIONS",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0,
                       check_range=True, abs_min=0, abs_max=100),
        ],
        asymmetry_rules=[],
    )


# ---------------------------------------------------------------------------
# SmartSpeed — sprint timing gates ("speed" family).
# ---------------------------------------------------------------------------

def build_speed_rule_set() -> QualityRuleSet:
    return QualityRuleSet(
        source_table=_SOURCE_TABLE,
        provider="vald",
        family="speed",
        family_filter="assessment_family = 'speed'",
        record_id_column="test_id",
        profile_id_column="provider_profile_id",
        tenant_id_column="provider_profile_id",
        test_type_column="test_type",
        test_date_column="test_date",
        team_group_column="team_group_id",
        metric_rules=[
            # Times — non-negative, typically 0..120s
            MetricRule(metric_name="SPLIT_TIME",
                       non_negative=True, check_iqr=True,
                       check_range=True, abs_min=0, abs_max=120,
                       iqr_multiplier=2.0),
            MetricRule(metric_name="TOTAL_TIME",
                       non_negative=True, check_iqr=True,
                       check_range=True, abs_min=0, abs_max=120,
                       iqr_multiplier=2.0),
            # Velocities — typically 0..15 m/s
            MetricRule(metric_name="VELOCITY",
                       non_negative=True, check_iqr=True,
                       check_range=True, abs_min=0, abs_max=15,
                       iqr_multiplier=2.0),
            MetricRule(metric_name="MAX_VELOCITY",
                       non_negative=True, check_iqr=True,
                       check_range=True, abs_min=0, abs_max=15,
                       iqr_multiplier=2.0),
        ],
        asymmetry_rules=[],
    )


# ---------------------------------------------------------------------------
# DynaMo — dynamometer / hand-held strength testing.
# ---------------------------------------------------------------------------

def build_dynamo_rule_set() -> QualityRuleSet:
    return QualityRuleSet(
        source_table=_SOURCE_TABLE,
        provider="vald",
        family="dynamo",
        family_filter="assessment_family = 'dynamo'",
        record_id_column="test_id",
        profile_id_column="provider_profile_id",
        tenant_id_column="provider_profile_id",
        test_type_column="test_type",
        test_date_column="test_date",
        team_group_column="team_group_id",
        metric_rules=[
            MetricRule(metric_name="MAX_FORCE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="AVG_FORCE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="MAX_IMPULSE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="AVG_IMPULSE",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="MAX_RFD",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="AVG_RFD",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0),
            MetricRule(metric_name="MAX_ROM",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0,
                       check_range=True, abs_min=0, abs_max=360),
            MetricRule(metric_name="AVG_ROM",
                       non_negative=True, check_iqr=True, iqr_multiplier=2.0,
                       check_range=True, abs_min=0, abs_max=360),
        ],
        asymmetry_rules=[],
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

ALL_RULE_SET_BUILDERS: dict[str, Callable[[], QualityRuleSet]] = {
    "forcedecks": build_forcedecks_rule_set,
    "forceframe": build_forceframe_rule_set,
    "nordics": build_nordics_rule_set,
    "speed": build_speed_rule_set,
    "dynamo": build_dynamo_rule_set,
}


def build_rule_set(family: str) -> QualityRuleSet:
    """Convenience: ``build_rule_set('forcedecks')`` -> rule set."""
    if family not in ALL_RULE_SET_BUILDERS:
        raise ValueError(
            f"Unknown VALD family {family!r}. "
            f"Known: {sorted(ALL_RULE_SET_BUILDERS.keys())}"
        )
    return ALL_RULE_SET_BUILDERS[family]()


__all__ = [
    "build_forcedecks_rule_set",
    "build_forceframe_rule_set",
    "build_nordics_rule_set",
    "build_speed_rule_set",
    "build_dynamo_rule_set",
    "ALL_RULE_SET_BUILDERS",
    "build_rule_set",
]
