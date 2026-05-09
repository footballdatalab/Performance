"""Phase 8.4 — replay endpoint dependency expansion.

Guards against the FK race observed on 2026-04-30 where
`replay:raw.catapult_tags` failed with
`insert or update on table "catapult_tags" violates foreign key
constraint "fk_catapult_tags_tag_type"`.

The fix lives in `ingestion.catapult.pipeline._expand_replay_dependencies`.
These tests must keep passing — a regression here means the FK race can come
back under partial replays (`--endpoints tags`, etc.).
"""

from __future__ import annotations

import pytest

from ingestion.catapult.pipeline import _expand_replay_dependencies


def test_none_passthrough() -> None:
    """None means 'all endpoints' — must not be expanded."""
    assert _expand_replay_dependencies(None) is None


def test_tags_pulls_in_tag_types() -> None:
    """The headline case from the 2026-04-30 incident."""
    result = _expand_replay_dependencies({"tags"})
    assert "tags" in result
    assert "tag_types" in result, (
        "Phase 8.4 regression: requesting `tags` must auto-include `tag_types` "
        "to avoid fk_catapult_tags_tag_type violations."
    )


def test_athletes_pulls_in_teams_and_positions() -> None:
    """Athletes have FK to teams and positions."""
    result = _expand_replay_dependencies({"athletes"})
    assert {"athletes", "teams", "positions"}.issubset(result)


def test_periods_pulls_in_activities() -> None:
    result = _expand_replay_dependencies({"periods"})
    assert {"periods", "activities"}.issubset(result)


def test_stats_pulls_in_activities_and_athletes_chain() -> None:
    """`stats` requires `activities` and `athletes`; `athletes` itself requires `teams` + `positions`.

    Closure must be applied iteratively.
    """
    result = _expand_replay_dependencies({"stats"})
    assert {"stats", "activities", "athletes", "teams", "positions"}.issubset(result)


def test_entity_tags_full_closure() -> None:
    """`entity_tags` is the most-coupled — pulls in tag_types, tags, athletes, teams, positions, venues."""
    result = _expand_replay_dependencies({"entity_tags"})
    assert {
        "entity_tags",
        "tag_types",
        "tags",
        "athletes",
        "teams",
        "positions",
        "venues",
    }.issubset(result)


def test_idempotent() -> None:
    """Running the expansion twice must produce the same set."""
    once = _expand_replay_dependencies({"tags", "stats"})
    twice = _expand_replay_dependencies(once)
    assert once == twice


def test_independent_endpoints_unchanged() -> None:
    """An endpoint with no parents must not gain unrelated companions."""
    result = _expand_replay_dependencies({"teams"})
    assert result == {"teams"}


def test_empty_set_stays_empty() -> None:
    assert _expand_replay_dependencies(set()) == set()


def test_does_not_mutate_input() -> None:
    """Input set must not be mutated in place — callers may reuse it."""
    original = {"tags"}
    snapshot = set(original)
    _expand_replay_dependencies(original)
    assert original == snapshot


@pytest.mark.parametrize(
    "child, required_parents",
    [
        ("tags", {"tag_types"}),
        ("athletes", {"teams", "positions"}),
        ("periods", {"activities"}),
        ("annotations", {"activities"}),
        ("stats", {"activities", "athletes", "teams", "positions"}),
        ("efforts", {"activities", "athletes", "teams", "positions"}),
        ("events", {"activities", "athletes", "teams", "positions"}),
        ("sensor_data", {"activities", "athletes", "teams", "positions"}),
    ],
)
def test_each_child_pulls_in_its_parents(child: str, required_parents: set[str]) -> None:
    """Smoke matrix of all parent→child FK edges in catapult bronze."""
    result = _expand_replay_dependencies({child})
    assert child in result
    assert required_parents.issubset(result), (
        f"`{child}` must auto-include {sorted(required_parents)} for FK safety; "
        f"got {sorted(result)}."
    )