from __future__ import annotations

import pytest

from script.clean_database import (
    TableInfo,
    build_truncate_sql,
    order_tables,
    parse_schemas,
    qualify_identifier,
)


def test_parse_schemas_accepts_all_keyword() -> None:
    assert parse_schemas("all") == ["gold", "silver", "bronze", "raw"]


def test_parse_schemas_deduplicates_and_validates() -> None:
    assert parse_schemas("raw, bronze, raw") == ["raw", "bronze"]

    with pytest.raises(ValueError):
        parse_schemas("raw, mart")


def test_order_tables_uses_downstream_first() -> None:
    tables = [
        TableInfo(schema="raw", name="sync_watermark"),
        TableInfo(schema="bronze", name="vald_forceframe_tests"),
        TableInfo(schema="gold", name="vald_forcedecks"),
        TableInfo(schema="silver", name="master_athlete"),
    ]

    ordered = order_tables(tables)

    assert [table.schema for table in ordered] == ["gold", "silver", "bronze", "raw"]


def test_build_truncate_sql_quotes_identifiers_and_flags() -> None:
    tables = [
        TableInfo(schema="raw", name="sync_watermark"),
        TableInfo(schema="bronze", name='odd"name'),
    ]

    sql = build_truncate_sql(tables)

    assert sql == (
        'TRUNCATE TABLE "bronze"."odd""name", "raw"."sync_watermark" '
        "RESTART IDENTITY CASCADE"
    )


def test_qualify_identifier_escapes_quotes() -> None:
    assert qualify_identifier('we"ird', 'ta"ble') == '"we""ird"."ta""ble"'
