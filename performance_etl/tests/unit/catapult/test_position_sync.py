from __future__ import annotations

import json
from pathlib import Path
import zipfile
import xml.etree.ElementTree as ET

import requests

from ingestion.catapult.client import CatapultAccountConfig, CatapultRuntimeConfig
from ingestion.catapult.position_sync import (
    STATUS_ACCOUNT_NOT_AVAILABLE,
    STATUS_ATHLETE_NOT_FOUND,
    STATUS_MISSING_POSITION,
    STATUS_SUCCESSFUL,
    STATUS_UPDATE_FAILED,
    XlsxStatusWorkbook,
    _build_athlete_update_payload,
    run_position_sync,
)

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_DOC_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

ET.register_namespace("", _MAIN_NS)
ET.register_namespace("r", _DOC_REL_NS)


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _runtime_config(*account_names: str) -> CatapultRuntimeConfig:
    return CatapultRuntimeConfig(
        provider="catapult",
        api_version="v6",
        base_url="https://connect-eu.catapultsports.com/api/v6",
        default_page_size=100,
        rate_limit_ms=0,
        max_retries=1,
        accounts=tuple(
            CatapultAccountConfig(
                name=name,
                api_key_env=f"{name}_API_KEY",
                api_key=f"token-{name}",
                team_code=name,
                team_level="senior",
            )
            for name in account_names
        ),
    )


def _make_client_factory(
    *,
    positions_by_account: dict[str, list[dict[str, str]]],
    athletes_by_account: dict[str, list[dict[str, str]]],
    put_mode: str = "success",
):
    shared_athletes = {
        account: [dict(athlete) for athlete in athletes]
        for account, athletes in athletes_by_account.items()
    }
    put_calls: list[tuple[str, str, dict[str, str]]] = []

    class _FakeClient:
        def __init__(self, runtime_config, account):
            self.account = account

        def get(self, path: str, params=None):
            if path == "/positions":
                return _FakeResponse([dict(position) for position in positions_by_account[self.account.name]])
            if path == "/athletes":
                return _FakeResponse([dict(athlete) for athlete in shared_athletes[self.account.name]])
            raise AssertionError(f"Unexpected GET path: {path}")

        def put(self, path: str, json=None, params=None):
            athlete_id = path.rsplit("/", 1)[-1]
            payload = dict(json or {})
            put_calls.append((self.account.name, athlete_id, payload))
            if put_mode == "success":
                for athlete in shared_athletes[self.account.name]:
                    if athlete["id"] == athlete_id:
                        athlete["position_id"] = payload["position_id"]
                        return _FakeResponse(dict(athlete))
                return _FakeResponse({})
            if put_mode == "response_wrong_refresh_wrong":
                return _FakeResponse({"id": athlete_id, "position_id": "wrong-position"})
            if put_mode == "http_error":
                response = requests.Response()
                response.status_code = 422
                response._content = b'{"message":"unprocessable"}'
                response.url = f"https://example.test/athletes/{athlete_id}"
                raise requests.exceptions.HTTPError(response=response)
            raise AssertionError(f"Unsupported put mode: {put_mode}")

        def close(self) -> None:
            return None

    return _FakeClient, put_calls


def _build_workbook(path: Path, sheets: dict[str, list[list[str]]]) -> None:
    shared_strings: list[str] = []
    shared_string_index: dict[str, int] = {}

    def intern(value: str) -> int:
        if value not in shared_string_index:
            shared_string_index[value] = len(shared_strings)
            shared_strings.append(value)
        return shared_string_index[value]

    sheet_targets: list[str] = []
    worksheet_payloads: dict[str, bytes] = {}
    for sheet_number, (sheet_name, rows) in enumerate(sheets.items(), start=1):
        target = f"xl/worksheets/sheet{sheet_number}.xml"
        sheet_targets.append(target)
        worksheet = ET.Element(f"{{{_MAIN_NS}}}worksheet")
        max_col = max((len(row) for row in rows), default=1)
        max_row = max(len(rows), 1)
        ET.SubElement(
            worksheet,
            f"{{{_MAIN_NS}}}dimension",
            {"ref": f"A1:{_column_letter(max_col)}{max_row}"},
        )
        sheet_data = ET.SubElement(worksheet, f"{{{_MAIN_NS}}}sheetData")
        for row_number, row_values in enumerate(rows, start=1):
            row = ET.SubElement(
                sheet_data,
                f"{{{_MAIN_NS}}}row",
                {"r": str(row_number), "spans": f"1:{max_col}"},
            )
            for column_number, value in enumerate(row_values, start=1):
                if value == "":
                    continue
                cell = ET.SubElement(
                    row,
                    f"{{{_MAIN_NS}}}c",
                    {"r": f"{_column_letter(column_number)}{row_number}", "t": "s"},
                )
                v = ET.SubElement(cell, f"{{{_MAIN_NS}}}v")
                v.text = str(intern(value))
        worksheet_payloads[target] = ET.tostring(worksheet, encoding="utf-8", xml_declaration=True)

    workbook = ET.Element(f"{{{_MAIN_NS}}}workbook")
    sheets_element = ET.SubElement(workbook, f"{{{_MAIN_NS}}}sheets")
    for sheet_number, sheet_name in enumerate(sheets.keys(), start=1):
        ET.SubElement(
            sheets_element,
            f"{{{_MAIN_NS}}}sheet",
            {
                "name": sheet_name,
                "sheetId": str(sheet_number),
                f"{{{_DOC_REL_NS}}}id": f"rId{sheet_number}",
            },
        )

    workbook_rels = ET.Element(f"{{{_PKG_REL_NS}}}Relationships")
    for sheet_number, target in enumerate(sheet_targets, start=1):
        ET.SubElement(
            workbook_rels,
            f"{{{_PKG_REL_NS}}}Relationship",
            {
                "Id": f"rId{sheet_number}",
                "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet",
                "Target": target.removeprefix("xl/"),
            },
        )
    ET.SubElement(
        workbook_rels,
        f"{{{_PKG_REL_NS}}}Relationship",
        {
            "Id": f"rId{len(sheet_targets) + 1}",
            "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings",
            "Target": "sharedStrings.xml",
        },
    )

    shared_strings_root = ET.Element(
        f"{{{_MAIN_NS}}}sst",
        {"count": str(len(shared_strings)), "uniqueCount": str(len(shared_strings))},
    )
    for value in shared_strings:
        si = ET.SubElement(shared_strings_root, f"{{{_MAIN_NS}}}si")
        t = ET.SubElement(si, f"{{{_MAIN_NS}}}t")
        t.text = value

    content_types = ET.Element("Types", xmlns="http://schemas.openxmlformats.org/package/2006/content-types")
    ET.SubElement(content_types, "Default", Extension="rels", ContentType="application/vnd.openxmlformats-package.relationships+xml")
    ET.SubElement(content_types, "Default", Extension="xml", ContentType="application/xml")
    ET.SubElement(
        content_types,
        "Override",
        PartName="/xl/workbook.xml",
        ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml",
    )
    ET.SubElement(
        content_types,
        "Override",
        PartName="/xl/sharedStrings.xml",
        ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml",
    )
    for target in sheet_targets:
        ET.SubElement(
            content_types,
            "Override",
            PartName=f"/{target}",
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml",
        )

    root_rels = ET.Element(f"{{{_PKG_REL_NS}}}Relationships")
    ET.SubElement(
        root_rels,
        f"{{{_PKG_REL_NS}}}Relationship",
        {
            "Id": "rId1",
            "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument",
            "Target": "xl/workbook.xml",
        },
    )

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", ET.tostring(content_types, encoding="utf-8", xml_declaration=True))
        zf.writestr("_rels/.rels", ET.tostring(root_rels, encoding="utf-8", xml_declaration=True))
        zf.writestr("xl/workbook.xml", ET.tostring(workbook, encoding="utf-8", xml_declaration=True))
        zf.writestr("xl/_rels/workbook.xml.rels", ET.tostring(workbook_rels, encoding="utf-8", xml_declaration=True))
        zf.writestr("xl/sharedStrings.xml", ET.tostring(shared_strings_root, encoding="utf-8", xml_declaration=True))
        for target, payload in worksheet_payloads.items():
            zf.writestr(target, payload)


def _column_letter(index: int) -> str:
    result = ""
    current = index
    while current:
        current, remainder = divmod(current - 1, 26)
        result = chr(ord("A") + remainder) + result
    return result


def test_xlsx_status_workbook_prefers_sheet2_and_appends_status_column_without_duplicates(tmp_path) -> None:
    workbook_path = tmp_path / "positions.xlsx"
    _build_workbook(
        workbook_path,
        {
            "Sheet1": [["note"], ["ignore"]],
            "Sheet2": [
                ["source_account", "athlete_id", "full_name", "new_position_name", "new_position_slug"],
                ["CATAPULT_A", "athlete-1", "Alice", "Médio", "M"],
                ["CATAPULT_A", "athlete-2", "Bob", "Avançado", "AV"],
            ],
        },
    )

    workbook = XlsxStatusWorkbook(workbook_path)
    assert workbook.sheet_name == "Sheet2"

    workbook.write_statuses([STATUS_SUCCESSFUL, STATUS_MISSING_POSITION])
    workbook.write_statuses([STATUS_MISSING_POSITION, STATUS_SUCCESSFUL])

    reloaded = XlsxStatusWorkbook(workbook_path)
    assert reloaded.headers[-1] == "status"
    assert reloaded.headers.count("status") == 1
    assert [row.values["full_name"] for row in reloaded.rows] == ["Alice", "Bob"]
    assert [row.values["status"] for row in reloaded.rows] == [STATUS_MISSING_POSITION, STATUS_SUCCESSFUL]


def test_build_athlete_update_payload_keeps_profile_fields_and_overrides_position_id() -> None:
    athlete = {
        "id": "athlete-1",
        "first_name": "Alice",
        "last_name": "Example",
        "date_of_birth": 551401200,
        "height": 170,
        "weight": 60,
        "position_id": "old-position",
    }

    payload = _build_athlete_update_payload(athlete, target_position_id="new-position")

    assert payload["first_name"] == "Alice"
    assert payload["last_name"] == "Example"
    assert payload["height"] == 170
    assert payload["weight"] == 60
    assert payload["date_of_birth"] == 551401200
    assert payload["date_of_birth_date"] == "1987-06-23"
    assert payload["position_id"] == "new-position"


def test_run_position_sync_marks_successful_for_existing_and_updated_rows_and_writes_summary(tmp_path) -> None:
    workbook_path = tmp_path / "positions.xlsx"
    summary_path = tmp_path / "summary.json"
    _build_workbook(
        workbook_path,
        {
            "Sheet2": [
                ["source_account", "athlete_id", "full_name", "new_position_name", "new_position_slug"],
                ["CATAPULT_U15", "athlete-1", "Alice", "Médio", "M"],
                ["CATAPULT_U15", "athlete-2", "Bob", "Avançado", "AV"],
            ]
        },
    )
    runtime = _runtime_config("CATAPULT_U15")
    client_factory, put_calls = _make_client_factory(
        positions_by_account={
            "CATAPULT_U15": [
                {"id": "position-m", "name": "Médio", "slug": "M"},
                {"id": "position-av", "name": "Avançado", "slug": "AV"},
            ]
        },
        athletes_by_account={
            "CATAPULT_U15": [
                {"id": "athlete-1", "position_id": "position-m"},
                {"id": "athlete-2", "position_id": "old-position"},
            ]
        },
    )

    summary = run_position_sync(
        workbook_path=workbook_path,
        summary_output_path=summary_path,
        runtime_config=runtime,
        client_factory=client_factory,
    )

    reloaded = XlsxStatusWorkbook(workbook_path)
    assert [row.values["status"] for row in reloaded.rows] == [STATUS_SUCCESSFUL, STATUS_SUCCESSFUL]
    assert put_calls == [("CATAPULT_U15", "athlete-2", {"position_id": "position-av"})]
    assert summary["missing_positions"] == []
    assert summary["status_counts"] == {STATUS_SUCCESSFUL: 2}
    assert json.loads(summary_path.read_text(encoding="utf-8"))["status_counts"] == {STATUS_SUCCESSFUL: 2}


def test_run_position_sync_marks_missing_position_in_team_for_slug_only_mismatch_and_groups_summary(tmp_path) -> None:
    workbook_path = tmp_path / "positions.xlsx"
    summary_path = tmp_path / "summary.json"
    _build_workbook(
        workbook_path,
        {
            "Sheet2": [
                ["source_account", "athlete_id", "full_name", "new_position_name", "new_position_slug"],
                ["CATAPULT_A", "athlete-1", "Alice", "Ala", "AL"],
                ["CATAPULT_A", "athlete-2", "Bob", "Ala", "AL"],
            ]
        },
    )
    runtime = _runtime_config("CATAPULT_A")
    client_factory, _ = _make_client_factory(
        positions_by_account={"CATAPULT_A": [{"id": "position-al", "name": "Alas", "slug": "AL"}]},
        athletes_by_account={
            "CATAPULT_A": [
                {"id": "athlete-1", "position_id": "position-al"},
                {"id": "athlete-2", "position_id": "position-al"},
            ]
        },
    )

    summary = run_position_sync(
        workbook_path=workbook_path,
        summary_output_path=summary_path,
        runtime_config=runtime,
        client_factory=client_factory,
    )

    reloaded = XlsxStatusWorkbook(workbook_path)
    assert [row.values["status"] for row in reloaded.rows] == [STATUS_MISSING_POSITION, STATUS_MISSING_POSITION]
    assert summary["missing_positions"] == [
        {
            "source_account": "CATAPULT_A",
            "new_position_name": "Ala",
            "new_position_slug": "AL",
            "affected_row_count": 2,
        }
    ]


def test_run_position_sync_marks_athlete_not_found_and_account_not_available(tmp_path) -> None:
    workbook_path = tmp_path / "positions.xlsx"
    _build_workbook(
        workbook_path,
        {
            "Sheet2": [
                ["source_account", "athlete_id", "full_name", "new_position_name", "new_position_slug"],
                ["CATAPULT_U15", "missing-athlete", "Alice", "Médio", "M"],
                ["CATAPULT_UNKNOWN", "athlete-2", "Bob", "Avançado", "AV"],
            ]
        },
    )
    runtime = _runtime_config("CATAPULT_U15")
    client_factory, _ = _make_client_factory(
        positions_by_account={
            "CATAPULT_U15": [
                {"id": "position-m", "name": "Médio", "slug": "M"},
                {"id": "position-av", "name": "Avançado", "slug": "AV"},
            ]
        },
        athletes_by_account={"CATAPULT_U15": []},
    )

    run_position_sync(
        workbook_path=workbook_path,
        runtime_config=runtime,
        client_factory=client_factory,
    )

    reloaded = XlsxStatusWorkbook(workbook_path)
    assert [row.values["status"] for row in reloaded.rows] == [
        STATUS_ATHLETE_NOT_FOUND,
        STATUS_ACCOUNT_NOT_AVAILABLE,
    ]


def test_run_position_sync_marks_update_failed_when_put_does_not_apply_target_position(tmp_path) -> None:
    workbook_path = tmp_path / "positions.xlsx"
    _build_workbook(
        workbook_path,
        {
            "Sheet2": [
                ["source_account", "athlete_id", "full_name", "new_position_name", "new_position_slug"],
                ["CATAPULT_U16", "athlete-1", "Alice", "Guarda-Redes", "GK"],
            ]
        },
    )
    runtime = _runtime_config("CATAPULT_U16")
    client_factory, put_calls = _make_client_factory(
        positions_by_account={"CATAPULT_U16": [{"id": "position-gk", "name": "Guarda-Redes", "slug": "GK"}]},
        athletes_by_account={"CATAPULT_U16": [{"id": "athlete-1", "position_id": "wrong-position"}]},
        put_mode="response_wrong_refresh_wrong",
    )

    run_position_sync(
        workbook_path=workbook_path,
        runtime_config=runtime,
        client_factory=client_factory,
    )

    reloaded = XlsxStatusWorkbook(workbook_path)
    assert [row.values["status"] for row in reloaded.rows] == [STATUS_UPDATE_FAILED]
    assert put_calls == [("CATAPULT_U16", "athlete-1", {"position_id": "position-gk"})]


def test_run_position_sync_marks_update_failed_when_put_raises_http_error(tmp_path) -> None:
    workbook_path = tmp_path / "positions.xlsx"
    _build_workbook(
        workbook_path,
        {
            "Sheet2": [
                ["source_account", "athlete_id", "full_name", "new_position_name", "new_position_slug"],
                ["CATAPULT_U16", "athlete-1", "Alice", "Guarda-Redes", "GK"],
            ]
        },
    )
    runtime = _runtime_config("CATAPULT_U16")
    client_factory, put_calls = _make_client_factory(
        positions_by_account={"CATAPULT_U16": [{"id": "position-gk", "name": "Guarda-Redes", "slug": "GK"}]},
        athletes_by_account={"CATAPULT_U16": [{"id": "athlete-1", "position_id": "wrong-position"}]},
        put_mode="http_error",
    )

    run_position_sync(
        workbook_path=workbook_path,
        runtime_config=runtime,
        client_factory=client_factory,
    )

    reloaded = XlsxStatusWorkbook(workbook_path)
    assert [row.values["status"] for row in reloaded.rows] == [STATUS_UPDATE_FAILED]
    assert put_calls == [("CATAPULT_U16", "athlete-1", {"position_id": "position-gk"})]


def test_run_position_sync_can_scope_to_single_account_and_preserve_other_statuses(tmp_path) -> None:
    workbook_path = tmp_path / "positions.xlsx"
    summary_path = tmp_path / "summary.json"
    _build_workbook(
        workbook_path,
        {
            "Sheet2": [
                ["source_account", "athlete_id", "full_name", "new_position_name", "new_position_slug", "status"],
                ["CATAPULT_A", "athlete-1", "Alice", "MÃ©dio", "M", ""],
                ["CATAPULT_B", "athlete-2", "Bob", "AvanÃ§ado", "AV", "keep-existing"],
            ]
        },
    )
    runtime = _runtime_config("CATAPULT_A", "CATAPULT_B")
    client_factory, put_calls = _make_client_factory(
        positions_by_account={
            "CATAPULT_A": [{"id": "position-m", "name": "MÃ©dio", "slug": "M"}],
            "CATAPULT_B": [{"id": "position-av", "name": "AvanÃ§ado", "slug": "AV"}],
        },
        athletes_by_account={
            "CATAPULT_A": [{"id": "athlete-1", "position_id": "position-m"}],
            "CATAPULT_B": [{"id": "athlete-2", "position_id": "old-position"}],
        },
    )

    summary = run_position_sync(
        workbook_path=workbook_path,
        summary_output_path=summary_path,
        accounts=("CATAPULT_A",),
        runtime_config=runtime,
        client_factory=client_factory,
    )

    reloaded = XlsxStatusWorkbook(workbook_path)
    assert [row.values["status"] for row in reloaded.rows] == [STATUS_SUCCESSFUL, "keep-existing"]
    assert put_calls == []
    assert summary["selected_accounts"] == ["CATAPULT_A"]
    assert summary["status_counts"] == {STATUS_SUCCESSFUL: 1}
