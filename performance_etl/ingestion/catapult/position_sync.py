"""
Synchronize Catapult athlete positions from the workbook mapping file.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

import requests

from ingestion.catapult.client import CatapultClient, CatapultRuntimeConfig, build_catapult_runtime_config
from ingestion.common.logging import get_logger

logger = get_logger(__name__)

_SHEET_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_OFFICE_DOC_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_SHEET_NS = {"main": _SHEET_MAIN_NS, "rel": _PACKAGE_REL_NS}

ET.register_namespace("", _SHEET_MAIN_NS)
ET.register_namespace("r", _OFFICE_DOC_REL_NS)

REQUIRED_COLUMNS = (
    "source_account",
    "athlete_id",
    "full_name",
    "new_position_name",
    "new_position_slug",
)

STATUS_COLUMN_NAME = "status"
STATUS_SUCCESSFUL = "successful"
STATUS_MISSING_POSITION = "missing_position_in_team"
STATUS_ATHLETE_NOT_FOUND = "athlete_not_found"
STATUS_UPDATE_FAILED = "update_failed"
STATUS_ACCOUNT_NOT_AVAILABLE = "account_not_available"
CATAPULT_DATE_FALLBACK_TIMEZONE = ZoneInfo("Europe/Lisbon")
ATHLETE_UPDATE_PROFILE_FIELDS = (
    "first_name",
    "last_name",
    "gender",
    "jersey",
    "nickname",
    "height",
    "weight",
    "date_of_birth",
    "date_of_birth_date",
    "velocity_max",
    "acceleration_max",
    "heart_rate_max",
    "player_load_max",
    "image",
    "icon",
    "stroke_colour",
    "fill_colour",
    "trail_colour_start",
    "trail_colour_end",
    "is_synced",
    "is_deleted",
    "is_demo",
    "current_team_id",
    "max_player_load_per_minute",
)


@dataclass(frozen=True)
class WorkbookRow:
    data_index: int
    excel_row_number: int
    values: dict[str, str]


@dataclass(frozen=True)
class PendingAthleteUpdate:
    row_index: int
    athlete_id: str
    target_position_id: str
    payload: dict[str, Any]


class XlsxStatusWorkbook:
    """Read and update an Excel workbook in place without external dependencies."""

    def __init__(
        self,
        path: str | Path,
        *,
        preferred_sheet_name: str = "Sheet2",
        required_columns: tuple[str, ...] = REQUIRED_COLUMNS,
    ) -> None:
        self.path = Path(path)
        self.preferred_sheet_name = preferred_sheet_name
        self.required_columns = required_columns
        self._entries = self._read_entries()
        self._shared_strings = self._load_shared_strings()
        (
            self.sheet_name,
            self.sheet_target,
            self._sheet_root,
            self._header_row,
            self._data_row_elements,
            self._header_column_indexes,
            self.headers,
            self.rows,
        ) = self._load_mapping_sheet()

    def _read_entries(self) -> dict[str, bytes]:
        with zipfile.ZipFile(self.path) as zf:
            return {name: zf.read(name) for name in zf.namelist()}

    def _load_shared_strings(self) -> list[str]:
        shared_strings_path = "xl/sharedStrings.xml"
        if shared_strings_path not in self._entries:
            return []
        root = ET.fromstring(self._entries[shared_strings_path])
        values: list[str] = []
        for item in root.findall("main:si", _SHEET_NS):
            values.append("".join(node.text or "" for node in item.findall(".//main:t", _SHEET_NS)))
        return values

    def _load_mapping_sheet(
        self,
    ) -> tuple[
        str,
        str,
        ET.Element,
        ET.Element,
        list[ET.Element],
        list[int],
        list[str],
        list[WorkbookRow],
    ]:
        workbook_root = ET.fromstring(self._entries["xl/workbook.xml"])
        rel_root = ET.fromstring(self._entries["xl/_rels/workbook.xml.rels"])
        relationship_targets = {
            rel.attrib["Id"]: _normalize_sheet_target(rel.attrib["Target"])
            for rel in rel_root.findall("rel:Relationship", _SHEET_NS)
        }

        sheet_candidates: list[tuple[str, str, ET.Element, ET.Element, list[ET.Element], list[int], list[str], list[WorkbookRow]]] = []
        sheets_element = workbook_root.find("main:sheets", _SHEET_NS)
        if sheets_element is None:
            raise ValueError(f"Workbook '{self.path}' does not contain any worksheet definitions.")

        for sheet in sheets_element:
            sheet_name = sheet.attrib.get("name", "")
            rel_id = sheet.attrib.get(f"{{{_OFFICE_DOC_REL_NS}}}id", "")
            sheet_target = relationship_targets.get(rel_id)
            if sheet_target is None or sheet_target not in self._entries:
                continue
            sheet_root = ET.fromstring(self._entries[sheet_target])
            header_row, data_rows, header_indexes, headers, workbook_rows = self._parse_sheet_rows(sheet_root)
            if not set(self.required_columns).issubset(headers):
                continue
            sheet_candidates.append(
                (
                    sheet_name,
                    sheet_target,
                    sheet_root,
                    header_row,
                    data_rows,
                    header_indexes,
                    headers,
                    workbook_rows,
                )
            )

        if not sheet_candidates:
            raise ValueError(
                f"No worksheet in '{self.path}' contains the required columns: {', '.join(self.required_columns)}."
            )

        for candidate in sheet_candidates:
            if candidate[0] == self.preferred_sheet_name:
                return candidate
        return sheet_candidates[0]

    def _parse_sheet_rows(
        self,
        sheet_root: ET.Element,
    ) -> tuple[ET.Element, list[ET.Element], list[int], list[str], list[WorkbookRow]]:
        sheet_data = sheet_root.find("main:sheetData", _SHEET_NS)
        if sheet_data is None:
            raise ValueError(f"Worksheet '{self.path}' does not contain sheetData.")

        row_elements = sheet_data.findall("main:row", _SHEET_NS)
        if not row_elements:
            raise ValueError(f"Worksheet '{self.path}' does not contain any rows.")

        header_row = row_elements[0]
        header_cells = sorted(
            ((_column_index_from_reference(cell.attrib.get("r", "")), _read_cell_value(cell, self._shared_strings)) for cell in header_row.findall("main:c", _SHEET_NS)),
            key=lambda item: item[0],
        )
        header_column_indexes = [column_index for column_index, _ in header_cells]
        headers = [value for _, value in header_cells]

        workbook_rows: list[WorkbookRow] = []
        data_rows = row_elements[1:]
        for data_index, row_element in enumerate(data_rows):
            row_values_by_index = {
                _column_index_from_reference(cell.attrib.get("r", "")): _read_cell_value(cell, self._shared_strings)
                for cell in row_element.findall("main:c", _SHEET_NS)
            }
            values = {
                header: row_values_by_index.get(column_index, "")
                for header, column_index in zip(headers, header_column_indexes)
            }
            workbook_rows.append(
                WorkbookRow(
                    data_index=data_index,
                    excel_row_number=_row_number_from_element(row_element, fallback=data_index + 2),
                    values=values,
                )
            )

        return header_row, data_rows, header_column_indexes, headers, workbook_rows

    def write_statuses(self, statuses: list[str]) -> None:
        if len(statuses) != len(self.rows):
            raise ValueError("Status count must match workbook row count.")

        status_column_index = self._ensure_status_header()
        for workbook_row, row_element, status in zip(self.rows, self._data_row_elements, statuses):
            cell_reference = f"{_column_letter(status_column_index)}{workbook_row.excel_row_number}"
            _set_inline_string_cell(row_element, cell_reference, status)

        self._update_sheet_metadata(status_column_index)
        self._entries[self.sheet_target] = ET.tostring(
            self._sheet_root,
            encoding="utf-8",
            xml_declaration=True,
        )
        self._write_entries()

    def _ensure_status_header(self) -> int:
        if STATUS_COLUMN_NAME in self.headers:
            status_index = self._header_column_indexes[self.headers.index(STATUS_COLUMN_NAME)]
        else:
            status_index = (max(self._header_column_indexes) if self._header_column_indexes else 0) + 1
            self._header_column_indexes.append(status_index)
            self.headers.append(STATUS_COLUMN_NAME)

        _set_inline_string_cell(self._header_row, f"{_column_letter(status_index)}1", STATUS_COLUMN_NAME)
        return status_index

    def _update_sheet_metadata(self, status_column_index: int) -> None:
        max_column_index = max(self._header_column_indexes + [status_column_index])
        max_row_number = max((row.excel_row_number for row in self.rows), default=1)
        dimension = self._sheet_root.find("main:dimension", _SHEET_NS)
        if dimension is not None:
            dimension.attrib["ref"] = f"A1:{_column_letter(max_column_index)}{max_row_number}"

        for row_element in [self._header_row, *self._data_row_elements]:
            row_element.attrib["spans"] = f"1:{max_column_index}"

    def _write_entries(self) -> None:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx", dir=self.path.parent) as temp_file:
            temp_path = Path(temp_file.name)

        try:
            with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for name, payload in self._entries.items():
                    zf.writestr(name, payload)
            temp_path.replace(self.path)
        finally:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)


def run_position_sync(
    *,
    workbook_path: str | Path = "positions.xlsx",
    preferred_sheet_name: str = "Sheet2",
    summary_output_path: str | Path | None = None,
    accounts: tuple[str, ...] | None = None,
    runtime_config: CatapultRuntimeConfig | None = None,
    client_factory: type[CatapultClient] = CatapultClient,
) -> dict[str, Any]:
    runtime = runtime_config or build_catapult_runtime_config()
    workbook = XlsxStatusWorkbook(workbook_path, preferred_sheet_name=preferred_sheet_name)
    statuses = [workbook_row.values.get(STATUS_COLUMN_NAME, "") for workbook_row in workbook.rows]
    available_accounts = {account.name: account for account in runtime.accounts}
    selected_accounts = {account.strip() for account in (accounts or ()) if account.strip()}
    account_rows: dict[str, list[WorkbookRow]] = defaultdict(list)
    missing_position_counter: Counter[tuple[str, str, str]] = Counter()
    pending_updates: dict[str, list[PendingAthleteUpdate]] = defaultdict(list)
    evaluated_row_indexes: set[int] = set()

    for workbook_row in workbook.rows:
        source_account = workbook_row.values.get("source_account", "").strip()
        if selected_accounts and source_account not in selected_accounts:
            continue
        evaluated_row_indexes.add(workbook_row.data_index)
        if source_account not in available_accounts:
            statuses[workbook_row.data_index] = STATUS_ACCOUNT_NOT_AVAILABLE
            continue
        account_rows[source_account].append(workbook_row)

    for source_account, rows in account_rows.items():
        client = client_factory(runtime, available_accounts[source_account])
        try:
            positions = _coerce_list_payload(client.get("/positions").json())
            athletes = _coerce_list_payload(client.get("/athletes").json())
            target_positions = {
                (_normalize_text(position.get("name")), _normalize_text(position.get("slug"))): position
                for position in positions
            }
            athlete_lookup = {
                _normalize_identifier(athlete.get("id")): athlete
                for athlete in athletes
                if _normalize_identifier(athlete.get("id"))
            }

            for workbook_row in rows:
                athlete_id = workbook_row.values.get("athlete_id", "").strip()
                athlete = athlete_lookup.get(athlete_id)
                if athlete is None:
                    statuses[workbook_row.data_index] = STATUS_ATHLETE_NOT_FOUND
                    continue

                target_key = (
                    _normalize_text(workbook_row.values.get("new_position_name")),
                    _normalize_text(workbook_row.values.get("new_position_slug")),
                )
                target_position = target_positions.get(target_key)
                if target_position is None:
                    statuses[workbook_row.data_index] = STATUS_MISSING_POSITION
                    missing_position_counter[(source_account, target_key[0], target_key[1])] += 1
                    continue

                target_position_id = _normalize_identifier(target_position.get("id"))
                if target_position_id is None:
                    statuses[workbook_row.data_index] = STATUS_MISSING_POSITION
                    missing_position_counter[(source_account, target_key[0], target_key[1])] += 1
                    continue

                current_position_id = _normalize_identifier(athlete.get("position_id"))
                if current_position_id == target_position_id:
                    statuses[workbook_row.data_index] = STATUS_SUCCESSFUL
                    continue

                pending_updates[source_account].append(
                    PendingAthleteUpdate(
                        row_index=workbook_row.data_index,
                        athlete_id=athlete_id,
                        target_position_id=target_position_id,
                        payload=_build_athlete_update_payload(athlete, target_position_id=target_position_id),
                    )
                )
        finally:
            client.close()

    missing_positions = [
        {
            "source_account": source_account,
            "new_position_name": position_name,
            "new_position_slug": position_slug,
            "affected_row_count": count,
        }
        for (source_account, position_name, position_slug), count in sorted(missing_position_counter.items())
    ]

    _log_missing_positions(missing_positions)

    for source_account, account_pending_updates in pending_updates.items():
        client = client_factory(runtime, available_accounts[source_account])
        try:
            for pending_update in account_pending_updates:
                try:
                    response = client.put(
                        f"/athletes/{pending_update.athlete_id}",
                        json=pending_update.payload,
                    )
                    updated_athlete = _coerce_mapping_payload(response.json())
                    if _normalize_identifier(updated_athlete.get("position_id")) == pending_update.target_position_id:
                        statuses[pending_update.row_index] = STATUS_SUCCESSFUL
                        continue

                    refreshed_lookup = _refresh_athlete_lookup(client)
                    refreshed_athlete = refreshed_lookup.get(pending_update.athlete_id)
                    if (
                        refreshed_athlete
                        and _normalize_identifier(refreshed_athlete.get("position_id")) == pending_update.target_position_id
                    ):
                        statuses[pending_update.row_index] = STATUS_SUCCESSFUL
                        continue
                except requests.exceptions.HTTPError as exc:
                    logger.warning(
                        "Catapult athlete update failed for athlete_id=%s account=%s status=%s body=%s",
                        pending_update.athlete_id,
                        source_account,
                        exc.response.status_code if exc.response is not None else "unknown",
                        exc.response.text[:500] if exc.response is not None and exc.response.text else "",
                    )

                statuses[pending_update.row_index] = STATUS_UPDATE_FAILED
        finally:
            client.close()

    workbook.write_statuses(statuses)

    summary = {
        "workbook_path": str(Path(workbook_path)),
        "sheet_name": workbook.sheet_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "selected_accounts": sorted(selected_accounts) if selected_accounts else [],
        "status_counts": dict(
            Counter(statuses[row_index] for row_index in evaluated_row_indexes if statuses[row_index])
        ),
        "missing_positions": missing_positions,
    }

    output_path = Path(summary_output_path) if summary_output_path else _default_summary_path(workbook.path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Position sync completed with status counts: %s", summary["status_counts"])
    logger.info("Missing-position summary written to %s", output_path)
    return summary


def main_run_position_sync(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Synchronize Catapult athlete positions from positions.xlsx.")
    parser.add_argument("--workbook", default="positions.xlsx", help="Workbook path. Defaults to positions.xlsx.")
    parser.add_argument("--sheet", default="Sheet2", help="Preferred sheet name. Defaults to Sheet2.")
    parser.add_argument(
        "--accounts",
        nargs="+",
        default=None,
        help="Optional Catapult account names to process, for example CATAPULT_A.",
    )
    parser.add_argument(
        "--summary-output",
        default=None,
        help="Optional JSON path for the missing-position summary output.",
    )
    args = parser.parse_args(argv)

    run_position_sync(
        workbook_path=args.workbook,
        preferred_sheet_name=args.sheet,
        summary_output_path=args.summary_output,
        accounts=tuple(args.accounts) if args.accounts else None,
    )
    return 0


def _default_summary_path(workbook_path: Path) -> Path:
    return workbook_path.with_name(f"{workbook_path.stem}_missing_positions.json")


def _log_missing_positions(missing_positions: list[dict[str, Any]]) -> None:
    if not missing_positions:
        logger.info("No missing canonical positions were found.")
        return

    logger.info("Missing canonical positions by account:")
    for item in missing_positions:
        logger.info(
            "  %s | %s / %s | affected athletes=%d",
            item["source_account"],
            item["new_position_name"],
            item["new_position_slug"],
            item["affected_row_count"],
        )


def _refresh_athlete_lookup(client: CatapultClient) -> dict[str, dict[str, Any]]:
    athletes = _coerce_list_payload(client.get("/athletes").json())
    return {
        athlete_id: athlete
        for athlete in athletes
        if (athlete_id := _normalize_identifier(athlete.get("id"))) is not None
    }


def _build_athlete_update_payload(
    athlete: dict[str, Any],
    *,
    target_position_id: str,
) -> dict[str, Any]:
    payload = {
        field: athlete[field]
        for field in ATHLETE_UPDATE_PROFILE_FIELDS
        if field in athlete
    }
    if "date_of_birth_date" not in payload:
        derived_date = _derive_date_of_birth_date(athlete.get("date_of_birth"))
        if derived_date is not None:
            payload["date_of_birth_date"] = derived_date
    payload["position_id"] = target_position_id
    return payload


def _normalize_sheet_target(target: str) -> str:
    normalized = target.lstrip("/")
    if not normalized.startswith("xl/"):
        normalized = f"xl/{normalized}"
    return normalized


def _coerce_list_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    raise ValueError("Expected list payload from Catapult API.")


def _coerce_mapping_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    return {}


def _normalize_identifier(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _normalize_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _derive_date_of_birth_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(timestamp, tz=CATAPULT_DATE_FALLBACK_TIMEZONE).date().isoformat()


def _column_index_from_reference(reference: str) -> int:
    letters = "".join(character for character in reference if character.isalpha())
    if not letters:
        return 0
    value = 0
    for character in letters.upper():
        value = value * 26 + (ord(character) - ord("A") + 1)
    return value


def _column_letter(index: int) -> str:
    if index < 1:
        raise ValueError("Excel column indexes are 1-based.")
    result = ""
    current = index
    while current:
        current, remainder = divmod(current - 1, 26)
        result = chr(ord("A") + remainder) + result
    return result


def _row_number_from_element(row_element: ET.Element, *, fallback: int) -> int:
    raw_row = row_element.attrib.get("r")
    if raw_row and raw_row.isdigit():
        return int(raw_row)
    return fallback


def _read_cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.findall(".//main:t", _SHEET_NS))

    value_element = cell.find("main:v", _SHEET_NS)
    raw_value = value_element.text if value_element is not None else ""
    if cell_type == "s" and raw_value:
        return shared_strings[int(raw_value)]
    return raw_value or ""


def _set_inline_string_cell(row_element: ET.Element, cell_reference: str, value: str) -> None:
    cell = None
    for candidate in row_element.findall("main:c", _SHEET_NS):
        if candidate.attrib.get("r") == cell_reference:
            cell = candidate
            break

    if cell is None:
        cell = ET.SubElement(row_element, f"{{{_SHEET_MAIN_NS}}}c", {"r": cell_reference})

    cell.attrib.clear()
    cell.attrib["r"] = cell_reference
    cell.attrib["t"] = "inlineStr"
    for child in list(cell):
        cell.remove(child)

    inline = ET.SubElement(cell, f"{{{_SHEET_MAIN_NS}}}is")
    text_node = ET.SubElement(inline, f"{{{_SHEET_MAIN_NS}}}t")
    text_node.text = value
