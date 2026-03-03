"""Validation pipeline for TSA monitor updates."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

MIN_TRAVELERS = 500_000
MAX_TRAVELERS = 5_000_000
VALID_DATE_GAPS = {1, 2, 3}


@dataclass
class ValidationResult:
    should_notify: bool = False
    critical_error: str | None = None
    no_update_reason: str | None = None
    warnings: list[str] = field(default_factory=list)
    newest_row: dict | None = None


def _compare_recent_rows(previous_rows: list[dict], current_rows: list[dict]) -> list[str]:
    warnings: list[str] = []

    prev_by_date = {
        row.get("date_iso"): row.get("travelers")
        for row in previous_rows
        if row.get("date_iso")
    }

    if not prev_by_date or not current_rows:
        return warnings

    overlap_count = 0
    changed_rows: list[str] = []
    for row in current_rows:
        date_iso = row.get("date_iso")
        travelers = row.get("travelers")
        if date_iso in prev_by_date:
            overlap_count += 1
            if prev_by_date[date_iso] != travelers:
                changed_rows.append(
                    f"{date_iso} changed from {prev_by_date[date_iso]:,} to {int(travelers):,}"
                )

    if overlap_count == 0:
        warnings.append(
            "Check 6 warning: no overlapping rows in periodic integrity check."
        )
    if changed_rows:
        warnings.append(
            "Check 6 anomaly: previously known row values changed: "
            + "; ".join(changed_rows)
        )

    return warnings


def merge_recent_rows(
    existing_rows: list[dict] | None,
    observed_rows: list[dict] | None,
    limit: int = 5,
) -> list[dict]:
    """Merge row snapshots keyed by date, keeping most recent entries."""
    merged_by_date: dict[str, dict] = {}

    for row in existing_rows or []:
        date_iso = row.get("date_iso")
        if date_iso:
            merged_by_date[date_iso] = row

    for row in observed_rows or []:
        date_iso = row.get("date_iso")
        if date_iso:
            merged_by_date[date_iso] = row

    merged = sorted(merged_by_date.values(), key=lambda item: item["date_iso"], reverse=True)
    return merged[:limit]


def validate_snapshot(
    previous_state: dict,
    snapshot: dict,
    poll_number: int,
) -> ValidationResult:
    result = ValidationResult(newest_row=snapshot.get("latest_row"))

    previous_count = int(previous_state.get("row_count", 0))
    new_count = int(snapshot.get("row_count", 0))

    # Check 1 — row count sanity (CRITICAL)
    if new_count < previous_count:
        result.critical_error = (
            "Check 1 failed: row count decreased "
            f"from {previous_count} to {new_count}."
        )
        return result

    latest_row = snapshot.get("latest_row")
    if not latest_row:
        result.critical_error = "Missing latest row in scrape snapshot."
        return result

    previous_date = date.fromisoformat(previous_state["last_date"])
    newest_date = date.fromisoformat(latest_row["date_iso"])

    # Check 2 — date comparison (CRITICAL)
    if newest_date <= previous_date:
        result.no_update_reason = (
            f"Check 2: latest date {newest_date.isoformat()} is not newer than "
            f"state date {previous_date.isoformat()}."
        )
        if poll_number % 10 == 0:
            result.warnings.extend(
                _compare_recent_rows(
                    previous_state.get("recent_rows", []),
                    snapshot.get("recent_rows", []),
                )
            )
        return result

    # Check 3 — row count increment (SOFT)
    if new_count != previous_count + 1:
        result.warnings.append(
            "Check 3 warning: row count changed by "
            f"{new_count - previous_count} (expected +1)."
        )

    # Check 4 — date continuity (SOFT)
    date_gap = (newest_date - previous_date).days
    if date_gap not in VALID_DATE_GAPS:
        result.warnings.append(
            f"Check 4 warning: unexpected date gap of {date_gap} day(s)."
        )

    # Check 5 — traveler number sanity range (CRITICAL)
    travelers = int(latest_row["travelers"])
    if travelers < MIN_TRAVELERS or travelers > MAX_TRAVELERS:
        result.critical_error = (
            "Check 5 failed: travelers "
            f"{travelers:,} outside [{MIN_TRAVELERS:,}, {MAX_TRAVELERS:,}]."
        )
        return result

    # Check 6 — periodic integrity check every 10th poll (SOFT)
    if poll_number % 10 == 0:
        result.warnings.extend(
            _compare_recent_rows(
                previous_state.get("recent_rows", []),
                snapshot.get("recent_rows", []),
            )
        )

    result.should_notify = True
    return result
