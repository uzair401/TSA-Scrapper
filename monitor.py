#!/usr/bin/env python3
"""Continuous monitor for TSA checkpoint updates."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv

from discord_client import DiscordClient
from questdb_client import QuestDBClient
from scraper import BASE_URL, PAGE_TIMEOUT_MS, ScrapeError, scrape_monitor_snapshot
from validator import merge_recent_rows, validate_snapshot


@dataclass
class MonitorConfig:
    target_url: str
    poll_interval_seconds: int
    timeout_ms: int
    state_path: Path
    updates_path: Path
    debug_dir: Path
    log_path: Path
    discord_webhook_url: str
    questdb_url: str
    run_once: bool


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TSA passenger volumes monitor")
    parser.add_argument("--once", action="store_true", help="Run one poll cycle then exit.")
    parser.add_argument(
        "--target-url",
        default=os.getenv("TARGET_URL", BASE_URL),
        help="TSA URL to monitor.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=int(os.getenv("POLL_INTERVAL_SECONDS", "60")),
        help="Polling interval in seconds.",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=int(os.getenv("POLL_TIMEOUT_MS", str(PAGE_TIMEOUT_MS))),
        help="Playwright timeout in milliseconds.",
    )
    parser.add_argument(
        "--state-path",
        type=Path,
        default=Path(os.getenv("STATE_PATH", "state.json")),
        help="State file path.",
    )
    parser.add_argument(
        "--debug-dir",
        type=Path,
        default=Path(os.getenv("DEBUG_DIR", "debug_artifacts")),
        help="Directory for timeout artifacts.",
    )
    parser.add_argument(
        "--updates-path",
        type=Path,
        default=Path(os.getenv("UPDATES_PATH", "updates.json")),
        help="Separate JSON file for delivered update records.",
    )
    parser.add_argument(
        "--log-path",
        type=Path,
        default=Path(os.getenv("MONITOR_LOG_PATH", "monitor.log")),
        help="Rotating log file path.",
    )
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> MonitorConfig:
    discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "")
    questdb_url = os.getenv("QUESTDB_URL", "http://localhost:9000")

    return MonitorConfig(
        target_url=args.target_url,
        poll_interval_seconds=max(1, int(args.poll_interval_seconds)),
        timeout_ms=max(1_000, int(args.timeout_ms)),
        state_path=args.state_path,
        updates_path=args.updates_path,
        debug_dir=args.debug_dir,
        log_path=args.log_path,
        discord_webhook_url=discord_webhook_url,
        questdb_url=questdb_url,
        run_once=args.once,
    )


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    file_handler = RotatingFileHandler(
        filename=log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)


def load_state(state_path: Path) -> dict | None:
    if not state_path.exists():
        return None

    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        logging.exception("Failed to load state file %s", state_path)
        return None

    required_keys = {"last_date", "last_travelers", "row_count", "last_updated_utc"}
    missing_keys = required_keys.difference(state.keys())
    if missing_keys:
        logging.error("State file missing required keys: %s", sorted(missing_keys))
        return None

    return state


def save_state(state_path: Path, state: dict) -> None:
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_updates(updates_path: Path) -> dict:
    if not updates_path.exists():
        return {"updates": []}
    try:
        payload = json.loads(updates_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and isinstance(payload.get("updates"), list):
            return payload
    except Exception:
        logging.exception("Failed to load updates file %s; resetting.", updates_path)
    return {"updates": []}


def save_updates(updates_path: Path, payload: dict) -> None:
    updates_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def get_or_create_update_record(updates_payload: dict, newest_row: dict) -> dict:
    date_iso = newest_row["date_iso"]
    travelers = int(newest_row["travelers"])

    for item in updates_payload["updates"]:
        if item.get("date_iso") == date_iso and int(item.get("travelers", -1)) == travelers:
            return item

    record = {
        "date_iso": date_iso,
        "date": newest_row["date"],
        "travelers": travelers,
        "first_seen_utc": utc_now_iso(),
        "last_attempt_utc": utc_now_iso(),
        "attempt_count": 0,
        "db_inserted": False,
        "discord_sent": False,
    }
    updates_payload["updates"].append(record)
    return record


def build_state(snapshot: dict, previous_state: dict | None = None) -> dict:
    latest_row = snapshot["latest_row"]

    return {
        "last_date": latest_row["date_iso"],
        "last_travelers": int(latest_row["travelers"]),
        "row_count": int(snapshot["row_count"]),
        "last_updated_utc": utc_now_iso(),
        "recent_rows": merge_recent_rows(
            (previous_state or {}).get("recent_rows", []),
            snapshot.get("recent_rows", []),
            limit=5,
        ),
    }


def compute_retry_delay(poll_interval_seconds: int, consecutive_failures: int) -> int:
    if consecutive_failures < 3:
        return poll_interval_seconds

    exponent = consecutive_failures - 3
    return min(300 * (2**exponent), 3600)


def format_update_message(latest_row: dict, warnings: list[str]) -> str:
    detected_at = utc_now_iso().replace("T", " ").replace("Z", " UTC")
    lines = [
        "🛫 New TSA Checkpoint Data Detected",
        f"Date: {latest_row['date']}",
        f"Travelers: {int(latest_row['travelers']):,}",
        f"Detected at: {detected_at}",
    ]

    for warning in warnings:
        lines.append(f"⚠️ Warning: {warning}")

    return "\n".join(lines)


async def run_monitor(config: MonitorConfig) -> None:
    state = load_state(config.state_path)
    updates_payload = load_updates(config.updates_path)
    save_updates(config.updates_path, updates_payload)
    discord_client = DiscordClient(config.discord_webhook_url)
    questdb_client = QuestDBClient(config.questdb_url)

    poll_number = 0
    consecutive_failures = 0
    monitor_failure_alert_sent = False

    try:
        table_ready = await questdb_client.ensure_table()
        if not table_ready:
            logging.error(
                "QuestDB table initialization failed. Monitor will continue; "
                "DB inserts may fail until QuestDB is reachable."
            )

        while True:
            poll_number += 1
            recent_rows_limit = 5 if poll_number % 10 == 0 else 1
            cycle_success = False

            logging.info(
                "Poll %s started. target=%s recent_rows_limit=%s",
                poll_number,
                config.target_url,
                recent_rows_limit,
            )

            try:
                snapshot = await scrape_monitor_snapshot(
                    target_url=config.target_url,
                    debug_prefix="monitor",
                    recent_rows_limit=recent_rows_limit,
                    timeout_ms=config.timeout_ms,
                    debug_dir=config.debug_dir,
                )
                latest_row = snapshot["latest_row"]

                logging.info(
                    "Poll %s scrape success. status=%s row_count=%s latest_date=%s travelers=%s",
                    poll_number,
                    snapshot.get("status_code"),
                    snapshot["row_count"],
                    latest_row["date_iso"],
                    latest_row["travelers"],
                )

                if state is None:
                    state = build_state(snapshot)
                    save_state(config.state_path, state)
                    baseline = get_or_create_update_record(
                        updates_payload,
                        snapshot["latest_row"],
                    )
                    baseline["last_attempt_utc"] = utc_now_iso()
                    baseline["bootstrap"] = True
                    save_updates(config.updates_path, updates_payload)
                    logging.info(
                        "State bootstrapped at date=%s travelers=%s row_count=%s",
                        state["last_date"],
                        state["last_travelers"],
                        state["row_count"],
                    )
                else:
                    validation = validate_snapshot(state, snapshot, poll_number)

                    for warning in validation.warnings:
                        logging.warning("Validation warning: %s", warning)

                    if validation.critical_error:
                        logging.error("Validation critical: %s", validation.critical_error)
                    elif validation.should_notify and validation.newest_row is not None:
                        record = get_or_create_update_record(
                            updates_payload,
                            validation.newest_row,
                        )
                        record["last_attempt_utc"] = utc_now_iso()
                        record["attempt_count"] = int(record.get("attempt_count", 0)) + 1

                        if not record.get("db_inserted", False):
                            inserted = await questdb_client.insert_checkpoint(
                                validation.newest_row["date_iso"],
                                int(validation.newest_row["travelers"]),
                            )
                            record["db_inserted"] = bool(inserted)
                            if inserted:
                                logging.info("QuestDB insert succeeded.")
                            else:
                                logging.error("QuestDB insert failed; state will not advance.")

                        if record.get("db_inserted", False) and not record.get("discord_sent", False):
                            message = format_update_message(
                                validation.newest_row,
                                validation.warnings,
                            )
                            sent = await discord_client.send_message(message)
                            record["discord_sent"] = bool(sent)
                            if sent:
                                logging.info("Discord notification sent.")
                            else:
                                logging.error("Discord notification failed.")

                        save_updates(config.updates_path, updates_payload)

                        if record.get("db_inserted", False):
                            state = build_state(snapshot, previous_state=state)
                            save_state(config.state_path, state)
                            logging.info(
                                "State updated to date=%s travelers=%s row_count=%s",
                                state["last_date"],
                                state["last_travelers"],
                                state["row_count"],
                            )
                        else:
                            logging.warning(
                                "State not advanced because DB insert is pending for date=%s.",
                                validation.newest_row["date_iso"],
                            )
                    else:
                        reason = validation.no_update_reason or "No new update."
                        logging.info("Poll %s no update: %s", poll_number, reason)

                        if recent_rows_limit == 5:
                            state["recent_rows"] = merge_recent_rows(
                                state.get("recent_rows", []),
                                snapshot.get("recent_rows", []),
                                limit=5,
                            )
                            state["last_integrity_check_utc"] = utc_now_iso()
                            save_state(config.state_path, state)

                cycle_success = True
            except ScrapeError as exc:
                logging.error("Poll %s scrape error: %s", poll_number, exc)
            except Exception:
                logging.exception("Poll %s unexpected error", poll_number)

            if cycle_success:
                consecutive_failures = 0
                monitor_failure_alert_sent = False
                delay_seconds = config.poll_interval_seconds
            else:
                consecutive_failures += 1
                delay_seconds = compute_retry_delay(
                    config.poll_interval_seconds,
                    consecutive_failures,
                )

                if consecutive_failures >= 10 and not monitor_failure_alert_sent:
                    await discord_client.send_message(
                        "🚨 TSA monitor failure alert\n"
                        f"Consecutive failures: {consecutive_failures}\n"
                        f"Detected at: {utc_now_iso().replace('T', ' ').replace('Z', ' UTC')}"
                    )
                    monitor_failure_alert_sent = True

            logging.info(
                "Poll %s completed. consecutive_failures=%s next_sleep_seconds=%s",
                poll_number,
                consecutive_failures,
                delay_seconds,
            )

            if config.run_once:
                break

            await asyncio.sleep(delay_seconds)
    finally:
        await discord_client.close()
        await questdb_client.close()


async def main() -> None:
    load_dotenv()
    args = parse_args()
    config = build_config(args)
    setup_logging(config.log_path)

    logging.info(
        "Starting TSA monitor. target=%s poll_interval=%ss questdb=%s updates=%s",
        config.target_url,
        config.poll_interval_seconds,
        config.questdb_url,
        config.updates_path,
    )

    try:
        await run_monitor(config)
    except Exception:
        logging.exception("Monitor terminated due to unexpected top-level exception.")


if __name__ == "__main__":
    asyncio.run(main())
