#!/usr/bin/env python3
"""Continuous monitor for TSA checkpoint updates."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import socket
from dataclasses import dataclass
from datetime import datetime, time, timezone
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
    peak_poll_interval_seconds: int
    peak_window_start_utc: str
    peak_window_end_utc: str
    proxies_only_in_peak: bool
    timeout_ms: int
    state_path: Path
    updates_path: Path
    debug_dir: Path
    log_path: Path
    discord_webhook_url: str
    alerts_enabled: bool
    server_name: str
    questdb_url: str
    questdb_username: str
    questdb_password: str
    workers: list["MonitorWorker"]
    run_once: bool


@dataclass
class MonitorWorker:
    name: str
    proxy_url: str | None = None


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


def parse_bool(value: str, default: bool = False) -> bool:
    text = (value or "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


def parse_proxy_urls(raw_value: str) -> list[str]:
    raw = (raw_value or "").strip()
    if not raw:
        return []
    parts = [part.strip() for part in re.split(r"[;\n]+", raw) if part.strip()]
    return parts


def build_workers() -> list[MonitorWorker]:
    workers: list[MonitorWorker] = []

    include_direct = parse_bool(os.getenv("ENABLE_DIRECT_WORKER", "true"), default=True)
    if include_direct:
        workers.append(MonitorWorker(name="direct", proxy_url=None))

    proxy_urls = parse_proxy_urls(os.getenv("PROXY_URLS", ""))
    proxy_names = [item.strip() for item in os.getenv("PROXY_NAMES", "").split(",") if item.strip()]

    for index, proxy_url in enumerate(proxy_urls, start=1):
        worker_name = proxy_names[index - 1] if index - 1 < len(proxy_names) else f"proxy-{index}"
        workers.append(MonitorWorker(name=worker_name, proxy_url=proxy_url))

    if not workers:
        raise ValueError(
            "No workers configured. Enable direct worker or provide PROXY_URLS."
        )

    return workers


def build_config(args: argparse.Namespace) -> MonitorConfig:
    discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "")
    alerts_enabled = parse_bool(os.getenv("ALERTS_ENABLED", "true"), default=True)
    server_name = os.getenv("SERVER_NAME", socket.gethostname())
    questdb_url = os.getenv("QUESTDB_URL", "http://localhost:9000")
    questdb_username = os.getenv("QUESTDB_USERNAME", "")
    questdb_password = os.getenv("QUESTDB_PASSWORD", "")
    peak_poll_interval_seconds = int(
        os.getenv("PEAK_POLL_INTERVAL_SECONDS", "10")
    )
    peak_window_start_utc = os.getenv("PEAK_WINDOW_START_UTC", "11:00")
    peak_window_end_utc = os.getenv("PEAK_WINDOW_END_UTC", "15:00")
    proxies_only_in_peak = parse_bool(
        os.getenv("PROXIES_ONLY_IN_PEAK", "false"),
        default=False,
    )
    workers = build_workers()

    if proxies_only_in_peak and not any(worker.proxy_url is None for worker in workers):
        raise ValueError(
            "PROXIES_ONLY_IN_PEAK=true requires ENABLE_DIRECT_WORKER=true "
            "so off-peak has at least one active worker."
        )

    return MonitorConfig(
        target_url=args.target_url,
        poll_interval_seconds=max(1, int(args.poll_interval_seconds)),
        peak_poll_interval_seconds=max(1, peak_poll_interval_seconds),
        peak_window_start_utc=peak_window_start_utc.strip(),
        peak_window_end_utc=peak_window_end_utc.strip(),
        proxies_only_in_peak=proxies_only_in_peak,
        timeout_ms=max(1_000, int(args.timeout_ms)),
        state_path=args.state_path,
        updates_path=args.updates_path,
        debug_dir=args.debug_dir,
        log_path=args.log_path,
        discord_webhook_url=discord_webhook_url,
        alerts_enabled=alerts_enabled,
        server_name=server_name,
        questdb_url=questdb_url,
        questdb_username=questdb_username,
        questdb_password=questdb_password,
        workers=workers,
        run_once=args.once,
    )


def parse_hhmm_utc(value: str, label: str) -> time:
    try:
        hour_text, minute_text = value.split(":")
        parsed = time(hour=int(hour_text), minute=int(minute_text))
        return parsed
    except Exception as exc:
        raise ValueError(f"Invalid {label}: {value!r}. Expected HH:MM in UTC.") from exc


def is_peak_window_utc(now_utc: datetime, start_utc: time, end_utc: time) -> bool:
    current = now_utc.time().replace(second=0, microsecond=0)
    if start_utc <= end_utc:
        return start_utc <= current <= end_utc
    return current >= start_utc or current <= end_utc


def active_poll_interval_seconds(config: MonitorConfig) -> tuple[int, str]:
    start_utc = parse_hhmm_utc(config.peak_window_start_utc, "PEAK_WINDOW_START_UTC")
    end_utc = parse_hhmm_utc(config.peak_window_end_utc, "PEAK_WINDOW_END_UTC")
    now_utc = datetime.now(tz=timezone.utc)

    if is_peak_window_utc(now_utc, start_utc, end_utc):
        return config.peak_poll_interval_seconds, "peak"
    return config.poll_interval_seconds, "offpeak"


def active_workers(config: MonitorConfig, interval_mode: str) -> list[MonitorWorker]:
    if not config.proxies_only_in_peak or interval_mode == "peak":
        return config.workers

    direct_workers = [worker for worker in config.workers if worker.proxy_url is None]
    if direct_workers:
        return direct_workers

    # Safety fallback; build_config should already prevent this state.
    return config.workers


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


def format_update_message(
    latest_row: dict,
    warnings: list[str],
    server_name: str,
    worker_name: str,
    via_proxy: bool,
    ip_metadata: dict | None,
    cdn_metadata: dict | None,
) -> str:
    detected_at = utc_now_iso().replace("T", " ").replace("Z", " UTC")
    lines = [
        "🛫 New TSA Checkpoint Data Detected",
        f"Date: {latest_row['date']}",
        f"Travelers: {int(latest_row['travelers']):,}",
        f"Detected at: {detected_at}",
        f"Source Server: {server_name}",
        f"First Seen By: {worker_name} ({'proxy' if via_proxy else 'direct'})",
    ]

    if ip_metadata:
        ip_parts = []
        if ip_metadata.get("city"):
            ip_parts.append(ip_metadata["city"])
        if ip_metadata.get("region"):
            ip_parts.append(ip_metadata["region"])
        if ip_metadata.get("country"):
            ip_parts.append(ip_metadata["country"])
        loc = ", ".join(ip_parts)
        ip_line = f"Worker IP: {ip_metadata.get('ip', 'unknown')}"
        if loc:
            ip_line += f" ({loc})"
        if ip_metadata.get("org"):
            ip_line += f" via {ip_metadata['org']}"
        lines.append(ip_line)

    if cdn_metadata:
        details = []
        if cdn_metadata.get("hostname"):
            details.append(f"host={cdn_metadata['hostname']}")
        if cdn_metadata.get("server"):
            details.append(f"server={cdn_metadata['server']}")
        if cdn_metadata.get("via"):
            details.append(f"via={cdn_metadata['via']}")
        if cdn_metadata.get("cf_ray"):
            details.append(f"cf-ray={cdn_metadata['cf_ray']}")
        if details:
            lines.append("CDN Info: " + " | ".join(details))

    for warning in warnings:
        lines.append(f"⚠️ Warning: {warning}")

    return "\n".join(lines)


def best_snapshot(snapshots: list[dict]) -> dict:
    return max(
        snapshots,
        key=lambda item: (
            item["latest_row"]["date_iso"],
            int(item.get("row_count", 0)),
        ),
    )


async def scrape_worker_snapshot(
    config: MonitorConfig,
    worker: MonitorWorker,
    recent_rows_limit: int,
    poll_number: int,
) -> dict:
    snapshot = await scrape_monitor_snapshot(
        target_url=config.target_url,
        debug_prefix=f"monitor_{worker.name}",
        recent_rows_limit=recent_rows_limit,
        proxy_url=worker.proxy_url,
        timeout_ms=config.timeout_ms,
        debug_dir=config.debug_dir,
    )
    snapshot["worker_name"] = worker.name
    snapshot["worker_proxy"] = bool(worker.proxy_url)
    logging.info(
        "Poll %s worker=%s success status=%s row_count=%s latest_date=%s",
        poll_number,
        worker.name,
        snapshot.get("status_code"),
        snapshot["row_count"],
        snapshot["latest_row"]["date_iso"],
    )
    return snapshot


async def run_monitor(config: MonitorConfig) -> None:
    state = load_state(config.state_path)
    updates_payload = load_updates(config.updates_path)
    save_updates(config.updates_path, updates_payload)
    discord_client = DiscordClient(config.discord_webhook_url)
    questdb_client = QuestDBClient(
        config.questdb_url,
        username=config.questdb_username or None,
        password=config.questdb_password or None,
    )

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
            base_interval_seconds, interval_mode = active_poll_interval_seconds(config)
            workers_for_poll = active_workers(config, interval_mode)

            logging.info(
                "Poll %s started. target=%s workers=%s recent_rows_limit=%s interval_mode=%s base_interval_seconds=%s",
                poll_number,
                config.target_url,
                len(workers_for_poll),
                recent_rows_limit,
                interval_mode,
                base_interval_seconds,
            )

            try:
                worker_tasks = [
                    scrape_worker_snapshot(
                        config=config,
                        worker=worker,
                        recent_rows_limit=recent_rows_limit,
                        poll_number=poll_number,
                    )
                    for worker in workers_for_poll
                ]
                results = await asyncio.gather(
                    *worker_tasks,
                    return_exceptions=True,
                )
                snapshots: list[dict] = []
                for worker, result in zip(workers_for_poll, results):
                    if isinstance(result, Exception):
                        logging.error(
                            "Poll %s worker=%s error: %s",
                            poll_number,
                            worker.name,
                            result,
                        )
                        continue
                    snapshots.append(result)

                if not snapshots:
                    raise ScrapeError("All workers failed in this poll cycle.")

                snapshot = best_snapshot(snapshots)
                latest_row = snapshot["latest_row"]

                logging.info(
                    "Poll %s selected worker=%s status=%s row_count=%s latest_date=%s travelers=%s",
                    poll_number,
                    snapshot.get("worker_name"),
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
                            if config.alerts_enabled:
                                message = format_update_message(
                                    validation.newest_row,
                                    validation.warnings,
                                    config.server_name,
                                    snapshot.get("worker_name", "unknown"),
                                    bool(snapshot.get("worker_proxy", False)),
                                    snapshot.get("ip_metadata"),
                                    snapshot.get("cdn_metadata"),
                                )
                                sent = await discord_client.send_message(message)
                                record["discord_sent"] = bool(sent)
                                if sent:
                                    logging.info("Discord notification sent.")
                                else:
                                    logging.error("Discord notification failed.")
                            else:
                                record["discord_sent"] = True
                                logging.info("Alerts disabled on this node; skipping Discord send.")

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
                delay_seconds = base_interval_seconds
            else:
                consecutive_failures += 1
                delay_seconds = compute_retry_delay(
                    base_interval_seconds,
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

    try:
        parse_hhmm_utc(config.peak_window_start_utc, "PEAK_WINDOW_START_UTC")
        parse_hhmm_utc(config.peak_window_end_utc, "PEAK_WINDOW_END_UTC")
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    logging.info(
        "Starting TSA monitor. target=%s poll_interval=%ss peak_poll_interval=%ss "
        "peak_window_utc=%s-%s proxies_only_in_peak=%s server=%s alerts=%s "
        "workers_total=%s questdb=%s auth=%s updates=%s",
        config.target_url,
        config.poll_interval_seconds,
        config.peak_poll_interval_seconds,
        config.peak_window_start_utc,
        config.peak_window_end_utc,
        config.proxies_only_in_peak,
        config.server_name,
        config.alerts_enabled,
        len(config.workers),
        config.questdb_url,
        "enabled" if config.questdb_username else "disabled",
        config.updates_path,
    )

    try:
        await run_monitor(config)
    except Exception:
        logging.exception("Monitor terminated due to unexpected top-level exception.")


if __name__ == "__main__":
    asyncio.run(main())
