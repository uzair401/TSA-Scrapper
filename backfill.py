#!/usr/bin/env python3
"""Backfill TSA passenger data from TSA website into QuestDB."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from questdb_client import QuestDBClient
from scraper import BASE_URL, PAGE_TIMEOUT_MS, ScrapeError, scrape_target


def parse_args() -> argparse.Namespace:
    current_year = datetime.now().year

    parser = argparse.ArgumentParser(
        description="Backfill TSA checkpoint rows into QuestDB using Playwright scraping."
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=current_year - 2,
        help="First year to scrape (inclusive).",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=current_year,
        help="Last year to scrape (inclusive).",
    )
    parser.add_argument(
        "--target-base-url",
        type=str,
        default=BASE_URL,
        help="Base TSA URL (default: main TSA passenger volumes URL).",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=int(os.getenv("POLL_TIMEOUT_MS", str(PAGE_TIMEOUT_MS))),
        help="Playwright timeout in milliseconds.",
    )
    parser.add_argument(
        "--debug-dir",
        type=Path,
        default=Path(os.getenv("DEBUG_DIR", "debug_artifacts")),
        help="Directory for timeout debug artifacts.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip rows already present in QuestDB by date lookup.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    return parser.parse_args()


def build_year_url(base_url: str, year: int) -> str:
    return f"{base_url.rstrip('/')}/{year}"


async def backfill_year(
    questdb: QuestDBClient,
    year: int,
    base_url: str,
    timeout_ms: int,
    debug_dir: Path,
    skip_existing: bool,
) -> tuple[int, int, int]:
    target_url = build_year_url(base_url, year)
    logging.info("Backfill year %s: scraping %s", year, target_url)

    try:
        rows = await scrape_target(
            target_url=target_url,
            debug_prefix=f"backfill_{year}",
            timeout_ms=timeout_ms,
            debug_dir=debug_dir,
        )
    except ScrapeError:
        current_year = datetime.now().year
        if year != current_year:
            raise

        fallback_url = base_url.rstrip("/")
        logging.warning(
            "Year %s URL failed; retrying with current-page URL %s",
            year,
            fallback_url,
        )
        rows = await scrape_target(
            target_url=fallback_url,
            debug_prefix=f"backfill_current_{year}",
            timeout_ms=timeout_ms,
            debug_dir=debug_dir,
        )

        rows = [row for row in rows if row.get("date_iso", "").startswith(f"{year}-")]
        logging.info(
            "Filtered current-page rows for %s: %s row(s)",
            year,
            len(rows),
        )

    rows_sorted = sorted(rows, key=lambda row: row["date_iso"])
    inserted = 0
    skipped = 0
    failed = 0

    for row in rows_sorted:
        date_iso = row["date_iso"]
        travelers = int(row["travelers"])

        if skip_existing:
            exists = await questdb.checkpoint_exists(date_iso)
            if exists:
                skipped += 1
                continue

        ok = await questdb.insert_checkpoint(date_iso, travelers)
        if ok:
            inserted += 1
        else:
            failed += 1
            logging.error(
                "Insert failed for year=%s date=%s travelers=%s",
                year,
                date_iso,
                travelers,
            )

    return inserted, skipped, failed


async def main() -> None:
    load_dotenv()
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.end_year < args.start_year:
        raise SystemExit("end-year must be greater than or equal to start-year")

    questdb_url = os.getenv("QUESTDB_URL", "http://localhost:9000")
    questdb_username = os.getenv("QUESTDB_USERNAME", "")
    questdb_password = os.getenv("QUESTDB_PASSWORD", "")
    questdb = QuestDBClient(
        questdb_url,
        username=questdb_username or None,
        password=questdb_password or None,
    )

    total_inserted = 0
    total_skipped = 0
    total_failed = 0

    try:
        ready = await questdb.ensure_table()
        if not ready:
            raise SystemExit("Unable to create/verify QuestDB table tsa_checkpoint")

        for year in range(args.start_year, args.end_year + 1):
            try:
                inserted, skipped, failed = await backfill_year(
                    questdb=questdb,
                    year=year,
                    base_url=args.target_base_url,
                    timeout_ms=args.timeout_ms,
                    debug_dir=args.debug_dir,
                    skip_existing=args.skip_existing,
                )
                total_inserted += inserted
                total_skipped += skipped
                total_failed += failed
                logging.info(
                    "Year %s complete: inserted=%s skipped=%s failed=%s",
                    year,
                    inserted,
                    skipped,
                    failed,
                )
            except ScrapeError as exc:
                total_failed += 1
                logging.error("Year %s scrape failed: %s", year, exc)

        logging.info(
            "Backfill complete: years=%s-%s inserted=%s skipped=%s failed=%s",
            args.start_year,
            args.end_year,
            total_inserted,
            total_skipped,
            total_failed,
        )
    finally:
        await questdb.close()


if __name__ == "__main__":
    asyncio.run(main())
