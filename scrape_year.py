#!/usr/bin/env python3
"""Scrape one full year of TSA passenger volume data and save it to JSON."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
from urllib.parse import urlparse
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

BASE_URL = "https://www.tsa.gov/travel/passenger-volumes"
# TSA yearly pages are not fully consistent. Some years use `table.table`,
# others render just `table` under `div.text-long`.
TABLE_SELECTOR = "div.text-long table"
PAGE_TIMEOUT_MS = 30_000
DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


class ScrapeError(Exception):
    """Raised when page content is not in the expected format."""


async def save_debug_artifacts(page, debug_dir: Path, prefix: str) -> None:
    debug_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    html_path = debug_dir / f"{prefix}_{timestamp}.html"
    png_path = debug_dir / f"{prefix}_{timestamp}.png"

    try:
        html_path.write_text(await page.content(), encoding="utf-8")
    except Exception as exc:  # pragma: no cover
        logging.error("Failed to write debug HTML: %s", exc)

    try:
        await page.screenshot(path=str(png_path), full_page=True)
    except Exception as exc:  # pragma: no cover
        logging.error("Failed to write debug screenshot: %s", exc)
    else:
        logging.error("Saved debug artifacts: %s and %s", html_path, png_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape TSA passenger volumes using Playwright."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=datetime.now().year,
        help="Year to scrape (default: current year).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSON path (default depends on target URL/year).",
    )
    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help=(
            "Optional direct URL to scrape (e.g., "
            "https://www.tsa.gov/travel/passenger-volumes). "
            "If omitted, the script uses --year."
        ),
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Run browser in headed mode for debugging.",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=PAGE_TIMEOUT_MS,
        help="Page/table wait timeout in milliseconds (default: 30000).",
    )
    parser.add_argument(
        "--debug-dir",
        type=Path,
        default=Path("debug_artifacts"),
        help="Directory for timeout debug artifacts.",
    )
    return parser.parse_args()


def build_year_url(year: int) -> str:
    return f"{BASE_URL}/{year}"


def build_target_url(year: int, override_url: str | None) -> str:
    if override_url:
        return override_url.rstrip("/")
    return build_year_url(year)


def infer_output_path(year: int, target_url: str, explicit_output: Path | None) -> Path:
    if explicit_output is not None:
        return explicit_output

    parsed = urlparse(target_url)
    suffix = parsed.path.rstrip("/").split("/")[-1]
    if re.fullmatch(r"\d{4}", suffix):
        return Path(f"tsa_passenger_volumes_{suffix}.json")
    return Path("tsa_passenger_volumes_current.json")


def parse_date(value: str) -> str:
    """Convert M/D/YYYY to ISO date."""
    parts = value.strip().split("/")
    if len(parts) != 3:
        raise ScrapeError(f"Invalid date value: {value!r}")

    try:
        month, day, year = (int(part) for part in parts)
        return datetime(year, month, day).date().isoformat()
    except ValueError as exc:
        raise ScrapeError(f"Invalid date value: {value!r}") from exc


def parse_travelers(value: str) -> int:
    digits = re.sub(r"[^0-9]", "", value)
    if not digits:
        raise ScrapeError(f"Invalid travelers value: {value!r}")
    return int(digits)


async def select_data_table(page, timeout_ms: int):
    """Pick the `div.text-long table` with the most body rows."""
    await page.wait_for_selector(TABLE_SELECTOR, timeout=timeout_ms)
    tables = page.locator(TABLE_SELECTOR)
    table_count = await tables.count()
    if table_count == 0:
        raise ScrapeError("No table elements found under div.text-long.")

    best_table = None
    best_row_count = -1
    for idx in range(table_count):
        candidate = tables.nth(idx)
        rows = candidate.locator("tbody tr")
        row_count = await rows.count()
        if row_count > best_row_count:
            best_row_count = row_count
            best_table = candidate

    logging.info("Detected %s table candidate(s); selected table with %s rows", table_count, best_row_count)
    if best_table is None or best_row_count <= 0:
        raise ScrapeError("Table found, but no data rows were present.")
    return best_table


async def scrape_target(
    target_url: str,
    debug_prefix: str,
    headful: bool = False,
    timeout_ms: int = PAGE_TIMEOUT_MS,
    debug_dir: Path = Path("debug_artifacts"),
) -> list[dict]:
    cache_buster = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    request_url = f"{target_url}?_cb={cache_buster}"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=not headful)
        context = await browser.new_context(
            user_agent=DEFAULT_UA,
            locale="en-US",
            extra_http_headers={
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Accept-Language": "en-US,en;q=0.9",
            },
            viewport={"width": 1366, "height": 768},
        )
        page = await context.new_page()

        try:
            logging.info("Navigating to %s", request_url)
            response = await page.goto(
                request_url, wait_until="domcontentloaded", timeout=timeout_ms
            )
            logging.info(
                "Navigation complete. final_url=%s status=%s",
                page.url,
                response.status if response else "unknown",
            )
            data_table = await select_data_table(page, timeout_ms)
            rows = data_table.locator("tbody tr")
            row_count = await rows.count()
            if row_count == 0:
                raise ScrapeError("No table rows found.")

            logging.info("Found %s rows", row_count)
            extracted_rows: list[dict] = []

            for index in range(row_count):
                row = rows.nth(index)
                cells = row.locator("td")
                if await cells.count() < 2:
                    continue

                date_text = (await cells.nth(0).inner_text()).strip()
                travelers_text = (await cells.nth(1).inner_text()).strip()

                extracted_rows.append(
                    {
                        "date": date_text,
                        "date_iso": parse_date(date_text),
                        "travelers": parse_travelers(travelers_text),
                        "travelers_raw": travelers_text,
                        "row_index": index,
                    }
                )

            return extracted_rows
        except PlaywrightTimeoutError as exc:
            logging.error("Timeout waiting for table. final_url=%s", page.url)
            try:
                logging.error("Page title at timeout: %s", await page.title())
            except Exception:
                pass
            await save_debug_artifacts(page, debug_dir, f"timeout_{debug_prefix}")
            raise ScrapeError("Timed out while waiting for TSA table.") from exc
        finally:
            await context.close()
            await browser.close()


async def main() -> None:
    args = parse_args()
    target_url = build_target_url(args.year, args.url)
    output_path = infer_output_path(args.year, target_url, args.output)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    debug_prefix = str(args.year) if args.url is None else "current"
    rows = await scrape_target(
        target_url=target_url,
        debug_prefix=debug_prefix,
        headful=args.headful,
        timeout_ms=args.timeout_ms,
        debug_dir=args.debug_dir,
    )
    payload = {
        "year": args.year if args.url is None else None,
        "source_url": target_url,
        "scraped_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "row_count": len(rows),
        "rows": rows,
    }

    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logging.info("Saved %s rows to %s", len(rows), output_path)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except ScrapeError as err:
        raise SystemExit(f"Scrape failed: {err}") from err
