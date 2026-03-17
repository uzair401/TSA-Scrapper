#!/usr/bin/env python3
"""Reusable TSA scraping helpers for historical extraction and monitoring."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

BASE_URL = "https://www.tsa.gov/travel/passenger-volumes"
TABLE_SELECTOR = "div.text-long table"
PAGE_TIMEOUT_MS = 30_000
DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


class ScrapeError(Exception):
    """Raised when page content is not in the expected format."""


def parse_date(value: str) -> str:
    """Convert M/D/YYYY text into an ISO date string (YYYY-MM-DD)."""
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


def build_target_url(year: int | None = None, override_url: str | None = None) -> str:
    if override_url:
        return override_url.rstrip("/")
    if year is not None:
        return f"{BASE_URL}/{year}"
    return BASE_URL


def infer_output_path(target_url: str) -> Path:
    parsed = urlparse(target_url)
    suffix = parsed.path.rstrip("/").split("/")[-1]
    if re.fullmatch(r"\d{4}", suffix):
        return Path(f"tsa_passenger_volumes_{suffix}.json")
    return Path("tsa_passenger_volumes_current.json")


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
        row_count = await candidate.locator("tbody tr").count()
        if row_count > best_row_count:
            best_row_count = row_count
            best_table = candidate

    if best_table is None or best_row_count <= 0:
        raise ScrapeError("Table found, but no data rows were present.")

    return best_table


async def _parse_row(row, index: int) -> dict:
    cells = row.locator("td")
    if await cells.count() < 2:
        raise ScrapeError(f"Row {index} did not contain two cells.")

    date_text = (await cells.nth(0).inner_text()).strip()
    travelers_text = (await cells.nth(1).inner_text()).strip()

    return {
        "date": date_text,
        "date_iso": parse_date(date_text),
        "travelers": parse_travelers(travelers_text),
        "travelers_raw": travelers_text,
        "row_index": index,
    }


def _append_cache_buster(url: str, cache_buster: int) -> str:
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}_cb={cache_buster}"


def _normalize_recent_rows(rows: list[dict]) -> list[dict]:
    # Keep rows newest-first regardless of table sort order.
    return sorted(rows, key=lambda item: item["date_iso"], reverse=True)


def _build_playwright_proxy(proxy_url: str) -> dict:
    raw = (proxy_url or "").strip()
    if not raw:
        raise ScrapeError("Proxy URL is empty.")

    if "://" not in raw:
        raw = f"http://{raw}"

    parsed = urlparse(raw)
    if not parsed.hostname or not parsed.port:
        raise ScrapeError(f"Invalid proxy URL (hostname/port missing): {proxy_url!r}")

    proxy = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
    if parsed.username is not None:
        proxy["username"] = unquote(parsed.username)
    if parsed.password is not None:
        proxy["password"] = unquote(parsed.password)
    return proxy


async def _fetch_ip_metadata(context) -> dict:
    try:
        response = await context.request.get("https://ipinfo.io/json", timeout=10_000)
        payload = await response.json()
        return {
            "ip": payload.get("ip"),
            "city": payload.get("city"),
            "region": payload.get("region"),
            "country": payload.get("country"),
            "org": payload.get("org"),
            "loc": payload.get("loc"),
        }
    except Exception as exc:
        logging.warning("Failed to fetch IP metadata: %s", exc)
    return {}


async def scrape_monitor_snapshot(
    target_url: str,
    debug_prefix: str,
    recent_rows_limit: int = 1,
    proxy_url: str | None = None,
    headful: bool = False,
    timeout_ms: int = PAGE_TIMEOUT_MS,
    debug_dir: Path = Path("debug_artifacts"),
) -> dict:
    """Scrape lightweight monitor payload (row count + latest row + recent rows)."""
    if recent_rows_limit < 1:
        recent_rows_limit = 1

    cache_buster = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    request_url = _append_cache_buster(target_url, cache_buster)
    launch_kwargs: dict = {"headless": not headful}
    proxy_config = _build_playwright_proxy(proxy_url) if proxy_url else None
    if proxy_config:
        launch_kwargs["proxy"] = proxy_config

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**launch_kwargs)
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
            response = await page.goto(
                request_url, wait_until="domcontentloaded", timeout=timeout_ms
            )
            table = await select_data_table(page, timeout_ms)
            rows = table.locator("tbody tr")
            row_count = await rows.count()
            if row_count == 0:
                raise ScrapeError("No table rows found.")

            first_row = await _parse_row(rows.nth(0), 0)
            last_index = row_count - 1
            last_row = await _parse_row(rows.nth(last_index), last_index)

            if first_row["date_iso"] >= last_row["date_iso"]:
                row_order = "descending"
                latest_row = first_row
                indices = range(0, min(recent_rows_limit, row_count))
            else:
                row_order = "ascending"
                latest_row = last_row
                start = max(0, row_count - recent_rows_limit)
                indices = range(start, row_count)

            recent_rows = []
            for idx in indices:
                recent_rows.append(await _parse_row(rows.nth(idx), idx))

            cdn_metadata = {
                "hostname": urlparse(page.url).hostname if page.url else None,
                "server": response.headers.get("server") if response else None,
                "via": response.headers.get("via") if response else None,
                "cf_ray": response.headers.get("cf-ray") if response else None,
            }
            ip_metadata = await _fetch_ip_metadata(context)

            return {
                "target_url": target_url,
                "request_url": request_url,
                "final_url": page.url,
                "status_code": response.status if response else None,
                "fetched_at_utc": datetime.now(tz=timezone.utc).isoformat(),
                "row_count": row_count,
                "row_order": row_order,
                "latest_row": latest_row,
                "recent_rows": _normalize_recent_rows(recent_rows),
                "cdn_metadata": cdn_metadata,
                "ip_metadata": ip_metadata,
            }
        except PlaywrightTimeoutError as exc:
            logging.error("Timeout while scraping monitor snapshot: %s", target_url)
            await save_debug_artifacts(page, debug_dir, f"timeout_{debug_prefix}")
            raise ScrapeError("Timed out while waiting for TSA table.") from exc
        finally:
            await context.close()
            await browser.close()


async def scrape_target(
    target_url: str,
    debug_prefix: str,
    headful: bool = False,
    timeout_ms: int = PAGE_TIMEOUT_MS,
    debug_dir: Path = Path("debug_artifacts"),
) -> list[dict]:
    """Scrape and return all table rows (historical extraction helper)."""
    cache_buster = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    request_url = _append_cache_buster(target_url, cache_buster)

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
            await page.goto(request_url, wait_until="domcontentloaded", timeout=timeout_ms)
            table = await select_data_table(page, timeout_ms)
            rows = table.locator("tbody tr")
            row_count = await rows.count()
            if row_count == 0:
                raise ScrapeError("No table rows found.")

            extracted_rows: list[dict] = []
            for index in range(row_count):
                extracted_rows.append(await _parse_row(rows.nth(index), index))

            return extracted_rows
        except PlaywrightTimeoutError as exc:
            await save_debug_artifacts(page, debug_dir, f"timeout_{debug_prefix}")
            raise ScrapeError("Timed out while waiting for TSA table.") from exc
        finally:
            await context.close()
            await browser.close()
