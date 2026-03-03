"""Discord webhook client for TSA monitor alerts."""

from __future__ import annotations

import logging

import httpx


class DiscordClient:
    def __init__(self, webhook_url: str | None, timeout_seconds: float = 15.0) -> None:
        self.webhook_url = webhook_url or ""
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    async def close(self) -> None:
        await self._client.aclose()

    async def send_message(self, content: str) -> bool:
        if not self.webhook_url:
            logging.warning("DISCORD_WEBHOOK_URL is not set; skipping Discord notification.")
            return False

        try:
            response = await self._client.post(
                self.webhook_url,
                json={"content": content},
            )
            if response.status_code in (200, 204):
                return True
            logging.error(
                "Discord webhook failed with status=%s body=%s",
                response.status_code,
                response.text[:500],
            )
            return False
        except Exception:
            logging.exception("Discord webhook request failed.")
            return False
