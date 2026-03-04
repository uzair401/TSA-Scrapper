"""QuestDB REST client for TSA monitor inserts."""

from __future__ import annotations

import logging

import httpx


class QuestDBClient:
    def __init__(
        self,
        base_url: str,
        timeout_seconds: float = 15.0,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        auth = None
        if username:
            auth = (username, password or "")
        self._client = httpx.AsyncClient(timeout=timeout_seconds, auth=auth)

    async def close(self) -> None:
        await self._client.aclose()

    async def _exec(self, sql: str) -> tuple[bool, dict | None]:
        endpoint = f"{self.base_url}/exec"
        try:
            response = await self._client.get(endpoint, params={"query": sql})
        except Exception:
            logging.exception("QuestDB request failed.")
            return False, None

        if response.status_code != 200:
            logging.error(
                "QuestDB query failed status=%s body=%s sql=%s",
                response.status_code,
                response.text[:500],
                sql,
            )
            return False, None

        try:
            payload = response.json()
        except Exception:
            payload = None

        return True, payload

    async def ensure_table(self) -> bool:
        sql = (
            "CREATE TABLE IF NOT EXISTS tsa_checkpoint ("
            "date TIMESTAMP, "
            "travelers LONG"
            ") TIMESTAMP(date) PARTITION BY YEAR;"
        )
        success, _ = await self._exec(sql)
        return success

    async def checkpoint_exists(self, date_iso: str) -> bool:
        sql = (
            "SELECT count() FROM tsa_checkpoint "
            f"WHERE date = '{date_iso}T00:00:00';"
        )
        success, payload = await self._exec(sql)
        if not success:
            return False

        try:
            dataset = (payload or {}).get("dataset", [])
            if not dataset:
                return False
            count_value = int(dataset[0][0])
            return count_value > 0
        except Exception:
            logging.exception("Failed to parse QuestDB count response.")
            return False

    async def insert_checkpoint(self, date_iso: str, travelers: int) -> bool:
        sql = (
            "INSERT INTO tsa_checkpoint VALUES("
            f"'{date_iso}T00:00:00', {int(travelers)}"
            ");"
        )
        success, _ = await self._exec(sql)
        return success
