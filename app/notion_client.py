from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Tuple

import httpx


class NotionClient:
    BASE_URL = "https://api.notion.com"

    def __init__(self, token: str, notion_version: str, timeout: float = 30.0) -> None:
        self.token = token
        self.notion_version = notion_version
        self.timeout = timeout
        self._client = httpx.AsyncClient(
            base_url=self.BASE_URL,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Notion-Version": self.notion_version,
                "Content-Type": "application/json",
            },
            timeout=self.timeout,
        )
        self._query_target_cache: dict[str, Tuple[str, str]] = {}
        self._source_schema_cache: dict[str, Dict[str, Any]] = {}

    async def close(self) -> None:
        await self._client.aclose()

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Handle rate limits (429) respecting Retry-After."""
        while True:
            response = await self._client.request(method, url, **kwargs)
            if response.status_code != 429:
                response.raise_for_status()
                return response

            retry_after = response.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else 1.0
            await asyncio.sleep(delay)

    async def get_query_target(self, database_id: str) -> Tuple[str, str]:
        """Resolve the correct query endpoint for a Notion database or data source."""
        if database_id in self._query_target_cache:
            return self._query_target_cache[database_id]

        try:
            resp = await self._request("GET", f"/v1/data_sources/{database_id}")
        except httpx.HTTPStatusError as exc:
            if exc.response is None or exc.response.status_code not in (400, 404):
                raise
        else:
            target = ("data_source", resp.json()["id"])
            self._query_target_cache[database_id] = target
            return target

        resp = await self._request("GET", f"/v1/databases/{database_id}")
        data = resp.json()
        data_sources = data.get("data_sources") or []
        if len(data_sources) > 1:
            raise ValueError(
                f"Database {database_id} exposes multiple Notion data_sources; "
                "configure a specific data_source id instead of relying on implicit selection."
            )
        if data_sources:
            target = ("data_source", data_sources[0]["id"])
        else:
            target = ("database", database_id)
        self._query_target_cache[database_id] = target
        return target

    async def get_source_schema(self, database_id: str) -> Dict[str, Any]:
        """Fetch property metadata for the underlying database or data source."""
        target_type, target_id = await self.get_query_target(database_id)
        cache_key = f"{target_type}:{target_id}"
        if cache_key in self._source_schema_cache:
            return self._source_schema_cache[cache_key]

        if target_type == "data_source":
            resp = await self._request("GET", f"/v1/data_sources/{target_id}")
        elif target_type == "database":
            resp = await self._request("GET", f"/v1/databases/{target_id}")
        else:
            raise ValueError(f"Unknown Notion target type: {target_type}")

        data = resp.json()
        schema = {
            "database_id": database_id,
            "target_type": target_type,
            "target_id": target_id,
            "properties": data.get("properties") or {},
        }
        self._source_schema_cache[cache_key] = schema
        return schema

    async def query_all_pages(self, target_type: str, target_id: str) -> List[Dict[str, Any]]:
        """Query pages using either data_sources or databases with pagination."""
        items: list[dict[str, Any]] = []
        payload: dict[str, Any] = {"page_size": 100}
        if target_type == "data_source":
            # Notion espera el ID de data_source tal cual lo devuelve /v1/databases/{id}
            endpoint = f"/v1/data_sources/{target_id}/query"
        elif target_type == "database":
            endpoint = f"/v1/databases/{target_id}/query"
        else:
            raise ValueError(f"Unknown Notion target type: {target_type}")

        while True:
            resp = await self._request("POST", endpoint, json=payload)
            data = resp.json()
            results = data.get("results", [])
            items.extend(results)
            has_more = data.get("has_more")
            next_cursor = data.get("next_cursor")
            if has_more and next_cursor:
                payload["start_cursor"] = next_cursor
            else:
                break

        return items
