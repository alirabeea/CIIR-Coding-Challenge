from __future__ import annotations

import asyncio
import hashlib
import json
from urllib.parse import urlparse

import httpx

from app.cache import TTLCache
from app.config import Settings
from app.models import OutputCell, OutputEntity, SourceRef
from app.persistent_cache import PersistentCache
from app.resilience import CircuitBreaker


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def _cache_key(repo: str) -> str:
    payload = json.dumps({"repo": repo.strip().lower()}, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _extract_repo_slug(row: OutputEntity) -> str:
    github_cell = row.attributes.get("github_repo")
    if github_cell and github_cell.value:
        return github_cell.value.strip().strip("/")
    homepage = row.homepage.value if row.homepage else ""
    domain = _domain(homepage)
    if "github.com" not in domain:
        return ""
    parts = [part for part in urlparse(homepage).path.split("/") if part]
    if len(parts) >= 2:
        return f"{parts[0]}/{parts[1]}"
    return ""


class GitHubEnricher:
    namespace = "enrich:github"

    def __init__(self, settings: Settings, persistent_cache: PersistentCache | None = None):
        self.settings = settings
        self.persistent_cache = persistent_cache
        self.breaker = CircuitBreaker(
            failure_threshold=settings.circuit_breaker_failures,
            reset_seconds=settings.circuit_breaker_reset_seconds,
        )
        self.cache = TTLCache[dict](
            maxsize=64,
            ttl_seconds=settings.search_cache_ttl_seconds,
        )
        self.client = httpx.AsyncClient(
            timeout=settings.github_request_timeout_seconds,
            follow_redirects=True,
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": "AgenticSearch/1.0",
            },
        )

    async def close(self) -> None:
        await self.client.aclose()

    def _get_cached(self, repo: str) -> dict | None:
        key = _cache_key(repo)
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        if self.persistent_cache is None:
            return None
        payload = self.persistent_cache.get(self.namespace, key)
        if payload is None:
            return None
        self.cache.set(key, payload)
        return payload

    def _store_cached(self, repo: str, payload: dict) -> dict:
        key = _cache_key(repo)
        self.cache.set(key, payload)
        if self.persistent_cache is not None:
            self.persistent_cache.set(
                self.namespace,
                key,
                payload,
                self.settings.search_cache_ttl_seconds,
            )
        return payload

    async def _fetch_repo(self, repo: str) -> dict | None:
        if not repo:
            return None
        cached = self._get_cached(repo)
        if cached is not None:
            return cached
        if not self.breaker.allow(self.namespace):
            return None
        try:
            response = await self.client.get(f"https://api.github.com/repos/{repo}")
            response.raise_for_status()
            payload = response.json()
            self.breaker.record_success(self.namespace)
            return self._store_cached(repo, payload)
        except Exception:
            self.breaker.record_failure(self.namespace)
            return None

    async def enrich(self, rows: list[OutputEntity]) -> int:
        repos = {row.entity_id: _extract_repo_slug(row) for row in rows}
        tasks = {
            entity_id: asyncio.create_task(self._fetch_repo(repo))
            for entity_id, repo in repos.items()
            if repo
        }
        if not tasks:
            return 0

        enriched = 0
        task_results = {entity_id: await task for entity_id, task in tasks.items()}
        for row in rows:
            payload = task_results.get(row.entity_id)
            if not payload:
                continue
            repo_html_url = payload.get("html_url") or f"https://github.com/{repos[row.entity_id]}"
            source = SourceRef(
                source_url=repo_html_url,
                source_title=payload.get("full_name", "GitHub repository"),
                quote=f"GitHub stars: {payload.get('stargazers_count', 0)}",
            )
            for field_name, value in {
                "github_stars": str(payload.get("stargazers_count", "")),
                "github_forks": str(payload.get("forks_count", "")),
                "github_license": (payload.get("license") or {}).get("spdx_id", ""),
                "github_last_pushed_at": payload.get("pushed_at", ""),
            }.items():
                if value and field_name not in row.attributes:
                    row.attributes[field_name] = OutputCell(value=value, sources=[source])
            language = payload.get("language", "")
            if language and "primary_language" not in row.attributes:
                row.attributes["primary_language"] = OutputCell(value=language, sources=[source])
            if repo_html_url and row.homepage is None:
                row.homepage = OutputCell(value=repo_html_url, sources=[source])
            if "GitHub repository enriched" not in row.highlights:
                row.highlights.append("GitHub repository enriched")
            row.aggregate_score = round(min(1.0, row.aggregate_score + 0.03), 3)
            enriched += 1
        return enriched
