from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Protocol
from urllib.parse import urlsplit, urlunsplit

import httpx

from app.cache import TTLCache
from app.config import Settings
from app.models import SearchHit
from app.persistent_cache import PersistentCache
from app.resilience import CircuitBreaker


def _clone_hits(hits: list[SearchHit]) -> list[SearchHit]:
    return [SearchHit.model_validate(hit.model_dump()) for hit in hits]


def _canonical_url(url: str) -> str:
    try:
        parts = urlsplit(url.strip())
    except Exception:
        return url.strip()
    normalized_path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), normalized_path, parts.query, ""))


def _domain(url: str) -> str:
    try:
        return urlsplit(url).netloc.lower()
    except Exception:
        return ""


def _persistent_key(query: str, limit: int) -> str:
    payload = json.dumps({"query": query.strip().lower(), "limit": limit}, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _dedupe_hits(hits: list[SearchHit], limit: int, per_domain_cap: int) -> list[SearchHit]:
    seen_urls: set[str] = set()
    domain_counts: dict[str, int] = {}
    deduped: list[SearchHit] = []

    for hit in hits:
        canonical_url = _canonical_url(hit.url)
        if not canonical_url or canonical_url in seen_urls:
            continue

        domain = _domain(hit.url)
        if domain and domain_counts.get(domain, 0) >= per_domain_cap:
            continue

        seen_urls.add(canonical_url)
        if domain:
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
        deduped.append(
            SearchHit(
                title=hit.title,
                url=hit.url,
                snippet=hit.snippet,
                rank=len(deduped) + 1,
                source_engine=hit.source_engine,
                published_at=hit.published_at,
            )
        )
        if len(deduped) >= limit:
            break

    return deduped


class SearchProvider(Protocol):
    async def search(self, query: str, limit: int) -> list[SearchHit]: ...

    async def close(self) -> None: ...


class BaseSearchProvider:
    namespace = "search"

    def __init__(self, settings: Settings, persistent_cache: PersistentCache | None = None):
        self.settings = settings
        self.persistent_cache = persistent_cache
        self.cache = TTLCache[list[SearchHit]](
            maxsize=settings.max_cached_searches,
            ttl_seconds=settings.search_cache_ttl_seconds,
        )
        self.breaker = CircuitBreaker(
            failure_threshold=settings.circuit_breaker_failures,
            reset_seconds=settings.circuit_breaker_reset_seconds,
        )
        self.client = httpx.AsyncClient(
            timeout=settings.request_timeout_seconds,
            follow_redirects=True,
        )

    def _cache_key(self, query: str, limit: int) -> tuple[str, int]:
        return query.strip().lower(), limit

    def _get_cached(self, query: str, limit: int) -> list[SearchHit] | None:
        cached = self.cache.get(self._cache_key(query, limit))
        if cached is not None:
            return _clone_hits(cached)

        if self.persistent_cache is None:
            return None

        payload = self.persistent_cache.get(
            self.namespace,
            _persistent_key(query, limit),
        )
        if payload is None:
            return None

        hits = [SearchHit.model_validate(item) for item in payload]
        self.cache.set(self._cache_key(query, limit), _clone_hits(hits))
        return hits

    def _store_cached(self, query: str, limit: int, hits: list[SearchHit]) -> list[SearchHit]:
        cloned = _clone_hits(hits)
        self.cache.set(self._cache_key(query, limit), cloned)
        if self.persistent_cache is not None:
            self.persistent_cache.set(
                self.namespace,
                _persistent_key(query, limit),
                [hit.model_dump() for hit in cloned],
                self.settings.search_cache_ttl_seconds,
            )
        return hits

    async def _get_with_retries(self, url: str, **kwargs: object) -> httpx.Response:
        last_error: Exception | None = None
        if not self.breaker.allow(self.namespace):
            raise RuntimeError(f"Circuit breaker open for {self.namespace}")
        for attempt in range(self.settings.http_retries + 1):
            try:
                response = await self.client.get(url, **kwargs)
                response.raise_for_status()
                self.breaker.record_success(self.namespace)
                return response
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in {429, 500, 502, 503, 504}:
                    raise
                last_error = exc
            except httpx.HTTPError as exc:
                last_error = exc

            if attempt < self.settings.http_retries:
                await asyncio.sleep(0.35 * (attempt + 1))

        self.breaker.record_failure(self.namespace)
        if last_error is None:
            raise RuntimeError("Search request failed without an HTTP error")
        raise last_error

    async def close(self) -> None:
        await self.client.aclose()


class BraveSearchProvider(BaseSearchProvider):
    namespace = "search:brave"

    def __init__(self, settings: Settings, persistent_cache: PersistentCache | None = None):
        if not settings.brave_api_key:
            raise ValueError("BRAVE_API_KEY is required when SEARCH_PROVIDER=brave")
        super().__init__(settings, persistent_cache=persistent_cache)

    async def search(self, query: str, limit: int) -> list[SearchHit]:
        cached = self._get_cached(query, limit)
        if cached is not None:
            return cached

        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": self.settings.brave_api_key,
        }
        params = {
            "q": query,
            "count": min(limit * 2, 20),
            "extra_snippets": "true",
            "safesearch": "moderate",
        }
        response = await self._get_with_retries(
            "https://api.search.brave.com/res/v1/web/search",
            headers=headers,
            params=params,
        )
        data = response.json()

        hits: list[SearchHit] = []
        for item in data.get("web", {}).get("results", []):
            snippet_parts = [item.get("description", "")]
            snippet_parts.extend(item.get("extra_snippets", [])[:2])
            snippet = "\n".join(part.strip() for part in snippet_parts if part and part.strip())
            hits.append(
                SearchHit(
                    title=item.get("title", ""),
                    url=item.get("url", ""),
                    snippet=snippet,
                    rank=0,
                    source_engine="brave",
                    published_at=item.get("page_age"),
                )
            )

        deduped_hits = _dedupe_hits(hits, limit, self.settings.max_results_per_domain)
        return self._store_cached(query, limit, deduped_hits)


class SerpAPISearchProvider(BaseSearchProvider):
    namespace = "search:serpapi"

    def __init__(self, settings: Settings, persistent_cache: PersistentCache | None = None):
        if not settings.serpapi_api_key:
            raise ValueError("SERPAPI_API_KEY is required when SEARCH_PROVIDER=serpapi")
        super().__init__(settings, persistent_cache=persistent_cache)

    async def search(self, query: str, limit: int) -> list[SearchHit]:
        cached = self._get_cached(query, limit)
        if cached is not None:
            return cached

        params = {
            "engine": "google",
            "q": query,
            "num": min(limit * 2, 10),
            "api_key": self.settings.serpapi_api_key,
        }
        response = await self._get_with_retries("https://serpapi.com/search.json", params=params)
        data = response.json()

        hits: list[SearchHit] = []
        for item in data.get("organic_results", []):
            hits.append(
                SearchHit(
                    title=item.get("title", ""),
                    url=item.get("link", ""),
                    snippet=item.get("snippet", "") or "",
                    rank=0,
                    source_engine="serpapi",
                    published_at=item.get("date"),
                )
            )

        deduped_hits = _dedupe_hits(hits, limit, self.settings.max_results_per_domain)
        return self._store_cached(query, limit, deduped_hits)


def build_search_provider(
    settings: Settings,
    persistent_cache: PersistentCache | None = None,
) -> SearchProvider:
    provider = settings.search_provider.lower().strip()
    if provider == "brave":
        return BraveSearchProvider(settings, persistent_cache=persistent_cache)
    if provider == "serpapi":
        return SerpAPISearchProvider(settings, persistent_cache=persistent_cache)
    raise ValueError(f"Unsupported SEARCH_PROVIDER: {settings.search_provider}")
