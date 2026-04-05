from __future__ import annotations

import asyncio
import hashlib
import json
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from app.cache import TTLCache
from app.config import Settings
from app.models import ScrapedPage, SearchHit
from app.persistent_cache import PersistentCache
from app.resilience import CircuitBreaker

_BLOCKED_EXTENSIONS = {
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".webp",
    ".zip",
    ".csv",
    ".xlsx",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
}


USER_AGENT = (
    "Mozilla/5.0 (compatible; AgenticSearch/1.0; +https://example.com/bot)"
)

_DATE_META_KEYS = (
    "article:published_time",
    "article:modified_time",
    "og:published_time",
    "og:updated_time",
    "pubdate",
    "publish-date",
    "last-modified",
    "date",
)


def _persistent_key(url: str) -> str:
    payload = json.dumps({"url": url.strip().lower()}, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _is_probably_html_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    return not any(path.endswith(ext) for ext in _BLOCKED_EXTENSIONS)


def _normalize_date(value: str | None) -> str | None:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    for parser in (
        lambda raw: datetime.fromisoformat(raw.replace("Z", "+00:00")),
        parsedate_to_datetime,
    ):
        try:
            parsed = parser(cleaned)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()
        except Exception:
            continue
    return cleaned if len(cleaned) <= 40 else None


def _extract_dates(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    published_at: str | None = None
    modified_at: str | None = None

    for key in _DATE_META_KEYS:
        node = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
        if node and node.get("content"):
            normalized = _normalize_date(node["content"])
            if normalized and published_at is None:
                published_at = normalized
            if normalized:
                modified_at = normalized

    if published_at is None:
        time_tag = soup.find("time")
        if time_tag is not None:
            published_at = _normalize_date(time_tag.get("datetime") or time_tag.get_text(" ", strip=True))

    return published_at, modified_at


def _clean_text(html: str) -> tuple[str, str, str | None, str | None]:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(
        ["script", "style", "noscript", "svg", "img", "footer", "nav", "aside", "form"]
    ):
        tag.decompose()

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    body_text = soup.get_text("\n", strip=True)
    body_text = re.sub(r"\n{3,}", "\n\n", body_text)
    body_text = re.sub(r"[ \t]{2,}", " ", body_text)
    published_at, modified_at = _extract_dates(soup)

    return title, body_text.strip(), published_at, modified_at


def _score_chunk(chunk: str, query: str, chunk_index: int) -> tuple[int, float, int]:
    terms = {term for term in re.findall(r"[a-z0-9]+", query.lower()) if len(term) > 2}
    lowered = chunk.lower()
    term_hits = sum(lowered.count(term) for term in terms)
    unique_hits = sum(1 for term in terms if term in lowered)
    return (unique_hits, term_hits + len(chunk) / 2000, -chunk_index)


def _all_chunks(text: str, chunk_size: int, overlap: int) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]

    chunks: list[str] = []
    start = 0
    text_len = len(text)

    while start < text_len:
        end = min(start + chunk_size, text_len)
        cut = text.rfind("\n\n", start, end)
        if cut <= start + int(chunk_size * 0.5):
            cut = end
        chunk = text[start:cut].strip()
        if chunk:
            chunks.append(chunk)
        if cut >= text_len:
            break
        start = max(cut - overlap, start + 1)

    return chunks


def chunk_text(text: str, chunk_size: int, overlap: int, max_chunks: int, query: str = "") -> list[str]:
    chunks = _all_chunks(text, chunk_size=chunk_size, overlap=overlap)
    if len(chunks) <= max_chunks:
        return chunks

    ranked = sorted(
        enumerate(chunks),
        key=lambda item: _score_chunk(item[1], query, item[0]),
        reverse=True,
    )
    chosen_indexes = sorted(index for index, _ in ranked[:max_chunks])
    return [chunks[index] for index in chosen_indexes]


class ScrapeService:
    namespace = "page"

    def __init__(self, settings: Settings, persistent_cache: PersistentCache | None = None):
        self.settings = settings
        self.persistent_cache = persistent_cache
        self.cache = TTLCache[ScrapedPage](
            maxsize=settings.max_cached_pages,
            ttl_seconds=settings.page_cache_ttl_seconds,
        )
        self.breaker = CircuitBreaker(
            failure_threshold=settings.circuit_breaker_failures,
            reset_seconds=settings.circuit_breaker_reset_seconds,
        )
        timeout = httpx.Timeout(settings.request_timeout_seconds)
        limits = httpx.Limits(
            max_connections=settings.max_concurrency,
            max_keepalive_connections=settings.max_concurrency,
        )
        self.client = httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            follow_redirects=True,
        )

    def _cache_key(self, url: str) -> str:
        return url.strip().lower()

    def _get_cached(self, url: str) -> ScrapedPage | None:
        cached = self.cache.get(self._cache_key(url))
        if cached is not None:
            return ScrapedPage.model_validate(cached.model_dump())

        if self.persistent_cache is None:
            return None

        payload = self.persistent_cache.get(self.namespace, _persistent_key(url))
        if payload is None:
            return None

        page = ScrapedPage.model_validate(payload)
        self.cache.set(self._cache_key(url), page)
        return page

    def _store_cached(self, page: ScrapedPage) -> ScrapedPage:
        cloned = ScrapedPage.model_validate(page.model_dump())
        self.cache.set(self._cache_key(page.url), cloned)
        if self.persistent_cache is not None:
            self.persistent_cache.set(
                self.namespace,
                _persistent_key(page.url),
                cloned.model_dump(),
                self.settings.page_cache_ttl_seconds,
            )
        return page

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
            raise RuntimeError("Page fetch failed without an HTTP error")
        raise last_error

    async def close(self) -> None:
        await self.client.aclose()

    async def _render_page_with_js(self, url: str) -> str | None:
        if not self.settings.js_render_fallback_enabled:
            return None
        try:
            from playwright.async_api import async_playwright
        except Exception:
            return None

        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                try:
                    page = await browser.new_page()
                    await page.goto(url, wait_until="networkidle", timeout=int(self.settings.js_render_timeout_seconds * 1000))
                    return await page.content()
                finally:
                    await browser.close()
        except Exception:
            return None

    async def fetch_page(self, hit: SearchHit) -> ScrapedPage:
        cached = self._get_cached(hit.url)
        if cached is not None:
            cached.source_rank = hit.rank
            cached.source_engine = hit.source_engine
            cached.snippet = hit.snippet
            return cached

        if not _is_probably_html_url(hit.url):
            return self._store_cached(
                ScrapedPage(
                    url=hit.url,
                    title=hit.title,
                    text=hit.snippet,
                    source_rank=hit.rank,
                    source_engine=hit.source_engine,
                    snippet=hit.snippet,
                    fetch_error="Skipped non-HTML-like URL",
                    published_at=hit.published_at,
                )
            )

        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            response = await self._get_with_retries(hit.url, headers=headers)
            content_type = response.headers.get("content-type", "").lower()
            if "html" not in content_type and "xml" not in content_type and hit.snippet:
                text = hit.snippet
                title = hit.title
                published_at = hit.published_at
                modified_at = _normalize_date(response.headers.get("last-modified"))
                fetched_via = "http"
            else:
                title, text, published_at, modified_at = _clean_text(response.text)
                fetched_via = "http"
                if len(text.strip()) < self.settings.min_page_text_chars:
                    rendered_html = await self._render_page_with_js(hit.url)
                    if rendered_html:
                        title, text, published_at, modified_at = _clean_text(rendered_html)
                        fetched_via = "playwright"
            text = (text or hit.snippet).strip()
            if len(text) > self.settings.max_page_text_chars:
                text = text[: self.settings.max_page_text_chars].rsplit(" ", 1)[0]
            page = ScrapedPage(
                url=hit.url,
                title=title or hit.title,
                text=text,
                source_rank=hit.rank,
                source_engine=hit.source_engine,
                snippet=hit.snippet,
                published_at=published_at or hit.published_at,
                modified_at=modified_at,
                fetched_via=fetched_via,
            )
        except Exception as exc:
            page = ScrapedPage(
                url=hit.url,
                title=hit.title,
                text=hit.snippet,
                source_rank=hit.rank,
                source_engine=hit.source_engine,
                snippet=hit.snippet,
                fetch_error=str(exc),
                published_at=hit.published_at,
            )

        return self._store_cached(page)

    async def scrape_hits(self, hits: list[SearchHit], limit: int | None = None) -> list[ScrapedPage]:
        semaphore = asyncio.Semaphore(self.settings.max_concurrency)

        async def _bounded_fetch(hit: SearchHit) -> ScrapedPage:
            async with semaphore:
                return await self.fetch_page(hit)

        tasks = [_bounded_fetch(hit) for hit in hits[: (limit or self.settings.max_pages_to_scrape)]]
        return await asyncio.gather(*tasks)
