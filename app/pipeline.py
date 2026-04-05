from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Awaitable, Callable
from urllib.parse import urlparse

from app.cache import TTLCache
from app.config import Settings
from app.enrich import GitHubEnricher
from app.extract import LLMExtractor
from app.models import (
    ChunkExtraction,
    OutputCell,
    OutputEntity,
    ScrapedPage,
    SearchHit,
    SearchMetrics,
    SearchResponse,
    SourceRef,
)
from app.persistent_cache import PersistentCache
from app.reports import ReportStore
from app.resilience import CircuitBreaker
from app.scrape import ScrapeService, chunk_text
from app.search import build_search_provider

_COMMON_TITLE_SPLITTER = re.compile(r"\s+(?:\||-|:|/|·|–|—)\s+")
_GITHUB_REPO_RE = re.compile(r"github\.com/([^/\s]+/[^/\s?#]+)")
_LOCATION_RE = re.compile(r"\b(?:based in|located in|headquartered in|in)\s+([A-Z][A-Za-z]+(?:[\s,]+[A-Z][A-Za-z]+){0,2})")
_RATING_RE = re.compile(r"\b([1-5](?:\.[0-9])?)\s*(?:/ ?5|stars?)\b", re.IGNORECASE)
_LISTING_HINTS = {
    "top",
    "best",
    "guide",
    "comparison",
    "alternatives",
    "review",
    "reviews",
    "ranked",
    "list",
    "awesome",
}
_QUERY_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "from",
    "this",
    "your",
    "open",
    "source",
    "top",
    "best",
    "companies",
    "company",
    "tools",
    "tool",
}
_LOCAL_QUERY_TERMS = {"restaurant", "restaurants", "pizza", "cafe", "coffee", "burger", "sushi"}
_COMPANY_QUERY_TERMS = {"startup", "startups", "company", "companies", "vendor", "vendors", "yc"}
_OSS_QUERY_TERMS = {
    "open",
    "source",
    "oss",
    "github",
    "database",
    "developer",
    "framework",
    "sdk",
    "tool",
    "tools",
    "library",
    "libraries",
    "cli",
}
_CUISINES = {
    "pizza",
    "italian",
    "sushi",
    "japanese",
    "thai",
    "burger",
    "mexican",
    "indian",
    "mediterranean",
    "chinese",
    "korean",
    "vietnamese",
}
_LANGUAGES = {"python", "typescript", "javascript", "go", "rust", "java", "ruby", "php", "c++", "c#"}
_LICENSE_PATTERNS = {
    "mit": "MIT",
    "apache": "Apache",
    "gpl": "GPL",
    "agpl": "AGPL",
    "bsd": "BSD",
    "mpl": "MPL",
}
_DEPLOYMENT_HINTS = {
    "self-hosted": "self-hosted",
    "self hosted": "self-hosted",
    "cloud": "cloud",
    "managed": "managed",
    "hosted": "hosted",
}
_COMPANY_FOCUS_HINTS = {
    "healthcare": "healthcare",
    "fintech": "fintech",
    "developer": "developer tools",
    "observability": "observability",
    "security": "security",
    "analytics": "analytics",
    "database": "data infrastructure",
}
_BREADTH_HINTS = {"best", "top", "companies", "tools", "alternatives", "startups", "projects", "platforms"}
ProgressCallback = Callable[[str, float, str], Awaitable[None] | None]
logger = logging.getLogger("agentic_search.pipeline")


@dataclass(frozen=True)
class QueryProfile:
    query_type: str
    comparison_fields: list[str]
    query_terms: set[str]
    expects_local: bool = False
    expects_company: bool = False
    expects_open_source: bool = False


@dataclass(frozen=True)
class RunPlan:
    run_mode: str
    search_limit: int
    scrape_limit: int
    desired_rows: int
    max_final_rows: int
    top_page_chunks: int
    mid_page_chunks: int
    verification_rows: int
    verification_enabled: bool
    github_enrichment_enabled: bool


def _normalize_entity_key(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", name.lower()).strip()
    return re.sub(r"\s+", " ", normalized)


def _normalize_words(value: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", value.lower())


def _query_terms(query: str) -> set[str]:
    return {
        term
        for term in _normalize_words(query)
        if len(term) > 2 and term not in _QUERY_STOPWORDS
    }


def _source_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def _domain_label(url: str) -> str:
    domain = _source_domain(url)
    if domain.startswith("www."):
        domain = domain[4:]
    parts = [part for part in domain.split(".") if part and part not in {"com", "org", "net", "io", "co", "app"}]
    return parts[0] if parts else domain


def _path_depth(url: str) -> int:
    try:
        path = urlparse(url).path.strip("/")
    except Exception:
        return 10
    if not path:
        return 0
    return len([part for part in path.split("/") if part])


def _dedupe_sources(sources: list[SourceRef]) -> list[SourceRef]:
    seen: set[tuple[str, str, str]] = set()
    out: list[SourceRef] = []
    for source in sources:
        key = (source.source_url, source.source_title, source.quote)
        if key not in seen:
            out.append(source)
            seen.add(key)
    return out


def _clone_response(response: SearchResponse) -> SearchResponse:
    return SearchResponse.model_validate(response.model_dump())


def _persistent_key(query: str, debug: bool, run_mode: str) -> str:
    payload = json.dumps(
        {"query": query.strip().lower(), "debug": debug, "run_mode": run_mode},
        sort_keys=True,
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _first_sentence(text: str, max_length: int = 220) -> str:
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if not cleaned:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", cleaned, maxsplit=1)
    sentence = parts[0].strip()
    if len(sentence) > max_length:
        return sentence[: max_length - 1].rstrip() + "…"
    return sentence


def _listing_like(value: str) -> bool:
    lowered = value.lower()
    return any(hint in lowered for hint in _LISTING_HINTS)


def _best_name_from_title(title: str, url: str, query: str) -> str:
    if not title.strip():
        return ""
    domain_hint = _domain_label(url).replace("-", " ")
    query_words = _query_terms(query)
    segments = [segment.strip(" -|:") for segment in _COMMON_TITLE_SPLITTER.split(title) if segment.strip(" -|:")]
    if not segments:
        segments = [title.strip()]

    def score(segment: str) -> tuple[int, int, int, int]:
        lowered = segment.lower()
        segment_words = set(_normalize_words(segment))
        domain_overlap = sum(1 for word in segment_words if word and word in domain_hint)
        query_overlap = len(segment_words & query_words)
        listing_penalty = -3 if _listing_like(segment) else 0
        generic_penalty = -2 if len(segment_words) <= 1 and segment_words <= query_words else 0
        return (
            domain_overlap,
            query_overlap,
            listing_penalty + generic_penalty,
            -len(segment),
        )

    best = max(segments, key=score)
    return best.strip()


def _is_probably_official_page(page: ScrapedPage, candidate_name: str) -> bool:
    if not candidate_name or _listing_like(page.title) or page.fetch_error:
        return False
    domain_hint = _domain_label(page.url).replace("-", " ")
    name_words = set(_normalize_words(candidate_name))
    if _path_depth(page.url) <= 1 and any(word in domain_hint for word in name_words if len(word) > 2):
        return True
    return False


def _infer_entity_type(query: str, text: str) -> str:
    query_words = set(_normalize_words(query))
    summary_words = set(_normalize_words(text))
    if query_words & _LOCAL_QUERY_TERMS or summary_words & _LOCAL_QUERY_TERMS:
        return "local business"
    if query_words & _COMPANY_QUERY_TERMS:
        return "company"
    if "database" in query_words or "sql" in query_words:
        return "data tool"
    if "developer" in query_words or "open" in query_words:
        return "software tool"
    return "entity"


def _fallback_relevance(query: str, text: str, source_rank: int, official: bool) -> float:
    terms = _query_terms(query)
    text_words = set(_normalize_words(text))
    overlap = len(terms & text_words)
    score = 0.28 + min(0.28, overlap * 0.06) + max(0.0, 0.18 - (source_rank - 1) * 0.02)
    if official:
        score += 0.18
    return round(min(0.92, score), 3)


def _build_fallback_source(page: ScrapedPage, quote: str) -> SourceRef:
    return SourceRef(
        source_url=page.url,
        source_title=page.title,
        quote=quote[:180],
    )


def _classify_query(query: str) -> QueryProfile:
    terms = _query_terms(query)
    lowered = query.lower()
    expects_local = bool(terms & _LOCAL_QUERY_TERMS)
    expects_company = bool(terms & _COMPANY_QUERY_TERMS)
    expects_open_source = "open source" in lowered or bool(terms & _OSS_QUERY_TERMS)

    if expects_local:
        return QueryProfile(
            query_type="local",
            comparison_fields=["category", "cuisine", "location", "rating", "price_range", "source_domain", "source_kind"],
            query_terms=terms,
            expects_local=True,
        )
    if expects_company:
        return QueryProfile(
            query_type="company",
            comparison_fields=["focus_area", "hq_location", "funding_stage", "source_domain", "source_kind"],
            query_terms=terms,
            expects_company=True,
        )
    if expects_open_source:
        return QueryProfile(
            query_type="open_source",
            comparison_fields=["license_type", "primary_language", "github_repo", "deployment_model", "source_domain", "source_kind"],
            query_terms=terms,
            expects_open_source=True,
        )
    return QueryProfile(
        query_type="general",
        comparison_fields=["category", "source_domain", "source_kind"],
        query_terms=terms,
    )


async def _emit_progress(
    callback: ProgressCallback | None,
    stage: str,
    progress: float,
    message: str,
) -> None:
    if callback is None:
        return
    result = callback(stage, progress, message)
    if inspect.isawaitable(result):
        await result


def _maybe_attribute(value: str | None, source: SourceRef) -> OutputCell | None:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    return OutputCell(value=cleaned, sources=[source])


def _extract_location(text: str) -> str:
    match = _LOCATION_RE.search(text)
    if match:
        return match.group(1).strip(" .,")
    return ""


def _extract_license(text: str) -> str:
    lowered = text.lower()
    for needle, label in _LICENSE_PATTERNS.items():
        if needle in lowered:
            return label
    if "open source" in lowered:
        return "Open source"
    return ""


def _extract_deployment_model(text: str) -> str:
    lowered = text.lower()
    matches = [label for needle, label in _DEPLOYMENT_HINTS.items() if needle in lowered]
    if not matches:
        return ""
    unique = list(dict.fromkeys(matches))
    return ", ".join(unique[:2])


def _extract_primary_language(text: str) -> str:
    lowered = text.lower()
    found = [language.title() for language in _LANGUAGES if language in lowered]
    return ", ".join(found[:2])


def _extract_focus_area(text: str) -> str:
    lowered = text.lower()
    for needle, label in _COMPANY_FOCUS_HINTS.items():
        if needle in lowered:
            return label
    return ""


def _extract_cuisine(text: str) -> str:
    lowered = text.lower()
    for cuisine in _CUISINES:
        if cuisine in lowered:
            return cuisine.title()
    return ""


def _extract_rating(text: str) -> str:
    match = _RATING_RE.search(text)
    if match:
        return f"{match.group(1)}/5"
    return ""


def _extract_github_repo(url: str, text: str) -> str:
    match = _GITHUB_REPO_RE.search(f"{url} {text}")
    if match:
        return match.group(1).strip("/")
    return ""


def _parse_timestamp(value: str | None) -> datetime | None:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    try:
        parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _freshness_days(*timestamps: str | None) -> int | None:
    candidates = [_parse_timestamp(value) for value in timestamps if value]
    valid = [candidate for candidate in candidates if candidate is not None]
    if not valid:
        return None
    freshest = max(valid)
    delta = datetime.now(timezone.utc) - freshest
    return max(0, delta.days)


def _token_similarity(left: str, right: str) -> float:
    left_tokens = set(_normalize_words(left))
    right_tokens = set(_normalize_words(right))
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    return intersection / max(union, 1)


def _breadth_score(query: str, profile: QueryProfile) -> int:
    terms = _query_terms(query)
    score = len(terms)
    score += sum(1 for term in terms if term in _BREADTH_HINTS)
    if profile.expects_company or profile.expects_open_source:
        score += 2
    if profile.expects_local:
        score += 1
    return score


def _build_run_plan(settings: Settings, query: str, profile: QueryProfile, requested_mode: str | None) -> RunPlan:
    run_mode = (requested_mode or settings.default_run_mode or "balanced").strip().lower()
    if run_mode not in {"fast", "balanced", "deep"}:
        run_mode = "balanced"

    breadth = _breadth_score(query, profile)
    mode_defaults = {
        "fast": {"search": 8, "pages": 6, "top_chunks": 2, "mid_chunks": 1, "max_rows": settings.fast_max_rows, "verify": 0},
        "balanced": {"search": 12, "pages": 9, "top_chunks": 3, "mid_chunks": 2, "max_rows": settings.balanced_max_rows, "verify": settings.verification_max_rows},
        "deep": {"search": 16, "pages": 12, "top_chunks": 4, "mid_chunks": 3, "max_rows": settings.deep_max_rows, "verify": max(settings.verification_max_rows + 2, 6)},
    }[run_mode]
    desired_rows = min(mode_defaults["max_rows"], max(10, 8 + breadth * 2))
    return RunPlan(
        run_mode=run_mode,
        search_limit=mode_defaults["search"] + max(0, breadth // 4),
        scrape_limit=mode_defaults["pages"] + max(0, breadth // 5),
        desired_rows=desired_rows,
        max_final_rows=mode_defaults["max_rows"],
        top_page_chunks=mode_defaults["top_chunks"],
        mid_page_chunks=mode_defaults["mid_chunks"],
        verification_rows=mode_defaults["verify"],
        verification_enabled=settings.verification_enabled and mode_defaults["verify"] > 0,
        github_enrichment_enabled=settings.github_enrichment_enabled and profile.expects_open_source,
    )


def _rank_explanation(row: OutputEntity) -> str:
    signals = row.ranking_signals or {}
    parts: list[str] = []
    if row.supporting_source_count:
        parts.append(f"supported by {row.supporting_source_count} domain{'s' if row.supporting_source_count != 1 else ''}")
    if signals.get("attribute_coverage", 0) >= 3:
        parts.append(f"{int(signals['attribute_coverage'])} useful attributes extracted")
    if row.verification_status == "verified":
        parts.append("passed second-pass verification")
    elif row.verification_status == "plausible":
        parts.append("verification found the row plausible")
    if row.freshness_days is not None:
        if row.freshness_days <= 30:
            parts.append("sources are recent")
        elif row.freshness_days <= 365:
            parts.append("sources are moderately recent")
    if not parts and row.highlights:
        parts.append(row.highlights[0].lower())
    return ". ".join(parts[:3]).capitalize() + ("." if parts else "")


def _official_candidate_score(hit: SearchHit, query: str) -> float:
    candidate_name = _best_name_from_title(hit.title, hit.url, query)
    if not candidate_name:
        return 0.0
    domain_hint = _domain_label(hit.url).replace("-", " ")
    name_words = [word for word in _normalize_words(candidate_name) if len(word) > 2]
    if name_words and any(word in domain_hint for word in name_words):
        return 0.16
    return 0.0


def _rerank_search_hits(profile: QueryProfile, query: str, hits: list[SearchHit]) -> list[SearchHit]:
    rescored: list[SearchHit] = []
    for hit in hits:
        title_words = set(_normalize_words(hit.title))
        snippet_words = set(_normalize_words(hit.snippet))
        overlap = len(profile.query_terms & title_words) * 0.11 + len(profile.query_terms & snippet_words) * 0.05
        score = 0.26 + max(0.0, 0.12 - (hit.rank - 1) * 0.015) + overlap
        score += _official_candidate_score(hit, query)
        if _listing_like(hit.title):
            score += 0.08 if profile.expects_local or profile.expects_company else -0.02
        if profile.expects_open_source and "github.com" in _source_domain(hit.url):
            score += 0.12
        if profile.expects_company and any(token in hit.snippet.lower() for token in ("funding", "founder", "series", "startup")):
            score += 0.06
        rescored.append(
            SearchHit(
                title=hit.title,
                url=hit.url,
                snippet=hit.snippet,
                rank=hit.rank,
                source_engine=hit.source_engine,
                published_at=hit.published_at,
                rerank_score=round(min(1.0, score), 3),
            )
        )

    rescored.sort(key=lambda item: (item.rerank_score, -item.rank), reverse=True)
    for index, hit in enumerate(rescored, start=1):
        hit.rank = index
    return rescored


class AgenticSearchPipeline:
    namespace = "query"

    def __init__(self, settings: Settings):
        self.settings = settings
        self.persistent_cache = (
            PersistentCache(settings.persistent_cache_path)
            if settings.persistent_cache_enabled
            else None
        )
        self.report_store = (
            ReportStore(settings.report_directory, max_reports=settings.max_saved_reports)
            if settings.save_reports
            else None
        )
        self.search_provider = build_search_provider(settings, persistent_cache=self.persistent_cache)
        self.scraper = ScrapeService(settings, persistent_cache=self.persistent_cache)
        self.extractor = LLMExtractor(settings, persistent_cache=self.persistent_cache)
        self.github_enricher = GitHubEnricher(settings, persistent_cache=self.persistent_cache)
        self.response_cache = TTLCache[SearchResponse](
            maxsize=settings.max_cached_queries,
            ttl_seconds=settings.query_cache_ttl_seconds,
        )

    async def close(self) -> None:
        await asyncio.gather(
            self.search_provider.close(),
            self.scraper.close(),
            self.extractor.close(),
            self.github_enricher.close(),
        )
        if self.persistent_cache is not None:
            self.persistent_cache.close()

    def list_reports(self, limit: int = 12):
        if self.report_store is None:
            return []
        return self.report_store.list_reports(limit=limit)

    def _cache_key(self, query: str, debug: bool, run_mode: str) -> tuple[str, bool, str]:
        return query.strip().lower(), debug, run_mode

    def _get_cached_response(
        self,
        query: str,
        debug: bool,
        run_mode: str,
        *,
        allow_stale: bool = False,
    ) -> tuple[SearchResponse | None, str]:
        memory_key = self._cache_key(query, debug, run_mode)
        cached = self.response_cache.get(memory_key)
        if cached is not None:
            return _clone_response(cached), "memory"

        if self.persistent_cache is None:
            return None, "live"

        payload, state = self.persistent_cache.get_with_state(
            self.namespace,
            _persistent_key(query, debug, run_mode),
            allow_stale=allow_stale and self.settings.stale_cache_enabled,
            stale_ttl_seconds=self.settings.stale_cache_ttl_seconds,
        )
        if payload is None:
            return None, "live"

        response = SearchResponse.model_validate(payload)
        self.response_cache.set(memory_key, _clone_response(response))
        return response, ("stale" if state == "stale" else "disk")

    def _store_cached_response(self, query: str, debug: bool, response: SearchResponse) -> None:
        cloned = _clone_response(response)
        self.response_cache.set(self._cache_key(query, debug, response.run_mode), cloned)
        if self.persistent_cache is not None:
            self.persistent_cache.set(
                self.namespace,
                _persistent_key(query, debug, response.run_mode),
                cloned.model_dump(),
                self.settings.query_cache_ttl_seconds,
            )

    def peek_cached_response(self, query: str, debug: bool = False, run_mode: str | None = None) -> SearchResponse | None:
        mode = (run_mode or self.settings.default_run_mode or "balanced").strip().lower()
        cached, _ = self._get_cached_response(query, debug, mode, allow_stale=True)
        return cached

    def _build_columns(self, rows: list[OutputEntity], profile: QueryProfile) -> list[str]:
        columns: list[str] = ["name", "entity_type", "summary", "homepage"]
        seen_attr_columns: set[str] = set()
        for row in rows:
            seen_attr_columns.update(row.attributes.keys())
        prioritized = [column for column in profile.comparison_fields if column in seen_attr_columns]
        remaining = sorted(column for column in seen_attr_columns if column not in prioritized)
        columns.extend(prioritized)
        columns.extend(remaining)
        return columns

    def _filtered_pages(self, pages: list[ScrapedPage]) -> list[ScrapedPage]:
        filtered: list[ScrapedPage] = []
        seen_signatures: set[str] = set()

        for page in sorted(pages, key=lambda item: item.source_rank):
            text = page.text.strip()
            if len(text) < self.settings.min_page_text_chars and not page.snippet.strip():
                continue

            signature_source = text or page.snippet.strip()
            signature_seed = re.sub(r"\s+", " ", signature_source[:2000].lower())
            signature = hashlib.sha1(signature_seed.encode("utf-8")).hexdigest()
            if signature in seen_signatures:
                continue

            seen_signatures.add(signature)
            filtered.append(page)

        return filtered

    def _page_chunk_budget(self, source_rank: int, plan: RunPlan) -> int:
        if source_rank <= 2:
            return plan.top_page_chunks
        if source_rank <= 5:
            return plan.mid_page_chunks
        return 0

    def _build_fallback_attributes(
        self,
        profile: QueryProfile,
        page: ScrapedPage,
        summary: str,
        source: SourceRef,
    ) -> dict[str, OutputCell]:
        text = f"{page.title}\n{page.snippet}\n{page.text}\n{summary}"
        attributes: dict[str, OutputCell] = {}

        if profile.expects_local:
            for field_name, value in {
                "category": "restaurant" if "restaurant" in text.lower() or "pizza" in text.lower() else "local business",
                "cuisine": _extract_cuisine(text),
                "location": _extract_location(text),
                "rating": _extract_rating(text),
            }.items():
                cell = _maybe_attribute(value, source)
                if cell is not None:
                    attributes[field_name] = cell

        if profile.expects_open_source:
            for field_name, value in {
                "license_type": _extract_license(text),
                "primary_language": _extract_primary_language(text),
                "github_repo": _extract_github_repo(page.url, text),
                "deployment_model": _extract_deployment_model(text),
            }.items():
                cell = _maybe_attribute(value, source)
                if cell is not None:
                    attributes[field_name] = cell

        if profile.expects_company:
            for field_name, value in {
                "focus_area": _extract_focus_area(text),
                "hq_location": _extract_location(text),
            }.items():
                cell = _maybe_attribute(value, source)
                if cell is not None:
                    attributes[field_name] = cell

        return attributes

    def _build_fallback_row(self, query: str, profile: QueryProfile, page: ScrapedPage) -> OutputEntity | None:
        candidate_name = _best_name_from_title(page.title, page.url, query)
        if not candidate_name:
            return None

        key = _normalize_entity_key(candidate_name)
        if not key:
            return None

        official = _is_probably_official_page(page, candidate_name)
        summary = _first_sentence(page.snippet or page.text)
        if not summary:
            summary = f"Relevant result for {query}."

        source = _build_fallback_source(page, summary)
        homepage = page.url if official else None
        attributes: dict[str, OutputCell] = {
            "source_domain": OutputCell(
                value=_source_domain(page.url),
                sources=[source],
            ),
            "source_kind": OutputCell(
                value="official_site" if official else "search_result",
                sources=[source],
            ),
        }
        attributes.update(self._build_fallback_attributes(profile, page, summary, source))

        entity_type = _infer_entity_type(query, f"{page.title} {summary}")
        score = _fallback_relevance(query, f"{candidate_name} {page.title} {summary}", page.source_rank, official)
        provenance_score = round(min(1.0, 0.45 + (0.2 if official else 0.0) + 0.05 * len(attributes)), 3)
        confidence_score = round(min(1.0, score * 0.8 + 0.12 * bool(summary) + 0.04 * len(attributes)), 3)
        highlights = [
            "Official site detected" if official else "Recovered from strong source",
            f"{len(attributes)} fallback attributes recovered" if attributes else "Source-backed fallback row",
        ]

        return OutputEntity(
            entity_id=key.replace(" ", "-"),
            name=OutputCell(value=candidate_name, sources=[source]),
            entity_type=OutputCell(value=entity_type, sources=[source]),
            summary=OutputCell(value=summary, sources=[source]),
            homepage=(OutputCell(value=homepage, sources=[source]) if homepage else None),
            attributes=attributes,
            supporting_source_count=1,
            aggregate_score=score,
            confidence_score=confidence_score,
            provenance_score=provenance_score,
            ranking_signals={
                "fallback_score": score,
                "source_diversity": 1.0,
                "attribute_coverage": float(len(attributes)),
                "official_source": 1.0 if official else 0.0,
            },
            highlights=highlights,
        )

    def _augment_rows_with_fallback(
        self,
        query: str,
        plan: RunPlan,
        pages: list[ScrapedPage],
        rows: list[OutputEntity],
    ) -> int:
        existing: dict[str, OutputEntity] = {
            _normalize_entity_key(row.name.value): row
            for row in rows
        }
        added = 0
        profile = _classify_query(query)

        for page in pages:
            fallback_row = self._build_fallback_row(query, profile, page)
            if fallback_row is None:
                continue

            key = _normalize_entity_key(fallback_row.name.value)
            current = existing.get(key)
            if current is None:
                rows.append(fallback_row)
                existing[key] = fallback_row
                added += 1
            else:
                current.name.sources = _dedupe_sources(current.name.sources + fallback_row.name.sources)
                if current.homepage is None and fallback_row.homepage is not None:
                    current.homepage = fallback_row.homepage
                    if "Official homepage identified" not in current.highlights:
                        current.highlights.append("Official homepage identified")
                current.supporting_source_count = max(
                    current.supporting_source_count,
                    fallback_row.supporting_source_count,
                )
                current.aggregate_score = round(
                    min(1.0, current.aggregate_score + 0.04),
                    3,
                )
                current.provenance_score = round(
                    min(1.0, max(current.provenance_score, fallback_row.provenance_score)),
                    3,
                )
                current.confidence_score = round(
                    min(1.0, max(current.confidence_score, fallback_row.confidence_score)),
                    3,
                )
                for attr_name, attr_value in fallback_row.attributes.items():
                    if attr_name not in current.attributes:
                        current.attributes[attr_name] = attr_value

            if len(rows) >= max(self.settings.fallback_max_rows, plan.desired_rows):
                break

        rows.sort(
            key=lambda row: (
                row.supporting_source_count,
                row.aggregate_score,
                len(row.attributes),
                -len(row.name.value),
            ),
            reverse=True,
        )
        return added

    async def _verify_rows(
        self,
        query: str,
        profile: QueryProfile,
        plan: RunPlan,
        rows: list[OutputEntity],
    ) -> None:
        if not plan.verification_enabled or not rows:
            return
        candidates = [
            row
            for row in rows
            if row.confidence_score < self.settings.verification_min_confidence
            or row.supporting_source_count <= 1
        ][: plan.verification_rows]
        if not candidates:
            candidates = rows[: plan.verification_rows]
        verification = await self.extractor.verify_rows(query, profile.query_type, candidates)
        for row in candidates:
            decision = verification.get(row.entity_id)
            if not decision:
                continue
            row.verification_status = str(decision.get("verification_status", "unverified"))
            row.verification_reason = str(decision.get("reason", ""))
            verification_score = float(decision.get("verification_score", 0.0) or 0.0)
            row.ranking_signals["verification_score"] = round(verification_score, 3)
            if row.verification_status == "verified":
                row.aggregate_score = round(min(1.0, row.aggregate_score + 0.05), 3)
                row.confidence_score = round(min(1.0, max(row.confidence_score, verification_score)), 3)
            elif row.verification_status == "plausible":
                row.aggregate_score = round(min(1.0, row.aggregate_score + 0.02), 3)
            elif row.verification_status == "weak":
                row.aggregate_score = round(max(0.0, row.aggregate_score - 0.08), 3)
                row.confidence_score = round(max(0.0, row.confidence_score - 0.08), 3)

    async def _enrich_rows(
        self,
        plan: RunPlan,
        rows: list[OutputEntity],
    ) -> int:
        enriched = 0
        if plan.github_enrichment_enabled:
            enriched += await self.github_enricher.enrich(rows)
        return enriched

    def _finalize_rows(self, rows: list[OutputEntity]) -> None:
        for row in rows:
            freshness_candidates = [row.freshness_date]
            freshness_candidates.extend(
                cell.value
                for key, cell in row.attributes.items()
                if key in {"github_last_pushed_at"}
            )
            row.freshness_days = _freshness_days(*freshness_candidates)
            if row.freshness_days is not None:
                row.ranking_signals["freshness_days"] = float(row.freshness_days)
                if row.freshness_days <= 30:
                    row.highlights.append("Recently updated")
                    row.aggregate_score = round(min(1.0, row.aggregate_score + 0.02), 3)
                elif row.freshness_days > 730:
                    row.aggregate_score = round(max(0.0, row.aggregate_score - 0.03), 3)
            row.rank_explanation = _rank_explanation(row)

        rows.sort(
            key=lambda row: (
                row.aggregate_score,
                row.confidence_score,
                row.supporting_source_count,
                row.provenance_score,
                len(row.attributes),
            ),
            reverse=True,
        )

    async def run(
        self,
        query: str,
        debug: bool = False,
        progress_callback: ProgressCallback | None = None,
        prefer_live: bool = False,
        run_mode: str | None = None,
    ) -> SearchResponse:
        total_started = perf_counter()
        profile = _classify_query(query)
        plan = _build_run_plan(self.settings, query, profile, run_mode)
        await _emit_progress(progress_callback, "cache", 0.05, "Checking query cache.")
        cached, cache_tier = (
            self._get_cached_response(query, debug, plan.run_mode, allow_stale=not prefer_live)
            if not prefer_live
            else (None, "live")
        )
        if cached is not None and not prefer_live:
            response = _clone_response(cached)
            response.metrics = response.metrics or SearchMetrics()
            response.metrics.cache_hit = True
            response.metrics.cache_tier = cache_tier
            response.metrics.total_ms = int((perf_counter() - total_started) * 1000)
            response.metrics.stale_served = cache_tier == "stale"
            if self.report_store is not None and not response.report_id:
                report = self.report_store.save(response)
                response.report_id = report.report_id
                response.report_title = report.title
            await _emit_progress(progress_callback, "completed", 1.0, "Returned cached response.")
            return response

        await _emit_progress(progress_callback, "search", 0.14, "Searching the web for candidate sources.")
        search_started = perf_counter()
        hits = await self.search_provider.search(query, plan.search_limit)
        search_ms = int((perf_counter() - search_started) * 1000)

        await _emit_progress(progress_callback, "rerank", 0.22, "Reranking search hits for stronger source coverage.")
        rerank_started = perf_counter()
        hits = _rerank_search_hits(profile, query, hits)
        rerank_ms = int((perf_counter() - rerank_started) * 1000)

        await _emit_progress(progress_callback, "scrape", 0.34, "Fetching and normalizing the strongest pages.")
        scrape_started = perf_counter()
        pages = await self.scraper.scrape_hits(hits, limit=plan.scrape_limit)
        pages = self._filtered_pages(pages)
        scrape_ms = int((perf_counter() - scrape_started) * 1000)

        await _emit_progress(progress_callback, "extract", 0.58, "Extracting entities from the most relevant chunks.")
        extract_started = perf_counter()
        extractions, chunks_processed = await self._extract_pages(query, profile, plan, pages)
        extract_ms = int((perf_counter() - extract_started) * 1000)

        await _emit_progress(progress_callback, "merge", 0.84, "Merging overlapping entities and computing rankings.")
        merge_started = perf_counter()
        rows = self._merge_extractions(extractions)
        fallback_rows_added = 0
        if len(rows) < max(self.settings.fallback_result_min_rows, plan.desired_rows):
            fallback_rows_added = self._augment_rows_with_fallback(query, plan, pages, rows)
        verify_started = perf_counter()
        await self._verify_rows(query, profile, plan, rows)
        verify_ms = int((perf_counter() - verify_started) * 1000)
        enrich_started = perf_counter()
        enriched_rows = await self._enrich_rows(plan, rows)
        enrich_ms = int((perf_counter() - enrich_started) * 1000)
        self._finalize_rows(rows)
        columns = self._build_columns(rows, profile)
        merge_ms = int((perf_counter() - merge_started) * 1000)

        debug_payload = None
        if debug:
            debug_payload = {
                "hits": [hit.model_dump() for hit in hits],
                "pages": [page.model_dump() for page in pages],
                "row_count": len(rows),
                "chunks_processed": chunks_processed,
                "fallback_rows_added": fallback_rows_added,
                "query_type": profile.query_type,
                "comparison_fields": profile.comparison_fields,
                "run_mode": plan.run_mode,
                "desired_rows": plan.desired_rows,
                "enriched_rows": enriched_rows,
            }

        metrics = SearchMetrics(
            cache_hit=False,
            cache_tier="live",
            search_ms=search_ms,
            rerank_ms=rerank_ms,
            scrape_ms=scrape_ms,
            extract_ms=extract_ms,
            verify_ms=verify_ms,
            enrich_ms=enrich_ms,
            merge_ms=merge_ms,
            total_ms=int((perf_counter() - total_started) * 1000),
            hits_returned=len(hits),
            pages_considered=len(pages),
            chunks_processed=chunks_processed,
            fallback_rows_added=fallback_rows_added,
            target_rows=plan.desired_rows,
        )

        response = SearchResponse(
            query=query,
            query_type=profile.query_type,
            run_mode=plan.run_mode,
            comparison_fields=profile.comparison_fields,
            columns=columns,
            rows=rows[: plan.max_final_rows],
            raw_sources_considered=len(pages),
            desired_row_count=plan.desired_rows,
            metrics=metrics,
            debug=debug_payload,
        )
        if self.report_store is not None:
            report = self.report_store.save(response)
            response.report_id = report.report_id
            response.report_title = report.title
        self._store_cached_response(query, debug, response)
        logger.info(
            "pipeline_complete query=%r query_type=%s run_mode=%s rows=%s cache=%s search_ms=%s scrape_ms=%s extract_ms=%s verify_ms=%s enrich_ms=%s",
            query,
            profile.query_type,
            plan.run_mode,
            len(response.rows),
            response.metrics.cache_tier if response.metrics else "live",
            search_ms,
            scrape_ms,
            extract_ms,
            verify_ms,
            enrich_ms,
        )
        await _emit_progress(progress_callback, "completed", 1.0, "Completed the query pipeline.")
        return response

    async def _extract_pages(
        self,
        query: str,
        profile: QueryProfile,
        plan: RunPlan,
        pages: list[ScrapedPage],
    ) -> tuple[list[tuple[ScrapedPage, ChunkExtraction]], int]:
        tasks: list[asyncio.Task[tuple[ScrapedPage, ChunkExtraction]]] = []
        semaphore = asyncio.Semaphore(self.settings.max_concurrency)

        async def _extract_chunk(page: ScrapedPage, chunk: str) -> tuple[ScrapedPage, ChunkExtraction]:
            async with semaphore:
                extraction = await self.extractor.extract_from_chunk(
                    query,
                    page,
                    chunk,
                    schema_fields=profile.comparison_fields,
                )
                return page, extraction

        for page in pages:
            chunk_budget = self._page_chunk_budget(page.source_rank, plan)
            if chunk_budget <= 0:
                continue
            chunks = chunk_text(
                page.text,
                chunk_size=self.settings.chunk_size_chars,
                overlap=self.settings.chunk_overlap_chars,
                max_chunks=chunk_budget,
                query=query,
            )
            if not chunks and page.snippet.strip():
                chunks = [page.snippet.strip()]
            for chunk in chunks:
                tasks.append(asyncio.create_task(_extract_chunk(page, chunk)))

        if not tasks:
            return [], 0
        return await asyncio.gather(*tasks), len(tasks)

    def _merge_extractions(self, extractions: list[tuple[ScrapedPage, ChunkExtraction]]) -> list[OutputEntity]:
        merged: dict[str, dict] = {}
        homepage_index: dict[str, str] = {}

        for page, extraction in extractions:
            for entity in extraction.entities:
                key = _normalize_entity_key(entity.name)
                if not key:
                    continue
                homepage_domain = _source_domain(entity.homepage or "")
                if homepage_domain and homepage_domain in homepage_index:
                    key = homepage_index[homepage_domain]
                else:
                    for existing_key, existing_bucket in merged.items():
                        similarity = _token_similarity(existing_bucket["name"], entity.name)
                        if similarity >= 0.74:
                            key = existing_key
                            break

                bucket = merged.setdefault(
                    key,
                    {
                        "name": entity.name,
                        "name_sources": [],
                        "entity_type": entity.entity_type,
                        "type_sources": [],
                        "summary": entity.summary,
                        "summary_sources": [],
                        "homepage": entity.homepage,
                        "homepage_sources": [],
                        "attributes": defaultdict(list),
                        "domains": set(),
                        "relevance_scores": [],
                        "freshness_dates": [],
                    },
                )
                if homepage_domain:
                    homepage_index.setdefault(homepage_domain, key)

                source = SourceRef(
                    source_url=page.url,
                    source_title=page.title,
                    quote=entity.summary[:180],
                )

                bucket["domains"].add(_source_domain(page.url))
                bucket["name_sources"].append(source)
                bucket["type_sources"].append(source)
                bucket["summary_sources"].append(source)
                bucket["relevance_scores"].append(entity.relevance_score)
                if page.published_at:
                    bucket["freshness_dates"].append(page.published_at)
                if page.modified_at:
                    bucket["freshness_dates"].append(page.modified_at)

                if len(entity.summary) > len(bucket["summary"]):
                    bucket["summary"] = entity.summary
                if len(entity.name) > len(bucket["name"]):
                    bucket["name"] = entity.name
                if entity.entity_type and len(entity.entity_type) > len(bucket["entity_type"]):
                    bucket["entity_type"] = entity.entity_type
                if entity.homepage and (not bucket["homepage"]):
                    bucket["homepage"] = entity.homepage
                    bucket["homepage_sources"].append(source)

                for cell in entity.cells:
                    bucket["domains"].add(_source_domain(cell.evidence.source_url))
                    bucket["attributes"][cell.field_name].append(
                        {
                            "value": cell.value,
                            "confidence": cell.confidence,
                            "source": cell.evidence,
                        }
                    )

        rows: list[OutputEntity] = []
        for key, bucket in merged.items():
            attributes: dict[str, OutputCell] = {}
            for field_name, candidates in bucket["attributes"].items():
                winner = max(
                    candidates,
                    key=lambda item: (
                        item["confidence"],
                        len(item["value"]),
                    ),
                )
                same_value_sources = [
                    item["source"]
                    for item in candidates
                    if item["value"].strip().lower() == winner["value"].strip().lower()
                ] or [winner["source"]]
                attributes[field_name] = OutputCell(
                    value=winner["value"],
                    sources=_dedupe_sources(same_value_sources),
                )

            relevance_scores = bucket["relevance_scores"] or [0.0]
            avg_relevance = sum(relevance_scores) / len(relevance_scores)
            provenance_score = min(1.0, min(0.6, 0.16 * len(bucket["domains"])) + min(0.4, 0.04 * len(attributes)))
            confidence_score = min(
                1.0,
                avg_relevance * 0.72
                + min(0.18, 0.03 * len(attributes))
                + (0.1 if bucket["homepage"] else 0.0),
            )
            score = min(
                1.0,
                avg_relevance
                + min(0.35, 0.12 * len(bucket["domains"]))
                + min(0.15, 0.02 * len(attributes))
                + (0.06 if bucket["homepage"] else 0.0),
            )
            score = round(score, 3)
            ranking_signals = {
                "avg_relevance": round(avg_relevance, 3),
                "source_diversity": float(len(bucket["domains"])),
                "attribute_coverage": float(len(attributes)),
                "has_homepage": 1.0 if bucket["homepage"] else 0.0,
            }
            highlights: list[str] = []
            if bucket["homepage"]:
                highlights.append("Official homepage identified")
            if len(bucket["domains"]) > 1:
                highlights.append(f"{len(bucket['domains'])} domains corroborate this entity")
            if attributes:
                highlights.append(f"{len(attributes)} comparable attributes extracted")
            freshest_date = None
            freshness_candidates = [_parse_timestamp(value) for value in bucket["freshness_dates"]]
            freshness_candidates = [value for value in freshness_candidates if value is not None]
            if freshness_candidates:
                freshest_date = max(freshness_candidates).replace(microsecond=0).isoformat()

            rows.append(
                OutputEntity(
                    entity_id=key.replace(" ", "-"),
                    name=OutputCell(
                        value=bucket["name"],
                        sources=_dedupe_sources(bucket["name_sources"]),
                    ),
                    entity_type=OutputCell(
                        value=bucket["entity_type"],
                        sources=_dedupe_sources(bucket["type_sources"]),
                    ),
                    summary=OutputCell(
                        value=bucket["summary"],
                        sources=_dedupe_sources(bucket["summary_sources"]),
                    ),
                    homepage=(
                        OutputCell(
                            value=bucket["homepage"],
                            sources=_dedupe_sources(bucket["homepage_sources"]),
                        )
                        if bucket["homepage"]
                        else None
                    ),
                    attributes=attributes,
                    supporting_source_count=len(bucket["domains"]),
                    aggregate_score=score,
                    confidence_score=round(confidence_score, 3),
                    provenance_score=round(provenance_score, 3),
                    ranking_signals=ranking_signals,
                    highlights=highlights,
                    freshness_date=freshest_date,
                )
            )

        rows.sort(
            key=lambda row: (
                row.supporting_source_count,
                row.aggregate_score,
                len(row.attributes),
                -len(row.name.value),
            ),
            reverse=True,
        )
        return rows[:25]
