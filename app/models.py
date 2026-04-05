from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


class SearchHit(BaseModel):
    title: str
    url: str
    snippet: str = ""
    rank: int = 0
    source_engine: str = ""
    rerank_score: float = 0.0
    published_at: str | None = None


class ScrapedPage(BaseModel):
    url: str
    title: str
    text: str
    source_rank: int = 0
    source_engine: str = ""
    snippet: str = ""
    fetch_error: str | None = None
    published_at: str | None = None
    modified_at: str | None = None
    fetched_via: str = "http"


class SourceRef(BaseModel):
    source_url: str
    source_title: str = ""
    quote: str = ""


class ExtractedCell(BaseModel):
    field_name: str = Field(description="Short snake_case attribute name.")
    value: str = Field(description="Literal value grounded in the page.")
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: SourceRef


class ExtractedEntity(BaseModel):
    name: str
    entity_type: str = Field(description="Company, restaurant, tool, project, clinic, etc.")
    summary: str = Field(description="1-2 sentence grounded summary.")
    homepage: str | None = Field(default=None, description="Entity homepage only if directly supported.")
    relevance_score: float = Field(ge=0.0, le=1.0)
    cells: list[ExtractedCell] = Field(default_factory=list)


class ChunkExtraction(BaseModel):
    entities: list[ExtractedEntity] = Field(default_factory=list)


class OutputCell(BaseModel):
    value: str
    sources: list[SourceRef] = Field(default_factory=list)


class OutputEntity(BaseModel):
    entity_id: str
    name: OutputCell
    entity_type: OutputCell
    summary: OutputCell
    homepage: OutputCell | None = None
    attributes: dict[str, OutputCell] = Field(default_factory=dict)
    supporting_source_count: int = 0
    aggregate_score: float = 0.0
    confidence_score: float = 0.0
    provenance_score: float = 0.0
    ranking_signals: dict[str, float] = Field(default_factory=dict)
    highlights: list[str] = Field(default_factory=list)
    rank_explanation: str = ""
    verification_status: str = "unverified"
    verification_reason: str = ""
    freshness_date: str | None = None
    freshness_days: int | None = None


class SearchMetrics(BaseModel):
    cache_hit: bool = False
    cache_tier: str = "live"
    search_ms: int = 0
    rerank_ms: int = 0
    scrape_ms: int = 0
    extract_ms: int = 0
    verify_ms: int = 0
    enrich_ms: int = 0
    merge_ms: int = 0
    total_ms: int = 0
    hits_returned: int = 0
    pages_considered: int = 0
    chunks_processed: int = 0
    fallback_rows_added: int = 0
    target_rows: int = 0
    stale_served: bool = False


class SavedReportSummary(BaseModel):
    report_id: str
    title: str
    query: str
    query_type: str = "general"
    run_mode: str = "balanced"
    created_at: str
    entity_count: int = 0
    json_url: str
    markdown_url: str


class SearchJobStatus(BaseModel):
    job_id: str
    query: str
    run_mode: str = "balanced"
    status: str
    stage: str = "queued"
    progress: float = 0.0
    message: str = ""
    created_at: str
    updated_at: str
    error: str | None = None
    result: SearchResponse | None = None
    preview_result: SearchResponse | None = None


class SearchResponse(BaseModel):
    query: str
    query_type: str = "general"
    run_mode: str = "balanced"
    comparison_fields: list[str] = Field(default_factory=list)
    columns: list[str]
    rows: list[OutputEntity]
    raw_sources_considered: int
    report_id: str | None = None
    report_title: str | None = None
    desired_row_count: int = 0
    metrics: SearchMetrics | None = None
    debug: dict[str, Any] | None = None


SearchJobStatus.model_rebuild()
