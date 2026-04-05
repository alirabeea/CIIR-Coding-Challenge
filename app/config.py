from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Agentic Search"
    search_provider: str = "serpapi"

    brave_api_key: str | None = None
    serpapi_api_key: str | None = None

    openai_api_key: str | None = None
    openai_base_url: str | None = None
    openai_model: str = "gpt-4.1-mini"
    default_run_mode: str = "balanced"

    max_search_results: int = 8
    max_pages_to_scrape: int = 6
    max_chunks_per_page: int = 2
    chunk_size_chars: int = 4000
    chunk_overlap_chars: int = 400
    max_entities_per_chunk: int = 4
    max_extraction_completion_tokens: int = 1200
    request_timeout_seconds: float = 15.0
    max_concurrency: int = 5
    max_page_text_chars: int = 12000
    min_page_text_chars: int = 160
    max_results_per_domain: int = 2
    fast_max_rows: int = 16
    balanced_max_rows: int = 28
    deep_max_rows: int = 40

    query_cache_ttl_seconds: float = 300.0
    search_cache_ttl_seconds: float = 900.0
    page_cache_ttl_seconds: float = 1800.0
    extraction_cache_ttl_seconds: float = 1800.0
    persistent_cache_enabled: bool = True
    persistent_cache_path: str = ".cache/agentic-search.sqlite3"
    stale_cache_enabled: bool = True
    stale_cache_ttl_seconds: float = 21600.0
    save_reports: bool = True
    report_directory: str = "reports"
    max_saved_reports: int = 25
    job_store_directory: str = "reports/jobs"

    max_cached_queries: int = 24
    max_cached_searches: int = 48
    max_cached_pages: int = 128
    max_cached_extractions: int = 256
    http_retries: int = 2
    fallback_result_min_rows: int = 8
    fallback_max_rows: int = 10
    verification_enabled: bool = True
    verification_max_rows: int = 4
    verification_min_confidence: float = 0.72
    github_enrichment_enabled: bool = True
    github_request_timeout_seconds: float = 8.0
    js_render_fallback_enabled: bool = False
    js_render_timeout_seconds: float = 18.0
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 120
    rate_limit_window_seconds: float = 60.0
    job_retention_seconds: float = 1800.0
    circuit_breaker_failures: int = 4
    circuit_breaker_reset_seconds: float = 60.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
