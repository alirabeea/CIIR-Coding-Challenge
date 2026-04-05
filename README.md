# Agentic Search

Agentic Search is a FastAPI service that turns a broad topic query into a structured, source-backed entity table. It combines web search, page retrieval, structured extraction, and multi-source consolidation behind a JSON API and a browser-based results interface.

## Highlights

- Topic-driven discovery for tools, companies, products, local businesses, and similar entity sets.
- Cell-level traceability with source URL, source title, and supporting quote for every grounded value.
- Search provider abstraction for Brave Search and SerpAPI.
- Concurrent page retrieval, query-aware chunk selection, retry handling, and multi-layer caching for faster repeated queries.
- Query classification, run modes (`fast`, `balanced`, `deep`), dynamic row targeting, source reranking, heuristic recovery, and ranking signals for more stable output quality.
- Background job execution with persisted job state, cached previews, and live progress polling for long-running queries.
- Optional second-pass verification for weak rows, stale-cache serving, and circuit breaking for upstream failures.
- GitHub enrichment for open-source queries and optional JS-render fallback for hard-to-scrape pages.
- Responsive frontend with result metrics, filtering, sorting, pinning, side-by-side entity comparison, saved reports, desktop table rendering, and mobile card layouts.
- JSON API suitable for internal tooling, data workflows, and lightweight integrations.
- CSV and Markdown export endpoints, saved snapshots, and shareable query URLs for downstream workflows.
- Docker, CI, benchmark scripts, and cache-warming utilities for release and evaluation workflows.

## Requirements

- Python `3.11+`
- One search provider credential: `BRAVE_API_KEY` or `SERPAPI_API_KEY`
- One OpenAI-compatible chat completion credential: `OPENAI_API_KEY`

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .envexample .env
uvicorn app.main:app --reload
```

The service is available at [http://127.0.0.1:8000](http://127.0.0.1:8000).

## Configuration

Core settings are loaded from environment variables or `.env`.

| Variable | Description | Default |
| --- | --- | --- |
| `SEARCH_PROVIDER` | Search backend to use. Supported values: `brave`, `serpapi` | `serpapi` |
| `BRAVE_API_KEY` | Brave Search API credential | unset |
| `SERPAPI_API_KEY` | SerpAPI credential | unset |
| `OPENAI_API_KEY` | Extraction provider credential | unset |
| `OPENAI_BASE_URL` | Optional compatible API base URL | unset |
| `OPENAI_MODEL` | Chat completion model used for extraction | `gpt-4.1-mini` |
| `DEFAULT_RUN_MODE` | Default execution mode: `fast`, `balanced`, or `deep` | `balanced` |
| `PERSISTENT_CACHE_ENABLED` | Enables SQLite-backed cache persistence across restarts | `true` |
| `PERSISTENT_CACHE_PATH` | Filesystem path for the persistent cache database | `.cache/agentic-search.sqlite3` |
| `STALE_CACHE_ENABLED` | Allows serving recently expired query cache entries | `true` |
| `SAVE_REPORTS` | Writes JSON and Markdown result snapshots to disk | `true` |
| `REPORT_DIRECTORY` | Directory used for saved report snapshots | `reports` |
| `RATE_LIMIT_REQUESTS` | Per-process request limit within the configured window | `120` |

Performance and runtime tuning options are included in [.envexample](/Users/alirabeea/Documents/Personal/Projects/CIIR/agentic-search-challenge/.envexample).

## Running

Development server:

```bash
uvicorn app.main:app --reload
```

Production-style server:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Container image:

```bash
cp .envexample .env
docker build -t agentic-search .
docker run --env-file .env -p 8000:8000 agentic-search
```

## API

### `GET /health`

Simple liveness check.

### `GET /api/search`

Query parameters:

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `query` | string | yes | Topic query used to discover and rank entities |
| `debug` | boolean | no | Includes internal hit and page payloads when set to `true` |
| `run_mode` | string | no | Execution profile: `fast`, `balanced`, or `deep` |
| `prefer_live` | boolean | no | Bypasses the query cache and forces a fresh pipeline run |

Example:

```http
GET /api/search?query=open%20source%20database%20tools
```

Response shape:

```json
{
  "query": "open source database tools",
  "columns": ["name", "entity_type", "summary", "homepage", "license_type"],
  "rows": [
    {
      "entity_id": "metabase",
      "name": {
        "value": "Metabase",
        "sources": [
          {
            "source_url": "https://example.com/page",
            "source_title": "Example page",
            "quote": "Metabase is an open-source business intelligence tool"
          }
        ]
      },
      "entity_type": {
        "value": "analytics platform",
        "sources": []
      },
      "summary": {
        "value": "Open-source analytics and dashboarding platform.",
        "sources": []
      },
      "homepage": {
        "value": "https://www.metabase.com/",
        "sources": []
      },
      "attributes": {
        "license_type": {
          "value": "open source",
          "sources": []
        }
      },
      "supporting_source_count": 3,
      "aggregate_score": 0.94
    }
  ],
  "raw_sources_considered": 6,
  "metrics": {
    "cache_hit": false,
    "search_ms": 120,
    "scrape_ms": 850,
    "extract_ms": 4200,
    "merge_ms": 8,
    "total_ms": 5178,
    "hits_returned": 8,
    "pages_considered": 6,
    "chunks_processed": 6
  }
}
```

### `GET /api/search.csv`

Returns the same search results as a flat CSV export with fixed entity fields, discovered attributes, ranking data, and aggregated source URLs.

### `GET /api/search.md`

Returns a Markdown report version of the current query, suitable for sharing or archiving.

### `POST /api/jobs`

Creates a background search job and returns an immediate job handle. Useful for long-running cold queries where the client wants progress updates. Jobs persist to disk so recent state survives process restarts.

### `GET /api/jobs/{job_id}`

Returns live job state, progress percentage, current stage, and the final `SearchResponse` when complete.

### `GET /api/reports`

Lists the most recent saved report snapshots with JSON and Markdown download URLs.

## Testing

```bash
python -m unittest discover -s tests -v
```

Additional utilities:

```bash
python scripts/benchmark.py
python scripts/evaluate.py
python scripts/warm_cache.py
```

## Project Layout

| Path | Purpose |
| --- | --- |
| `app/main.py` | FastAPI entrypoint, middleware, and endpoint registration |
| `app/pipeline.py` | Request orchestration, caching, filtering, extraction scheduling, merge logic |
| `app/persistent_cache.py` | SQLite-backed cache persistence |
| `app/reports.py` | Saved snapshot storage and Markdown report rendering |
| `app/jobs.py` | Background job execution and progress tracking |
| `app/rate_limit.py` | In-process request rate limiting |
| `app/export.py` | Flat export helpers for CSV generation |
| `app/search.py` | Search provider adapters and hit deduplication |
| `app/scrape.py` | Page retrieval, HTML cleanup, query-aware chunking |
| `app/extract.py` | Structured extraction client and chunk-level extraction caching |
| `app/models.py` | Pydantic request and response models |
| `app/static/index.html` | Browser UI |
| `scripts/` | Benchmarking and cache warm-up utilities |
| `benchmarks/` | Default benchmark query fixtures |
| `tests/` | Unit and mocked integration coverage |

Generated runtime directories such as `.cache/` and `reports/` are created automatically and should not be committed.

## Documentation

Detailed implementation notes, runtime flow, technology stack, and component reference are available in [ARCHITECTURE.md](/Users/alirabeea/Documents/Personal/Projects/CIIR/agentic-search-challenge/ARCHITECTURE.md).

## Operational Notes

- Cold queries are bounded by upstream search, fetch, and extraction latency.
- Repeated queries benefit from in-memory query, search, page, and extraction caches.
- Persistent cache entries survive process restarts when enabled.
- Saved reports persist JSON and Markdown snapshots for recent searches.
- Background jobs make long-running queries non-blocking for the frontend.
- Background jobs persist their latest known state to disk.
- Query responses can be served from a stale cache entry while a fresh run is requested separately.
- Open-source queries can be enriched with GitHub repository metadata when a repo is detected.
- Optional Playwright-based rendering can be enabled for pages that require client-side execution.
- Request logging and lightweight rate limiting are enabled for API routes by default.
- Pages that depend heavily on client-side rendering may yield limited text extraction.
- In-memory caches are process-local; the optional SQLite cache is shared only within the configured filesystem path.
