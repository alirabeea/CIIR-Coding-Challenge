from __future__ import annotations

import logging
import re
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from app.config import get_settings
from app.export import response_to_csv
from app.jobs import SearchJobManager
from app.models import SavedReportSummary, SearchJobStatus, SearchResponse
from app.pipeline import AgenticSearchPipeline
from app.rate_limit import SlidingWindowRateLimiter
from app.reports import response_to_markdown

settings = get_settings()
logger = logging.getLogger("agentic_search")
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

_pipeline: AgenticSearchPipeline | None = None
_job_manager: SearchJobManager | None = None
_rate_limiter = SlidingWindowRateLimiter(
    max_requests=settings.rate_limit_requests,
    window_seconds=settings.rate_limit_window_seconds,
)


def get_pipeline() -> AgenticSearchPipeline:
    global _pipeline
    if _pipeline is None:
        _pipeline = AgenticSearchPipeline(settings)
    return _pipeline


def get_job_manager() -> SearchJobManager:
    global _job_manager
    if _job_manager is None:
        _job_manager = SearchJobManager(settings=settings, pipeline_factory=get_pipeline)
    return _job_manager


app = FastAPI(title=settings.app_name)
app.add_middleware(GZipMiddleware, minimum_size=1200)
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or uuid4().hex
    request.state.request_id = request_id

    if settings.rate_limit_enabled and request.url.path.startswith("/api/"):
        client_host = request.client.host if request.client else "anonymous"
        allowed, retry_after = _rate_limiter.check(client_host)
        if not allowed:
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Rate limit exceeded. Please retry shortly.",
                    "request_id": request_id,
                },
                headers={
                    "Retry-After": str(max(1, int(retry_after))),
                    "X-Request-ID": request_id,
                },
            )

    started = perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception(
            "request_failed request_id=%s method=%s path=%s",
            request_id,
            request.method,
            request.url.path,
        )
        raise

    duration_ms = int((perf_counter() - started) * 1000)
    response.headers["X-Request-ID"] = request_id
    logger.info(
        "request_complete request_id=%s method=%s path=%s status=%s duration_ms=%s",
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


@app.on_event("shutdown")
async def shutdown_event() -> None:
    global _pipeline, _job_manager
    if _pipeline is not None:
        await _pipeline.close()
        _pipeline = None
    _job_manager = None


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/search", response_model=SearchResponse)
async def search(
    query: str = Query(min_length=2),
    debug: bool = False,
    prefer_live: bool = False,
    run_mode: str | None = Query(default=None, pattern="^(fast|balanced|deep)?$"),
) -> SearchResponse:
    try:
        return await get_pipeline().run(query=query, debug=debug, prefer_live=prefer_live, run_mode=run_mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/search.csv")
async def search_csv(
    query: str = Query(min_length=2),
    debug: bool = False,
    prefer_live: bool = False,
    run_mode: str | None = Query(default=None, pattern="^(fast|balanced|deep)?$"),
) -> PlainTextResponse:
    try:
        response = await get_pipeline().run(query=query, debug=debug, prefer_live=prefer_live, run_mode=run_mode)
        csv_payload = response_to_csv(response)
        filename = re.sub(r"[^a-z0-9]+", "-", query.lower()).strip("-") or "agentic-search"
        headers = {"Content-Disposition": f'attachment; filename="{filename}.csv"'}
        return PlainTextResponse(csv_payload, media_type="text/csv", headers=headers)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/search.md")
async def search_markdown(
    query: str = Query(min_length=2),
    debug: bool = False,
    prefer_live: bool = False,
    run_mode: str | None = Query(default=None, pattern="^(fast|balanced|deep)?$"),
) -> PlainTextResponse:
    try:
        response = await get_pipeline().run(query=query, debug=debug, prefer_live=prefer_live, run_mode=run_mode)
        markdown_payload = response_to_markdown(response)
        filename = re.sub(r"[^a-z0-9]+", "-", query.lower()).strip("-") or "agentic-search-report"
        headers = {"Content-Disposition": f'attachment; filename="{filename}.md"'}
        return PlainTextResponse(markdown_payload, media_type="text/markdown", headers=headers)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/jobs", response_model=SearchJobStatus, status_code=202)
async def create_job(
    query: str = Query(min_length=2),
    debug: bool = False,
    prefer_live: bool = False,
    run_mode: str | None = Query(default=None, pattern="^(fast|balanced|deep)?$"),
) -> SearchJobStatus:
    try:
        return await get_job_manager().create_job(query=query, debug=debug, prefer_live=prefer_live, run_mode=run_mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/jobs/{job_id}", response_model=SearchJobStatus)
async def get_job(job_id: str) -> SearchJobStatus:
    status = await get_job_manager().get_job(job_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return status


@app.get("/api/reports", response_model=list[SavedReportSummary])
async def list_reports(limit: int = Query(default=8, ge=1, le=25)) -> list[SavedReportSummary]:
    return list(get_pipeline().list_reports(limit=limit))


@app.get("/api/reports/{report_id}.json")
async def report_json(report_id: str) -> FileResponse:
    report_store = get_pipeline().report_store
    if report_store is None:
        raise HTTPException(status_code=404, detail="Report storage is disabled")
    try:
        path = report_store.json_path(report_id)
        return FileResponse(path, media_type="application/json", filename=f"{report_id}.json")
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Report not found") from exc


@app.get("/api/reports/{report_id}.md")
async def report_markdown(report_id: str) -> FileResponse:
    report_store = get_pipeline().report_store
    if report_store is None:
        raise HTTPException(status_code=404, detail="Report storage is disabled")
    try:
        path = report_store.markdown_path(report_id)
        return FileResponse(path, media_type="text/markdown", filename=f"{report_id}.md")
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Report not found") from exc
