from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Awaitable, Callable
from uuid import uuid4

from app.config import Settings
from app.job_store import FileJobStore
from app.models import SearchJobStatus, SearchResponse

ProgressCallback = Callable[[str, float, str], Awaitable[None] | None]
PipelineFactory = Callable[[], object]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class SearchJobManager:
    def __init__(self, settings: Settings, pipeline_factory: PipelineFactory):
        self.settings = settings
        self.pipeline_factory = pipeline_factory
        self.job_store = FileJobStore(settings.job_store_directory)
        self._jobs: dict[str, SearchJobStatus] = {
            status.job_id: status
            for status in self.job_store.list()
        }
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()

    async def create_job(
        self,
        query: str,
        debug: bool = False,
        prefer_live: bool = False,
        run_mode: str | None = None,
    ) -> SearchJobStatus:
        await self._cleanup()
        job_id = uuid4().hex
        pipeline = self.pipeline_factory()
        peek_cached_response = getattr(pipeline, "peek_cached_response", None)
        preview = (
            peek_cached_response(query, debug=debug, run_mode=run_mode)
            if callable(peek_cached_response)
            else None
        )
        status = SearchJobStatus(
            job_id=job_id,
            query=query,
            run_mode=run_mode or self.settings.default_run_mode,
            status="queued",
            stage="queued",
            progress=0.0,
            message="Job created and waiting to start.",
            created_at=_utc_now(),
            updated_at=_utc_now(),
            preview_result=preview,
        )
        async with self._lock:
            self._jobs[job_id] = status
            self.job_store.save(status)
            self._tasks[job_id] = asyncio.create_task(
                self._run_job(job_id, query=query, debug=debug, prefer_live=prefer_live, run_mode=run_mode)
            )
        return status

    async def get_job(self, job_id: str) -> SearchJobStatus | None:
        await self._cleanup()
        async with self._lock:
            status = self._jobs.get(job_id)
            if status is None:
                return None
            return SearchJobStatus.model_validate(status.model_dump())

    async def _run_job(self, job_id: str, query: str, debug: bool, prefer_live: bool, run_mode: str | None) -> None:
        async def _progress(stage: str, progress: float, message: str) -> None:
            await self._update(job_id, stage=stage, progress=progress, message=message, status="running")

        try:
            await _progress("queued", 0.02, "Queued for execution.")
            pipeline = self.pipeline_factory()
            await _progress("starting", 0.05, "Preparing the search pipeline.")
            result = await pipeline.run(
                query=query,
                debug=debug,
                progress_callback=_progress,
                prefer_live=prefer_live,
                run_mode=run_mode,
            )
            await self._update(
                job_id,
                status="completed",
                stage="completed",
                progress=1.0,
                message="Search finished successfully.",
                result=result,
                error=None,
            )
        except Exception as exc:
            await self._update(
                job_id,
                status="failed",
                stage="failed",
                progress=1.0,
                message="Search job failed.",
                error=str(exc),
                result=None,
            )

    async def _update(
        self,
        job_id: str,
        *,
        status: str | None = None,
        stage: str | None = None,
        progress: float | None = None,
        message: str | None = None,
        result: SearchResponse | None = None,
        error: str | None = None,
    ) -> None:
        async with self._lock:
            current = self._jobs[job_id]
            payload = current.model_dump()
            if status is not None:
                payload["status"] = status
            if stage is not None:
                payload["stage"] = stage
            if progress is not None:
                payload["progress"] = round(max(0.0, min(1.0, progress)), 3)
            if message is not None:
                payload["message"] = message
            if result is not None:
                payload["result"] = result.model_dump()
            if error is not None or status == "completed":
                payload["error"] = error
            payload["updated_at"] = _utc_now()
            model = SearchJobStatus.model_validate(payload)
            self._jobs[job_id] = model
            self.job_store.save(model)

    async def _cleanup(self) -> None:
        cutoff = datetime.now(timezone.utc).timestamp() - self.settings.job_retention_seconds
        async with self._lock:
            stale_job_ids = [
                job_id
                for job_id, job in self._jobs.items()
                if datetime.fromisoformat(job.updated_at).timestamp() <= cutoff
                and job.status in {"completed", "failed"}
            ]
            for job_id in stale_job_ids:
                task = self._tasks.pop(job_id, None)
                if task is not None and not task.done():
                    task.cancel()
                self._jobs.pop(job_id, None)
                self.job_store.delete(job_id)
