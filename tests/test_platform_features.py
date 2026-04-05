from __future__ import annotations

import asyncio
import tempfile
import unittest

from app.job_store import FileJobStore
from app.jobs import SearchJobManager
from app.models import OutputCell, OutputEntity, SearchJobStatus, SearchResponse
from app.pipeline import _build_run_plan, _classify_query, _rerank_search_hits
from app.rate_limit import SlidingWindowRateLimiter
from app.reports import ReportStore, response_to_markdown
from app.config import Settings
from app.models import SearchHit
from app.persistent_cache import PersistentCache


def _sample_response(query: str = "open source database tools") -> SearchResponse:
    return SearchResponse(
        query=query,
        query_type="open_source",
        run_mode="balanced",
        comparison_fields=["license_type", "primary_language"],
        columns=["name", "entity_type", "summary", "homepage", "license_type"],
        rows=[
            OutputEntity(
                entity_id="metabase",
                name=OutputCell(value="Metabase"),
                entity_type=OutputCell(value="software tool"),
                summary=OutputCell(value="Open-source analytics platform."),
                homepage=OutputCell(value="https://www.metabase.com/"),
                attributes={"license_type": OutputCell(value="Open source")},
                supporting_source_count=2,
                aggregate_score=0.93,
                confidence_score=0.88,
                provenance_score=0.72,
                highlights=["Official homepage identified"],
            )
        ],
        raw_sources_considered=4,
    )


class ReportStoreTests(unittest.TestCase):
    def test_report_store_saves_and_lists_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ReportStore(tmp_dir, max_reports=5)
            summary = store.save(_sample_response())

            self.assertTrue(store.json_path(summary.report_id).exists())
            self.assertTrue(store.markdown_path(summary.report_id).exists())

            reports = store.list_reports(limit=3)
            self.assertEqual(len(reports), 1)
            self.assertEqual(reports[0].query, "open source database tools")
            self.assertEqual(reports[0].run_mode, "balanced")

    def test_response_to_markdown_contains_ranked_table(self) -> None:
        markdown = response_to_markdown(_sample_response())
        self.assertIn("# Agentic Search Report: open source database tools", markdown)
        self.assertIn("| Rank | Name | Entity Type | Summary | Homepage | License Type | Score | Sources |", markdown)


class RateLimiterTests(unittest.TestCase):
    def test_sliding_window_rate_limiter_blocks_when_limit_exceeded(self) -> None:
        limiter = SlidingWindowRateLimiter(max_requests=2, window_seconds=30)
        self.assertEqual(limiter.check("127.0.0.1"), (True, 0.0))
        self.assertEqual(limiter.check("127.0.0.1"), (True, 0.0))

        allowed, retry_after = limiter.check("127.0.0.1")
        self.assertFalse(allowed)
        self.assertGreater(retry_after, 0.0)


class QueryHeuristicTests(unittest.TestCase):
    def test_query_classification_detects_verticals(self) -> None:
        self.assertEqual(_classify_query("top pizza places in Brooklyn").query_type, "local")
        self.assertEqual(_classify_query("AI startups in healthcare").query_type, "company")
        self.assertEqual(_classify_query("open source database tools").query_type, "open_source")

    def test_rerank_prefers_github_for_open_source_queries(self) -> None:
        profile = _classify_query("open source feature flag tools")
        hits = [
            SearchHit(title="Vendor Homepage", url="https://vendor.example.com", snippet="Commercial feature flags", rank=1, source_engine="serpapi"),
            SearchHit(title="GitHub - flagsmith/flagsmith", url="https://github.com/Flagsmith/flagsmith", snippet="Open source feature flag service", rank=2, source_engine="serpapi"),
        ]

        reranked = _rerank_search_hits(profile, "open source feature flag tools", hits)
        self.assertEqual(reranked[0].url, "https://github.com/Flagsmith/flagsmith")

    def test_run_plan_expands_for_deep_mode(self) -> None:
        settings = Settings(
            search_provider="serpapi",
            serpapi_api_key="test",
            openai_api_key="test",
            persistent_cache_enabled=False,
        )
        profile = _classify_query("open source database tools")
        fast = _build_run_plan(settings, "open source database tools", profile, "fast")
        deep = _build_run_plan(settings, "open source database tools", profile, "deep")
        self.assertGreater(deep.search_limit, fast.search_limit)
        self.assertGreater(deep.max_final_rows, fast.max_final_rows)


class CacheAndStoreTests(unittest.TestCase):
    def test_persistent_cache_can_return_stale_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache = PersistentCache(f"{tmp_dir}/cache.sqlite3")
            cache.set("query", "topic", {"value": 1}, ttl_seconds=0.01)
            import time

            time.sleep(0.02)
            payload, state = cache.get_with_state("query", "topic", allow_stale=True, stale_ttl_seconds=30)
            self.assertEqual(payload, {"value": 1})
            self.assertEqual(state, "stale")
            cache.close()

    def test_file_job_store_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = FileJobStore(tmp_dir)
            status = SearchJobStatus(
                job_id="job-1",
                query="open source database tools",
                run_mode="deep",
                status="completed",
                stage="completed",
                progress=1.0,
                message="done",
                created_at="2026-04-04T00:00:00+00:00",
                updated_at="2026-04-04T00:00:00+00:00",
                result=_sample_response(),
            )
            store.save(status)
            loaded = store.load("job-1")
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.run_mode, "deep")


class SearchJobManagerTests(unittest.TestCase):
    def test_job_manager_tracks_progress_and_result(self) -> None:
        class FakePipeline:
            def peek_cached_response(self, query: str, debug: bool = False, run_mode: str | None = None):
                return _sample_response(query)

            async def run(self, query: str, debug: bool = False, progress_callback=None, prefer_live: bool = False, run_mode: str | None = None):
                if progress_callback is not None:
                    await progress_callback("search", 0.2, "Searching")
                    await progress_callback("extract", 0.7, "Extracting")
                return _sample_response(query)

        async def exercise() -> None:
            manager = SearchJobManager(
                settings=Settings(
                    search_provider="serpapi",
                    serpapi_api_key="test",
                    openai_api_key="test",
                    persistent_cache_enabled=False,
                ),
                pipeline_factory=lambda: FakePipeline(),
            )
            created = await manager.create_job("open source database tools", run_mode="deep")
            self.assertEqual(created.status, "queued")
            self.assertIsNotNone(created.preview_result)
            self.assertEqual(created.run_mode, "deep")

            for _ in range(20):
                status = await manager.get_job(created.job_id)
                assert status is not None
                if status.status == "completed":
                    self.assertIsNotNone(status.result)
                    self.assertEqual(status.result.query, "open source database tools")
                    self.assertEqual(status.stage, "completed")
                    return
                await asyncio.sleep(0.01)
            self.fail("Job did not complete in time")

        asyncio.run(exercise())


if __name__ == "__main__":
    unittest.main()
