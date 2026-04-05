from __future__ import annotations

import asyncio
import tempfile
import time
import unittest

from app.config import Settings
from app.export import response_to_csv
from app.models import OutputCell, OutputEntity, ScrapedPage, SearchResponse, SourceRef
from app.persistent_cache import PersistentCache
from app.pipeline import AgenticSearchPipeline, _classify_query


class PersistentCacheTests(unittest.TestCase):
    def test_persistent_cache_round_trip_and_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache = PersistentCache(f"{tmp_dir}/cache.sqlite3")
            cache.set("query", "abc", {"value": 1}, ttl_seconds=0.02)
            self.assertEqual(cache.get("query", "abc"), {"value": 1})

            time.sleep(0.03)
            self.assertIsNone(cache.get("query", "abc"))
            cache.close()


class ExportTests(unittest.TestCase):
    def test_response_to_csv_flattens_rows(self) -> None:
        response = SearchResponse(
            query="developer analytics tools",
            columns=["name", "entity_type", "summary", "homepage", "license_type"],
            rows=[
                OutputEntity(
                    entity_id="metabase",
                    name=OutputCell(
                        value="Metabase",
                        sources=[SourceRef(source_url="https://metabase.com", source_title="Metabase", quote="Metabase")],
                    ),
                    entity_type=OutputCell(value="software tool"),
                    summary=OutputCell(value="Open-source analytics platform."),
                    homepage=OutputCell(value="https://metabase.com"),
                    attributes={"license_type": OutputCell(value="open source")},
                    supporting_source_count=2,
                    aggregate_score=0.9,
                )
            ],
            raw_sources_considered=3,
        )

        csv_payload = response_to_csv(response)
        self.assertIn("entity_id,name,entity_type,summary,homepage,license_type,aggregate_score,confidence_score,verification_status,freshness_date,rank_explanation,supporting_source_count,source_urls", csv_payload)
        self.assertIn("metabase,Metabase,software tool,Open-source analytics platform.,https://metabase.com,open source,0.9,0.0,unverified,,,2,https://metabase.com", csv_payload)


class FallbackPipelineTests(unittest.TestCase):
    def test_fallback_row_promotes_official_page(self) -> None:
        settings = Settings(
            search_provider="serpapi",
            serpapi_api_key="test",
            openai_api_key="test",
            persistent_cache_enabled=False,
        )
        pipeline = AgenticSearchPipeline(settings)
        try:
            page = ScrapedPage(
                url="https://www.metabase.com/",
                title="Metabase | Open-source business intelligence",
                text="Metabase is the easy, open source way for everyone in your company to ask questions and learn from data.",
                source_rank=1,
                source_engine="serpapi",
                snippet="Metabase is an open-source business intelligence platform for analytics teams.",
            )

            row = pipeline._build_fallback_row("open source analytics tools", _classify_query("open source analytics tools"), page)
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row.name.value, "Metabase")
            self.assertEqual(row.homepage.value, "https://www.metabase.com/")
            self.assertEqual(row.attributes["source_kind"].value, "official_site")
            self.assertIn("license_type", row.attributes)
        finally:
            asyncio.run(pipeline.close())


if __name__ == "__main__":
    unittest.main()
