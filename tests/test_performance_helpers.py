from __future__ import annotations

import time
import unittest

from app.cache import TTLCache
from app.models import SearchHit
from app.scrape import chunk_text
from app.search import _dedupe_hits


class TTLCacheTests(unittest.TestCase):
    def test_cache_expires_items(self) -> None:
        cache = TTLCache[str](maxsize=2, ttl_seconds=0.01)
        cache.set("topic", "cached-result")
        self.assertEqual(cache.get("topic"), "cached-result")

        time.sleep(0.02)
        self.assertIsNone(cache.get("topic"))


class SearchDeduplicationTests(unittest.TestCase):
    def test_dedupe_hits_removes_duplicate_urls_and_caps_domains(self) -> None:
        hits = [
            SearchHit(title="A1", url="https://example.com/a", snippet="", rank=1, source_engine="x"),
            SearchHit(title="A2", url="https://example.com/a/", snippet="", rank=2, source_engine="x"),
            SearchHit(title="B1", url="https://example.com/b", snippet="", rank=3, source_engine="x"),
            SearchHit(title="C1", url="https://other.com/c", snippet="", rank=4, source_engine="x"),
        ]

        deduped = _dedupe_hits(hits, limit=5, per_domain_cap=1)

        self.assertEqual([hit.url for hit in deduped], ["https://example.com/a", "https://other.com/c"])
        self.assertEqual([hit.rank for hit in deduped], [1, 2])


class ChunkSelectionTests(unittest.TestCase):
    def test_chunk_text_prioritizes_query_relevant_sections(self) -> None:
        text = "\n\n".join(
            [
                "General article introduction about many unrelated startup topics.",
                "Travel guides and restaurant roundups with no mention of developer tools.",
                "Open source database tools often include Postgres clients, admin dashboards, and schema browsers.",
            ]
        )

        chunks = chunk_text(
            text,
            chunk_size=90,
            overlap=10,
            max_chunks=1,
            query="open source database tools",
        )

        self.assertEqual(len(chunks), 1)
        self.assertIn("database tools", chunks[0].lower())


if __name__ == "__main__":
    unittest.main()
