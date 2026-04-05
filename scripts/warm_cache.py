from __future__ import annotations

import argparse
import asyncio
import json

from app.config import get_settings
from app.pipeline import AgenticSearchPipeline


def load_queries(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return [str(item).strip() for item in payload if str(item).strip()]


async def main_async(queries_path: str, prefer_live: bool, run_mode: str) -> None:
    pipeline = AgenticSearchPipeline(get_settings())
    try:
        for query in load_queries(queries_path):
            response = await pipeline.run(query=query, prefer_live=prefer_live, run_mode=run_mode)
            print(
                json.dumps(
                    {
                        "query": query,
                        "query_type": response.query_type,
                        "rows": len(response.rows),
                        "cache_tier": response.metrics.cache_tier if response.metrics else "live",
                    }
                )
            )
    finally:
        await pipeline.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-warm Agentic Search caches with representative queries.")
    parser.add_argument("--queries", default="benchmarks/queries.json", help="JSON file containing warm-up queries.")
    parser.add_argument("--prefer-live", action="store_true", help="Bypass the query cache during warm-up.")
    parser.add_argument("--run-mode", default="balanced", choices=["fast", "balanced", "deep"], help="Pipeline run mode.")
    args = parser.parse_args()
    asyncio.run(main_async(args.queries, args.prefer_live, args.run_mode))


if __name__ == "__main__":
    main()
