from __future__ import annotations

import argparse
import json
from statistics import mean
from time import perf_counter
from urllib.parse import quote

import httpx


def load_queries(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return [str(item).strip() for item in payload if str(item).strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Agentic Search queries against a running server.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Base URL for the running API.")
    parser.add_argument("--queries", default="benchmarks/queries.json", help="JSON file containing benchmark queries.")
    parser.add_argument("--prefer-live", action="store_true", help="Bypass the query cache when benchmarking.")
    parser.add_argument("--run-mode", default="balanced", choices=["fast", "balanced", "deep"], help="Pipeline run mode.")
    args = parser.parse_args()

    queries = load_queries(args.queries)
    results: list[dict[str, object]] = []

    with httpx.Client(timeout=120.0) as client:
        for query in queries:
            url = f"{args.base_url}/api/search?query={quote(query)}&run_mode={args.run_mode}"
            if args.prefer_live:
                url += "&prefer_live=true"

            started = perf_counter()
            response = client.get(url)
            elapsed_ms = int((perf_counter() - started) * 1000)
            response.raise_for_status()
            payload = response.json()
            metrics = payload.get("metrics") or {}
            results.append(
                {
                    "query": query,
                    "elapsed_ms": elapsed_ms,
                    "total_ms": metrics.get("total_ms", 0),
                    "rows": len(payload.get("rows") or []),
                    "sources": payload.get("raw_sources_considered", 0),
                    "cache_tier": metrics.get("cache_tier", "live"),
                }
            )

    print(json.dumps(results, indent=2))
    if results:
        avg = mean(item["total_ms"] for item in results if isinstance(item["total_ms"], int))
        print(f"\nAverage pipeline total_ms: {avg:.1f}")


if __name__ == "__main__":
    main()
