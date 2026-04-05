from __future__ import annotations

import argparse
import json
from statistics import mean
from urllib.parse import quote

import httpx


def normalize(value: str) -> str:
    return " ".join(value.lower().split())


def load_goldens(path: str) -> list[dict[str, object]]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return [item for item in payload if isinstance(item, dict)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate Agentic Search against golden query fixtures.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--fixtures", default="benchmarks/goldens.json")
    parser.add_argument("--run-mode", default="balanced", choices=["fast", "balanced", "deep"])
    parser.add_argument("--prefer-live", action="store_true")
    args = parser.parse_args()

    fixtures = load_goldens(args.fixtures)
    scores: list[float] = []
    report: list[dict[str, object]] = []

    with httpx.Client(timeout=180.0) as client:
        for fixture in fixtures:
            query = str(fixture["query"])
            expected_names = [normalize(str(item)) for item in fixture.get("expected_names", [])]
            url = f"{args.base_url}/api/search?query={quote(query)}&run_mode={args.run_mode}"
            if args.prefer_live:
                url += "&prefer_live=true"
            response = client.get(url)
            response.raise_for_status()
            payload = response.json()
            actual_names = [normalize(row.get("name", {}).get("value", "")) for row in payload.get("rows", [])]
            hits = sum(1 for name in expected_names if any(name in actual for actual in actual_names))
            coverage = hits / max(len(expected_names), 1)
            scores.append(coverage)
            report.append(
                {
                    "query": query,
                    "coverage": round(coverage, 3),
                    "expected": expected_names,
                    "actual_top_names": actual_names[:10],
                }
            )

    print(json.dumps(report, indent=2))
    print(f"\nAverage coverage: {mean(scores):.3f}" if scores else "\nAverage coverage: 0.000")


if __name__ == "__main__":
    main()
