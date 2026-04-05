from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from app.models import OutputCell, SavedReportSummary, SearchResponse


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "report"


def _cell_value(cell: OutputCell | None) -> str:
    if cell is None:
        return ""
    return cell.value.strip()


def _report_title(response: SearchResponse) -> str:
    return f"{response.query} ({response.run_mode.title()})"


def response_to_markdown(response: SearchResponse) -> str:
    columns = response.columns or ["name", "entity_type", "summary", "homepage"]
    header = ["Rank", *[column.replace("_", " ").title() for column in columns], "Score", "Sources"]
    table_lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(["---"] * len(header)) + " |",
    ]

    for index, row in enumerate(response.rows, start=1):
        source_count = str(row.supporting_source_count)
        values: list[str] = [str(index)]
        for column in columns:
            if column == "name":
                cell = row.name
            elif column == "entity_type":
                cell = row.entity_type
            elif column == "summary":
                cell = row.summary
            elif column == "homepage":
                cell = row.homepage
            else:
                cell = row.attributes.get(column)
            values.append(_cell_value(cell).replace("|", "\\|"))
        values.append(f"{row.aggregate_score:.3f}")
        values.append(source_count)
        table_lines.append("| " + " | ".join(values) + " |")

    citation_lines: list[str] = []
    for row in response.rows[:10]:
        citation_lines.append(f"## {row.name.value}")
        citation_lines.append("")
        citation_lines.append(f"- Entity type: {row.entity_type.value}")
        citation_lines.append(f"- Aggregate score: {row.aggregate_score:.3f}")
        citation_lines.append(f"- Confidence score: {row.confidence_score:.3f}")
        citation_lines.append(f"- Verification: {row.verification_status}")
        citation_lines.append(f"- Supporting domains: {row.supporting_source_count}")
        if row.highlights:
            citation_lines.append(f"- Highlights: {'; '.join(row.highlights[:3])}")
        if row.rank_explanation:
            citation_lines.append(f"- Ranking rationale: {row.rank_explanation}")
        for label, cell in [("Summary", row.summary), ("Homepage", row.homepage)]:
            value = _cell_value(cell)
            if value:
                citation_lines.append(f"- {label}: {value}")
        for attribute_name, cell in list(row.attributes.items())[:8]:
            citation_lines.append(f"- {attribute_name.replace('_', ' ').title()}: {_cell_value(cell)}")
        if row.name.sources:
            citation_lines.append("- Evidence:")
            for source in row.name.sources[:3]:
                label = source.source_title or source.source_url
                quote = f' "{source.quote}"' if source.quote else ""
                citation_lines.append(f"  - {label}: {source.source_url}{quote}")
        citation_lines.append("")

    metrics = response.metrics
    metrics_lines = [
        f"- Query type: {response.query_type}",
        f"- Run mode: {response.run_mode}",
        f"- Entities: {len(response.rows)}",
        f"- Sources considered: {response.raw_sources_considered}",
    ]
    if metrics is not None:
        metrics_lines.extend(
            [
                f"- Total time: {metrics.total_ms} ms",
                f"- Search time: {metrics.search_ms} ms",
                f"- Scrape time: {metrics.scrape_ms} ms",
                f"- Extract time: {metrics.extract_ms} ms",
                f"- Cache tier: {metrics.cache_tier}",
                f"- Fallback rows added: {metrics.fallback_rows_added}",
            ]
        )

    parts = [
        f"# Agentic Search Report: {response.query}",
        "",
        "## Summary",
        "",
        *metrics_lines,
        "",
        "## Results",
        "",
        *table_lines,
        "",
        "## Entity Notes",
        "",
        *citation_lines,
    ]
    return "\n".join(parts).strip() + "\n"


class ReportStore:
    def __init__(self, directory: str, max_reports: int = 25):
        self.root = Path(directory)
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_reports = max(1, max_reports)

    def _json_path(self, report_id: str) -> Path:
        return self.root / f"{report_id}.json"

    def _markdown_path(self, report_id: str) -> Path:
        return self.root / f"{report_id}.md"

    def _build_summary(self, report_id: str, response: SearchResponse, created_at: str) -> SavedReportSummary:
        return SavedReportSummary(
            report_id=report_id,
            title=response.report_title or _report_title(response),
            query=response.query,
            query_type=response.query_type,
            run_mode=response.run_mode,
            created_at=created_at,
            entity_count=len(response.rows),
            json_url=f"/api/reports/{report_id}.json",
            markdown_url=f"/api/reports/{report_id}.md",
        )

    def save(self, response: SearchResponse) -> SavedReportSummary:
        created_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        report_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{_slugify(response.query)[:48]}"

        self._json_path(report_id).write_text(
            json.dumps(response.model_dump(), indent=2, ensure_ascii=True),
            encoding="utf-8",
        )
        self._markdown_path(report_id).write_text(
            response_to_markdown(response),
            encoding="utf-8",
        )
        self._prune()
        return self._build_summary(report_id, response, created_at)

    def list_reports(self, limit: int = 12) -> list[SavedReportSummary]:
        reports: list[SavedReportSummary] = []
        for json_path in sorted(self.root.glob("*.json"), reverse=True):
            try:
                payload = json.loads(json_path.read_text(encoding="utf-8"))
                response = SearchResponse.model_validate(payload)
                created_at = datetime.fromtimestamp(
                    json_path.stat().st_mtime,
                    tz=timezone.utc,
                ).replace(microsecond=0).isoformat()
                reports.append(self._build_summary(json_path.stem, response, created_at))
            except Exception:
                continue
            if len(reports) >= limit:
                break
        return reports

    def json_path(self, report_id: str) -> Path:
        path = self._json_path(report_id)
        if not path.exists():
            raise FileNotFoundError(report_id)
        return path

    def markdown_path(self, report_id: str) -> Path:
        path = self._markdown_path(report_id)
        if not path.exists():
            raise FileNotFoundError(report_id)
        return path

    def _prune(self) -> None:
        json_reports = sorted(self.root.glob("*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
        stale_json = json_reports[self.max_reports :]
        for json_path in stale_json:
            markdown_path = self._markdown_path(json_path.stem)
            json_path.unlink(missing_ok=True)
            markdown_path.unlink(missing_ok=True)
