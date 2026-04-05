from __future__ import annotations

import csv
from io import StringIO

from app.models import OutputCell, SearchResponse


def _collect_source_urls(cells: list[OutputCell | None]) -> str:
    seen: list[str] = []
    for cell in cells:
        if cell is None:
            continue
        for source in cell.sources:
            if source.source_url and source.source_url not in seen:
                seen.append(source.source_url)
    return " | ".join(seen[:5])


def response_to_flat_rows(response: SearchResponse) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in response.rows:
        flat_row: dict[str, str] = {
            "entity_id": row.entity_id,
            "aggregate_score": str(row.aggregate_score),
            "supporting_source_count": str(row.supporting_source_count),
            "confidence_score": str(row.confidence_score),
            "verification_status": row.verification_status,
            "rank_explanation": row.rank_explanation,
            "freshness_date": row.freshness_date or "",
        }
        source_cells: list[OutputCell | None] = [row.name, row.entity_type, row.summary, row.homepage]

        for column in response.columns:
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
            flat_row[column] = cell.value if cell else ""
            if cell is not None:
                source_cells.append(cell)

        flat_row["source_urls"] = _collect_source_urls(source_cells)
        rows.append(flat_row)
    return rows


def response_to_csv(response: SearchResponse) -> str:
    flat_rows = response_to_flat_rows(response)
    fieldnames = [
        "entity_id",
        *response.columns,
        "aggregate_score",
        "confidence_score",
        "verification_status",
        "freshness_date",
        "rank_explanation",
        "supporting_source_count",
        "source_urls",
    ]
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in flat_rows:
        writer.writerow(row)
    return buffer.getvalue()
