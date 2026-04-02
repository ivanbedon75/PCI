from __future__ import annotations
from typing import Any

def calculate_corpus_integrity_metrics(
    records: list[dict[str, str]],
) -> dict[str, Any]:
    total = len(records)
    if total == 0:
        return {
            "total_records": 0,
            "doi_completion_rate": 0.0,
            "title_completion_rate": 0.0,
            "year_completion_rate": 0.0,
            "source_completion_rate": 0.0,
            "abstract_completion_rate": 0.0,
        }
    def completion_rate(column: str) -> float:
        non_empty = 0
        for record in records:
            value = str(record.get(column, "")).strip()
            if value and value.upper() != "NA":
                non_empty += 1
        return round(non_empty / total, 4)
    return {
        "total_records": total,
        "doi_completion_rate": completion_rate("DOI"),
        "title_completion_rate": completion_rate("Title"),
        "year_completion_rate": completion_rate("Year"),
        "source_completion_rate": completion_rate("Source title"),
        "abstract_completion_rate": completion_rate("Abstract"),
    }

def summarize_missing_critical_fields(
    records: list[dict[str, str]],
) -> dict[str, int]:
    critical_fields = ["Title", "Year", "Source title", "DOI", "Abstract"]
    summary: dict[str, int] = {}
    for field in critical_fields:
        missing = 0
        for record in records:
            value = str(record.get(field, "")).strip()
            if not value or value.upper() == "NA":
                missing += 1
        summary[field] = missing
    return summary
