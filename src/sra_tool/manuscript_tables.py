from __future__ import annotations

from typing import Any


def _markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    header_line = "| " + " | ".join(headers) + " |"
    separator = "| " + " | ".join(["---"] * len(headers)) + " |"
    row_lines = ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]
    return "\n".join([header_line, separator, *row_lines])


def build_manuscript_tables(
    quality_profile: dict[str, Any],
    prisma_counts: dict[str, Any],
) -> str:
    source_rows = []
    for source, count in sorted(
        quality_profile.get("source_distribution_after_dedup", {}).items()
    ):
        source_rows.append([source, count])

    field_rows = []
    field_profile = quality_profile.get("field_profile", {})
    for field, metrics in sorted(field_profile.items()):
        field_rows.append(
            [
                field,
                metrics.get("raw_completion_rate", ""),
                metrics.get("deduplicated_completion_rate", ""),
                metrics.get("minimum_required_rate", ""),
            ]
        )

    prisma = prisma_counts
    prisma_rows = [
        ["records_identified_total", prisma["identification"]["records_identified_total"]],
        ["duplicates_removed", prisma["deduplication"]["duplicates_removed"]],
        ["records_after_deduplication", prisma["deduplication"]["records_after_deduplication"]],
        ["records_screened", prisma["screening"]["records_screened"]],
        ["records_excluded_title_abstract", prisma["screening"]["records_excluded_title_abstract"]],
        ["reports_sought_for_retrieval", prisma["screening"]["reports_sought_for_retrieval"]],
        ["studies_included", prisma["screening"]["studies_included"]],
    ]

    sections = [
        "# Manuscript Tables",
        "",
        "## Table 1. Source distribution after deduplication",
        _markdown_table(["Source", "Count"], source_rows or [["NA", 0]]),
        "",
        "## Table 2. Metadata completion profile",
        _markdown_table(
            ["Field", "Raw completion", "Deduplicated completion", "Minimum required"],
            field_rows or [["NA", "", "", ""]],
        ),
        "",
        "## Table 3. PRISMA counts",
        _markdown_table(["Metric", "Value"], prisma_rows),
        "",
    ]
    return "\n".join(sections)