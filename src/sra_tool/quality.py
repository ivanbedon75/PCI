from __future__ import annotations

from typing import Any

import pandas as pd


def completion_rate(df: pd.DataFrame, column: str) -> float:
    if df.empty or column not in df.columns:
        return 0.0
    series = df[column].fillna("").astype(str).str.strip()
    return round(float((series != "").sum()) / float(len(df)), 4)


def build_quality_profile(
    raw_df: pd.DataFrame,
    dedup_df: pd.DataFrame,
    protocol: dict[str, Any],
) -> dict[str, Any]:
    minimums = protocol.get("minimum_completion_rate", {})
    field_profile = {}

    all_fields = sorted(set(list(minimums.keys()) + list(raw_df.columns)))
    for field in all_fields:
        if field in raw_df.columns or field in minimums:
            field_profile[field] = {
                "raw_completion_rate": completion_rate(raw_df, field),
                "deduplicated_completion_rate": completion_rate(dedup_df, field),
                "minimum_required_rate": minimums.get(field),
            }

    source_counts = (
        dedup_df["source"].value_counts(dropna=False).sort_index().to_dict()
        if not dedup_df.empty and "source" in dedup_df.columns
        else {}
    )

    protocol_pass = True
    failures = []
    for field, minimum in minimums.items():
        actual = completion_rate(dedup_df, field)
        if actual < float(minimum):
            protocol_pass = False
            failures.append(
                {
                    "field": field,
                    "actual_completion_rate": actual,
                    "minimum_required_rate": minimum,
                }
            )

    return {
        "raw_record_count": int(len(raw_df)),
        "deduplicated_record_count": int(len(dedup_df)),
        "duplicate_records_removed": int(len(raw_df) - len(dedup_df)),
        "source_distribution_after_dedup": source_counts,
        "field_profile": field_profile,
        "protocol_pass": protocol_pass,
        "protocol_failures": failures,
    }