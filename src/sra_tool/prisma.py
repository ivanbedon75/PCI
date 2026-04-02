from __future__ import annotations

import pandas as pd


def build_prisma_counts(
    raw_df: pd.DataFrame,
    duplicates_df: pd.DataFrame,
    dedup_df: pd.DataFrame,
    screening_df: pd.DataFrame | None = None,
) -> dict:
    source_counts = (
        raw_df["source"].value_counts(dropna=False).sort_index().to_dict()
        if not raw_df.empty and "source" in raw_df.columns
        else {}
    )

    screened = int(len(dedup_df))
    full_text = 0
    included = 0
    excluded_title_abstract = 0

    if screening_df is not None and not screening_df.empty:
        final = screening_df["final_decision"].fillna("").astype(str).str.strip().str.lower()
        included = int((final == "include").sum())
        excluded_title_abstract = int((final == "exclude").sum())
        full_text = int((final == "full_text").sum())

    return {
        "identification": {
            "records_identified_total": int(len(raw_df)),
            "records_identified_by_source": source_counts,
        },
        "deduplication": {
            "duplicates_removed": int(len(duplicates_df)),
            "records_after_deduplication": int(len(dedup_df)),
        },
        "screening": {
            "records_screened": screened,
            "records_excluded_title_abstract": excluded_title_abstract,
            "reports_sought_for_retrieval": full_text,
            "studies_included": included,
        },
    }