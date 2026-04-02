from __future__ import annotations

from typing import Any

import pandas as pd

from .utils import normalize_title


def _metadata_score(row: pd.Series) -> int:
    score = 0
    for field in ["doi", "title", "year", "journal", "abstract", "authors", "issn"]:
        if str(row.get(field, "")).strip():
            score += 1
    return score


def deduplicate_records(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        empty = df.copy()
        return empty, empty

    work = df.copy()
    work["normalized_title"] = work["title"].apply(normalize_title)
    work["metadata_score"] = work.apply(_metadata_score, axis=1)
    work["dedup_key"] = ""

    doi_mask = work["doi"].fillna("").astype(str).str.strip() != ""
    work.loc[doi_mask, "dedup_key"] = "doi::" + work.loc[doi_mask, "doi"].astype(str)

    no_doi_mask = ~doi_mask
    work.loc[no_doi_mask, "dedup_key"] = "title::" + work.loc[no_doi_mask, "normalized_title"].astype(str)

    work = work.sort_values(
        by=["dedup_key", "metadata_score", "year", "source"],
        ascending=[True, False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    duplicate_flags = work.duplicated(subset=["dedup_key"], keep="first")
    duplicates = work.loc[duplicate_flags].copy().reset_index(drop=True)
    deduplicated = work.loc[~duplicate_flags].copy().reset_index(drop=True)

    for temp_col in ["normalized_title", "metadata_score", "dedup_key"]:
        if temp_col in deduplicated.columns:
            pass

    return deduplicated, duplicates