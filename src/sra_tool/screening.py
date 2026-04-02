from __future__ import annotations

import pandas as pd


def build_screening_matrix(df: pd.DataFrame, minimum_reviewers: int = 2) -> pd.DataFrame:
    records = df.copy()

    if "record_id" not in records.columns:
        records["record_id"] = [
            f"R{i+1:06d}" for i in range(len(records))
        ]

    base = pd.DataFrame(
        {
            "record_id": records["record_id"],
            "title": records["title"],
            "doi": records["doi"],
            "journal": records["journal"],
            "year": records["year"],
            "reviewer_1_decision": "",
            "reviewer_1_reason": "",
            "reviewer_2_decision": "" if minimum_reviewers >= 2 else "",
            "reviewer_2_reason": "" if minimum_reviewers >= 2 else "",
            "conflict": "",
            "final_decision": "",
            "final_reason": "",
        }
    )

    return base