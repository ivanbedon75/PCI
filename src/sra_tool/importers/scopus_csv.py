from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_scopus_csv(path: str) -> pd.DataFrame:
    csv_path = Path(path)
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    df["__source_file__"] = str(csv_path)
    return df