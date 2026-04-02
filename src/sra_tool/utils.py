from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def stable_json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(stable_json_dumps(payload), encoding="utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def normalize_doi(value: Any) -> str:
    if value is None:
        return ""
    doi = str(value).strip().lower()
    doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    doi = doi.replace("doi:", "").strip()
    return doi


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_title(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).lower()
    text = normalize_whitespace(text)
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = normalize_whitespace(text)
    return text


def deterministic_sort_df(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["doi", "title", "year", "source", "source_record_id"]:
        if col not in df.columns:
            df[col] = ""
    return (
        df.fillna("")
        .sort_values(
            by=["doi", "title", "year", "source", "source_record_id"],
            kind="mergesort",
        )
        .reset_index(drop=True)
    )


def environment_snapshot() -> dict[str, Any]:
    return {
        "python_version": sys.version,
        "platform": platform.platform(),
        "cwd": os.getcwd(),
    }


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    clean_df = df.where(pd.notnull(df), "")
    return clean_df.to_dict(orient="records")