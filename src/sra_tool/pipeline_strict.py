from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .clients.openalex import get_repo_root, validate_and_save_openalex_input


ProgressCallback = Optional[Callable[[str], None]]


# =========================================================
# Paths
# =========================================================

def get_data_dir() -> Path:
    return get_repo_root() / "data"


def get_raw_dir() -> Path:
    return get_data_dir() / "raw"


def get_raw_scopus_dir() -> Path:
    path = get_raw_dir() / "scopus"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_processed_dir() -> Path:
    path = get_data_dir() / "processed"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_state_path() -> Path:
    return get_processed_dir() / "pipeline_state.json"


def get_protocol_path() -> Path:
    return get_processed_dir() / "protocol.json"


# =========================================================
# Utilities
# =========================================================

def emit(progress: ProgressCallback, message: str) -> None:
    if progress:
        progress(message)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return " ".join(text.split())


def normalize_lower(value) -> str:
    return normalize_text(value).lower()


def safe_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series([""] * len(df), index=df.index)


def first_non_empty(row: pd.Series, candidates: List[str]) -> str:
    for col in candidates:
        if col in row.index:
            val = normalize_text(row[col])
            if val:
                return val
    return ""


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def read_json(path: Path, default: Optional[dict] = None) -> dict:
    if not path.exists():
        return {} if default is None else default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def append_jsonl(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def dataframe_to_md_or_text(df: pd.DataFrame) -> str:
    if df.empty:
        return "No available data."
    try:
        return df.to_markdown(index=False)
    except Exception:
        return df.to_string(index=False)


def load_df_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, encoding="utf-8-sig")


def display_value(value) -> str:
    return "Sin evaluación aún" if value is None else str(value)


def ensure_required_columns(df: pd.DataFrame, required_columns: List[str], label: str) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise RuntimeError(f"{label} no contiene las columnas requeridas: {', '.join(missing)}")


def copy_source_file(src: Path, destination_name: str) -> Path:
    dst = get_raw_scopus_dir() / destination_name
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return dst


def audit_event(stage: str, event: str, details: Optional[dict] = None) -> None:
    audit_path = get_processed_dir() / "audit_trail.jsonl"
    payload = {
        "timestamp": now_iso(),
        "stage": stage,
        "event": event,
    }
    if details:
        payload.update(details)
    append_jsonl(audit_path, payload)