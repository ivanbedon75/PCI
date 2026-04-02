from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from .constants import OUTPUT_FILES
from .utils import ensure_dir, sha256_file, stable_json_dumps, write_json


def export_dataframe_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False, encoding="utf-8")


def export_dataframe_parquet(df: pd.DataFrame, path: Path) -> str | None:
    try:
        df.to_parquet(path, index=False)
        return str(path)
    except Exception:
        return None


def export_sqlite(raw_df: pd.DataFrame, dedup_df: pd.DataFrame, path: Path) -> None:
    with sqlite3.connect(path) as conn:
        raw_df.to_sql("raw_records", conn, if_exists="replace", index=False)
        dedup_df.to_sql("deduplicated_records", conn, if_exists="replace", index=False)


def build_manifest(
    output_dir: Path,
    run_summary: dict[str, Any],
    protocol: dict[str, Any],
) -> dict[str, Any]:
    files = {}
    for file_path in sorted(output_dir.iterdir(), key=lambda p: p.name):
        if file_path.is_file():
            files[file_path.name] = {
                "sha256": sha256_file(file_path),
                "size_bytes": file_path.stat().st_size,
            }

    return {
        "run_summary": run_summary,
        "protocol": protocol,
        "files": files,
    }


def export_all_outputs(
    output_dir: str,
    raw_df: pd.DataFrame,
    dedup_df: pd.DataFrame,
    quality_profile: dict[str, Any],
    screening_df: pd.DataFrame,
    prisma_counts: dict[str, Any],
    manuscript_tables_md: str,
    run_summary: dict[str, Any],
    protocol: dict[str, Any],
) -> dict[str, str | None]:
    out_dir = ensure_dir(Path(output_dir))

    raw_json_path = out_dir / OUTPUT_FILES["raw_json"]
    harmonized_csv_path = out_dir / OUTPUT_FILES["harmonized_csv"]
    dedup_csv_path = out_dir / OUTPUT_FILES["deduplicated_csv"]
    quality_json_path = out_dir / OUTPUT_FILES["quality_json"]
    screening_csv_path = out_dir / OUTPUT_FILES["screening_csv"]
    prisma_json_path = out_dir / OUTPUT_FILES["prisma_json"]
    manuscript_md_path = out_dir / OUTPUT_FILES["manuscript_md"]
    sqlite_path = out_dir / OUTPUT_FILES["sqlite_db"]
    parquet_path = out_dir / OUTPUT_FILES["parquet_file"]
    manifest_path = out_dir / OUTPUT_FILES["manifest_json"]

    raw_json_path.write_text(
        stable_json_dumps(raw_df.fillna("").to_dict(orient="records")),
        encoding="utf-8",
    )
    export_dataframe_csv(raw_df, harmonized_csv_path)
    export_dataframe_csv(dedup_df, dedup_csv_path)
    write_json(quality_json_path, quality_profile)
    screening_df.to_csv(screening_csv_path, index=False, encoding="utf-8")
    write_json(prisma_json_path, prisma_counts)
    manuscript_md_path.write_text(manuscript_tables_md, encoding="utf-8")
    export_sqlite(raw_df, dedup_df, sqlite_path)

    parquet_result = export_dataframe_parquet(dedup_df, parquet_path)

    manifest = build_manifest(out_dir, run_summary, protocol)
    write_json(manifest_path, manifest)

    return {
        "raw_json_path": str(raw_json_path),
        "harmonized_csv_path": str(harmonized_csv_path),
        "deduplicated_csv_path": str(dedup_csv_path),
        "quality_json_path": str(quality_json_path),
        "screening_csv_path": str(screening_csv_path),
        "prisma_json_path": str(prisma_json_path),
        "manuscript_md_path": str(manuscript_md_path),
        "sqlite_db_path": str(sqlite_path),
        "parquet_path": parquet_result,
        "manifest_path": str(manifest_path),
    }