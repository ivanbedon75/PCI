from __future__ import annotations

import hashlib
import json
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


# =========================================================
# Data model
# =========================================================

@dataclass
class PipelineStats:
    records_identified_total: int = 0
    records_scopus_core: int = 0
    records_scopus_exploratory: int = 0
    records_openalex: int = 0

    duplicates_removed: int = 0
    records_after_deduplication: int = 0

    records_screened_title_abstract: Optional[int] = None
    records_excluded_title_abstract: Optional[int] = None

    reports_sought_for_retrieval: Optional[int] = None
    reports_not_retrieved: Optional[int] = None

    reports_assessed_for_eligibility: Optional[int] = None
    reports_excluded_full_text: Optional[int] = None

    studies_included_review: Optional[int] = None
    reports_included_review: Optional[int] = None

    doi_coverage_pct: float = 0.0
    abstract_coverage_pct: float = 0.0
    year_min: Optional[int] = None
    year_max: Optional[int] = None


# =========================================================
# State / protocol
# =========================================================

def load_pipeline_state() -> dict:
    return read_json(
        get_state_path(),
        default={"completed_stages": [], "artifacts": {}, "stats": {}, "sources": {}},
    )


def save_pipeline_state(state: dict) -> None:
    write_json(get_state_path(), state)


def mark_stage_completed(stage_name: str, state: Optional[dict] = None) -> dict:
    state = load_pipeline_state() if state is None else state
    completed = set(state.get("completed_stages", []))
    completed.add(stage_name)
    state["completed_stages"] = sorted(completed)
    save_pipeline_state(state)
    return state


def save_protocol(protocol: dict) -> Path:
    path = get_protocol_path()
    write_json(path, protocol)
    return path


def clear_statistical_data() -> None:
    processed_dir = get_processed_dir()
    state = load_pipeline_state()

    artifact_keys_to_remove = [
        "harmonized_records_csv",
        "deduplicated_records_csv",
        "deduplication_groups_csv",
        "screening_matrix_csv",
        "quality_profile_json",
        "prisma_counts_json",
        "prisma_diagram_png",
        "prisma_diagram_svg",
        "manuscript_tables_md",
        "records_sqlite",
        "manifest_json",
        "reproducibility_package_zip",
    ]

    for key in artifact_keys_to_remove:
        raw_path = state.get("artifacts", {}).get(key)
        if raw_path:
            path = Path(raw_path)
            if path.exists():
                try:
                    path.unlink()
                except IsADirectoryError:
                    pass

    optional_files = [
        processed_dir / "harmonized_records.csv",
        processed_dir / "deduplicated_records.csv",
        processed_dir / "deduplication_groups.csv",
        processed_dir / "screening_matrix.csv",
        processed_dir / "quality_profile.json",
        processed_dir / "prisma_counts.json",
        processed_dir / "prisma_diagram.png",
        processed_dir / "prisma_diagram.svg",
        processed_dir / "manuscript_tables.md",
        processed_dir / "records.sqlite",
        processed_dir / "manifest.json",
        processed_dir / "reproducibility_package.zip",
        processed_dir / "audit_trail.jsonl",
        processed_dir / "run_log.jsonl",
    ]
    for path in optional_files:
        if path.exists():
            path.unlink()

    state["stats"] = {}
    state["sources"] = {}
    state["artifacts"] = {
        key: value
        for key, value in state.get("artifacts", {}).items()
        if key not in artifact_keys_to_remove
    }
    state["completed_stages"] = [
        stage for stage in state.get("completed_stages", [])
        if stage == "stage_1_pico"
    ]
    save_pipeline_state(state)


# =========================================================
# Source loading
# =========================================================

def load_scopus_csv(csv_path: Path, source_label: str, search_group: str) -> pd.DataFrame:
    if not csv_path.exists():
        return pd.DataFrame()

    df = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            df = pd.read_csv(csv_path, encoding=encoding)
            break
        except Exception:
            df = None

    if df is None:
        raise RuntimeError(f"No se pudo leer el CSV de Scopus: {csv_path}")

    df["source_db"] = source_label
    df["search_group"] = search_group
    df["raw_source_file"] = str(csv_path)
    return df


def load_openalex_csv(csv_path: Optional[str]) -> pd.DataFrame:
    if not csv_path:
        return pd.DataFrame()
    path = Path(csv_path)
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, encoding="utf-8-sig")
    df["source_db"] = "openalex"
    df["search_group"] = "openalex"
    df["raw_source_file"] = str(path)
    return df


# =========================================================
# Harmonization
# =========================================================

def harmonized_columns() -> List[str]:
    return [
        "record_id",
        "source_db",
        "search_group",
        "title",
        "title_norm",
        "authors",
        "authors_norm",
        "year",
        "journal",
        "abstract",
        "abstract_present",
        "keywords",
        "doi",
        "doi_norm",
        "document_type",
        "language",
        "affiliations",
        "citations",
        "openalex_id",
        "raw_source_file",
    ]


def harmonize_scopus(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=harmonized_columns())

    rows = []
    for _, row in df.iterrows():
        title = first_non_empty(row, ["Title"])
        authors = first_non_empty(row, ["Authors", "Author full names", "Author(s)"])
        abstract = first_non_empty(row, ["Abstract"])
        doi = normalize_lower(first_non_empty(row, ["DOI"]))
        journal = first_non_empty(row, ["Source title"])

        try:
            year_val = first_non_empty(row, ["Year"])
            year = int(float(year_val)) if year_val else None
        except Exception:
            year = None

        try:
            cited_val = first_non_empty(row, ["Cited by"])
            citations = int(float(cited_val)) if cited_val else 0
        except Exception:
            citations = 0

        auth_kw = first_non_empty(row, ["Author Keywords"])
        idx_kw = first_non_empty(row, ["Index Keywords"])
        keywords = " | ".join([x for x in [auth_kw, idx_kw] if x])

        rows.append({
            "record_id": "",
            "source_db": normalize_text(row.get("source_db", "scopus")),
            "search_group": normalize_text(row.get("search_group", "")),
            "title": title,
            "title_norm": normalize_lower(title),
            "authors": authors,
            "authors_norm": normalize_lower(authors),
            "year": year,
            "journal": journal,
            "abstract": abstract,
            "abstract_present": bool(normalize_text(abstract)),
            "keywords": keywords,
            "doi": doi,
            "doi_norm": doi,
            "document_type": normalize_lower(first_non_empty(row, ["Document Type"])),
            "language": normalize_lower(first_non_empty(row, ["Language of Original Document", "Language"])),
            "affiliations": first_non_empty(row, ["Affiliations"]),
            "citations": citations,
            "openalex_id": "",
            "raw_source_file": normalize_text(row.get("raw_source_file", "")),
        })

    out = pd.DataFrame(rows)
    out["record_id"] = [f"REC-{i+1:06d}" for i in range(len(out))]
    return out[harmonized_columns()]


def harmonize_openalex(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=harmonized_columns())

    rows = []
    for _, row in df.iterrows():
        title = normalize_text(row.get("title", ""))
        doi = normalize_lower(row.get("doi", ""))
        abstract_present = bool(row.get("abstract_inverted_index_present", False))

        try:
            year = int(float(row.get("publication_year"))) if pd.notna(row.get("publication_year")) else None
        except Exception:
            year = None

        rows.append({
            "record_id": "",
            "source_db": normalize_text(row.get("source_db", "openalex")),
            "search_group": normalize_text(row.get("search_group", "openalex")),
            "title": title,
            "title_norm": normalize_lower(title),
            "authors": normalize_text(row.get("authors", "")),
            "authors_norm": normalize_lower(row.get("authors", "")),
            "year": year,
            "journal": normalize_text(row.get("source_display_name", "")),
            "abstract": "",
            "abstract_present": abstract_present,
            "keywords": normalize_text(row.get("keywords", "")),
            "doi": doi,
            "doi_norm": doi,
            "document_type": normalize_lower(row.get("type", "")),
            "language": normalize_lower(row.get("language", "")),
            "affiliations": normalize_text(row.get("institutions", "")),
            "citations": int(row.get("cited_by_count", 0)) if pd.notna(row.get("cited_by_count")) else 0,
            "openalex_id": normalize_text(row.get("id", "")),
            "raw_source_file": normalize_text(row.get("raw_source_file", "")),
        })

    out = pd.DataFrame(rows)
    out["record_id"] = [f"REC-{i+1:06d}" for i in range(len(out))]
    return out[harmonized_columns()]


def harmonize_sources(scopus_core_df: pd.DataFrame, scopus_exploratory_df: pd.DataFrame, openalex_df: pd.DataFrame) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []

    if not scopus_core_df.empty:
        parts.append(harmonize_scopus(scopus_core_df))
    if not scopus_exploratory_df.empty:
        parts.append(harmonize_scopus(scopus_exploratory_df))
    if not openalex_df.empty:
        parts.append(harmonize_openalex(openalex_df))

    if not parts:
        return pd.DataFrame(columns=harmonized_columns())

    out = pd.concat(parts, ignore_index=True)
    out["record_id"] = [f"REC-{i+1:06d}" for i in range(len(out))]
    return out[harmonized_columns()]


# =========================================================
# Deduplication
# =========================================================

def build_dedup_key(row: pd.Series) -> str:
    doi = normalize_lower(row.get("doi_norm", ""))
    title = normalize_lower(row.get("title_norm", ""))
    year = normalize_text(row.get("year", ""))
    authors = normalize_lower(row.get("authors_norm", ""))

    if doi:
        return f"doi::{doi}"
    if title and year:
        return f"title_year::{title}::{year}"
    return f"title_authors::{title}::{authors}"


def source_priority(source_db: str, search_group: str) -> int:
    source_db = normalize_lower(source_db)
    search_group = normalize_lower(search_group)

    if source_db == "scopus" and search_group == "core":
        return 1
    if source_db == "scopus" and search_group == "exploratory":
        return 2
    if source_db == "openalex":
        return 3
    return 9


def deduplicate_records(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df.copy(), pd.DataFrame(columns=["dedup_key", "n_records", "record_ids", "sources"])

    work = df.copy()
    work["dedup_key"] = work.apply(build_dedup_key, axis=1)
    work["priority"] = work.apply(lambda r: source_priority(r["source_db"], r["search_group"]), axis=1)
    work["title_len"] = safe_series(work, "title").astype(str).str.len()

    work = work.sort_values(
        by=["priority", "abstract_present", "doi_norm", "title_len"],
        ascending=[True, False, False, False],
    )

    dedup_groups = (
        work.groupby("dedup_key", dropna=False)
        .agg(
            n_records=("record_id", "count"),
            record_ids=("record_id", lambda x: " | ".join(x.astype(str))),
            sources=("source_db", lambda x: " | ".join(sorted(set(x.astype(str))))),
        )
        .reset_index()
    )

    deduplicated = work.drop_duplicates(subset=["dedup_key"], keep="first").copy()
    deduplicated = deduplicated.drop(columns=["priority", "title_len"])
    deduplicated = deduplicated.reset_index(drop=True)

    return deduplicated, dedup_groups


# =========================================================
# Screening matrix
# =========================================================

def build_screening_matrix(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "record_id", "title", "year", "source_db", "search_group", "doi",
        "screen_title_abstract", "reason_title_abstract",
        "retrieve_full_text", "retrieval_status",
        "screen_full_text", "reason_full_text",
        "include_final", "study_id", "notes",
    ]

    if df.empty:
        return pd.DataFrame(columns=cols)

    screening = df[["record_id", "title", "year", "source_db", "search_group", "doi"]].copy()
    screening["screen_title_abstract"] = "pending"
    screening["reason_title_abstract"] = ""
    screening["retrieve_full_text"] = "pending"
    screening["retrieval_status"] = "pending"
    screening["screen_full_text"] = "pending"
    screening["reason_full_text"] = ""
    screening["include_final"] = "pending"
    screening["study_id"] = ""
    screening["notes"] = ""
    return screening[cols]


# =========================================================
# PRISMA computation and validation
# =========================================================

def compute_prisma_counts(harmonized_df: pd.DataFrame, deduplicated_df: pd.DataFrame, screening_df: pd.DataFrame) -> PipelineStats:
    stats = PipelineStats()

    stats.records_identified_total = len(harmonized_df)
    stats.records_scopus_core = int((harmonized_df["search_group"] == "core").sum()) if "search_group" in harmonized_df.columns else 0
    stats.records_scopus_exploratory = int((harmonized_df["search_group"] == "exploratory").sum()) if "search_group" in harmonized_df.columns else 0
    stats.records_openalex = int((harmonized_df["source_db"] == "openalex").sum()) if "source_db" in harmonized_df.columns else 0

    stats.records_after_deduplication = len(deduplicated_df)
    stats.duplicates_removed = stats.records_identified_total - stats.records_after_deduplication

    if not screening_df.empty:
        stats.records_screened_title_abstract = len(screening_df)

        sta = screening_df["screen_title_abstract"].astype(str).str.lower().str.strip()
        retrieve = screening_df["retrieve_full_text"].astype(str).str.lower().str.strip()
        retrieval = screening_df["retrieval_status"].astype(str).str.lower().str.strip()
        fulltext = screening_df["screen_full_text"].astype(str).str.lower().str.strip()
        final_inc = screening_df["include_final"].astype(str).str.lower().str.strip()

        if (sta != "pending").any():
            stats.records_excluded_title_abstract = int((sta == "exclude").sum())
            stats.reports_sought_for_retrieval = int((sta == "include").sum())

        if (retrieve != "pending").any():
            stats.reports_sought_for_retrieval = int((retrieve == "yes").sum())

        if (retrieval != "pending").any():
            stats.reports_not_retrieved = int((retrieval == "not_retrieved").sum())
            retrieved_count = int((retrieval == "retrieved").sum())
            stats.reports_assessed_for_eligibility = retrieved_count

        if (fulltext != "pending").any():
            stats.reports_assessed_for_eligibility = int((fulltext.isin(["include", "exclude"])).sum())
            stats.reports_excluded_full_text = int((fulltext == "exclude").sum())

        if (final_inc != "pending").any():
            stats.studies_included_review = int((final_inc == "yes").sum())
            stats.reports_included_review = stats.studies_included_review

    if not deduplicated_df.empty:
        stats.doi_coverage_pct = round(
            (deduplicated_df["doi"].astype(str).str.strip().ne("").sum() / len(deduplicated_df)) * 100,
            2,
        ) if "doi" in deduplicated_df.columns else 0.0

        stats.abstract_coverage_pct = round(
            (deduplicated_df["abstract_present"].fillna(False).astype(bool).sum() / len(deduplicated_df)) * 100,
            2,
        ) if "abstract_present" in deduplicated_df.columns else 0.0

        years = pd.to_numeric(deduplicated_df["year"], errors="coerce").dropna()
        if not years.empty:
            stats.year_min = int(years.min())
            stats.year_max = int(years.max())

    validate_prisma_stats(stats)
    return stats


def validate_prisma_stats(stats: PipelineStats) -> None:
    if stats.records_identified_total < 0:
        raise RuntimeError("PRISMA inválido: records_identified_total < 0.")

    if stats.records_after_deduplication < 0:
        raise RuntimeError("PRISMA inválido: records_after_deduplication < 0.")

    if stats.duplicates_removed != (stats.records_identified_total - stats.records_after_deduplication):
        raise RuntimeError("PRISMA inválido: duplicates_removed no coincide con identified - deduplicated.")

    if stats.records_after_deduplication > stats.records_identified_total:
        raise RuntimeError("PRISMA inválido: records_after_deduplication > records_identified_total.")

    if stats.records_screened_title_abstract is not None:
        if stats.records_screened_title_abstract != stats.records_after_deduplication:
            raise RuntimeError("PRISMA inválido: records_screened_title_abstract debe coincidir con records_after_deduplication.")

    if stats.records_excluded_title_abstract is not None and stats.records_screened_title_abstract is not None:
        if stats.records_excluded_title_abstract > stats.records_screened_title_abstract:
            raise RuntimeError("PRISMA inválido: records_excluded_title_abstract > records_screened_title_abstract.")

    if stats.reports_sought_for_retrieval is not None and stats.records_screened_title_abstract is not None and stats.records_excluded_title_abstract is not None:
        expected_upper = stats.records_screened_title_abstract - stats.records_excluded_title_abstract
        if stats.reports_sought_for_retrieval > expected_upper:
            raise RuntimeError("PRISMA inválido: reports_sought_for_retrieval excede los registros incluidos tras screening.")

    if stats.reports_not_retrieved is not None and stats.reports_sought_for_retrieval is not None:
        if stats.reports_not_retrieved > stats.reports_sought_for_retrieval:
            raise RuntimeError("PRISMA inválido: reports_not_retrieved > reports_sought_for_retrieval.")

    if stats.reports_assessed_for_eligibility is not None and stats.reports_sought_for_retrieval is not None and stats.reports_not_retrieved is not None:
        expected_upper = stats.reports_sought_for_retrieval - stats.reports_not_retrieved
        if stats.reports_assessed_for_eligibility > expected_upper:
            raise RuntimeError("PRISMA inválido: reports_assessed_for_eligibility excede los reports recuperados.")

    if stats.reports_excluded_full_text is not None and stats.reports_assessed_for_eligibility is not None:
        if stats.reports_excluded_full_text > stats.reports_assessed_for_eligibility:
            raise RuntimeError("PRISMA inválido: reports_excluded_full_text > reports_assessed_for_eligibility.")

    if stats.studies_included_review is not None and stats.reports_assessed_for_eligibility is not None and stats.reports_excluded_full_text is not None:
        expected_upper = stats.reports_assessed_for_eligibility - stats.reports_excluded_full_text
        if stats.studies_included_review > expected_upper:
            raise RuntimeError("PRISMA inválido: studies_included_review excede reports elegibles tras full text.")


def build_quality_profile(deduplicated_df: pd.DataFrame) -> dict:
    if deduplicated_df.empty:
        return {
            "n_records": 0,
            "doi_coverage_pct": 0.0,
            "abstract_coverage_pct": 0.0,
            "language_distribution": {},
            "document_type_distribution": {},
        }

    lang_dist = (
        deduplicated_df["language"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", "unknown")
        .value_counts()
        .to_dict()
        if "language" in deduplicated_df.columns else {}
    )

    doc_type_dist = (
        deduplicated_df["document_type"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", "unknown")
        .value_counts()
        .to_dict()
        if "document_type" in deduplicated_df.columns else {}
    )

    return {
        "n_records": len(deduplicated_df),
        "doi_coverage_pct": round((deduplicated_df["doi"].astype(str).str.strip().ne("").sum() / len(deduplicated_df)) * 100, 2) if "doi" in deduplicated_df.columns else 0.0,
        "abstract_coverage_pct": round((deduplicated_df["abstract_present"].fillna(False).astype(bool).sum() / len(deduplicated_df)) * 100, 2) if "abstract_present" in deduplicated_df.columns else 0.0,
        "language_distribution": lang_dist,
        "document_type_distribution": doc_type_dist,
    }


# =========================================================
# PRISMA diagram
# =========================================================

def generate_prisma_diagram(stats: PipelineStats) -> Tuple[Path, Path]:
    processed_dir = get_processed_dir()
    png_path = processed_dir / "prisma_diagram.png"
    svg_path = processed_dir / "prisma_diagram.svg"

    fig, ax = plt.subplots(figsize=(10.5, 13))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 13)
    ax.axis("off")

    def box(x, y, w, h, text, fontsize=10):
        rect = plt.Rectangle((x, y), w, h, fill=False, linewidth=1.2)
        ax.add_patch(rect)
        ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=fontsize, wrap=True)

    def arrow(x1, y1, x2, y2):
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1), arrowprops=dict(arrowstyle="->", lw=1.2))

    side_labels = [
        (0.3, 10.8, "Identification"),
        (0.3, 8.0, "Screening"),
        (0.3, 5.0, "Eligibility"),
        (0.3, 2.0, "Included"),
    ]
    for x, y, label in side_labels:
        rect = plt.Rectangle((x, y), 0.5, 1.5, fill=False, linewidth=1.0)
        ax.add_patch(rect)
        ax.text(x + 0.25, y + 0.75, label, ha="center", va="center", rotation=90, fontsize=9)

    identified_text = (
        "Records identified from:\n"
        f"- Scopus core (n = {stats.records_scopus_core})\n"
        f"- Scopus exploratory (n = {stats.records_scopus_exploratory})\n"
        f"- OpenAlex (n = {stats.records_openalex})\n"
        f"Total (N = {stats.records_identified_total})"
    )

    box(2.0, 11.2, 3.5, 1.2, identified_text, fontsize=9.5)
    box(2.0, 9.5, 3.5, 1.0, f"Records after deduplication\nN = {stats.records_after_deduplication}")
    box(6.2, 9.5, 2.6, 1.0, f"Duplicates removed\nN = {stats.duplicates_removed}")

    box(2.0, 7.8, 3.5, 1.0, f"Records screened\nN = {display_value(stats.records_screened_title_abstract)}")
    box(6.2, 7.8, 2.6, 1.0, f"Records excluded\nN = {display_value(stats.records_excluded_title_abstract)}")

    box(2.0, 6.1, 3.5, 1.0, f"Reports sought for retrieval\nN = {display_value(stats.reports_sought_for_retrieval)}")
    box(6.2, 6.1, 2.6, 1.0, f"Reports not retrieved\nN = {display_value(stats.reports_not_retrieved)}")

    box(2.0, 4.4, 3.5, 1.0, f"Reports assessed for eligibility\nN = {display_value(stats.reports_assessed_for_eligibility)}")
    box(6.2, 4.2, 2.6, 1.4, f"Reports excluded\nN = {display_value(stats.reports_excluded_full_text)}")

    box(2.0, 2.2, 3.5, 1.0, f"Studies included in review\nN = {display_value(stats.studies_included_review)}")
    box(6.2, 2.2, 2.6, 1.0, f"Reports included\nN = {display_value(stats.reports_included_review)}")

    arrow(3.75, 11.2, 3.75, 10.5)
    arrow(5.5, 10.0, 6.2, 10.0)

    arrow(3.75, 9.5, 3.75, 8.8)
    arrow(5.5, 8.3, 6.2, 8.3)

    arrow(3.75, 7.8, 3.75, 7.1)
    arrow(5.5, 6.6, 6.2, 6.6)

    arrow(3.75, 6.1, 3.75, 5.4)
    arrow(5.5, 4.9, 6.2, 4.9)

    arrow(3.75, 4.4, 3.75, 3.2)
    arrow(5.5, 2.7, 6.2, 2.7)

    plt.tight_layout()
    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    fig.savefig(svg_path, bbox_inches="tight")
    plt.close(fig)

    return png_path, svg_path


# =========================================================
# Manuscript tables
# =========================================================

def generate_manuscript_tables(harmonized_df: pd.DataFrame, deduplicated_df: pd.DataFrame, screening_df: pd.DataFrame, stats: PipelineStats) -> str:
    source_summary = []
    if not harmonized_df.empty:
        src = (
            harmonized_df.groupby(["source_db", "search_group"])
            .size()
            .reset_index(name="n")
            .sort_values(["source_db", "search_group"])
        )
        source_summary = src.to_dict(orient="records")

    year_summary_md = "No available data."
    if not deduplicated_df.empty and "year" in deduplicated_df.columns:
        years = (
            deduplicated_df.dropna(subset=["year"])
            .groupby("year")
            .size()
            .reset_index(name="n")
            .sort_values("year")
        )
        if not years.empty:
            year_summary_md = dataframe_to_md_or_text(years)

    top_journals_md = "No available data."
    if not deduplicated_df.empty and "journal" in deduplicated_df.columns:
        journals = (
            deduplicated_df[deduplicated_df["journal"].astype(str).str.strip() != ""]
            .groupby("journal")
            .size()
            .reset_index(name="n")
            .sort_values("n", ascending=False)
            .head(15)
        )
        if not journals.empty:
            top_journals_md = dataframe_to_md_or_text(journals)

    prisma_table = pd.DataFrame([
        ["Records identified from Scopus core", stats.records_scopus_core],
        ["Records identified from Scopus exploratory", stats.records_scopus_exploratory],
        ["Records identified from OpenAlex", stats.records_openalex],
        ["Total records identified", stats.records_identified_total],
        ["Duplicates removed", stats.duplicates_removed],
        ["Records screened", display_value(stats.records_screened_title_abstract)],
        ["Records excluded (title/abstract)", display_value(stats.records_excluded_title_abstract)],
        ["Reports sought for retrieval", display_value(stats.reports_sought_for_retrieval)],
        ["Reports not retrieved", display_value(stats.reports_not_retrieved)],
        ["Reports assessed for eligibility", display_value(stats.reports_assessed_for_eligibility)],
        ["Reports excluded (full text)", display_value(stats.reports_excluded_full_text)],
        ["Studies included in review", display_value(stats.studies_included_review)],
    ], columns=["Stage", "N"])

    source_table_md = dataframe_to_md_or_text(pd.DataFrame(source_summary)) if source_summary else "No available data."
    prisma_table_md = dataframe_to_md_or_text(prisma_table)

    md = f"""# Manuscript Tables

## Table 1. Source identification summary

{source_table_md}

## Table 2. PRISMA counts

{prisma_table_md}

## Table 3. Publication years in deduplicated corpus

{year_summary_md}

## Table 4. Top journals/sources

{top_journals_md}
"""
    return md


# =========================================================
# Exports
# =========================================================

def save_sqlite(harmonized_df: pd.DataFrame, deduplicated_df: pd.DataFrame, screening_df: pd.DataFrame) -> Path:
    db_path = get_processed_dir() / "records.sqlite"
    conn = sqlite3.connect(db_path)

    harmonized_df.to_sql("harmonized_records", conn, if_exists="replace", index=False)
    deduplicated_df.to_sql("deduplicated_records", conn, if_exists="replace", index=False)
    screening_df.to_sql("screening_matrix", conn, if_exists="replace", index=False)

    conn.close()
    return db_path


def save_manifest(paths: List[Path]) -> Path:
    manifest = {"generated_at": now_iso(), "files": []}
    for path in paths:
        if path.exists():
            manifest["files"].append({
                "path": str(path),
                "sha256": file_sha256(path),
                "size_bytes": path.stat().st_size,
            })

    manifest_path = get_processed_dir() / "manifest.json"
    write_json(manifest_path, manifest)
    return manifest_path


def create_reproducibility_package(paths: List[Path]) -> Path:
    zip_path = get_processed_dir() / "reproducibility_package.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in paths:
            if path.exists():
                zf.write(path, arcname=path.name)
    return zip_path


# =========================================================
# Stage helpers
# =========================================================

def _important_stats(stats: PipelineStats) -> dict:
    return {
        "records_identified_total": stats.records_identified_total,
        "duplicates_removed": stats.duplicates_removed,
        "records_after_deduplication": stats.records_after_deduplication,
        "records_screened_title_abstract": stats.records_screened_title_abstract,
        "records_excluded_title_abstract": stats.records_excluded_title_abstract,
        "reports_sought_for_retrieval": stats.reports_sought_for_retrieval,
        "reports_not_retrieved": stats.reports_not_retrieved,
        "reports_assessed_for_eligibility": stats.reports_assessed_for_eligibility,
        "reports_excluded_full_text": stats.reports_excluded_full_text,
        "studies_included_review": stats.studies_included_review,
        "doi_coverage_pct": stats.doi_coverage_pct,
        "abstract_coverage_pct": stats.abstract_coverage_pct,
        "year_min": stats.year_min,
        "year_max": stats.year_max,
    }


# =========================================================
# Stage 1
# =========================================================

def run_pico_stage(protocol_data: dict, progress: ProgressCallback = None) -> dict:
    emit(progress, "⏳ Guardando etapa 1...")
    protocol_path = save_protocol(protocol_data)

    state = load_pipeline_state()
    state["protocol_path"] = str(protocol_path)
    state = mark_stage_completed("stage_1_pico", state)

    emit(progress, "✅ Etapa 1 guardada.")
    return {
        "protocol_path": str(protocol_path),
        "completed_stages": state.get("completed_stages", []),
    }


# =========================================================
# Stage 2
# =========================================================

def run_search_stage(
    user_input_openalex: Optional[str] = None,
    scopus_core_csv: Optional[str] = None,
    scopus_exploratory_csv: Optional[str] = None,
    progress: ProgressCallback = None,
    per_page_openalex: int = 100,
) -> Dict:
    processed_dir = get_processed_dir()
    audit_path = processed_dir / "audit_trail.jsonl"
    run_log_path = processed_dir / "run_log.jsonl"

    append_jsonl(run_log_path, {"timestamp": now_iso(), "event": "stage_2_started"})

    emit(progress, "[1/5] 🔍 Validando y descargando OpenAlex (sin límite, usando cursor cuando corresponde)...")
    openalex_validation = None
    openalex_csv_path = None

    if user_input_openalex and normalize_text(user_input_openalex):
        openalex_validation = validate_and_save_openalex_input(
            user_input=user_input_openalex,
            basename="openalex_validation",
            fetch_all=True,
            per_page=per_page_openalex,
        )
        if not openalex_validation.ok:
            raise RuntimeError(f"Error OpenAlex: {openalex_validation.error}")
        openalex_csv_path = openalex_validation.csv_path
        emit(progress, f"     OpenAlex registros descargados: {openalex_validation.records}")
    else:
        emit(progress, "     OpenAlex no fue ingresado.")

    emit(progress, "[2/5] 📥 Cargando archivos de Scopus...")
    scopus_core_path = Path(scopus_core_csv) if scopus_core_csv else get_raw_scopus_dir() / "core.csv"
    scopus_exploratory_path = Path(scopus_exploratory_csv) if scopus_exploratory_csv else get_raw_scopus_dir() / "exploratory.csv"

    scopus_core_df = load_scopus_csv(scopus_core_path, source_label="scopus", search_group="core")
    scopus_exploratory_df = load_scopus_csv(scopus_exploratory_path, source_label="scopus", search_group="exploratory")
    openalex_df = load_openalex_csv(openalex_csv_path)

    emit(progress, f"     Scopus core: {len(scopus_core_df)}")
    emit(progress, f"     Scopus exploratory: {len(scopus_exploratory_df)}")
    emit(progress, f"     OpenAlex: {len(openalex_df)}")

    emit(progress, "[3/5] 🧹 Harmonizando registros...")
    harmonized_df = harmonize_sources(scopus_core_df, scopus_exploratory_df, openalex_df)
    harmonized_path = processed_dir / "harmonized_records.csv"
    harmonized_df.to_csv(harmonized_path, index=False, encoding="utf-8-sig")
    emit(progress, f"     Registros harmonizados: {len(harmonized_df)}")

    emit(progress, "[4/5] 🔁 Deduplicando registros...")
    deduplicated_df, dedup_groups_df = deduplicate_records(harmonized_df)
    deduplicated_path = processed_dir / "deduplicated_records.csv"
    dedup_groups_path = processed_dir / "deduplication_groups.csv"
    deduplicated_df.to_csv(deduplicated_path, index=False, encoding="utf-8-sig")
    dedup_groups_df.to_csv(dedup_groups_path, index=False, encoding="utf-8-sig")
    emit(progress, f"     Duplicados eliminados: {len(harmonized_df) - len(deduplicated_df)}")
    emit(progress, f"     Registros tras deduplicación: {len(deduplicated_df)}")

    emit(progress, "[5/5] 📊 Generando screening matrix inicial y estadísticas reales disponibles...")
    screening_path = processed_dir / "screening_matrix.csv"

    screening_df = build_screening_matrix(deduplicated_df)
    screening_df.to_csv(screening_path, index=False, encoding="utf-8-sig")

    quality_profile = build_quality_profile(deduplicated_df)
    quality_profile_path = processed_dir / "quality_profile.json"
    write_json(quality_profile_path, quality_profile)

    stats = compute_prisma_counts(harmonized_df, deduplicated_df, screening_df)
    prisma_counts_path = processed_dir / "prisma_counts.json"
    write_json(prisma_counts_path, asdict(stats))

    prisma_png_path, prisma_svg_path = generate_prisma_diagram(stats)

    manuscript_md = generate_manuscript_tables(harmonized_df, deduplicated_df, screening_df, stats)
    manuscript_path = processed_dir / "manuscript_tables.md"
    manuscript_path.write_text(manuscript_md, encoding="utf-8")

    sqlite_path = save_sqlite(harmonized_df, deduplicated_df, screening_df)

    exported_paths = [
        harmonized_path,
        deduplicated_path,
        dedup_groups_path,
        screening_path,
        quality_profile_path,
        prisma_counts_path,
        prisma_png_path,
        prisma_svg_path,
        manuscript_path,
        sqlite_path,
        audit_path,
        run_log_path,
    ]
    manifest_path = save_manifest(exported_paths)

    state = load_pipeline_state()
    state["artifacts"] = {
        "harmonized_records_csv": str(harmonized_path),
        "deduplicated_records_csv": str(deduplicated_path),
        "deduplication_groups_csv": str(dedup_groups_path),
        "screening_matrix_csv": str(screening_path),
        "quality_profile_json": str(quality_profile_path),
        "prisma_counts_json": str(prisma_counts_path),
        "prisma_diagram_png": str(prisma_png_path),
        "prisma_diagram_svg": str(prisma_svg_path),
        "manuscript_tables_md": str(manuscript_path),
        "records_sqlite": str(sqlite_path),
        "manifest_json": str(manifest_path),
    }
    state["stats"] = asdict(stats)
    state["sources"] = {
        "scopus_core_rows": len(scopus_core_df),
        "scopus_exploratory_rows": len(scopus_exploratory_df),
        "openalex_rows": len(openalex_df),
    }
    state = mark_stage_completed("stage_2_search", state)
    save_pipeline_state(state)

    append_jsonl(audit_path, {
        "timestamp": now_iso(),
        "stage": "stage_2_search",
        "status": "ok",
        "stats": asdict(stats),
        "sources": state["sources"],
    })
    append_jsonl(run_log_path, {"timestamp": now_iso(), "event": "stage_2_finished"})

    emit(progress, "✅ Etapa 2 completada.")

    return {
        "stats": _important_stats(stats),
        "stats_full": asdict(stats),
        "sources": state["sources"],
        "artifacts": state["artifacts"],
        "completed_stages": state["completed_stages"],
        "openalex_validation": None if not openalex_validation else {
            "count_api": openalex_validation.count_api,
            "records": openalex_validation.records,
            "rows": openalex_validation.rows,
            "json_path": openalex_validation.json_path,
            "csv_path": openalex_validation.csv_path,
            "request_url": openalex_validation.request_url,
        },
    }


# =========================================================
# Stage 3
# =========================================================

def run_screening_stage(progress: ProgressCallback = None) -> Dict:
    processed_dir = get_processed_dir()

    emit(progress, "⏳ Procesando etapa 3...")
    harmonized_df = load_df_if_exists(processed_dir / "harmonized_records.csv")
    deduplicated_df = load_df_if_exists(processed_dir / "deduplicated_records.csv")

    if deduplicated_df.empty:
        raise RuntimeError("No existe corpus deduplicado. Ejecute primero la etapa 2.")

    screening_path = processed_dir / "screening_matrix.csv"
    if not screening_path.exists():
        screening_df = build_screening_matrix(deduplicated_df)
        screening_df.to_csv(screening_path, index=False, encoding="utf-8-sig")
        emit(progress, "📄 Screening matrix creada.")
    else:
        screening_df = load_df_if_exists(screening_path)
        emit(progress, "📄 Screening matrix leída.")

    expected_ids = set(deduplicated_df["record_id"].astype(str))

    if screening_df.empty or "record_id" not in screening_df.columns:
        screening_df = build_screening_matrix(deduplicated_df)
        screening_df.to_csv(screening_path, index=False, encoding="utf-8-sig")
        emit(progress, "♻️ Screening matrix regenerada por inconsistencia estructural.")
    else:
        screening_df["record_id"] = screening_df["record_id"].astype(str)
        screening_df = screening_df[screening_df["record_id"].isin(expected_ids)].copy()

        existing_ids = set(screening_df["record_id"])
        missing_ids = expected_ids - existing_ids
        if missing_ids:
            missing_df = deduplicated_df[deduplicated_df["record_id"].astype(str).isin(missing_ids)].copy()
            missing_screening = build_screening_matrix(missing_df)
            screening_df = pd.concat([screening_df, missing_screening], ignore_index=True)

        screening_df = screening_df.drop_duplicates(subset=["record_id"], keep="first").copy()

        order = deduplicated_df["record_id"].astype(str).tolist()
        screening_df = screening_df.set_index("record_id").reindex(order).reset_index()

        screening_df.to_csv(screening_path, index=False, encoding="utf-8-sig")
        emit(progress, "♻️ Screening matrix alineada con el corpus deduplicado actual.")

    stats = compute_prisma_counts(harmonized_df, deduplicated_df, screening_df)
    prisma_counts_path = processed_dir / "prisma_counts.json"
    write_json(prisma_counts_path, asdict(stats))
    prisma_png_path, prisma_svg_path = generate_prisma_diagram(stats)

    state = load_pipeline_state()
    state["stats"] = asdict(stats)
    state.setdefault("artifacts", {})
    state["artifacts"]["screening_matrix_csv"] = str(screening_path)
    state["artifacts"]["prisma_counts_json"] = str(prisma_counts_path)
    state["artifacts"]["prisma_diagram_png"] = str(prisma_png_path)
    state["artifacts"]["prisma_diagram_svg"] = str(prisma_svg_path)
    state = mark_stage_completed("stage_3_screening", state)
    save_pipeline_state(state)

    emit(progress, "✅ Etapa 3 actualizada.")

    return {
        "stats": _important_stats(stats),
        "stats_full": asdict(stats),
        "sources": state.get("sources", {}),
        "artifacts": state.get("artifacts", {}),
        "completed_stages": state.get("completed_stages", []),
    }


# =========================================================
# Stage 4
# =========================================================

def run_extraction_stage(progress: ProgressCallback = None) -> Dict:
    emit(progress, "⏳ Etapa 4: registrando preparación para extracción...")
    state = load_pipeline_state()
    state = mark_stage_completed("stage_4_extraction", state)
    emit(progress, "✅ Etapa 4 completada.")
    return {"completed_stages": state.get("completed_stages", [])}


# =========================================================
# Stage 5
# =========================================================

def run_quality_stage(progress: ProgressCallback = None) -> Dict:
    emit(progress, "⏳ Etapa 5: actualizando quality profile...")
    processed_dir = get_processed_dir()
    deduplicated_df = load_df_if_exists(processed_dir / "deduplicated_records.csv")
    if deduplicated_df.empty:
        raise RuntimeError("No existe corpus deduplicado. Ejecute primero la etapa 2.")

    quality_profile = build_quality_profile(deduplicated_df)
    quality_profile_path = processed_dir / "quality_profile.json"
    write_json(quality_profile_path, quality_profile)

    state = load_pipeline_state()
    state.setdefault("artifacts", {})
    state["artifacts"]["quality_profile_json"] = str(quality_profile_path)
    state = mark_stage_completed("stage_5_quality", state)
    save_pipeline_state(state)

    emit(progress, "✅ Etapa 5 completada.")
    return {"completed_stages": state.get("completed_stages", []), "artifacts": state.get("artifacts", {})}


# =========================================================
# Stage 6
# =========================================================

def run_synthesis_stage(progress: ProgressCallback = None) -> Dict:
    emit(progress, "⏳ Etapa 6: preparando síntesis...")
    processed_dir = get_processed_dir()
    harmonized_df = load_df_if_exists(processed_dir / "harmonized_records.csv")
    deduplicated_df = load_df_if_exists(processed_dir / "deduplicated_records.csv")
    screening_df = load_df_if_exists(processed_dir / "screening_matrix.csv")

    sqlite_path = save_sqlite(harmonized_df, deduplicated_df, screening_df)

    state = load_pipeline_state()
    state.setdefault("artifacts", {})
    state["artifacts"]["records_sqlite"] = str(sqlite_path)
    state = mark_stage_completed("stage_6_synthesis", state)
    save_pipeline_state(state)

    emit(progress, "✅ Etapa 6 completada.")
    return {"completed_stages": state.get("completed_stages", []), "artifacts": state.get("artifacts", {})}


# =========================================================
# Stage 7
# =========================================================

def run_prisma_stage(progress: ProgressCallback = None) -> Dict:
    emit(progress, "⏳ Etapa 7: actualizando reporte PRISMA...")
    processed_dir = get_processed_dir()
    harmonized_df = load_df_if_exists(processed_dir / "harmonized_records.csv")
    deduplicated_df = load_df_if_exists(processed_dir / "deduplicated_records.csv")
    screening_df = load_df_if_exists(processed_dir / "screening_matrix.csv")

    if harmonized_df.empty or deduplicated_df.empty:
        raise RuntimeError("Faltan datos base. Ejecute primero la etapa 2.")

    stats = compute_prisma_counts(harmonized_df, deduplicated_df, screening_df)
    prisma_counts_path = processed_dir / "prisma_counts.json"
    write_json(prisma_counts_path, asdict(stats))
    prisma_png_path, prisma_svg_path = generate_prisma_diagram(stats)

    manuscript_md = generate_manuscript_tables(harmonized_df, deduplicated_df, screening_df, stats)
    manuscript_path = processed_dir / "manuscript_tables.md"
    manuscript_path.write_text(manuscript_md, encoding="utf-8")

    exported_paths = [
        processed_dir / "harmonized_records.csv",
        processed_dir / "deduplicated_records.csv",
        processed_dir / "screening_matrix.csv",
        prisma_counts_path,
        prisma_png_path,
        prisma_svg_path,
        manuscript_path,
    ]
    manifest_path = save_manifest(exported_paths)
    reproducibility_zip_path = create_reproducibility_package(exported_paths + [manifest_path])

    state = load_pipeline_state()
    state["stats"] = asdict(stats)
    state.setdefault("artifacts", {})
    state["artifacts"]["prisma_counts_json"] = str(prisma_counts_path)
    state["artifacts"]["prisma_diagram_png"] = str(prisma_png_path)
    state["artifacts"]["prisma_diagram_svg"] = str(prisma_svg_path)
    state["artifacts"]["manuscript_tables_md"] = str(manuscript_path)
    state["artifacts"]["manifest_json"] = str(manifest_path)
    state["artifacts"]["reproducibility_package_zip"] = str(reproducibility_zip_path)
    state = mark_stage_completed("stage_7_prisma", state)
    save_pipeline_state(state)

    emit(progress, "✅ Etapa 7 completada.")

    return {
        "stats": _important_stats(stats),
        "stats_full": asdict(stats),
        "sources": state.get("sources", {}),
        "artifacts": state.get("artifacts", {}),
        "completed_stages": state.get("completed_stages", []),
    }


# Compatibilidad heredada
def run_validation_stage(
    user_input: str,
    progress: ProgressCallback = None,
    basename: str = "openalex_validation",
    per_page: int = 100,
) -> Dict:
    emit(progress, "⏳ Validando OpenAlex...")
    result = validate_and_save_openalex_input(
        user_input=user_input,
        basename=basename,
        fetch_all=True,
        per_page=per_page,
    )
    if not result.ok:
        raise RuntimeError(result.error)

    emit(progress, "✅ Validación OpenAlex completada.")
    return {
        "stats": {
            "records_identified_total": result.records,
            "duplicates_removed": 0,
            "records_after_deduplication": result.records,
            "records_screened_title_abstract": result.records,
            "records_excluded_title_abstract": None,
            "reports_sought_for_retrieval": None,
            "reports_not_retrieved": None,
            "reports_assessed_for_eligibility": None,
            "reports_excluded_full_text": None,
            "studies_included_review": None,
            "doi_coverage_pct": 0.0,
            "abstract_coverage_pct": 0.0,
            "year_min": None,
            "year_max": None,
        },
        "artifacts": {
            "openalex_json": result.json_path,
            "openalex_csv": result.csv_path,
        },
        "openalex_validation": {
            "count_api": result.count_api,
            "records": result.records,
            "rows": result.rows,
            "json_path": result.json_path,
            "csv_path": result.csv_path,
            "request_url": result.request_url,
        },
    }