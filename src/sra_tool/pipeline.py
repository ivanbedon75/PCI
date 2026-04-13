#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

"""Pipeline profesional para integración, trazabilidad y auditoría de búsquedas
sistemáticas con fuentes Scopus y OpenAlex.
"""

import hashlib
import json
import shutil
import sqlite3
import zipfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .clients.openalex import get_repo_root


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


def get_raw_openalex_dir() -> Path:
    path = get_raw_dir() / "openalex"
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
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
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
        raise RuntimeError(
            f"{label} no contiene las columnas requeridas: {', '.join(missing)}"
        )


def copy_source_file(src: Path, destination_name: str, destination_dir: Path) -> Path:
    destination_dir.mkdir(parents=True, exist_ok=True)
    dst = destination_dir / destination_name
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


def save_source_manifest(data: dict) -> Path:
    path = get_processed_dir() / "source_manifest.json"
    write_json(path, data)
    return path


def save_search_summary(data: dict) -> Path:
    path = get_processed_dir() / "search_summary.json"
    write_json(path, data)
    return path


def _safe_join(parts: List[str], sep: str = "; ") -> str:
    clean = [normalize_text(x) for x in parts if normalize_text(x)]
    return sep.join(clean)


# =========================================================
# Validation model
# =========================================================

@dataclass
class ValidationCheck:
    check_id: str
    stage: str
    name: str
    status: str
    severity: str
    expected: str
    observed: str
    evidence: str
    action_required: str = ""


def validation_status_from_checks(checks: List[ValidationCheck]) -> str:
    if any(c.status == "FAIL" and c.severity.lower() == "critical" for c in checks):
        return "FAIL"
    if any(c.status == "FAIL" for c in checks):
        return "FAIL"
    if any(c.status == "WARN" for c in checks):
        return "PASS_WITH_WARNINGS"
    return "PASS"


def checks_to_dataframe(checks: List[ValidationCheck]) -> pd.DataFrame:
    if not checks:
        return pd.DataFrame(
            columns=[
                "check_id",
                "stage",
                "name",
                "status",
                "severity",
                "expected",
                "observed",
                "evidence",
                "action_required",
            ]
        )
    return pd.DataFrame([asdict(c) for c in checks])


def save_validation_reports(stage_name: str, checks: List[ValidationCheck]) -> dict:
    processed_dir = get_processed_dir()
    df = checks_to_dataframe(checks)
    status = validation_status_from_checks(checks)

    json_path = processed_dir / f"validation_report_{stage_name}.json"
    csv_path = processed_dir / f"validation_report_{stage_name}.csv"
    md_path = processed_dir / f"validation_report_{stage_name}.md"

    payload = {
        "generated_at": now_iso(),
        "stage": stage_name,
        "global_status": status,
        "checks": [asdict(c) for c in checks],
    }
    write_json(json_path, payload)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    md = f"# Validation report: {stage_name}\n\n"
    md += f"**Global status:** {status}\n\n"
    md += dataframe_to_md_or_text(df)
    md_path.write_text(md, encoding="utf-8")

    history_path = processed_dir / "validation_history.jsonl"
    append_jsonl(history_path, payload)

    return {
        "global_status": status,
        "json": str(json_path),
        "csv": str(csv_path),
        "md": str(md_path),
        "history": str(history_path),
    }


def save_record_validation_csv(stage_name: str, label: str, rows: List[dict]) -> Path:
    path = get_processed_dir() / f"record_validation_{label}_{stage_name}.csv"
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def _pass(
    check_id: str,
    stage: str,
    name: str,
    expected: str,
    observed: str,
    evidence: str,
    action_required: str = "",
) -> ValidationCheck:
    return ValidationCheck(
        check_id, stage, name, "PASS", "low", expected, observed, evidence, action_required
    )


def _warn(
    check_id: str,
    stage: str,
    name: str,
    expected: str,
    observed: str,
    evidence: str,
    action_required: str = "",
) -> ValidationCheck:
    return ValidationCheck(
        check_id, stage, name, "WARN", "medium", expected, observed, evidence, action_required
    )


def _fail(
    check_id: str,
    stage: str,
    name: str,
    severity: str,
    expected: str,
    observed: str,
    evidence: str,
    action_required: str = "",
) -> ValidationCheck:
    return ValidationCheck(
        check_id, stage, name, "FAIL", severity, expected, observed, evidence, action_required
    )


def validate_protocol_data(protocol_data: dict) -> List[ValidationCheck]:
    stage = "stage_1_pico"
    checks: List[ValidationCheck] = []

    rq = normalize_text(protocol_data.get("research_question", ""))
    inc = normalize_text(protocol_data.get("inclusion_criteria", ""))
    exc = normalize_text(protocol_data.get("exclusion_criteria", ""))

    checks.append(
        _pass("V01", stage, "research_question_present", "Non-empty", "Provided", "protocol.json")
        if rq
        else _fail(
            "V01",
            stage,
            "research_question_present",
            "critical",
            "Non-empty",
            "Empty",
            "protocol.json",
            "Completar pregunta de investigación",
        )
    )

    checks.append(
        _pass("V02", stage, "inclusion_criteria_present", "Non-empty", "Provided", "protocol.json")
        if inc
        else _fail(
            "V02",
            stage,
            "inclusion_criteria_present",
            "critical",
            "Non-empty",
            "Empty",
            "protocol.json",
            "Completar criterios de inclusión",
        )
    )

    checks.append(
        _pass("V03", stage, "exclusion_criteria_present", "Non-empty", "Provided", "protocol.json")
        if exc
        else _fail(
            "V03",
            stage,
            "exclusion_criteria_present",
            "critical",
            "Non-empty",
            "Empty",
            "protocol.json",
            "Completar criterios de exclusión",
        )
    )

    return checks


def validate_stage2_source_bundle(openalex_inputs: dict, scopus_inputs: dict) -> None:
    has_any_openalex = any(normalize_text(v) for v in openalex_inputs.values())
    has_any_scopus = any(normalize_text(v) for v in scopus_inputs.values())

    if not has_any_openalex and not has_any_scopus:
        raise RuntimeError(
            "Debe ingresar al menos una base de datos completa (OpenAlex o Scopus)."
        )

    if has_any_openalex:
        missing = [k for k, v in openalex_inputs.items() if not normalize_text(v)]
        if missing:
            raise RuntimeError(
                "Para OpenAlex debe ingresar las tres estrategias: "
                "core, exploratory_1 y exploratory_2. Faltan: " + ", ".join(missing)
            )

    if has_any_scopus:
        missing = [k for k, v in scopus_inputs.items() if not normalize_text(v)]
        if missing:
            raise RuntimeError(
                "Para Scopus debe ingresar los tres CSV: "
                "core, exploratory_1 y exploratory_2. Faltan: " + ", ".join(missing)
            )


def validate_stage2_inputs_for_report(
    openalex_inputs: dict, scopus_inputs: dict
) -> List[ValidationCheck]:
    stage = "stage_2_search"
    checks: List[ValidationCheck] = []

    for check_id, key in [("V04", "core"), ("V05", "exploratory_1"), ("V06", "exploratory_2")]:
        exists = bool(scopus_inputs.get(key))
        checks.append(
            _pass(check_id, stage, f"scopus_{key}_provided", "Input provided", "Provided", f"Scopus {key}")
            if exists
            else _fail(
                check_id,
                stage,
                f"scopus_{key}_provided",
                "critical",
                "Input provided",
                "Missing",
                f"Scopus {key}",
                "Ingresar archivo requerido",
            )
        )

    for check_id, key in [("V07", "core"), ("V08", "exploratory_1"), ("V09", "exploratory_2")]:
        exists = bool(openalex_inputs.get(key))
        checks.append(
            _pass(check_id, stage, f"openalex_{key}_provided", "Input provided", "Provided", f"OpenAlex {key}")
            if exists
            else _fail(
                check_id,
                stage,
                f"openalex_{key}_provided",
                "critical",
                "Input provided",
                "Missing",
                f"OpenAlex {key}",
                "Ingresar archivo requerido",
            )
        )

    return checks


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


@dataclass
class AuditSnapshot:
    generated_at: str
    stage: str
    row_counts: dict
    coverage: dict
    distributions: dict
    source_inventory: dict
    notes: List[str]


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
        "source_manifest_json",
        "search_summary_json",
        "audit_trail_jsonl",
        "run_log_jsonl",
        "reproducibility_package_zip",
        "validation_report_stage_1_pico_json",
        "validation_report_stage_1_pico_csv",
        "validation_report_stage_1_pico_md",
        "validation_report_stage_2_search_json",
        "validation_report_stage_2_search_csv",
        "validation_report_stage_2_search_md",
        "validation_report_stage_3_screening_json",
        "validation_report_stage_3_screening_csv",
        "validation_report_stage_3_screening_md",
        "validation_report_stage_5_quality_json",
        "validation_report_stage_5_quality_csv",
        "validation_report_stage_5_quality_md",
        "validation_report_stage_6_synthesis_json",
        "validation_report_stage_6_synthesis_csv",
        "validation_report_stage_6_synthesis_md",
        "validation_report_stage_7_prisma_json",
        "validation_report_stage_7_prisma_csv",
        "validation_report_stage_7_prisma_md",
        "validation_history_jsonl",
        "record_validation_harmonized_stage_2_search_csv",
        "record_validation_deduplicated_stage_2_search_csv",
        "record_validation_screening_stage_2_search_csv",
        "record_validation_screening_stage_3_screening_csv",
        "executive_report_html",
        "methodology_checklist_json",
        "methodology_checklist_md",
        "std_protocol_path",
        "std_source_manifest_json",
        "std_search_summary_json",
        "std_harmonized_records_csv",
        "std_deduplicated_records_csv",
        "std_deduplication_groups_csv",
        "std_screening_matrix_csv",
        "std_quality_profile_json",
        "std_prisma_counts_json",
        "std_audit_snapshot_json",
        "std_audit_summary_md",
        "std_executive_report_html",
        "std_methodology_checklist_json",
        "std_methodology_checklist_md",
        "std_reproducibility_package_zip",
    ]

    for key in artifact_keys_to_remove:
        raw_path = state.get("artifacts", {}).get(key)
        if raw_path:
            path = Path(raw_path)
            if path.exists() and path.is_file():
                try:
                    path.unlink()
                except Exception:
                    pass

    files_to_remove = (
        list(processed_dir.glob("validation_report_*.json"))
        + list(processed_dir.glob("validation_report_*.csv"))
        + list(processed_dir.glob("validation_report_*.md"))
        + list(processed_dir.glob("record_validation_*.csv"))
        + list(processed_dir.glob("sr_*"))
    )

    files_to_remove += [
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
        processed_dir / "source_manifest.json",
        processed_dir / "search_summary.json",
        processed_dir / "reproducibility_package.zip",
        processed_dir / "audit_trail.jsonl",
        processed_dir / "run_log.jsonl",
        processed_dir / "validation_history.jsonl",
        processed_dir / "executive_audit_report.html",
        processed_dir / "methodology_audit_checklist.json",
        processed_dir / "methodology_audit_checklist.md",
        processed_dir / "audit_snapshot.json",
        processed_dir / "audit_summary.md",
        get_protocol_path(),
        get_state_path(),
    ]

    for path in files_to_remove:
        if path.exists() and path.is_file():
            try:
                path.unlink()
            except Exception:
                pass

    state = {
        "completed_stages": [],
        "artifacts": {},
        "stats": {},
        "sources": {},
    }
    save_pipeline_state(state)


# =========================================================
# OpenAlex JSON -> Scopus-like CSV
# =========================================================

def _scopus_like_columns() -> List[str]:
    return [
        "Authors",
        "Author full names",
        "Author(s) ID",
        "Title",
        "Year",
        "Source title",
        "Volume",
        "Issue",
        "Art. No.",
        "Page start",
        "Page end",
        "Cited by",
        "DOI",
        "Link",
        "Affiliations",
        "Authors with affiliations",
        "Abstract",
        "Author Keywords",
        "Index Keywords",
        "Funding Details",
        "Funding Texts",
        "References",
        "Correspondence Address",
        "Publisher",
        "ISSN",
        "ISBN",
        "CODEN",
        "Language of Original Document",
        "Abbreviated Source Title",
        "Document Type",
        "Publication Stage",
        "Open Access",
        "Source",
        "EID",
        "OpenAlex ID",
    ]


def _extract_openalex_results(payload: dict) -> List[dict]:
    if isinstance(payload, dict) and isinstance(payload.get("results"), list):
        return payload["results"]
    if isinstance(payload, list):
        return payload
    raise RuntimeError("El archivo JSON de OpenAlex no contiene una lista válida en 'results'.")


def _openalex_authors(work: dict) -> Tuple[str, str, str, str]:
    authors = []
    author_full_names = []
    author_ids = []
    authors_with_aff = []

    for item in work.get("authorships") or []:
        author = item.get("author") or {}
        name = normalize_text(author.get("display_name", ""))
        author_id = normalize_text(author.get("id", ""))
        raw_name = normalize_text(item.get("raw_author_name", "")) or name
        short_id = author_id.rsplit("/", 1)[-1] if author_id else ""

        if name:
            authors.append(name)
            author_full_names.append(f"{raw_name} ({short_id})" if short_id else raw_name)

        if short_id:
            author_ids.append(short_id)

        inst_names = [
            normalize_text((inst or {}).get("display_name", ""))
            for inst in (item.get("institutions") or [])
        ]
        inst_names = [x for x in inst_names if x]

        if name and inst_names:
            authors_with_aff.append(f"{name}, {' | '.join(inst_names)}")
        elif name:
            authors_with_aff.append(name)

    return (
        _safe_join(authors),
        _safe_join(author_full_names),
        _safe_join(author_ids),
        _safe_join(authors_with_aff),
    )


def _openalex_abstract(work: dict) -> str:
    inverted = work.get("abstract_inverted_index") or {}
    if not isinstance(inverted, dict) or not inverted:
        return normalize_text(work.get("abstract", ""))

    indexed = []
    for token, positions in inverted.items():
        if isinstance(positions, list):
            for pos in positions:
                try:
                    indexed.append((int(pos), str(token)))
                except Exception:
                    pass

    if not indexed:
        return ""

    indexed.sort(key=lambda x: x[0])
    return normalize_text(" ".join(token for _, token in indexed))


def _openalex_keywords(work: dict) -> str:
    values = []
    for collection_name in ("keywords", "concepts"):
        for item in work.get(collection_name) or []:
            if isinstance(item, dict):
                name = normalize_text(
                    item.get("display_name") or item.get("keyword") or item.get("name") or ""
                )
            else:
                name = normalize_text(item)
            if name:
                values.append(name)
    return _safe_join(values)


def _openalex_references(work: dict) -> str:
    refs = work.get("referenced_works") or []
    cleaned = []
    for ref in refs[:50]:
        val = normalize_text(ref)
        if val:
            cleaned.append(val.rsplit("/", 1)[-1])
    return _safe_join(cleaned)


def openalex_json_to_scopus_like_df(json_path: Path) -> pd.DataFrame:
    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    works = _extract_openalex_results(payload)
    rows = []

    for work in works:
        primary_location = work.get("primary_location") or {}
        source = primary_location.get("source") or {}
        biblio = work.get("biblio") or {}
        ids = work.get("ids") or {}
        open_access = work.get("open_access") or {}

        doi = normalize_text(ids.get("doi") or work.get("doi") or "")
        doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")

        authors, author_full_names, author_ids, authors_with_aff = _openalex_authors(work)

        institutions = []
        for inst in work.get("institutions") or []:
            if isinstance(inst, dict):
                name = normalize_text(inst.get("display_name", ""))
                if name:
                    institutions.append(name)

        if not institutions:
            for auth in work.get("authorships") or []:
                for inst in auth.get("institutions") or []:
                    name = normalize_text((inst or {}).get("display_name", ""))
                    if name:
                        institutions.append(name)

        oa_status = normalize_text(open_access.get("oa_status", ""))
        is_oa = open_access.get("is_oa")

        row = {c: "" for c in _scopus_like_columns()}
        row.update(
            {
                "Authors": authors,
                "Author full names": author_full_names,
                "Author(s) ID": author_ids,
                "Title": normalize_text(work.get("display_name") or work.get("title") or ""),
                "Year": work.get("publication_year") or "",
                "Source title": normalize_text(source.get("display_name", "")),
                "Volume": normalize_text(biblio.get("volume", "")),
                "Issue": normalize_text(biblio.get("issue", "")),
                "Page start": normalize_text(biblio.get("first_page", "")),
                "Page end": normalize_text(biblio.get("last_page", "")),
                "Cited by": work.get("cited_by_count") or 0,
                "DOI": doi,
                "Link": normalize_text(work.get("id", "")),
                "Affiliations": _safe_join(institutions),
                "Authors with affiliations": authors_with_aff,
                "Abstract": _openalex_abstract(work),
                "Author Keywords": _openalex_keywords(work),
                "Index Keywords": _openalex_keywords(work),
                "References": _openalex_references(work),
                "Publisher": normalize_text(source.get("host_organization_name", "")),
                "ISSN": _safe_join(source.get("issn") or []),
                "Language of Original Document": normalize_text(work.get("language", "")),
                "Abbreviated Source Title": normalize_text(source.get("abbreviated_title", "")),
                "Document Type": normalize_text(work.get("type", "")),
                "Publication Stage": "Final",
                "Open Access": oa_status or ("Open Access" if is_oa else ""),
                "Source": "OpenAlex",
                "EID": normalize_text(work.get("id", "")).rsplit("/", 1)[-1],
                "OpenAlex ID": normalize_text(work.get("id", "")),
            }
        )
        rows.append(row)

    return pd.DataFrame(rows, columns=_scopus_like_columns())


def convert_openalex_json_to_csv(json_path: Path, search_group: str) -> Path:
    out_dir = get_raw_openalex_dir()
    output_path = out_dir / f"openalex_{search_group}.csv"
    df = openalex_json_to_scopus_like_df(json_path)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def resolve_openalex_input_to_csv(path_str: Optional[str], search_group: str) -> Optional[Path]:
    if not normalize_text(path_str):
        return None

    path = Path(path_str)
    if not path.exists():
        raise RuntimeError(f"No existe el archivo de OpenAlex para {search_group}: {path}")

    suffix = path.suffix.lower()
    if suffix == ".json":
        return convert_openalex_json_to_csv(path, search_group)
    if suffix == ".csv":
        return path

    raise RuntimeError(
        f"Formato no soportado para OpenAlex en {search_group}: {path.suffix}. "
        "Use JSON o CSV."
    )


# =========================================================
# Source loading
# =========================================================

SCOPUS_REQUIRED_COLUMNS = ["Title"]


def load_scopus_csv(csv_path: Path, source_label: str, search_group: str) -> pd.DataFrame:
    if not csv_path.exists():
        raise RuntimeError(f"No existe el archivo de {source_label}: {csv_path}")

    df = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            df = pd.read_csv(csv_path, encoding=encoding)
            break
        except Exception:
            df = None

    if df is None:
        raise RuntimeError(f"No se pudo leer el CSV de {source_label}: {csv_path}")

    ensure_required_columns(df, SCOPUS_REQUIRED_COLUMNS, f"{source_label} {search_group}")
    df["source_db"] = source_label
    df["search_group"] = search_group
    df["raw_source_file"] = str(csv_path)
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

        rows.append(
            {
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
                "language": normalize_lower(
                    first_non_empty(row, ["Language of Original Document", "Language"])
                ),
                "affiliations": first_non_empty(row, ["Affiliations"]),
                "citations": citations,
                "openalex_id": normalize_text(row.get("OpenAlex ID", "")),
                "raw_source_file": normalize_text(row.get("raw_source_file", "")),
            }
        )

    out = pd.DataFrame(rows)
    out["record_id"] = [f"REC-{i+1:06d}" for i in range(len(out))]
    return out[harmonized_columns()]


def harmonize_sources(
    scopus_core_df: pd.DataFrame,
    scopus_exploratory_1_df: pd.DataFrame,
    scopus_exploratory_2_df: pd.DataFrame,
    openalex_core_df: pd.DataFrame,
    openalex_exploratory_1_df: pd.DataFrame,
    openalex_exploratory_2_df: pd.DataFrame,
) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []

    if not scopus_core_df.empty:
        parts.append(harmonize_scopus(scopus_core_df))
    if not scopus_exploratory_1_df.empty:
        parts.append(harmonize_scopus(scopus_exploratory_1_df))
    if not scopus_exploratory_2_df.empty:
        parts.append(harmonize_scopus(scopus_exploratory_2_df))

    if not openalex_core_df.empty:
        parts.append(harmonize_scopus(openalex_core_df))
    if not openalex_exploratory_1_df.empty:
        parts.append(harmonize_scopus(openalex_exploratory_1_df))
    if not openalex_exploratory_2_df.empty:
        parts.append(harmonize_scopus(openalex_exploratory_2_df))

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
    if source_db == "openalex" and search_group == "core":
        return 2
    if source_db == "scopus" and search_group == "exploratory_1":
        return 3
    if source_db == "scopus" and search_group == "exploratory_2":
        return 4
    if source_db == "openalex" and search_group == "exploratory_1":
        return 5
    if source_db == "openalex" and search_group == "exploratory_2":
        return 6
    return 9


def deduplicate_records(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df.copy(), pd.DataFrame(
            columns=["dedup_key", "n_records", "record_ids", "sources"]
        )

    work = df.copy()
    work["dedup_key"] = work.apply(build_dedup_key, axis=1)
    work["priority"] = work.apply(
        lambda r: source_priority(r["source_db"], r["search_group"]), axis=1
    )
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
        "record_id",
        "title",
        "year",
        "source_db",
        "search_group",
        "doi",
        "screen_title_abstract",
        "reason_title_abstract",
        "retrieve_full_text",
        "retrieval_status",
        "screen_full_text",
        "reason_full_text",
        "include_final",
        "study_id",
        "notes",
    ]

    if df.empty:
        return pd.DataFrame(columns=cols)

    screening = df[
        ["record_id", "title", "year", "source_db", "search_group", "doi"]
    ].copy()
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
# PRISMA computation
# =========================================================

def compute_prisma_counts(
    harmonized_df: pd.DataFrame,
    deduplicated_df: pd.DataFrame,
    screening_df: pd.DataFrame,
) -> PipelineStats:
    stats = PipelineStats()

    stats.records_identified_total = len(harmonized_df)
    stats.records_scopus_core = int(
        (
            (harmonized_df["source_db"] == "scopus")
            & (harmonized_df["search_group"] == "core")
        ).sum()
    ) if "search_group" in harmonized_df.columns else 0

    stats.records_scopus_exploratory = int(
        (
            (harmonized_df["source_db"] == "scopus")
            & (harmonized_df["search_group"].isin(["exploratory_1", "exploratory_2"]))
        ).sum()
    ) if "search_group" in harmonized_df.columns else 0

    stats.records_openalex = int(
        (harmonized_df["source_db"] == "openalex").sum()
    ) if "source_db" in harmonized_df.columns else 0

    stats.records_after_deduplication = len(deduplicated_df)
    stats.duplicates_removed = (
        stats.records_identified_total - stats.records_after_deduplication
    )

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
            stats.reports_assessed_for_eligibility = int((retrieval == "retrieved").sum())

        if (fulltext != "pending").any():
            stats.reports_assessed_for_eligibility = int(
                (fulltext.isin(["include", "exclude"])).sum()
            )
            stats.reports_excluded_full_text = int((fulltext == "exclude").sum())

        if (final_inc != "pending").any():
            stats.studies_included_review = int((final_inc == "yes").sum())
            stats.reports_included_review = stats.studies_included_review

    if not deduplicated_df.empty:
        stats.doi_coverage_pct = round(
            (
                deduplicated_df["doi"].astype(str).str.strip().ne("").sum()
                / len(deduplicated_df)
            ) * 100,
            2,
        ) if "doi" in deduplicated_df.columns else 0.0

        stats.abstract_coverage_pct = round(
            (
                deduplicated_df["abstract_present"].fillna(False).astype(bool).sum()
                / len(deduplicated_df)
            ) * 100,
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

    if stats.duplicates_removed != (
        stats.records_identified_total - stats.records_after_deduplication
    ):
        raise RuntimeError(
            "PRISMA inválido: duplicates_removed no coincide con identified - deduplicated."
        )

    if stats.records_after_deduplication > stats.records_identified_total:
        raise RuntimeError(
            "PRISMA inválido: records_after_deduplication > records_identified_total."
        )

    if stats.records_screened_title_abstract is not None:
        if stats.records_screened_title_abstract != stats.records_after_deduplication:
            raise RuntimeError(
                "PRISMA inválido: records_screened_title_abstract debe coincidir con records_after_deduplication."
            )

    if (
        stats.records_excluded_title_abstract is not None
        and stats.records_screened_title_abstract is not None
        and stats.records_excluded_title_abstract > stats.records_screened_title_abstract
    ):
        raise RuntimeError(
            "PRISMA inválido: records_excluded_title_abstract > records_screened_title_abstract."
        )

    if (
        stats.reports_not_retrieved is not None
        and stats.reports_sought_for_retrieval is not None
        and stats.reports_not_retrieved > stats.reports_sought_for_retrieval
    ):
        raise RuntimeError(
            "PRISMA inválido: reports_not_retrieved > reports_sought_for_retrieval."
        )

    if (
        stats.reports_excluded_full_text is not None
        and stats.reports_assessed_for_eligibility is not None
        and stats.reports_excluded_full_text > stats.reports_assessed_for_eligibility
    ):
        raise RuntimeError(
            "PRISMA inválido: reports_excluded_full_text > reports_assessed_for_eligibility."
        )


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
        if "language" in deduplicated_df.columns
        else {}
    )

    doc_type_dist = (
        deduplicated_df["document_type"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", "unknown")
        .value_counts()
        .to_dict()
        if "document_type" in deduplicated_df.columns
        else {}
    )

    return {
        "n_records": len(deduplicated_df),
        "doi_coverage_pct": round(
            (
                deduplicated_df["doi"].astype(str).str.strip().ne("").sum()
                / len(deduplicated_df)
            ) * 100,
            2,
        )
        if "doi" in deduplicated_df.columns
        else 0.0,
        "abstract_coverage_pct": round(
            (
                deduplicated_df["abstract_present"].fillna(False).astype(bool).sum()
                / len(deduplicated_df)
            ) * 100,
            2,
        )
        if "abstract_present" in deduplicated_df.columns
        else 0.0,
        "language_distribution": lang_dist,
        "document_type_distribution": doc_type_dist,
    }


# =========================================================
# Audit snapshot / exports
# =========================================================

def _important_stats(stats: PipelineStats) -> dict:
    return {
        "records_identified_total": stats.records_identified_total,
        "records_scopus_core": stats.records_scopus_core,
        "records_scopus_exploratory": stats.records_scopus_exploratory,
        "records_openalex": stats.records_openalex,
        "duplicates_removed": stats.duplicates_removed,
        "records_after_deduplication": stats.records_after_deduplication,
        "records_screened_title_abstract": stats.records_screened_title_abstract,
        "records_excluded_title_abstract": stats.records_excluded_title_abstract,
        "reports_sought_for_retrieval": stats.reports_sought_for_retrieval,
        "reports_not_retrieved": stats.reports_not_retrieved,
        "reports_assessed_for_eligibility": stats.reports_assessed_for_eligibility,
        "reports_excluded_full_text": stats.reports_excluded_full_text,
        "studies_included_review": stats.studies_included_review,
        "reports_included_review": stats.reports_included_review,
        "doi_coverage_pct": stats.doi_coverage_pct,
        "abstract_coverage_pct": stats.abstract_coverage_pct,
        "year_min": stats.year_min,
        "year_max": stats.year_max,
    }


def _path_metadata(path_like: Optional[Path | str]) -> dict:
    if not path_like:
        return {"path": "", "exists": False, "sha256": "", "size_bytes": 0}
    path = Path(path_like)
    if not path.exists():
        return {"path": str(path), "exists": False, "sha256": "", "size_bytes": 0}
    return {
        "path": str(path),
        "exists": True,
        "sha256": file_sha256(path),
        "size_bytes": path.stat().st_size,
    }


def build_source_inventory(
    openalex_inputs: dict,
    openalex_csv_paths: dict,
    scopus_inputs: dict,
) -> dict:
    inventory = {"openalex": {}, "scopus": {}}

    for key, original in openalex_inputs.items():
        resolved = openalex_csv_paths.get(key)
        inventory["openalex"][key] = {
            "original": _path_metadata(original),
            "resolved_csv": _path_metadata(resolved),
            "input_format": Path(original).suffix.lower().lstrip(".") if original else "",
        }

    for key, original in scopus_inputs.items():
        inventory["scopus"][key] = _path_metadata(original)

    return inventory


def build_audit_snapshot(
    stage: str,
    harmonized_df: pd.DataFrame,
    deduplicated_df: pd.DataFrame,
    screening_df: pd.DataFrame,
    stats: PipelineStats,
    source_inventory: dict,
) -> AuditSnapshot:
    source_dist = Counter()
    group_dist = Counter()

    if not harmonized_df.empty:
        if "source_db" in harmonized_df.columns:
            source_dist.update(
                harmonized_df["source_db"].fillna("unknown").astype(str).tolist()
            )
        if "search_group" in harmonized_df.columns:
            group_dist.update(
                harmonized_df["search_group"].fillna("unknown").astype(str).tolist()
            )

    notes = [
        "Los recuentos PRISMA se recalculan desde archivos procesados, no manualmente.",
        "Cada artefacto principal se registra con hash SHA-256 en manifest.json.",
        "La deduplicación usa DOI y reglas de prioridad entre base y estrategia de búsqueda.",
    ]

    return AuditSnapshot(
        generated_at=now_iso(),
        stage=stage,
        row_counts={
            "harmonized": int(len(harmonized_df)),
            "deduplicated": int(len(deduplicated_df)),
            "screening": int(len(screening_df)),
            "duplicates_removed": int(len(harmonized_df) - len(deduplicated_df)),
        },
        coverage={
            "doi_coverage_pct": float(stats.doi_coverage_pct),
            "abstract_coverage_pct": float(stats.abstract_coverage_pct),
            "year_min": stats.year_min,
            "year_max": stats.year_max,
        },
        distributions={
            "records_by_source_db": dict(source_dist),
            "records_by_search_group": dict(group_dist),
        },
        source_inventory=source_inventory,
        notes=notes,
    )


def save_audit_snapshot(snapshot: AuditSnapshot) -> Tuple[Path, Path]:
    json_path = get_processed_dir() / "audit_snapshot.json"
    md_path = get_processed_dir() / "audit_summary.md"
    write_json(json_path, asdict(snapshot))

    md_lines = [
        "# Audit Summary",
        "",
        f"Generated at: {snapshot.generated_at}",
        f"Stage: {snapshot.stage}",
        "",
        "## Row counts",
        "",
    ]
    for key, value in snapshot.row_counts.items():
        md_lines.append(f"- {key}: {value}")

    md_lines += ["", "## Coverage", ""]
    for key, value in snapshot.coverage.items():
        md_lines.append(f"- {key}: {value}")

    md_lines += ["", "## Distributions", ""]
    for section, values in snapshot.distributions.items():
        md_lines.append(f"### {section}")
        if values:
            for key, value in values.items():
                md_lines.append(f"- {key}: {value}")
        else:
            md_lines.append("- No available data")
        md_lines.append("")

    md_lines += ["## Audit notes", ""]
    for note in snapshot.notes:
        md_lines.append(f"- {note}")

    md_lines += [
        "",
        "## Source inventory",
        "",
        json.dumps(snapshot.source_inventory, ensure_ascii=False, indent=2),
    ]
    md_path.write_text("\n".join(md_lines), encoding="utf-8")
    return json_path, md_path


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


def generate_manuscript_tables(
    harmonized_df: pd.DataFrame,
    deduplicated_df: pd.DataFrame,
    screening_df: pd.DataFrame,
    stats: PipelineStats,
) -> str:
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

    prisma_table = pd.DataFrame(
        [
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
        ],
        columns=["Stage", "N"],
    )

    source_table_md = dataframe_to_md_or_text(pd.DataFrame(source_summary)) if source_summary else "No available data."
    prisma_table_md = dataframe_to_md_or_text(prisma_table)

    return f"""# Manuscript Tables

## Table 1. Source identification summary

{source_table_md}

## Table 2. PRISMA counts

{prisma_table_md}

## Table 3. Publication years in deduplicated corpus

{year_summary_md}

## Table 4. Top journals/sources

{top_journals_md}
"""


def save_sqlite(
    harmonized_df: pd.DataFrame,
    deduplicated_df: pd.DataFrame,
    screening_df: pd.DataFrame,
) -> Path:
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
            manifest["files"].append(
                {
                    "path": str(path),
                    "sha256": file_sha256(path),
                    "size_bytes": path.stat().st_size,
                }
            )
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
# Enterprise outputs
# =========================================================

def _artifact_catalog() -> Dict[str, str]:
    return {
        "protocol_path": "sr_01_protocol.json",
        "source_manifest_json": "sr_02_source_manifest.json",
        "search_summary_json": "sr_03_search_summary.json",
        "harmonized_records_csv": "sr_04_records_harmonized.csv",
        "deduplicated_records_csv": "sr_05_records_deduplicated.csv",
        "deduplication_groups_csv": "sr_06_deduplication_groups.csv",
        "screening_matrix_csv": "sr_07_screening_matrix.csv",
        "quality_profile_json": "sr_08_quality_profile.json",
        "prisma_counts_json": "sr_09_prisma_counts.json",
        "prisma_diagram_png": "sr_10_prisma_diagram.png",
        "prisma_diagram_svg": "sr_10_prisma_diagram.svg",
        "manuscript_tables_md": "sr_11_manuscript_tables.md",
        "records_sqlite": "sr_12_records.sqlite",
        "manifest_json": "sr_13_manifest.json",
        "audit_snapshot_json": "sr_14_audit_snapshot.json",
        "audit_summary_md": "sr_15_audit_summary.md",
        "executive_report_html": "sr_16_executive_report.html",
        "methodology_checklist_json": "sr_17_methodology_checklist.json",
        "methodology_checklist_md": "sr_18_methodology_checklist.md",
        "reproducibility_package_zip": "sr_19_reproducibility_package.zip",
        "validation_report_stage_1_pico_json": "sr_20_validation_stage_1.json",
        "validation_report_stage_1_pico_csv": "sr_20_validation_stage_1.csv",
        "validation_report_stage_1_pico_md": "sr_20_validation_stage_1.md",
        "validation_report_stage_2_search_json": "sr_21_validation_stage_2.json",
        "validation_report_stage_2_search_csv": "sr_21_validation_stage_2.csv",
        "validation_report_stage_2_search_md": "sr_21_validation_stage_2.md",
        "validation_report_stage_3_screening_json": "sr_22_validation_stage_3.json",
        "validation_report_stage_3_screening_csv": "sr_22_validation_stage_3.csv",
        "validation_report_stage_3_screening_md": "sr_22_validation_stage_3.md",
        "validation_report_stage_5_quality_json": "sr_23_validation_stage_5.json",
        "validation_report_stage_5_quality_csv": "sr_23_validation_stage_5.csv",
        "validation_report_stage_5_quality_md": "sr_23_validation_stage_5.md",
        "validation_report_stage_6_synthesis_json": "sr_24_validation_stage_6.json",
        "validation_report_stage_6_synthesis_csv": "sr_24_validation_stage_6.csv",
        "validation_report_stage_6_synthesis_md": "sr_24_validation_stage_6.md",
        "validation_report_stage_7_prisma_json": "sr_25_validation_stage_7.json",
        "validation_report_stage_7_prisma_csv": "sr_25_validation_stage_7.csv",
        "validation_report_stage_7_prisma_md": "sr_25_validation_stage_7.md",
    }


def _copy_artifact_if_available(src_path_like: Optional[str | Path], dst_name: str) -> Optional[Path]:
    if not src_path_like:
        return None
    src_path = Path(src_path_like)
    if not src_path.exists():
        return None
    dst_path = get_processed_dir() / dst_name
    if src_path.resolve() == dst_path.resolve():
        return dst_path
    shutil.copy2(src_path, dst_path)
    return dst_path


def _build_methodology_checklist(state: dict) -> dict:
    completed = set(state.get("completed_stages", []))
    protocol = read_json(Path(state.get("protocol_path", get_protocol_path())), default={})
    stats = state.get("stats", {}) or {}
    artifacts = state.get("artifacts", {}) or {}
    checks = []

    def add(check_id: str, category: str, description: str, passed: bool, evidence: str, severity: str = "medium") -> None:
        checks.append(
            {
                "check_id": check_id,
                "category": category,
                "description": description,
                "status": "PASS" if passed else "FAIL",
                "severity": severity,
                "evidence": evidence,
            }
        )

    add("M01", "Protocol", "Existe pregunta de investigación", bool(normalize_text(protocol.get("research_question", ""))), "protocol.json", "critical")
    add("M02", "Protocol", "Existen criterios de inclusión", bool(normalize_text(protocol.get("inclusion_criteria", ""))), "protocol.json", "critical")
    add("M03", "Protocol", "Existen criterios de exclusión", bool(normalize_text(protocol.get("exclusion_criteria", ""))), "protocol.json", "critical")
    add("M04", "Search", "La etapa 2 fue completada", "stage_2_search" in completed, "pipeline_state.completed_stages", "critical")
    add("M05", "Search", "Existe manifiesto de fuentes", Path(artifacts.get("source_manifest_json", "missing")).exists(), artifacts.get("source_manifest_json", ""), "high")
    add("M06", "Search", "Existe resumen de búsqueda", Path(artifacts.get("search_summary_json", "missing")).exists(), artifacts.get("search_summary_json", ""), "high")
    add("M07", "Data", "Existe corpus harmonizado", Path(artifacts.get("harmonized_records_csv", "missing")).exists(), artifacts.get("harmonized_records_csv", ""), "critical")
    add("M08", "Data", "Existe corpus deduplicado", Path(artifacts.get("deduplicated_records_csv", "missing")).exists(), artifacts.get("deduplicated_records_csv", ""), "critical")
    add("M09", "Data", "La deduplicación no incrementa registros", int(stats.get("records_after_deduplication", 0)) <= int(stats.get("records_identified_total", 0)), f"identified={stats.get('records_identified_total')} deduplicated={stats.get('records_after_deduplication')}", "critical")
    add("M10", "Data", "Se removieron duplicados con valor no negativo", int(stats.get("duplicates_removed", 0)) >= 0, f"duplicates_removed={stats.get('duplicates_removed')}", "high")
    add("M11", "Screening", "Existe screening matrix", Path(artifacts.get("screening_matrix_csv", "missing")).exists(), artifacts.get("screening_matrix_csv", ""), "critical")
    add("M12", "Quality", "Existe perfil de calidad", Path(artifacts.get("quality_profile_json", "missing")).exists(), artifacts.get("quality_profile_json", ""), "medium")
    add("M13", "PRISMA", "Existe conteo PRISMA", Path(artifacts.get("prisma_counts_json", "missing")).exists(), artifacts.get("prisma_counts_json", ""), "high")
    add("M14", "Audit", "Existe snapshot de auditoría", Path(artifacts.get("audit_snapshot_json", "missing")).exists(), artifacts.get("audit_snapshot_json", ""), "high")
    add("M15", "Audit", "Existe paquete de reproducibilidad", Path(artifacts.get("reproducibility_package_zip", "missing")).exists(), artifacts.get("reproducibility_package_zip", ""), "high")

    doi_cov = float(stats.get("doi_coverage_pct", 0.0) or 0.0)
    abs_cov = float(stats.get("abstract_coverage_pct", 0.0) or 0.0)
    add("M16", "Quality", "Cobertura DOI calculada en rango 0-100", 0.0 <= doi_cov <= 100.0, f"doi_coverage_pct={doi_cov}", "medium")
    add("M17", "Quality", "Cobertura abstract calculada en rango 0-100", 0.0 <= abs_cov <= 100.0, f"abstract_coverage_pct={abs_cov}", "medium")

    summary = {
        "total": len(checks),
        "passed": sum(1 for c in checks if c["status"] == "PASS"),
        "failed": sum(1 for c in checks if c["status"] == "FAIL"),
        "critical_failed": sum(1 for c in checks if c["status"] == "FAIL" and c["severity"] == "critical"),
    }
    overall = "PASS" if summary["failed"] == 0 else ("FAIL" if summary["critical_failed"] else "PASS_WITH_FINDINGS")
    return {
        "generated_at": now_iso(),
        "overall_status": overall,
        "summary": summary,
        "checks": checks,
    }


def save_methodology_checklist(state: dict) -> Tuple[Path, Path]:
    checklist = _build_methodology_checklist(state)
    json_path = get_processed_dir() / "methodology_audit_checklist.json"
    md_path = get_processed_dir() / "methodology_audit_checklist.md"
    write_json(json_path, checklist)

    lines = [
        "# Checklist de auditoría metodológica",
        "",
        f"Generado: {checklist['generated_at']}",
        f"Estado global: {checklist['overall_status']}",
        "",
        "## Resumen",
        "",
    ]
    for k, v in checklist["summary"].items():
        lines.append(f"- {k}: {v}")
    lines += ["", "## Checks", ""]
    for item in checklist["checks"]:
        lines.append(
            f"- [{item['status']}] {item['check_id']} | {item['category']} | "
            f"{item['description']} | evidencia: {item['evidence']}"
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path


def save_executive_report_html(state: dict) -> Path:
    stats = state.get("stats", {}) or {}
    artifacts = state.get("artifacts", {}) or {}
    completed = state.get("completed_stages", []) or []
    checklist = read_json(
        Path(
            artifacts.get(
                "methodology_checklist_json",
                get_processed_dir() / "methodology_audit_checklist.json",
            )
        ),
        default={},
    )
    audit = read_json(
        Path(
            artifacts.get(
                "audit_snapshot_json",
                get_processed_dir() / "audit_snapshot.json",
            )
        ),
        default={},
    )

    def row(label: str, value) -> str:
        return f"<tr><th>{escape(str(label))}</th><td>{escape(str(value))}</td></tr>"

    key_metrics = [
        ("Records identified", stats.get("records_identified_total", 0)),
        ("Duplicates removed", stats.get("duplicates_removed", 0)),
        ("Records after deduplication", stats.get("records_after_deduplication", 0)),
        ("DOI coverage %", stats.get("doi_coverage_pct", 0.0)),
        ("Abstract coverage %", stats.get("abstract_coverage_pct", 0.0)),
        ("Year min", stats.get("year_min", "")),
        ("Year max", stats.get("year_max", "")),
    ]

    artifact_rows = []
    for key in sorted(artifacts):
        path = Path(artifacts[key])
        artifact_rows.append(
            f"<tr><td>{escape(key)}</td><td>{escape(str(path))}</td><td>{'yes' if path.exists() else 'no'}</td></tr>"
        )

    checklist_rows = []
    for item in checklist.get("checks", [])[:50]:
        checklist_rows.append(
            "<tr>"
            f"<td>{escape(item.get('check_id', ''))}</td>"
            f"<td>{escape(item.get('category', ''))}</td>"
            f"<td>{escape(item.get('status', ''))}</td>"
            f"<td>{escape(item.get('description', ''))}</td>"
            f"<td>{escape(item.get('evidence', ''))}</td>"
            "</tr>"
        )

    dist_rows = []
    for section, values in (audit.get("distributions", {}) or {}).items():
        for key, value in (values or {}).items():
            dist_rows.append(
                f"<tr><td>{escape(str(section))}</td><td>{escape(str(key))}</td><td>{escape(str(value))}</td></tr>"
            )

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Executive Audit Report</title>
<style>
body{{font-family:Arial,sans-serif;margin:28px;color:#1f2937;}}
h1,h2{{color:#0f172a;}}
.grid{{display:grid;grid-template-columns:repeat(3,minmax(180px,1fr));gap:12px;margin:18px 0;}}
.card{{border:1px solid #cbd5e1;border-radius:10px;padding:14px;background:#f8fafc;}}
.table{{border-collapse:collapse;width:100%;margin:12px 0 24px 0;}}
.table th,.table td{{border:1px solid #cbd5e1;padding:8px;text-align:left;vertical-align:top;}}
.badge{{display:inline-block;padding:4px 10px;border-radius:999px;border:1px solid #94a3b8;background:#eff6ff;}}
.small{{font-size:12px;color:#475569;}}
</style>
</head>
<body>
<h1>Systematic Review Enterprise Report</h1>
<p class="small">Generated at {escape(now_iso())}</p>
<p><span class="badge">Checklist status: {escape(str(checklist.get('overall_status', 'N/A')))}</span></p>

<h2>Executive summary</h2>
<div class="grid">
{''.join(f'<div class="card"><div class="small">{escape(str(k))}</div><div><strong>{escape(str(v))}</strong></div></div>' for k, v in key_metrics)}
</div>

<h2>Completed stages</h2>
<p>{escape(', '.join(completed) if completed else 'No completed stages')}</p>

<h2>Key metrics</h2>
<table class="table">
{''.join(row(k, v) for k, v in key_metrics)}
</table>

<h2>Audit distributions</h2>
<table class="table">
<tr><th>Section</th><th>Key</th><th>Value</th></tr>
{''.join(dist_rows) if dist_rows else '<tr><td colspan="3">No available data</td></tr>'}
</table>

<h2>Methodology checklist</h2>
<table class="table">
<tr><th>ID</th><th>Category</th><th>Status</th><th>Description</th><th>Evidence</th></tr>
{''.join(checklist_rows) if checklist_rows else '<tr><td colspan="5">No checklist available</td></tr>'}
</table>

<h2>Artifact register</h2>
<table class="table">
<tr><th>Artifact key</th><th>Path</th><th>Exists</th></tr>
{''.join(artifact_rows) if artifact_rows else '<tr><td colspan="3">No artifacts registered</td></tr>'}
</table>
</body>
</html>"""

    path = get_processed_dir() / "executive_audit_report.html"
    path.write_text(html, encoding="utf-8")
    return path


def refresh_enterprise_outputs(state: dict) -> dict:
    state.setdefault("artifacts", {})

    checklist_json, checklist_md = save_methodology_checklist(state)
    state["artifacts"]["methodology_checklist_json"] = str(checklist_json)
    state["artifacts"]["methodology_checklist_md"] = str(checklist_md)

    exec_html = save_executive_report_html(state)
    state["artifacts"]["executive_report_html"] = str(exec_html)

    catalog = _artifact_catalog()
    standardized = {}
    for key, dst_name in catalog.items():
        src_path_like = state.get("protocol_path") if key == "protocol_path" else state["artifacts"].get(key)
        dst = _copy_artifact_if_available(src_path_like, dst_name)
        if dst is not None:
            standardized[f"std_{key}"] = str(dst)

    state["artifacts"].update(standardized)
    save_pipeline_state(state)
    return state


# =========================================================
# Stage 1
# =========================================================

def run_pico_stage(protocol_data: dict, progress: ProgressCallback = None) -> dict:
    emit(progress, "⏳ Guardando etapa 1...")
    protocol_path = save_protocol(protocol_data)

    checks = validate_protocol_data(protocol_data)
    validation = save_validation_reports("stage_1_pico", checks)

    state = load_pipeline_state()
    state["protocol_path"] = str(protocol_path)
    state.setdefault("artifacts", {})
    state["artifacts"]["validation_report_stage_1_pico_json"] = validation["json"]
    state["artifacts"]["validation_report_stage_1_pico_csv"] = validation["csv"]
    state["artifacts"]["validation_report_stage_1_pico_md"] = validation["md"]
    state["artifacts"]["validation_history_jsonl"] = validation["history"]
    state = mark_stage_completed("stage_1_pico", state)
    save_pipeline_state(state)
    state = refresh_enterprise_outputs(state)

    audit_event("stage_1_pico", "finished", {"validation_status": validation["global_status"]})
    emit(progress, f"✅ Etapa 1 guardada. Validación: {validation['global_status']}")

    return {
        "protocol_path": str(protocol_path),
        "validation": validation,
        "completed_stages": state.get("completed_stages", []),
        "artifacts": state.get("artifacts", {}),
        "stats": state.get("stats", {}),
        "sources": state.get("sources", {}),
    }


# =========================================================
# Stage 2
# =========================================================

def run_search_stage(
    openalex_core_input: Optional[str] = None,
    openalex_exploratory_1_input: Optional[str] = None,
    openalex_exploratory_2_input: Optional[str] = None,
    scopus_core_csv: Optional[str] = None,
    scopus_exploratory_1_csv: Optional[str] = None,
    scopus_exploratory_2_csv: Optional[str] = None,
    user_input_openalex: Optional[str] = None,
    scopus_exploratory_csv: Optional[str] = None,
    progress: ProgressCallback = None,
    per_page_openalex: int = 100,
) -> Dict:
    del per_page_openalex

    emit(progress, "⏳ Validando fuentes de la etapa 2...")

    openalex_inputs = {
        "core": openalex_core_input or user_input_openalex,
        "exploratory_1": openalex_exploratory_1_input,
        "exploratory_2": openalex_exploratory_2_input,
    }
    scopus_inputs = {
        "core": scopus_core_csv,
        "exploratory_1": scopus_exploratory_1_csv or scopus_exploratory_csv,
        "exploratory_2": scopus_exploratory_2_csv,
    }

    validate_stage2_source_bundle(openalex_inputs, scopus_inputs)

    emit(progress, "📥 Convirtiendo archivos OpenAlex JSON a CSV cuando corresponde...")
    openalex_csv_paths = {
        key: resolve_openalex_input_to_csv(value, key)
        for key, value in openalex_inputs.items()
    }

    emit(progress, "📚 Leyendo archivos Scopus y OpenAlex...")
    scopus_core_df = (
        load_scopus_csv(Path(scopus_inputs["core"]), "scopus", "core")
        if scopus_inputs.get("core") else pd.DataFrame()
    )
    scopus_exploratory_1_df = (
        load_scopus_csv(Path(scopus_inputs["exploratory_1"]), "scopus", "exploratory_1")
        if scopus_inputs.get("exploratory_1") else pd.DataFrame()
    )
    scopus_exploratory_2_df = (
        load_scopus_csv(Path(scopus_inputs["exploratory_2"]), "scopus", "exploratory_2")
        if scopus_inputs.get("exploratory_2") else pd.DataFrame()
    )

    openalex_core_df = (
        load_scopus_csv(openalex_csv_paths["core"], "openalex", "core")
        if openalex_csv_paths.get("core") else pd.DataFrame()
    )
    openalex_exploratory_1_df = (
        load_scopus_csv(openalex_csv_paths["exploratory_1"], "openalex", "exploratory_1")
        if openalex_csv_paths.get("exploratory_1") else pd.DataFrame()
    )
    openalex_exploratory_2_df = (
        load_scopus_csv(openalex_csv_paths["exploratory_2"], "openalex", "exploratory_2")
        if openalex_csv_paths.get("exploratory_2") else pd.DataFrame()
    )

    emit(progress, "🧩 Harmonizando registros...")
    harmonized_df = harmonize_sources(
        scopus_core_df,
        scopus_exploratory_1_df,
        scopus_exploratory_2_df,
        openalex_core_df,
        openalex_exploratory_1_df,
        openalex_exploratory_2_df,
    )
    harmonized_path = get_processed_dir() / "harmonized_records.csv"
    harmonized_df.to_csv(harmonized_path, index=False, encoding="utf-8-sig")

    emit(progress, "🧹 Deduplicando registros...")
    deduplicated_df, dedup_groups_df = deduplicate_records(harmonized_df)
    deduplicated_path = get_processed_dir() / "deduplicated_records.csv"
    dedup_groups_path = get_processed_dir() / "deduplication_groups.csv"
    deduplicated_df.to_csv(deduplicated_path, index=False, encoding="utf-8-sig")
    dedup_groups_df.to_csv(dedup_groups_path, index=False, encoding="utf-8-sig")

    emit(progress, "📝 Generando matriz de cribado...")
    screening_df = build_screening_matrix(deduplicated_df)
    screening_path = get_processed_dir() / "screening_matrix.csv"
    screening_df.to_csv(screening_path, index=False, encoding="utf-8-sig")

    stats = compute_prisma_counts(harmonized_df, deduplicated_df, screening_df)
    quality_profile = build_quality_profile(deduplicated_df)

    quality_profile_path = get_processed_dir() / "quality_profile.json"
    prisma_counts_path = get_processed_dir() / "prisma_counts.json"
    write_json(quality_profile_path, quality_profile)
    write_json(prisma_counts_path, asdict(stats))

    prisma_png_path, prisma_svg_path = generate_prisma_diagram(stats)

    manuscript_tables = generate_manuscript_tables(
        harmonized_df, deduplicated_df, screening_df, stats
    )
    manuscript_tables_path = get_processed_dir() / "manuscript_tables.md"
    manuscript_tables_path.write_text(manuscript_tables, encoding="utf-8")

    sqlite_path = save_sqlite(harmonized_df, deduplicated_df, screening_df)

    source_inventory = build_source_inventory(openalex_inputs, openalex_csv_paths, scopus_inputs)
    source_manifest = {
        "generated_at": now_iso(),
        "source_inventory": source_inventory,
        "openalex_inputs": {k: str(v) if v else "" for k, v in openalex_csv_paths.items()},
        "scopus_inputs": scopus_inputs,
    }
    source_manifest_path = save_source_manifest(source_manifest)

    audit_snapshot = build_audit_snapshot(
        "stage_2_search",
        harmonized_df,
        deduplicated_df,
        screening_df,
        stats,
        source_inventory,
    )
    audit_snapshot_json, audit_summary_md = save_audit_snapshot(audit_snapshot)

    search_summary_path = save_search_summary(
        {
            "generated_at": now_iso(),
            "harmonized_rows": len(harmonized_df),
            "deduplicated_rows": len(deduplicated_df),
            "duplicates_removed": len(harmonized_df) - len(deduplicated_df),
            "stats": _important_stats(stats),
            "source_inventory": source_inventory,
            "audit_snapshot_json": str(audit_snapshot_json),
        }
    )

    validation_stage2 = save_validation_reports(
        "stage_2_search",
        validate_stage2_inputs_for_report(openalex_inputs, scopus_inputs),
    )

    manifest_path = save_manifest(
        [
            harmonized_path,
            deduplicated_path,
            dedup_groups_path,
            screening_path,
            quality_profile_path,
            prisma_counts_path,
            prisma_png_path,
            prisma_svg_path,
            manuscript_tables_path,
            sqlite_path,
            source_manifest_path,
            search_summary_path,
            audit_snapshot_json,
            audit_summary_md,
            Path(validation_stage2["json"]),
            Path(validation_stage2["csv"]),
            Path(validation_stage2["md"]),
        ]
    )

    reproducibility_zip = create_reproducibility_package(
        [
            harmonized_path,
            deduplicated_path,
            dedup_groups_path,
            screening_path,
            quality_profile_path,
            prisma_counts_path,
            prisma_png_path,
            prisma_svg_path,
            manuscript_tables_path,
            sqlite_path,
            source_manifest_path,
            search_summary_path,
            audit_snapshot_json,
            audit_summary_md,
            manifest_path,
        ]
    )

    state = load_pipeline_state()
    state["stats"] = _important_stats(stats)
    state["sources"] = source_manifest
    state.setdefault("artifacts", {})
    state["artifacts"].update(
        {
            "harmonized_records_csv": str(harmonized_path),
            "deduplicated_records_csv": str(deduplicated_path),
            "deduplication_groups_csv": str(dedup_groups_path),
            "screening_matrix_csv": str(screening_path),
            "quality_profile_json": str(quality_profile_path),
            "prisma_counts_json": str(prisma_counts_path),
            "prisma_diagram_png": str(prisma_png_path),
            "prisma_diagram_svg": str(prisma_svg_path),
            "manuscript_tables_md": str(manuscript_tables_path),
            "records_sqlite": str(sqlite_path),
            "manifest_json": str(manifest_path),
            "source_manifest_json": str(source_manifest_path),
            "search_summary_json": str(search_summary_path),
            "audit_snapshot_json": str(audit_snapshot_json),
            "audit_summary_md": str(audit_summary_md),
            "reproducibility_package_zip": str(reproducibility_zip),
            "validation_report_stage_2_search_json": validation_stage2["json"],
            "validation_report_stage_2_search_csv": validation_stage2["csv"],
            "validation_report_stage_2_search_md": validation_stage2["md"],
            "validation_history_jsonl": validation_stage2["history"],
        }
    )

    state = mark_stage_completed("stage_2_search", state)
    save_pipeline_state(state)
    state = refresh_enterprise_outputs(state)

    audit_event(
        "stage_2_search",
        "finished",
        {
            "harmonized_rows": len(harmonized_df),
            "deduplicated_rows": len(deduplicated_df),
            "duplicates_removed": len(harmonized_df) - len(deduplicated_df),
        },
    )

    emit(
        progress,
        "✅ Etapa 2 finalizada. "
        f"Harmonizados: {len(harmonized_df)} | "
        f"Deduplicados: {len(deduplicated_df)} | "
        f"Duplicados removidos: {len(harmonized_df) - len(deduplicated_df)}",
    )

    return {
        "stats": state["stats"],
        "sources": state["sources"],
        "artifacts": state["artifacts"],
        "completed_stages": state.get("completed_stages", []),
        "validation": validation_stage2,
    }


# =========================================================
# Stage 3
# =========================================================

def run_screening_stage(progress: ProgressCallback = None) -> Dict:
    emit(progress, "ℹ️ La etapa 3 usa screening_matrix.csv ya generado en la etapa 2.")

    screening_path = get_processed_dir() / "screening_matrix.csv"
    dedup_path = get_processed_dir() / "deduplicated_records.csv"

    if not screening_path.exists() or not dedup_path.exists():
        raise RuntimeError(
            "Primero debe ejecutar la etapa 2 para generar "
            "screening_matrix.csv y deduplicated_records.csv."
        )

    screening_df = pd.read_csv(screening_path, encoding="utf-8-sig")

    validation = save_validation_reports("stage_3_screening", [])

    state = load_pipeline_state()
    state.setdefault("artifacts", {})
    state["artifacts"].update(
        {
            "validation_report_stage_3_screening_json": validation["json"],
            "validation_report_stage_3_screening_csv": validation["csv"],
            "validation_report_stage_3_screening_md": validation["md"],
            "screening_matrix_csv": str(screening_path),
        }
    )

    state = mark_stage_completed("stage_3_screening", state)
    save_pipeline_state(state)
    state = refresh_enterprise_outputs(state)

    emit(progress, f"✅ Etapa 3 actualizada. Registros en screening: {len(screening_df)}")

    return {
        "artifacts": state["artifacts"],
        "completed_stages": state.get("completed_stages", []),
        "validation": validation,
        "stats": state.get("stats", {}),
        "sources": state.get("sources", {}),
    }


# =========================================================
# Stages 4, 5, 6, 7
# =========================================================

def run_extraction_stage(progress: ProgressCallback = None) -> Dict:
    emit(progress, "ℹ️ Etapa 4 registrada sin cambios adicionales.")
    state = mark_stage_completed("stage_4_extraction", load_pipeline_state())
    save_pipeline_state(state)
    state = refresh_enterprise_outputs(state)
    return {
        "artifacts": state.get("artifacts", {}),
        "completed_stages": state.get("completed_stages", []),
        "stats": state.get("stats", {}),
        "sources": state.get("sources", {}),
    }


def run_quality_stage(progress: ProgressCallback = None) -> Dict:
    dedup_path = get_processed_dir() / "deduplicated_records.csv"
    if not dedup_path.exists():
        raise RuntimeError(
            "Primero debe ejecutar la etapa 2 para generar deduplicated_records.csv."
        )

    dedup_df = pd.read_csv(dedup_path, encoding="utf-8-sig")
    profile = build_quality_profile(dedup_df)
    quality_path = get_processed_dir() / "quality_profile.json"
    write_json(quality_path, profile)

    validation = save_validation_reports("stage_5_quality", [])

    state = load_pipeline_state()
    state.setdefault("artifacts", {})
    state["artifacts"].update(
        {
            "quality_profile_json": str(quality_path),
            "validation_report_stage_5_quality_json": validation["json"],
            "validation_report_stage_5_quality_csv": validation["csv"],
            "validation_report_stage_5_quality_md": validation["md"],
        }
    )
    state = mark_stage_completed("stage_5_quality", state)
    save_pipeline_state(state)
    state = refresh_enterprise_outputs(state)

    emit(progress, "✅ Etapa 5 actualizada.")
    return {
        "artifacts": state["artifacts"],
        "completed_stages": state.get("completed_stages", []),
        "validation": validation,
        "stats": state.get("stats", {}),
        "sources": state.get("sources", {}),
    }


def run_synthesis_stage(progress: ProgressCallback = None) -> Dict:
    harm = load_df_if_exists(get_processed_dir() / "harmonized_records.csv")
    dedup = load_df_if_exists(get_processed_dir() / "deduplicated_records.csv")
    screening = load_df_if_exists(get_processed_dir() / "screening_matrix.csv")

    stats_data = load_pipeline_state().get("stats", {})
    stats = PipelineStats(
        **{
            k: stats_data.get(k)
            for k in PipelineStats.__dataclass_fields__.keys()
            if k in stats_data
        }
    )

    manuscript_tables = generate_manuscript_tables(harm, dedup, screening, stats)
    manuscript_tables_path = get_processed_dir() / "manuscript_tables.md"
    manuscript_tables_path.write_text(manuscript_tables, encoding="utf-8")

    validation = save_validation_reports("stage_6_synthesis", [])

    state = load_pipeline_state()
    state.setdefault("artifacts", {})
    state["artifacts"].update(
        {
            "manuscript_tables_md": str(manuscript_tables_path),
            "validation_report_stage_6_synthesis_json": validation["json"],
            "validation_report_stage_6_synthesis_csv": validation["csv"],
            "validation_report_stage_6_synthesis_md": validation["md"],
        }
    )
    state = mark_stage_completed("stage_6_synthesis", state)
    save_pipeline_state(state)
    state = refresh_enterprise_outputs(state)

    emit(progress, "✅ Etapa 6 actualizada.")
    return {
        "artifacts": state["artifacts"],
        "completed_stages": state.get("completed_stages", []),
        "validation": validation,
        "stats": state.get("stats", {}),
        "sources": state.get("sources", {}),
    }


def run_prisma_stage(progress: ProgressCallback = None) -> Dict:
    harm = load_df_if_exists(get_processed_dir() / "harmonized_records.csv")
    dedup = load_df_if_exists(get_processed_dir() / "deduplicated_records.csv")
    screening = load_df_if_exists(get_processed_dir() / "screening_matrix.csv")

    if harm.empty or dedup.empty:
        raise RuntimeError(
            "Primero debe ejecutar la etapa 2 para generar los archivos base de PRISMA."
        )

    stats = compute_prisma_counts(harm, dedup, screening)
    prisma_counts_path = get_processed_dir() / "prisma_counts.json"
    write_json(prisma_counts_path, asdict(stats))
    prisma_png_path, prisma_svg_path = generate_prisma_diagram(stats)

    validation = save_validation_reports("stage_7_prisma", [])

    state = load_pipeline_state()
    state["stats"] = _important_stats(stats)
    state.setdefault("artifacts", {})
    state["artifacts"].update(
        {
            "prisma_counts_json": str(prisma_counts_path),
            "prisma_diagram_png": str(prisma_png_path),
            "prisma_diagram_svg": str(prisma_svg_path),
            "validation_report_stage_7_prisma_json": validation["json"],
            "validation_report_stage_7_prisma_csv": validation["csv"],
            "validation_report_stage_7_prisma_md": validation["md"],
        }
    )
    state = mark_stage_completed("stage_7_prisma", state)
    save_pipeline_state(state)
    state = refresh_enterprise_outputs(state)

    emit(progress, "✅ Etapa 7 actualizada.")
    return {
        "artifacts": state["artifacts"],
        "completed_stages": state.get("completed_stages", []),
        "validation": validation,
        "stats": state.get("stats", {}),
        "sources": state.get("sources", {}),
    }