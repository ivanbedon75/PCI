from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


VALID_SOURCES = {"openalex", "scopus_csv"}
VALID_STRATEGIES = {"core", "exploratory"}


@dataclass
class SourceInput:
    source: str
    strategy: str
    label: str
    query: str | None = None
    file: str | None = None
    notes: str | None = None

    def __post_init__(self) -> None:
        if self.source not in VALID_SOURCES:
            raise ValueError(f"Fuente no soportada: {self.source}")
        if self.strategy not in VALID_STRATEGIES:
            raise ValueError(f"Estrategia no soportada: {self.strategy}")
        if not self.label.strip():
            raise ValueError("label no puede estar vacío")

        if self.source == "openalex" and not (self.query or "").strip():
            raise ValueError("OpenAlex requiere query")
        if self.source == "scopus_csv" and not (self.file or "").strip():
            raise ValueError("scopus_csv requiere file")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class InputValidationResult:
    label: str
    source: str
    strategy: str
    is_valid: bool
    exists: bool | None = None
    row_count: int | None = None
    sha256: str | None = None
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RunArtifacts:
    raw_json_path: str | None = None
    harmonized_csv_path: str | None = None
    deduplicated_csv_path: str | None = None
    quality_json_path: str | None = None
    screening_csv_path: str | None = None
    prisma_json_path: str | None = None
    manuscript_md_path: str | None = None
    sqlite_db_path: str | None = None
    parquet_path: str | None = None
    manifest_path: str | None = None
    audit_jsonl_path: str | None = None
    run_log_jsonl_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RunSummary:
    run_id: str
    created_at_utc: str
    protocol_name: str
    protocol_version: str
    inputs: list[dict[str, Any]]
    total_raw_records: int
    total_harmonized_records: int
    total_deduplicated_records: int
    duplicates_removed: int
    artifacts: dict[str, Any]
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)