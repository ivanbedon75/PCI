from __future__ import annotations

SUPPORTED_SOURCES = {"openalex", "scopus_csv"}

CANONICAL_COLUMNS = [
    "source",
    "source_record_id",
    "doi",
    "title",
    "year",
    "journal",
    "volume",
    "issue",
    "pages",
    "authors",
    "affiliations",
    "abstract",
    "keywords",
    "document_type",
    "language",
    "url",
    "cited_by",
    "issn",
    "publisher",
    "raw_source_file",
]

OUTPUT_FILES = {
    "raw_json": "raw_records.json",
    "harmonized_csv": "harmonized_records.csv",
    "deduplicated_csv": "deduplicated_records.csv",
    "quality_json": "quality_profile.json",
    "screening_csv": "screening_matrix.csv",
    "prisma_json": "prisma_counts.json",
    "manuscript_md": "manuscript_tables.md",
    "manifest_json": "manifest.json",
    "sqlite_db": "records.sqlite",
    "parquet_file": "deduplicated_records.parquet",
    "audit_jsonl": "audit_trail.jsonl",
    "run_log_jsonl": "run_log.jsonl",
}