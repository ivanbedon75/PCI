from __future__ import annotations

from typing import Any

import pandas as pd

from .constants import CANONICAL_COLUMNS
from .utils import normalize_doi


def _safe_join(values: list[str]) -> str:
    cleaned = [str(v).strip() for v in values if str(v).strip()]
    return "; ".join(cleaned)


def _reconstruct_openalex_abstract(abstract_inverted_index: dict[str, list[int]] | None) -> str:
    if not abstract_inverted_index:
        return ""
    max_pos = -1
    for positions in abstract_inverted_index.values():
        if positions:
            max_pos = max(max_pos, max(positions))
    if max_pos < 0:
        return ""
    words = [""] * (max_pos + 1)
    for token, positions in abstract_inverted_index.items():
        for pos in positions:
            if 0 <= pos < len(words):
                words[pos] = token
    return " ".join(word for word in words if word).strip()


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = {key: "" for key in CANONICAL_COLUMNS}
    normalized.update(record)
    normalized["doi"] = normalize_doi(normalized.get("doi", ""))
    normalized["year"] = str(normalized.get("year", "")).strip()
    normalized["cited_by"] = str(normalized.get("cited_by", "")).strip()
    return normalized


def harmonize_openalex(records: list[dict[str, Any]]) -> pd.DataFrame:
    output: list[dict[str, Any]] = []

    for item in records:
        primary_location = item.get("primary_location") or {}
        source = primary_location.get("source") or {}
        authorships = item.get("authorships") or []

        authors = []
        affiliations = []

        for authorship in authorships:
            author = authorship.get("author") or {}
            institutions = authorship.get("institutions") or []
            author_name = (author.get("display_name") or "").strip()
            if author_name:
                authors.append(author_name)
            for inst in institutions:
                name = (inst.get("display_name") or "").strip()
                if name:
                    affiliations.append(name)

        output.append(
            _normalize_record(
                {
                    "source": "openalex",
                    "source_record_id": item.get("id", ""),
                    "doi": item.get("doi", "") or (item.get("ids") or {}).get("doi", ""),
                    "title": item.get("title", "") or item.get("display_name", ""),
                    "year": item.get("publication_year", ""),
                    "journal": source.get("display_name", ""),
                    "volume": (item.get("biblio") or {}).get("volume", ""),
                    "issue": (item.get("biblio") or {}).get("issue", ""),
                    "pages": " - ".join(
                        [
                            str((item.get("biblio") or {}).get("first_page", "")).strip(),
                            str((item.get("biblio") or {}).get("last_page", "")).strip(),
                        ]
                    ).strip(" -"),
                    "authors": _safe_join(authors),
                    "affiliations": _safe_join(sorted(set(affiliations))),
                    "abstract": _reconstruct_openalex_abstract(item.get("abstract_inverted_index")),
                    "keywords": _safe_join(
                        [
                            k.get("display_name", "")
                            for k in (item.get("keywords") or [])
                            if isinstance(k, dict)
                        ]
                    ),
                    "document_type": item.get("type", ""),
                    "language": item.get("language", ""),
                    "url": primary_location.get("landing_page_url", "") or item.get("id", ""),
                    "cited_by": item.get("cited_by_count", ""),
                    "issn": source.get("issn_l", "") or _safe_join(source.get("issn", []) or []),
                    "publisher": source.get("host_organization_name", "") or source.get("publisher", ""),
                    "raw_source_file": "",
                }
            )
        )

    return pd.DataFrame(output, columns=CANONICAL_COLUMNS)


def harmonize_scopus_csv(df: pd.DataFrame) -> pd.DataFrame:
    def pick(row: pd.Series, *candidates: str) -> str:
        for col in candidates:
            if col in row.index and str(row[col]).strip():
                return str(row[col]).strip()
        return ""

    output = []
    for _, row in df.iterrows():
        output.append(
            _normalize_record(
                {
                    "source": "scopus_csv",
                    "source_record_id": pick(row, "EID"),
                    "doi": pick(row, "DOI"),
                    "title": pick(row, "Title"),
                    "year": pick(row, "Year"),
                    "journal": pick(row, "Source title"),
                    "volume": pick(row, "Volume"),
                    "issue": pick(row, "Issue"),
                    "pages": " - ".join(
                        [pick(row, "Page start"), pick(row, "Page end")]
                    ).strip(" -"),
                    "authors": pick(row, "Author full names", "Authors"),
                    "affiliations": pick(row, "Affiliations"),
                    "abstract": pick(row, "Abstract"),
                    "keywords": pick(row, "Author Keywords"),
                    "document_type": pick(row, "Document Type"),
                    "language": pick(row, "Language of Original Document"),
                    "url": pick(row, "Link"),
                    "cited_by": pick(row, "Cited by"),
                    "issn": pick(row, "ISSN"),
                    "publisher": pick(row, "Publisher"),
                    "raw_source_file": pick(row, "__source_file__"),
                }
            )
        )
    return pd.DataFrame(output, columns=CANONICAL_COLUMNS)