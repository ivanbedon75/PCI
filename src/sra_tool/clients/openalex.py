from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests


OPENALEX_DEFAULT_BASE = "https://api.openalex.org/works"
DEFAULT_PER_PAGE = 100
REQUEST_TIMEOUT = 60


def get_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def get_raw_openalex_dir() -> Path:
    path = get_repo_root() / "data" / "raw" / "openalex"
    path.mkdir(parents=True, exist_ok=True)
    return path


@dataclass
class OpenAlexRequest:
    base_url: str = OPENALEX_DEFAULT_BASE
    params: Dict[str, str] = field(default_factory=dict)


@dataclass
class OpenAlexValidationResult:
    ok: bool
    mode: str
    base_url: str
    params: Dict[str, str]
    request_url: Optional[str] = None
    count_api: Optional[int] = None
    rows: int = 0
    records: int = 0
    exists_json: bool = False
    exists_csv: bool = False
    json_path: Optional[str] = None
    csv_path: Optional[str] = None
    error: Optional[str] = None


def is_openalex_url(text: str) -> bool:
    text = (text or "").strip()
    return text.startswith("https://api.openalex.org/")


def infer_openalex_base_url(parsed_path: str) -> str:
    if not parsed_path:
        return OPENALEX_DEFAULT_BASE
    if parsed_path.startswith("/"):
        return f"https://api.openalex.org{parsed_path}"
    return f"https://api.openalex.org/{parsed_path}"


def normalize_per_page(value: Any, default: int = DEFAULT_PER_PAGE) -> str:
    try:
        n = int(value)
    except (TypeError, ValueError):
        n = default
    n = max(1, min(n, 200))
    return str(n)


def normalize_params(params: Dict[str, Any]) -> Dict[str, str]:
    allowed = {
        "search",
        "search.exact",
        "search.semantic",
        "filter",
        "sort",
        "per_page",
        "page",
        "cursor",
        "select",
        "sample",
        "seed",
        "group_by",
        "mailto",
        "api_key",
    }

    clean: Dict[str, str] = {}
    for key, value in params.items():
        if key not in allowed:
            continue
        if value is None:
            continue
        value_str = str(value).strip()
        if not value_str:
            continue
        clean[key] = value_str

    if "per_page" in clean:
        clean["per_page"] = normalize_per_page(clean["per_page"])

    return clean


def parse_openalex_url(url: str) -> OpenAlexRequest:
    parsed = urlparse(url.strip())
    params_raw = parse_qs(parsed.query, keep_blank_values=False)
    params = {k: v[0] for k, v in params_raw.items() if v}
    params = normalize_params(params)
    base_url = infer_openalex_base_url(parsed.path)
    return OpenAlexRequest(base_url=base_url, params=params)


def compact_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def remove_trailing_semicolons(text: str) -> str:
    return re.sub(r";+$", "", text.strip())


def extract_date_filters(text: str) -> tuple[str, Dict[str, str]]:
    filters: Dict[str, str] = {}

    m_from = re.search(r"from_publication_date:([0-9]{4}-[0-9]{2}-[0-9]{2})", text, flags=re.I)
    m_to = re.search(r"to_publication_date:([0-9]{4}-[0-9]{2}-[0-9]{2})", text, flags=re.I)

    if m_from:
        filters["from_publication_date"] = m_from.group(1)
    if m_to:
        filters["to_publication_date"] = m_to.group(1)

    year_match = re.search(r"publication_year\s*:\s*([0-9]{4})", text, flags=re.I)
    if year_match:
        filters["publication_year"] = year_match.group(1)

    cleaned = text
    cleaned = re.sub(r",?\s*from_publication_date:[0-9\-]+", "", cleaned, flags=re.I)
    cleaned = re.sub(r",?\s*to_publication_date:[0-9\-]+", "", cleaned, flags=re.I)
    cleaned = re.sub(r",?\s*publication_year\s*:\s*[0-9]{4}", "", cleaned, flags=re.I)

    return cleaned, filters


def extract_language_filter(text: str) -> tuple[str, Dict[str, str]]:
    filters: Dict[str, str] = {}
    cleaned = text

    if re.search(r"LANGUAGE\s*\(\s*english\s*\)", cleaned, flags=re.I):
        filters["language"] = "en"
        cleaned = re.sub(r"\s*AND\s*LANGUAGE\s*\(\s*english\s*\)", "", cleaned, flags=re.I)
        cleaned = re.sub(r"LANGUAGE\s*\(\s*english\s*\)", "", cleaned, flags=re.I)

    if re.search(r"language\s*:\s*en\b", cleaned, flags=re.I):
        filters["language"] = "en"
        cleaned = re.sub(r",?\s*language\s*:\s*en\b", "", cleaned, flags=re.I)

    return cleaned, filters


def extract_type_filter(text: str) -> tuple[str, Dict[str, str]]:
    filters: Dict[str, str] = {}
    cleaned = text
    doc_types: List[str] = []

    if re.search(r"\barticle\b", cleaned, flags=re.I):
        doc_types.append("article")
    if re.search(r"\breview\b", cleaned, flags=re.I):
        doc_types.append("review")

    if re.search(r"type\s*:", cleaned, flags=re.I):
        if doc_types:
            filters["type"] = "|".join(sorted(set(doc_types)))
        cleaned = re.sub(r",?\s*type\s*:\s*[^,\s]+", "", cleaned, flags=re.I)

    return cleaned, filters


def strip_embedded_url_params(text: str) -> str:
    cleaned = text
    cleaned = re.sub(r"\bmode\s*=\s*url\b", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\burl\s*=\s*https?://\S+", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\bdetail\s*=\s*\d+\b", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\bquery\s*=\s*['\"].*?['\"]", "", cleaned, flags=re.I)
    return compact_whitespace(cleaned)


def strip_filter_tail(text: str) -> str:
    return re.sub(r"&filter=.*$", "", text, flags=re.I)


def scopus_to_openalex_request(raw_query: str, per_page: int = DEFAULT_PER_PAGE) -> OpenAlexRequest:
    text = compact_whitespace(raw_query)
    text = remove_trailing_semicolons(text)
    text = strip_embedded_url_params(text)
    text = strip_filter_tail(text)

    text, date_filters = extract_date_filters(text)
    text, lang_filters = extract_language_filter(text)
    text, type_filters = extract_type_filter(text)

    search_text = compact_whitespace(text).strip(" ,")
    filter_parts: List[str] = []

    if "from_publication_date" in date_filters:
        filter_parts.append(f"from_publication_date:{date_filters['from_publication_date']}")
    if "to_publication_date" in date_filters:
        filter_parts.append(f"to_publication_date:{date_filters['to_publication_date']}")
    if "publication_year" in date_filters:
        filter_parts.append(f"publication_year:{date_filters['publication_year']}")
    if "type" in type_filters:
        filter_parts.append(f"type:{type_filters['type']}")
    if "language" in lang_filters:
        filter_parts.append(f"language:{lang_filters['language']}")

    params: Dict[str, Any] = {
        "search": search_text,
        "per_page": normalize_per_page(per_page),
    }

    if filter_parts:
        params["filter"] = ",".join(filter_parts)

    return OpenAlexRequest(
        base_url=OPENALEX_DEFAULT_BASE,
        params=normalize_params(params),
    )


def build_openalex_request(user_input: str, per_page: int = DEFAULT_PER_PAGE) -> OpenAlexRequest:
    user_input = (user_input or "").strip()

    if is_openalex_url(user_input):
        req = parse_openalex_url(user_input)
        if "per_page" not in req.params:
            req.params["per_page"] = normalize_per_page(per_page)
        return OpenAlexRequest(base_url=req.base_url, params=normalize_params(req.params))

    return scopus_to_openalex_request(user_input, per_page=per_page)


def get_requests_session(mailto: Optional[str] = None) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "ICH-SR/1.0 (Systematic Review Tool; mailto:contact@example.com)"
    })
    if mailto:
        session.params = {"mailto": mailto}
    return session


def execute_openalex_request(
    request: OpenAlexRequest,
    timeout: int = REQUEST_TIMEOUT,
    session: Optional[requests.Session] = None,
) -> tuple[dict, str]:
    sess = session or get_requests_session()
    response = sess.get(request.base_url, params=request.params, timeout=timeout)
    response.raise_for_status()
    return response.json(), response.request.url


def fetch_openalex_page(
    user_input: str,
    per_page: int = DEFAULT_PER_PAGE,
    timeout: int = REQUEST_TIMEOUT,
) -> tuple[dict, OpenAlexRequest, str]:
    req = build_openalex_request(user_input, per_page=per_page)
    payload, request_url = execute_openalex_request(req, timeout=timeout)
    return payload, req, request_url


def fetch_openalex_all(
    user_input: str,
    per_page: int = DEFAULT_PER_PAGE,
    timeout: int = REQUEST_TIMEOUT,
    max_records: Optional[int] = None,
) -> tuple[dict, OpenAlexRequest, str]:
    req = build_openalex_request(user_input, per_page=per_page)

    params = dict(req.params)
    if "cursor" not in params and "page" not in params:
        params["cursor"] = "*"

    current_req = OpenAlexRequest(base_url=req.base_url, params=params)
    session = get_requests_session()

    all_results: List[dict] = []
    first_request_url: Optional[str] = None
    last_meta: Dict[str, Any] = {}

    while True:
        payload, request_url = execute_openalex_request(current_req, timeout=timeout, session=session)

        if first_request_url is None:
            first_request_url = request_url

        results = payload.get("results", [])
        meta = payload.get("meta", {})
        last_meta = meta

        if not results:
            break

        if max_records is not None:
            remaining = max_records - len(all_results)
            if remaining <= 0:
                break
            results = results[:remaining]

        all_results.extend(results)

        if max_records is not None and len(all_results) >= max_records:
            break

        next_cursor = meta.get("next_cursor")
        if not next_cursor:
            break

        current_req.params["cursor"] = next_cursor

    consolidated = {
        "meta": last_meta,
        "results": all_results,
    }
    return consolidated, req, (first_request_url or "")


def safe_join_list(values: Any, key: Optional[str] = None) -> str:
    if not isinstance(values, list):
        return ""

    out: List[str] = []
    for item in values:
        if isinstance(item, dict):
            if key and key in item and item[key]:
                out.append(str(item[key]))
            elif "display_name" in item and item["display_name"]:
                out.append(str(item["display_name"]))
            elif "id" in item and item["id"]:
                out.append(str(item["id"]))
        elif item:
            out.append(str(item))
    return " | ".join(out)


def flatten_work_record(record: Dict[str, Any]) -> Dict[str, Any]:
    primary_location = record.get("primary_location") or {}
    source = primary_location.get("source") or {}
    open_access = record.get("open_access") or {}
    biblio = record.get("biblio") or {}

    authorships = record.get("authorships") or []
    author_names = []
    institution_names = []

    for a in authorships:
        author = a.get("author") or {}
        insts = a.get("institutions") or []

        if author.get("display_name"):
            author_names.append(author["display_name"])

        for inst in insts:
            if inst.get("display_name"):
                institution_names.append(inst["display_name"])

    keywords = record.get("keywords") or []
    concepts = record.get("concepts") or []
    referenced_works = record.get("referenced_works") or []

    return {
        "id": record.get("id"),
        "doi": record.get("doi"),
        "title": record.get("display_name"),
        "publication_year": record.get("publication_year"),
        "publication_date": record.get("publication_date"),
        "type": record.get("type"),
        "language": record.get("language"),
        "cited_by_count": record.get("cited_by_count"),
        "is_oa": open_access.get("is_oa"),
        "oa_status": open_access.get("oa_status"),
        "source_display_name": source.get("display_name"),
        "source_issn_l": source.get("issn_l"),
        "host_organization": source.get("host_organization_name"),
        "volume": biblio.get("volume"),
        "issue": biblio.get("issue"),
        "first_page": biblio.get("first_page"),
        "last_page": biblio.get("last_page"),
        "authors": " | ".join(author_names),
        "institutions": " | ".join(sorted(set(institution_names))),
        "concepts": safe_join_list(concepts, key="display_name"),
        "keywords": safe_join_list(keywords, key="display_name"),
        "referenced_works_count": len(referenced_works),
        "abstract_inverted_index_present": record.get("abstract_inverted_index") is not None,
    }


def payload_to_dataframe(payload: Dict[str, Any], flatten: bool = True) -> pd.DataFrame:
    results = payload.get("results", []) or []
    if not results:
        return pd.DataFrame()

    if flatten:
        rows = [flatten_work_record(r) for r in results]
    else:
        rows = results

    return pd.DataFrame(rows)


def save_json(payload: Dict[str, Any], path: Path) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path.exists()


def save_csv_from_payload(payload: Dict[str, Any], path: Path, flatten: bool = True) -> int:
    df = payload_to_dataframe(payload, flatten=flatten)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return len(df)


def validate_and_save_openalex_input(
    user_input: str,
    basename: str = "openalex_records",
    fetch_all: bool = False,
    per_page: int = DEFAULT_PER_PAGE,
    timeout: int = REQUEST_TIMEOUT,
    flatten_csv: bool = True,
) -> OpenAlexValidationResult:
    raw_dir = get_raw_openalex_dir()
    json_path = raw_dir / f"{basename}.json"
    csv_path = raw_dir / f"{basename}.csv"

    try:
        if fetch_all:
            payload, req, request_url = fetch_openalex_all(
                user_input=user_input,
                per_page=per_page,
                timeout=timeout,
            )
        else:
            payload, req, request_url = fetch_openalex_page(
                user_input=user_input,
                per_page=per_page,
                timeout=timeout,
            )

        count_api = payload.get("meta", {}).get("count")
        records = len(payload.get("results", []) or [])

        exists_json = save_json(payload, json_path)
        rows = save_csv_from_payload(payload, csv_path, flatten=flatten_csv)
        exists_csv = csv_path.exists()

        mode = "url" if is_openalex_url(user_input) else "query"

        return OpenAlexValidationResult(
            ok=True,
            mode=mode,
            base_url=req.base_url,
            params=req.params,
            request_url=request_url,
            count_api=count_api,
            rows=rows,
            records=records,
            exists_json=exists_json,
            exists_csv=exists_csv,
            json_path=str(json_path),
            csv_path=str(csv_path),
            error=None,
        )

    except requests.HTTPError as e:
        response = getattr(e, "response", None)
        detail = ""
        if response is not None:
            try:
                detail = response.text[:1000]
            except Exception:
                detail = str(response)

        return OpenAlexValidationResult(
            ok=False,
            mode="url" if is_openalex_url(user_input) else "query",
            base_url="",
            params={},
            error=f"{e}. Detail: {detail}",
        )

    except Exception as e:
        return OpenAlexValidationResult(
            ok=False,
            mode="url" if is_openalex_url(user_input) else "query",
            base_url="",
            params={},
            error=str(e),
        )


def fetch_openalex_records(
    user_input: str | None = None,
    query: str | None = None,
    per_page: int = 100,
    fetch_all: bool = False,
    basename: str = "openalex_records",
    flatten_csv: bool = True,
    **kwargs,
) -> dict:
    effective_input = user_input or query
    if not effective_input:
        raise ValueError("Se requiere 'user_input' o 'query'.")

    mode = "url" if is_openalex_url(effective_input) else "query"

    if fetch_all:
        payload, req, request_url = fetch_openalex_all(
            user_input=effective_input,
            per_page=per_page,
        )
    else:
        payload, req, request_url = fetch_openalex_page(
            user_input=effective_input,
            per_page=per_page,
        )

    results = payload.get("results", []) or []
    meta = payload.get("meta", {}) or {}

    raw_dir = get_raw_openalex_dir()
    json_path = raw_dir / f"{basename}.json"
    csv_path = raw_dir / f"{basename}.csv"

    exists_json = save_json(payload, json_path)
    rows = save_csv_from_payload(payload, csv_path, flatten=flatten_csv)
    exists_csv = csv_path.exists()

    compatible_payload = dict(payload)
    compatible_payload["records"] = results
    compatible_payload["rows"] = rows
    compatible_payload["count_api"] = meta.get("count")
    compatible_payload["exists_json"] = exists_json
    compatible_payload["exists_csv"] = exists_csv
    compatible_payload["json_path"] = str(json_path)
    compatible_payload["csv_path"] = str(csv_path)

    compatible_payload["query_info"] = {
        "input": effective_input,
        "query": effective_input,
        "mode": mode,
        "base_url": req.base_url,
        "params": req.params,
        "request_url": request_url,
        "per_page": req.params.get("per_page", str(per_page)),
        "count_api": meta.get("count"),
        "rows": rows,
        "records": len(results),
        "exists_json": exists_json,
        "exists_csv": exists_csv,
        "json_path": str(json_path),
        "csv_path": str(csv_path),
    }

    return compatible_payload