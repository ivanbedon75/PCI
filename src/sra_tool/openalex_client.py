from __future__ import annotations
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
import requests
from .config import AppConfig

class OpenAlexClientError(Exception):
    """Raised for OpenAlex retrieval and parsing errors."""

@dataclass
class OpenAlexCaptureResult:
    raw_records: list[dict[str, Any]]
    pages_downloaded: int
    total_reported: int | None
    resolved_url: str
    resolved_params: dict[str, Any]
    raw_json_path: Path

class OpenAlexClient:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "systematic-review-openalex-tool/0.1.0"}
        )
    def _default_params(self) -> dict[str, Any]:
        params: dict[str, Any] = {"per_page": self.config.default_per_page}
        if self.config.openalex_api_key:
            params["api_key"] = self.config.openalex_api_key
        if self.config.openalex_mailto:
            params["mailto"] = self.config.openalex_mailto
        return params
    def resolve_user_input(self, user_input: str) -> tuple[str, dict[str, Any]]:
        text = user_input.strip()
        if not text:
            raise OpenAlexClientError("La consulta o URL de OpenAlex no puede estar vacía.")
        if text.startswith("http://") or text.startswith("https://"):
            return self._resolve_from_url(text)
        if text.startswith("/"):
            endpoint = text
            if not endpoint.startswith("/works"):
                raise OpenAlexClientError(
                    "Solo se soporta el endpoint /works en esta versión."
                )
            return (
                f"{self.config.openalex_base_url}{endpoint}",
                self._default_params(),
            )
        if "=" in text or "&" in text:
            params = self._default_params()
            for key, values in parse_qs(text, keep_blank_values=True).items():
                params[key] = values[0] if values else ""
            return f"{self.config.openalex_base_url}/works", params
        params = self._default_params()
        params["search"] = text
        return f"{self.config.openalex_base_url}/works", params
    def _resolve_from_url(self, url: str) -> tuple[str, dict[str, Any]]:
        parsed = urlparse(url)
        if "api.openalex.org" not in parsed.netloc:
            raise OpenAlexClientError("La URL no pertenece a api.openalex.org.")
        path = parsed.path or ""
        if not path.startswith("/works"):
            raise OpenAlexClientError(
                "Solo se soportan capturas desde el endpoint /works."
            )
        raw_params = parse_qs(parsed.query, keep_blank_values=True)
        params = self._default_params()
        for key, values in raw_params.items():
            params[key] = values[0] if values else ""
        params.pop("page", None)
        params.pop("cursor", None)
        return f"{self.config.openalex_base_url}{path}", params
    def fetch_all(self, user_input: str, raw_dir: Path, run_id: str) -> OpenAlexCaptureResult:
        url, params = self.resolve_user_input(user_input)
        params["cursor"] = "*"
        params["per_page"] = min(int(params.get("per_page", self.config.default_per_page)), 100)
        all_records: list[dict[str, Any]] = []
        pages_downloaded = 0
        total_reported: int | None = None
        while True:
            response = self.session.get(
                url,
                params=params,
                timeout=self.config.request_timeout_seconds,
            )
            if response.status_code >= 400:
                raise OpenAlexClientError(
                    f"Error HTTP {response.status_code} al consultar OpenAlex: {response.text[:500]}"
                )
            try:
                payload = response.json()
            except json.JSONDecodeError as exc:
                raise OpenAlexClientError("Respuesta JSON inválida desde OpenAlex.") from exc
            if "results" not in payload or "meta" not in payload:
                raise OpenAlexClientError(
                    "La respuesta no contiene la estructura esperada de OpenAlex."
                )
            results = payload.get("results", [])
            meta = payload.get("meta", {})
            next_cursor = meta.get("next_cursor")
            total_reported = meta.get("count", total_reported)
            pages_downloaded += 1
            all_records.extend(results)
            if not results or not next_cursor:
                break
            params["cursor"] = next_cursor
        raw_json_path = raw_dir / f"{run_id}_openalex_raw.json"
        raw_json_path.write_text(
            json.dumps(all_records, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return OpenAlexCaptureResult(
            raw_records=all_records,
            pages_downloaded=pages_downloaded,
            total_reported=total_reported,
            resolved_url=url,
            resolved_params=params,
            raw_json_path=raw_json_path,
        )

def derive_internal_eid(openalex_id: str | None) -> str:
    if not openalex_id:
        return "OPENALEX:NA"
    suffix = re.sub(r"^https?://openalex\.org/", "", openalex_id).strip()
    return f"OPENALEX:{suffix or 'NA'}"
