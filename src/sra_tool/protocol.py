from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_PROTOCOL = {
    "protocol_name": "default_protocol",
    "protocol_version": "1.0.0",
    "critical_fields": ["title", "year", "journal", "doi"],
    "minimum_completion_rate": {
        "title": 0.95,
        "year": 0.95,
        "journal": 0.80,
        "doi": 0.50,
        "abstract": 0.30,
    },
    "require_doi_for_dedup_primary": True,
    "require_abstract_for_text_screening": False,
    "minimum_reviewers": 2,
}


def load_protocol(path: str | None) -> dict[str, Any]:
    if not path:
        return dict(DEFAULT_PROTOCOL)

    protocol_path = Path(path)
    payload = json.loads(protocol_path.read_text(encoding="utf-8"))

    merged = dict(DEFAULT_PROTOCOL)
    merged.update(payload)

    if "minimum_completion_rate" in payload:
        rates = dict(DEFAULT_PROTOCOL["minimum_completion_rate"])
        rates.update(payload["minimum_completion_rate"])
        merged["minimum_completion_rate"] = rates

    return merged


def validate_protocol(protocol: dict[str, Any]) -> None:
    required = [
        "protocol_name",
        "protocol_version",
        "critical_fields",
        "minimum_completion_rate",
        "minimum_reviewers",
    ]
    missing = [key for key in required if key not in protocol]
    if missing:
        raise ValueError(f"Faltan claves en el protocolo: {missing}")

    if int(protocol["minimum_reviewers"]) < 1:
        raise ValueError("minimum_reviewers debe ser >= 1")