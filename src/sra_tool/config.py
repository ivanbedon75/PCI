from __future__ import annotations
import os
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    data_raw_dir: Path
    data_processed_dir: Path
    logs_dir: Path
    outputs_dir: Path
    runs_dir: Path
    state_file: Path
    audit_file: Path
    app_log_file: Path
    openalex_base_url: str
    openalex_api_key: str | None
    openalex_mailto: str | None
    request_timeout_seconds: int
    default_per_page: int

def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()

def get_config() -> AppConfig:
    project_root = Path(__file__).resolve().parents[2]
    data_raw_dir = project_root / "data" / "raw"
    data_processed_dir = project_root / "data" / "processed"
    logs_dir = project_root / "logs"
    outputs_dir = project_root / "outputs"
    runs_dir = outputs_dir / "runs"
    for directory in [
        data_raw_dir,
        data_processed_dir,
        logs_dir,
        outputs_dir,
        runs_dir,
    ]:
        directory.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        project_root=project_root,
        data_raw_dir=data_raw_dir,
        data_processed_dir=data_processed_dir,
        logs_dir=logs_dir,
        outputs_dir=outputs_dir,
        runs_dir=runs_dir,
        state_file=outputs_dir / "run_state.json",
        audit_file=logs_dir / "audit_trail.jsonl",
        app_log_file=logs_dir / "app.log",
        openalex_base_url="https://api.openalex.org",
        openalex_api_key=_env("OPENALEX_API_KEY"),
        openalex_mailto=_env("OPENALEX_MAILTO"),
        request_timeout_seconds=int(_env("REQUEST_TIMEOUT_SECONDS", "60")),
        default_per_page=min(int(_env("OPENALEX_PER_PAGE", "100")), 100),
    )
