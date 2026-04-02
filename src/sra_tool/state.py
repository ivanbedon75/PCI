from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from .constants import STAGES
from .models import RunState, StageState

def build_new_run_state(run_id: str) -> RunState:
    state = RunState(run_id=run_id)
    for stage_name in STAGES:
        state.stages[stage_name] = StageState(name=stage_name)
    return state

def save_state(path: Path, state: RunState) -> None:
    state.updated_at = datetime.now(timezone.utc).isoformat()
    path.write_text(
        json.dumps(state.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

def load_state(path: Path) -> RunState | None:
    if not path.exists():
        return None
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return RunState.from_dict(payload)
