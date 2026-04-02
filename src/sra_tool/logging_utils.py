from __future__ import annotations

import json
import logging
from pathlib import Path


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
            "created": record.created,
        }
        if hasattr(record, "event_payload"):
            payload["event_payload"] = record.event_payload
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def build_json_logger(log_path: Path, logger_name: str = "sra_tool") -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    already_configured = False
    for handler in logger.handlers:
        if getattr(handler, "_sra_json_handler", False):
            already_configured = True
            break

    if not already_configured:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(JsonFormatter())
        file_handler._sra_json_handler = True  # type: ignore[attr-defined]
        logger.addHandler(file_handler)

    return logger


def log_event(logger: logging.Logger, message: str, event_payload: dict) -> None:
    logger.info(message, extra={"event_payload": event_payload})