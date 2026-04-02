from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from .gui import launch_gui
from .models import SourceInput
from .pipeline import run_pipeline


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sra-tool",
        description="Auditable systematic review pipeline with CLI and GUI modes",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Execute the pipeline in non-interactive mode")
    run_parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="Source name: openalex, scopus_csv",
    )
    run_parser.add_argument(
        "--strategy",
        action="append",
        default=[],
        help="Strategy name: core, exploratory",
    )
    run_parser.add_argument(
        "--query",
        action="append",
        default=[],
        help="Query for OpenAlex entries. Order matters.",
    )
    run_parser.add_argument(
        "--file",
        action="append",
        default=[],
        help="CSV file path for Scopus entries. Order matters.",
    )
    run_parser.add_argument(
        "--protocol",
        default=None,
        help="Path to protocol JSON",
    )
    run_parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where outputs will be written",
    )
    run_parser.add_argument(
        "--max-records-per-api-source",
        type=int,
        default=200,
        help="Maximum number of records per API-based source",
    )

    subparsers.add_parser("gui", help="Launch graphical user interface")

    return parser.parse_args(argv)


def _build_source_inputs(args: argparse.Namespace) -> list[SourceInput]:
    sources = list(args.source)
    strategies = list(args.strategy)
    queries = list(args.query)
    files = list(args.file)

    if len(sources) != len(strategies):
        raise ValueError("Cada --source debe tener un --strategy correspondiente.")

    source_inputs: list[SourceInput] = []

    for source, strategy in zip(sources, strategies):
        label = f"{source}_{strategy}"

        if source == "openalex":
            if not queries:
                raise ValueError(f"La fuente {source} requiere una --query correspondiente.")
            source_inputs.append(
                SourceInput(
                    source=source,
                    strategy=strategy,
                    label=label,
                    query=queries.pop(0),
                )
            )
        elif source == "scopus_csv":
            if not files:
                raise ValueError(f"La fuente {source} requiere un --file correspondiente.")
            source_inputs.append(
                SourceInput(
                    source=source,
                    strategy=strategy,
                    label=label,
                    file=files.pop(0),
                )
            )
        else:
            raise ValueError(f"Fuente no soportada: {source}")

    if queries:
        raise ValueError("Se proporcionaron más --query de las necesarias.")
    if files:
        raise ValueError("Se proporcionaron más --file de los necesarios.")

    return source_inputs


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)

    if args.command == "run":
        source_inputs = _build_source_inputs(args)
        result = run_pipeline(
            source_inputs=source_inputs,
            output_dir=args.output_dir,
            protocol_path=args.protocol,
            max_records_per_api_source=args.max_records_per_api_source,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "gui":
        launch_gui()
        return

    raise ValueError("Comando no soportado.")


if __name__ == "__main__":
    main(sys.argv[1:])