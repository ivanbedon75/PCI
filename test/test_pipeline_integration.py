import json
from pathlib import Path

import pandas as pd

from sra_tool.models import SourceInput
from sra_tool.pipeline import run_pipeline


def test_pipeline_with_local_csv_sources(tmp_path: Path):
    scopus_path = tmp_path / "scopus.csv"
    wos_path = tmp_path / "wos.csv"
    protocol_path = tmp_path / "protocol.json"
    output_dir = tmp_path / "outputs"

    pd.DataFrame(
        [
            {
                "EID": "2-s2.0-1",
                "DOI": "10.1111/test",
                "Title": "Reproducibility in Reviews",
                "Year": "2024",
                "Source title": "Journal A",
                "Author full names": "Alice Example",
                "Abstract": "Abstract A",
            }
        ]
    ).to_csv(scopus_path, index=False)

    pd.DataFrame(
        [
            {
                "UT": "WOS:1",
                "DI": "10.1111/test",
                "TI": "Reproducibility in Reviews",
                "PY": "2024",
                "SO": "Journal A",
                "AB": "Abstract A",
            }
        ]
    ).to_csv(wos_path, index=False)

    protocol_payload = {
        "protocol_name": "integration_protocol",
        "protocol_version": "1.0.0",
        "critical_fields": ["title", "year", "journal", "doi"],
        "minimum_completion_rate": {
            "title": 0.9,
            "year": 0.9,
            "journal": 0.9,
            "doi": 0.5,
            "abstract": 0.2,
        },
        "minimum_reviewers": 2,
    }
    protocol_path.write_text(json.dumps(protocol_payload), encoding="utf-8")

    result = run_pipeline(
        source_inputs=[
            SourceInput(source="scopus_csv", file=str(scopus_path)),
            SourceInput(source="wos_csv", file=str(wos_path)),
        ],
        output_dir=str(output_dir),
        protocol_path=str(protocol_path),
        max_records_per_api_source=5,
    )

    assert result["run_summary"]["total_raw_records"] == 2
    assert result["run_summary"]["total_deduplicated_records"] == 1
    assert Path(result["artifacts"]["manifest_path"]).exists()
    assert Path(result["artifacts"]["deduplicated_csv_path"]).exists()
    assert Path(result["artifacts"]["screening_csv_path"]).exists()