import pandas as pd

from sra_tool.dedup import deduplicate_records


def test_deduplicate_by_exact_doi():
    df = pd.DataFrame(
        [
            {"source": "openalex", "source_record_id": "1", "doi": "10.1/abc", "title": "Title A", "year": "2020", "journal": "J1", "abstract": "x", "authors": "a", "issn": "1"},
            {"source": "crossref", "source_record_id": "2", "doi": "10.1/abc", "title": "Title A variant", "year": "2020", "journal": "J1", "abstract": "", "authors": "a", "issn": "1"},
        ]
    )

    dedup_df, duplicates_df = deduplicate_records(df)
    assert len(dedup_df) == 1
    assert len(duplicates_df) == 1


def test_deduplicate_by_normalized_title_when_doi_missing():
    df = pd.DataFrame(
        [
            {"source": "scopus_csv", "source_record_id": "1", "doi": "", "title": "A Study on Reproducibility!", "year": "2021", "journal": "J", "abstract": "", "authors": "", "issn": ""},
            {"source": "wos_csv", "source_record_id": "2", "doi": "", "title": "A study on reproducibility", "year": "2021", "journal": "J", "abstract": "Has abstract", "authors": "", "issn": ""},
        ]
    )

    dedup_df, duplicates_df = deduplicate_records(df)
    assert len(dedup_df) == 1
    assert len(duplicates_df) == 1
    assert dedup_df.iloc[0]["abstract"] == "Has abstract"