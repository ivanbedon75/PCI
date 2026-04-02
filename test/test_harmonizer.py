import pandas as pd

from sra_tool.harmonizer import (
    harmonize_openalex,
    harmonize_scopus_csv,
    harmonize_wos_csv,
)


def test_harmonize_openalex_basic():
    records = [
        {
            "id": "https://openalex.org/W123",
            "title": "Example title",
            "publication_year": 2024,
            "doi": "https://doi.org/10.1234/example",
            "cited_by_count": 5,
            "type": "article",
            "language": "en",
            "biblio": {"volume": "1", "issue": "2", "first_page": "10", "last_page": "20"},
            "primary_location": {
                "landing_page_url": "https://example.org",
                "source": {
                    "display_name": "Journal X",
                    "host_organization_name": "Publisher X",
                    "issn_l": "1234-5678",
                },
            },
            "authorships": [
                {
                    "author": {"display_name": "Jane Doe"},
                    "institutions": [{"display_name": "University A"}],
                }
            ],
            "abstract_inverted_index": {"hello": [0], "world": [1]},
            "keywords": [{"display_name": "reproducibility"}],
        }
    ]

    df = harmonize_openalex(records)
    assert len(df) == 1
    assert df.iloc[0]["doi"] == "10.1234/example"
    assert df.iloc[0]["journal"] == "Journal X"
    assert df.iloc[0]["abstract"] == "hello world"


def test_harmonize_scopus_csv_basic():
    df = pd.DataFrame(
        [
            {
                "EID": "2-s2.0-123",
                "DOI": "10.1000/xyz",
                "Title": "A title",
                "Year": "2023",
                "Source title": "Scopus Journal",
                "Author full names": "Alice; Bob",
                "Abstract": "Test abstract",
            }
        ]
    )
    out = harmonize_scopus_csv(df)
    assert out.iloc[0]["source"] == "scopus_csv"
    assert out.iloc[0]["title"] == "A title"
    assert out.iloc[0]["doi"] == "10.1000/xyz"


def test_harmonize_wos_csv_basic():
    df = pd.DataFrame(
        [
            {
                "UT": "WOS:123",
                "DI": "10.2000/abc",
                "TI": "WOS Title",
                "PY": "2022",
                "SO": "WoS Journal",
                "AB": "Abstract text",
            }
        ]
    )
    out = harmonize_wos_csv(df)
    assert out.iloc[0]["source"] == "wos_csv"
    assert out.iloc[0]["title"] == "WOS Title"
    assert out.iloc[0]["doi"] == "10.2000/abc"