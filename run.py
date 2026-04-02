from src.sra_tool.cli import main
if __name__ == "__main__":
    main()
src/sra_tool/__init__.py
__all__ = ["__version__"]
__version__ = "0.1.0"
src/sra_tool/constants.py
from __future__ import annotations
CSV_COLUMNS = [
    "Authors",
    "Author full names",
    "Author(s) ID",
    "Title",
    "Year",
    "Source title",
    "Volume",
    "Issue",
    "Art. No.",
    "Page start",
    "Page end",
    "Cited by",
    "DOI",
    "Link",
    "Affiliations",
    "Authors with affiliations",
    "Abstract",
    "Author Keywords",
    "Index Keywords",
    "Funding Details",
    "Funding Texts",
    "References",
    "Correspondence Address",
    "Publisher",
    "ISSN",
    "ISBN",
    "CODEN",
    "Language of Original Document",
    "Abbreviated Source Title",
    "Document Type",
    "Publication Stage",
    "Open Access",
    "Source",
    "EID",
]
ALLOWED_STRATEGIES = {"core", "exploratory"}
STAGES = [
    "strategy_definition",
    "openalex_capture",
    "harmonization",
    "csv_export",
    "corpus_validation",
]
ADVANCE_ALLOWED = {"aprobado", "aprobado con observaciones"}
BLOCKING_STATES = {"revision requerida", "rechazado"}
