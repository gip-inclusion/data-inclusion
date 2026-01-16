import pathlib

import pytest

from data_inclusion.pipeline.dags.import_reseau_alpha import scraping

FIXTURES_PATH = pathlib.Path(__file__).parent / "fixtures"


@pytest.mark.parametrize(
    ("html_path", "expected"),
    [
        (
            (FIXTURES_PATH / "structures" / "alpha-iv.html"),
            {
                "date_derniere_modification": (
                    "Date de la dernière modification : 5 juin 2025"
                ),
                "url": "http://www.alpha4.fr",
            },
        ),
        (
            (FIXTURES_PATH / "structures" / "abc-formation.html"),
            {
                "date_derniere_modification": (
                    "Date de la dernière modification : 22 décembre 2025"
                ),
                "url": "http://www.abcformation1986.fr",
            },
        ),
    ],
)
def test_scrap_structure(html_path: pathlib.Path, expected):
    assert scraping.scrap_structure(html_path.read_text()) == expected
