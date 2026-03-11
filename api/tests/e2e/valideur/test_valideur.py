from pathlib import Path

import pytest
from playwright import sync_api as pw


@pytest.mark.playwright
@pytest.mark.parametrize(
    ("filename"),
    [
        ("dataset.xlsx"),
    ],
)
def test_valideur(snapshot, fixture_path: Path, page: pw.Page):
    response = page.goto("/valideur")
    assert response is not None
    assert response.status == 200

    pw.expect(page).to_have_title("Valideur data·inclusion")

    file_input = page.get_by_label("Sélectionner un fichier")
    file_input.set_input_files(fixture_path)

    results_heading = page.get_by_text("Résultat de l'analyse")
    pw.expect(results_heading).to_be_visible()

    assert page.locator("body").aria_snapshot() == snapshot()


@pytest.mark.parametrize(
    ("filename", "expected_status_code"),
    [
        ("dataset.xlsx", 200),
        ("dataset.csv", 422),
        ("dataset.json", 422),
    ],
)
def test_upload_files(api_client, filename, fixture_path: Path, expected_status_code):
    content = fixture_path.read_bytes() if fixture_path is not None else b""

    response = api_client.post(
        "/valideur/fragments/analyze", files={"dataset": (filename, content)}
    )
    assert response.status_code == expected_status_code
