from pathlib import Path
from unittest import mock

import pytest
from playwright import sync_api as pw

from data_inclusion.api.valideur import readers, routes


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


def test_upload_too_large_is_rejected_before_reading(api_client, monkeypatch):
    read_file = mock.Mock()
    monkeypatch.setattr(readers, "read_file", read_file)

    content = b"\0" * (routes.MAX_FILE_SIZE_BYTES + 1)
    response = api_client.post(
        "/valideur/fragments/analyze", files={"dataset": ("dataset.xlsx", content)}
    )
    assert response.status_code == 413
    read_file.assert_not_called()


def test_upload_too_many_parts(api_client):
    response = api_client.post(
        "/valideur/fragments/analyze",
        files=[("dataset", ("dataset.xlsx", b""))] * 1001,
    )
    assert response.status_code == 400


def test_upload_while_another_analysis_runs(api_client, fixtures_dir: Path):
    content = (fixtures_dir / "dataset.xlsx").read_bytes()

    with routes.analyze_lock:
        response = api_client.post(
            "/valideur/fragments/analyze", files={"dataset": ("dataset.xlsx", content)}
        )
    assert response.status_code == 503
