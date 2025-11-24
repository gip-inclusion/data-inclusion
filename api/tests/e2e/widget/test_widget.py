import os

import pytest
from playwright import sync_api as pw


@pytest.mark.skipif(os.getenv("CI") != "1", reason="Skipping test in CI")
def test_widget(page: pw.Page):
    """Necessite un serveur de test comportant des données sur Lille
    en cours d'exécution avec TOKEN_ENABLED = False
    """
    response = page.goto("/widget?token=foobar")  # token is mandatory !
    assert response is not None
    assert response.status == 200

    pw.expect(page).to_have_title("Widget data·inclusion")

    geo_filter = page.get_by_role("combobox", name="Lieu")
    geo_filter.press_sequentially("Lille", delay=50)

    suggestions = page.locator("#suggestions option")
    pw.expect(suggestions.first).to_be_attached(timeout=1000)

    option_value = suggestions.first.get_attribute("value")
    geo_filter.fill(option_value)

    hidden_input = page.locator("#hidden-code_commune")
    pw.expect(hidden_input).not_to_have_value("", timeout=1000)

    card_locator = page.locator(".di-card")
    pw.expect(card_locator.first).to_be_visible(timeout=1000)
    assert card_locator.count() == 10
