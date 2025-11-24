from playwright import sync_api as pw


def test_widget(page: pw.Page, service_factory):
    target_city = "59350"  # Lille

    service_factory(
        code_insee=target_city,
        nom="Service A",
        lien_source="https://example.com/service-a",
    )

    response = page.goto("/widget")

    assert response is not None
    assert response.status == 200

    pw.expect(page).to_have_title("Widget data·inclusion")

    form_locator = page.get_by_role("form")
    pw.expect(form_locator).to_be_visible()

    geo_filter = page.get_by_role("combobox", name="Lieu")
    pw.expect(geo_filter).to_be_visible()

    with page.expect_response(
        lambda response: "/widget" in response.url,
        timeout=1000,
    ):
        page.pause()
        geo_filter.fill(target_city)
        page.wait_for_timeout(200)
        geo_filter.press("ArrowDown")
        geo_filter.press("Enter")

    card_locator = page.get_by_role("article")

    pw.expect(card_locator).to_have_count(9)
