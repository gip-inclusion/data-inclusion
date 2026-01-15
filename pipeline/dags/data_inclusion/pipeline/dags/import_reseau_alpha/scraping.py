import bs4


def scrap_structure(html_content: str) -> dict:
    soup = bs4.BeautifulSoup(markup=html_content, features="lxml")
    soup = soup.select_one("body > .main-container > .container")

    if soup is None:
        raise ValueError("Could not find main container in the HTML content.")

    lieu_element = soup.select_one(".lieu")
    dates_element = soup.select_one(".structures-dates")

    content = {
        "date_derniere_modification": dates_element.find(
            string=lambda text: "modification" in text
        )
        if dates_element
        else None,
        "url": lieu_element.find(
            string=lambda text: "http://" in text or "https://" in text
        )
        if lieu_element
        else None,
    }

    return {
        k: v.get_text(separator=" ", strip=True) if v is not None else None
        for k, v in content.items()
    }
