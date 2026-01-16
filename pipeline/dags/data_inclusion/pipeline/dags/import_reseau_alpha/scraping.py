import bs4


def scrap_structure(html_content: str) -> dict:
    soup = bs4.BeautifulSoup(markup=html_content, features="lxml")
    soup = soup.select_one("body > .main-container > .container")

    lieu_element = soup.select_one(".lieu")
    dates_element = soup.select_one(".structures-dates")

    content = {
        "date_derniere_modification": dates_element.find(
            string=lambda text: "modification" in text
        ),
        "url": lieu_element.find(
            string=lambda text: "http://" in text or "https://" in text
        ),
    }

    return {
        k: v.get_text(separator=" ", strip=True) if v is not None else None
        for k, v in content.items()
    }
