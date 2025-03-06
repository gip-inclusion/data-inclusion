import contextlib
import enum
import json
import logging
import os
import unicodedata
from pathlib import Path

import httpx
from playwright.sync_api import (
    Browser,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)
from twocaptcha import TwoCaptcha

from . import utils

logger = logging.getLogger(__name__)


def normalize(text: str) -> str:
    # Decompose the unicode string into its base and combining characters
    nfkd_form = unicodedata.normalize("NFKD", text)

    # Filter out the combining characters (like accents)
    normalized_text = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    # replace ligature oe
    return normalized_text.replace("œ", "oe")


def get_location(city_code: str, commune: str, region: str) -> str:
    """Return a location string suitable for the search form on monenfant.fr.

    The location string is formatted as "Xeme Arrondissement Paris" for Paris.
    For other cities, it is formatted like "Lille Nord".
    """
    commune = normalize(commune)
    region = normalize(region)

    if "Arrondissement" in commune:
        commune = commune.split()[0]
        num_arrondissement = city_code[3:].lstrip("0")
        suffix = "er" if num_arrondissement == "1" else "eme"
        return f"{num_arrondissement}{suffix} Arrondissement {commune}"

    return f"{commune} {region}"


@contextlib.contextmanager
def solve_captcha(page, timeout: int = 1000):
    """Wait for captcha and solve it if ever it shows up.

    Args:
        timeout: maximum time in milliseconds to wait for captcha to show up. Keep this
            value low to avoid waiting too long when captcha is not triggered, which is
            the most frequent case.
    """

    try:
        with page.expect_response("**/captcha", timeout=timeout) as response_info:
            # callee should make actions that could trigger the captcha
            yield
    except PlaywrightTimeoutError:
        # captcha did not show up : simply ignore and move on
        return

    response = response_info.value
    data = response.json()

    twocaptcha_api_key = os.environ.get("TWOCAPTCHA_API_KEY")
    captcha_solver = TwoCaptcha(apiKey=twocaptcha_api_key)

    # The captcha solver usually succeeds the first time, but the service could fail
    # for reasons beyond our control. Given that the current solver is pay-per-use,
    # setting a hard limit on the number of tries will prevent excessive costs.
    max_tries = 5

    for _ in range(max_tries):
        logger.info("Solving captcha...")
        result = captcha_solver.normal(data["captchaImg"], minLen=8, maxLen=8)
        page.locator("#captchaSaisie").fill(result["code"])

        with page.expect_response("**/captcha*") as response_info:
            page.get_by_role("button", name="Je ne suis pas un robot").click()

        response = response_info.value
        data = response.json()

        if data["captchaSuccess"]:
            logger.info("Captcha solved")
            return

        logger.warning("Captcha failed")


# this empirically defined timeout is used to wait for search results to load
# it should be large enough to cover the time needed to solve captcha, taking into
# that the captcha solver may need several attempts to succeed
SEARCH_TIMEOUT_MS = 2 * 60 * 1000


class SearchRadius(enum.IntEnum):
    """Selectable search radius as defined monenfant.fr"""

    ONE_KM = 1
    TWO_KM = 2
    FIVE_KM = 5
    TEN_KM = 10
    THIRTY_KM = 30


DEFAULT_RADIUS = SearchRadius.FIVE_KM


def search_at_location(
    browser: Browser,
    location: str,
    radius: SearchRadius = SearchRadius.ONE_KM,
) -> list:
    # user action code is partly generated using `playwright codegen https://monenfant.fr`

    base_url = os.environ.get("MONENFANT_BASE_URL")
    page = browser.new_page(base_url=base_url)
    search_results = []

    # 1. fill search form
    page.goto("/que-recherchez-vous")
    page.get_by_placeholder("Précisez votre recherche").click()
    page.get_by_label("Une crèche").check()
    page.get_by_placeholder("Indiquez une adresse").press_sequentially(location)
    page.locator(".input-geoloc").locator(".spaced-items").first.click()

    if (
        delta := (
            list(SearchRadius).index(radius) - list(SearchRadius).index(DEFAULT_RADIUS)
        )
    ) >= 0:
        for _ in range(delta):
            page.get_by_label("Dans un rayon autour de").press("ArrowRight")
    else:
        for _ in range(-delta):
            page.get_by_label("Dans un rayon autour de").press("ArrowLeft")

    # 2. submit form
    with page.expect_response(
        lambda response: "/search?" in response.url and response.status == 200,
        timeout=SEARCH_TIMEOUT_MS,
    ) as response_info:
        with solve_captcha(page):
            page.get_by_title("Afficher la liste").click()

    response = response_info.value
    data = response.json()

    total_pages = data["totalPages"]
    nb_of_main_results = data["nbOfMainResults"]
    search_results += data["mainResults"]

    logger.info("%s search results, %s pages", nb_of_main_results, total_pages)

    # 3. iterate over remaining pages
    for _ in range(1, total_pages):
        with page.expect_response(
            lambda response: "/search?" in response.url and response.status == 200,
            timeout=SEARCH_TIMEOUT_MS,
        ) as response_info:
            with solve_captcha(page):
                page.get_by_role("link", name="›", exact=True).click()

        response = response_info.value
        data = response.json()

        current_page = data["page"]
        logger.info("Page %s/%s", current_page + 1, total_pages)
        search_results += data["mainResults"]

        if current_page == total_pages - 1:
            break

    return search_results


def extract_structure(structure_id: str) -> dict:
    response = httpx.get("https://monenfant.fr/o/isu-service/isu/getAngularToken")
    response.raise_for_status()
    access_token = response.json()["access_token"]

    response = httpx.get(
        f"https://monenfant.fr/api/monenfantmodedegardefront/v1/modedegarde/details/{structure_id}",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    response.raise_for_status()
    return response.json()


def extract(
    city_code: str,
    commune: str,
    region: str,
    debug: bool = False,
) -> bytes:
    data = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=not debug)
        location = get_location(city_code=city_code, commune=commune, region=region)

        if city_code[:3] in [
            "750",  # arrondissements de Paris
            "132",  # arrondissements de Marseille
            "693",  # arrondissements de Lyon
        ]:
            radius = SearchRadius.ONE_KM
        else:
            radius = SearchRadius.FIVE_KM

        logger.info(
            "Searching for creches at %s, %s (radius=%skm)", location, city_code, radius
        )
        search_results = search_at_location(
            browser=browser,
            location=location,
            radius=radius,
        )

    logger.info("Extracting structures details...")

    data = [extract_structure(search_result["id"]) for search_result in search_results]

    return json.dumps(data).encode()


def read(path: Path):
    import pandas as pd

    # utils.df_from_json is enough
    # but this adds the conversion of descriptions from html to markdown
    # should eventually be implemented as a python dbt model

    with path.open() as file:
        data = json.load(file)

    for creche_data in data:
        creche_data["description"] = {
            k: utils.html_to_markdown(v) for k, v in creche_data["description"].items()
        }

    df = pd.DataFrame.from_records(data)
    return utils.df_clear_nan(df)
