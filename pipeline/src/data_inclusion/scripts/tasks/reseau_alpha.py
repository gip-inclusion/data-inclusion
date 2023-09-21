import io
import json
import logging
import tarfile
import time
from pathlib import Path
from typing import Optional

import bs4
import numpy as np
import pandas as pd
import requests
import trafilatura
from tqdm import tqdm

logger = logging.getLogger(__name__)

tqdm.pandas()


def log_and_raise(resp: requests.Response, *args, **kwargs):
    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        logger.error(resp.json())
        raise err


def extract_structures(url: str, **kwargs) -> bytes:
    url = url.lstrip("/")

    session = requests.Session()
    session.hooks["response"] = [log_and_raise]

    response = session.get(url + "/cartographie.json")
    data = response.json()

    structures_df = pd.DataFrame.from_records(data["structures"])

    with io.BytesIO() as out_buf:
        with tarfile.open(fileobj=out_buf, mode="w:gz") as tar:
            with io.BytesIO(response.content) as buf:
                tar_info = tarfile.TarInfo("metadata.json")
                tar_info.size = len(response.content)
                tar.addfile(tar_info, buf)

            for _, row in tqdm(structures_df.iterrows()):
                response = session.get(row.url)

                with io.BytesIO(response.content) as buf:
                    tar_info = tarfile.TarInfo(f"{row.id}.html")
                    tar_info.size = len(response.content)
                    tar.addfile(tar_info, buf)

                time.sleep(0.1)
        return out_buf.getvalue()


def extract_formations(url: str, **kwargs) -> bytes:
    url = url.lstrip("/")

    session = requests.Session()
    session.hooks["response"] = [log_and_raise]

    response = session.get(url + "/cartographie.json")
    data = response.json()

    formations_df = pd.json_normalize(
        data["structures"],
        record_path="formations",
        meta="id",
        meta_prefix="structure_",
        max_level=0,
    )

    with io.BytesIO() as out_buf:
        with tarfile.open(fileobj=out_buf, mode="w:gz") as tar:
            with io.BytesIO(response.content) as buf:
                tar_info = tarfile.TarInfo("metadata.json")
                tar_info.size = len(response.content)
                tar.addfile(tar_info, buf)

            for _, row in tqdm(formations_df.iterrows()):
                response = session.get(row.url)

                with io.BytesIO(response.content) as buf:
                    tar_info = tarfile.TarInfo(f"{row.id}.html")
                    tar_info.size = len(response.content)
                    tar.addfile(tar_info, buf)

                time.sleep(0.1)
        return out_buf.getvalue()


def scrap_structure_html(html_path: Path) -> dict:
    with html_path.open() as f:
        soup = bs4.BeautifulSoup(f, features="lxml")
        data = {}

        NODE_BY_CONTENT_NAME = {
            "adresse": soup.select_one(".adresse"),
            "date_maj": soup.find(class_="structures-dates").find(
                string=lambda text: "Date de la dernière modification :" in text
            ),
            "telephone": soup.select_one(".telephone > a"),
            "site_web": soup.select_one(".contact-content").find(
                string=lambda t: t.startswith("http://")
            ),
            "courriel": soup.select_one(".email > a:nth-child(1)"),
        }

        for content_name, node in NODE_BY_CONTENT_NAME.items():
            data[f"content__{content_name}"] = html_to_markdown(node)

        return data


def scrap_formation_html(html_path: Path) -> dict:
    with html_path.open() as f:
        soup = bs4.BeautifulSoup(f, features="lxml")
        data = {}

        NODE_BY_CONTENT_NAME = {
            "contenu_et_objectifs": soup.select_one(
                "div.container:nth-child(2) > div:nth-child(2)"
                " > div:nth-child(1) > div:nth-child(1)"
            ),
            "public_attendu": soup.select_one(
                "div.container:nth-child(2) > div:nth-child(2)"
                " > div:nth-child(1) > div:nth-child(2)"
            ),
            "inscription": soup.select_one(
                "div.col-lg-6:nth-child(2) > div:nth-child(1)"
            ),
            "contact_prenom_nom": soup.select_one(
                "#formation-inscription > div:nth-child(2) > div:nth-child(2)"
            ),
            "telephone": soup.select_one("#formation-inscription > div:nth-child(3)"),
            "courriel": soup.select_one(".email > a:nth-child(1)"),
            "informations_pratiques": soup.select_one(
                "div.col-lg-6:nth-child(2) > div:nth-child(3)"
            ),
        }

        for content_name, node in NODE_BY_CONTENT_NAME.items():
            data[f"content__{content_name}"] = html_to_markdown(node)

        return data


def html_to_markdown(s) -> Optional[str]:
    if s is None or s == "":
        return s
    return trafilatura.extract(trafilatura.load_html("<html>" + str(s) + "</html>"))


def read_structures(path: Path) -> pd.DataFrame:
    with tarfile.open(path, "r:gz") as tar:
        tar.extractall(path=path.parent)

    with (path.parent / "metadata.json").open() as f:
        df = pd.DataFrame.from_records(json.load(f)["structures"])

    df = df.join(
        df.apply(
            lambda row: scrap_structure_html(html_path=path.parent / f"{row.id}.html"),
            axis=1,
            result_type="expand",
        )
    )
    df = df.replace({np.nan: None})

    return df


def read_formations(path: Path) -> pd.DataFrame:
    with tarfile.open(path, "r:gz") as tar:
        tar.extractall(path=path.parent)

    with (path.parent / "metadata.json").open() as f:
        df = pd.json_normalize(
            json.load(f)["structures"],
            record_path="formations",
            meta="id",
            meta_prefix="structure_",
            max_level=0,
        )

    df = df.join(
        df.apply(
            lambda row: scrap_formation_html(html_path=path.parent / f"{row.id}.html"),
            axis=1,
            result_type="expand",
        )
    )
    df = df.replace({np.nan: None})

    return df
