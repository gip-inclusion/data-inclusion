import io
import json
import logging
import tarfile
import time
from pathlib import Path

from . import utils

logger = logging.getLogger(__name__)


def extract_structures(url: str, **kwargs) -> bytes:
    import pandas as pd

    session = utils.logging_raising_session()
    response = session.get(utils.safe_urljoin(url, "cartographie.json"))

    data = response.json()

    structures_df = pd.DataFrame.from_records(data["structures"])

    with io.BytesIO() as out_buf:
        with tarfile.open(fileobj=out_buf, mode="w:gz") as tar:
            with io.BytesIO(response.content) as buf:
                tar_info = tarfile.TarInfo("metadata.json")
                tar_info.size = len(response.content)
                tar.addfile(tar_info, buf)

            for _, row in structures_df.iterrows():
                response = session.get(row.url)

                with io.BytesIO(response.content) as buf:
                    tar_info = tarfile.TarInfo(f"{row.id}.html")
                    tar_info.size = len(response.content)
                    tar.addfile(tar_info, buf)

                time.sleep(0.1)
        return out_buf.getvalue()


def extract_formations(url: str, **kwargs) -> bytes:
    import pandas as pd

    session = utils.logging_raising_session()
    response = session.get(utils.safe_urljoin(url, "cartographie.json"))
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

            for _, row in formations_df.iterrows():
                response = session.get(row.url)

                with io.BytesIO(response.content) as buf:
                    tar_info = tarfile.TarInfo(f"{row.id}.html")
                    tar_info.size = len(response.content)
                    tar.addfile(tar_info, buf)

                time.sleep(0.1)
        return out_buf.getvalue()


def scrap_structure_html(html_path: Path) -> dict:
    import bs4

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
            data[f"content__{content_name}"] = utils.html_to_markdown(str(node))

        return data


def scrap_formation_html(html_path: Path) -> dict:
    import bs4

    def get_parent(node):
        return node.parent if node is not None else None

    with html_path.open() as f:
        soup = bs4.BeautifulSoup(f, features="lxml")
        data = {}

        contenu_et_objectifs_selector = (
            ".container > .row:nth-child(2) > div:nth-child(1) > .row:nth-child(1)"
        )
        public_attendu_selector = (
            ".container > .row:nth-child(2) > div:nth-child(1) > .row:nth-child(2)"
        )
        inscription_selector = (
            ".container > .row:nth-child(2) > div:nth-child(2) > .row:nth-child(1)"
        )
        informations_pratiques_selector = (
            ".container > .row:nth-child(2) > div:nth-child(2) > .row:nth-child(3)"
        )

        NODE_BY_CONTENT_NAME = {
            "date_maj": soup.select_one(".entete").find(
                string=lambda text: "Date de la dernière modification :" in text
            ),
            "contenu_et_objectifs__titre": soup.select_one(
                f"{contenu_et_objectifs_selector} > div:nth-child(2)"
            ),
            "contenu_et_objectifs__objectifs": soup.select_one(
                f"{contenu_et_objectifs_selector} > div:nth-child(3)"
            ),
            "contenu_et_objectifs__niveau": soup.select_one(
                f"{contenu_et_objectifs_selector} > div:nth-child(4)"
            ),
            "public_attendu__niveau": soup.select_one(
                f"{public_attendu_selector} > div:nth-child(2)"
            ),
            "public_attendu__competences": soup.select_one(
                f"{public_attendu_selector} > div:nth-child(3)"
            ),
            "public_attendu__type_de_public": soup.select_one(
                f"{public_attendu_selector} > div:nth-child(4)"
            ),
            "inscription__informations_en_ligne": get_parent(
                get_parent(
                    soup.select_one(inscription_selector).find(
                        string=lambda text: "Informations en ligne" in text
                    )
                )
            ),
            "inscription__places": get_parent(
                get_parent(
                    soup.select_one(inscription_selector).find(
                        string=lambda text: "Places disponibles" in text
                    )
                )
            ),
            "inscription__entree_sortie": get_parent(
                get_parent(
                    soup.select_one(inscription_selector).find(
                        string=lambda text: "Entrée / sortie permanente" in text
                    )
                )
            ),
            "contact_inscription__adresse": get_parent(
                get_parent(soup.select_one("#formation-inscription .fa-home"))
            ),
            "contact_inscription__contact": get_parent(
                get_parent(soup.select_one("#formation-inscription .fa-user"))
            ),
            "contact_inscription__telephone": get_parent(
                get_parent(soup.select_one("#formation-inscription .fa-phone"))
            ),
            "contact_inscription__courriel": get_parent(
                get_parent(soup.select_one("#formation-inscription .fa-inbox"))
            ),
            "informations_pratiques__etendue": get_parent(
                get_parent(
                    soup.select_one(informations_pratiques_selector).find(
                        string=lambda text: "Étendue de la formation" in text
                    )
                )
            ),
            "informations_pratiques__volume": get_parent(
                get_parent(
                    soup.select_one(informations_pratiques_selector).find(
                        string=lambda text: "Volume horaire" in text
                    )
                )
            ),
            "informations_pratiques__cout": get_parent(
                get_parent(
                    soup.select_one(informations_pratiques_selector).find(
                        string=lambda text: (
                            "Adhésion annuelle à la structure obligatoire"
                        )
                        in text
                    )
                )
            ),
            "informations_pratiques__prise_en_charge": get_parent(
                get_parent(
                    soup.select_one(informations_pratiques_selector).find(
                        string=lambda text: "Coût d'inscription à la formation" in text
                    )
                )
            ),
            "informations_pratiques__remuneration": get_parent(
                get_parent(
                    soup.select_one(informations_pratiques_selector).find(
                        string=lambda text: "Rémunération" in text
                    )
                )
            ),
            "informations_pratiques__garde": get_parent(
                get_parent(
                    soup.select_one(informations_pratiques_selector).find(
                        string=lambda text: "Garde d'enfant" in text
                    )
                )
            ),
            "lieux_et_horaires_formation__adresse": soup.select_one(
                "#lieux-formation .lieu-formation .adresse"
            ),
            "lieux_et_horaires_formation__horaires": "\n".join(
                soup.select_one("#lieux-formation").find_all(
                    string=lambda text: "de" in text and "à" in text
                )
            ),
        }

        for content_name, node in NODE_BY_CONTENT_NAME.items():
            data[f"content__{content_name}"] = utils.html_to_markdown(str(node))

        return data


def read_structures(path: Path):
    import pandas as pd

    with tarfile.open(path, "r:gz") as tar:
        tar.extractall(path=path.parent)

    with (path.parent / "metadata.json").open() as f:
        data = json.load(f)["structures"]

    for structure_data in data:
        structure_data["description"] = utils.html_to_markdown(
            str(structure_data["description"])
        )

    df = pd.DataFrame.from_records(data)

    df = df.join(
        df.apply(
            lambda row: scrap_structure_html(html_path=path.parent / f"{row.id}.html"),
            axis=1,
            result_type="expand",
        )
    )
    return utils.df_clear_nan(df)


def read_formations(path: Path):
    import pandas as pd

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
    return utils.df_clear_nan(df)
