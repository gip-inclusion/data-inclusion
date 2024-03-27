import logging
import pathlib
import tempfile

import geopandas
import httpx
import py7zr
from sqlalchemy.dialects.postgresql import insert
from tqdm import tqdm

from data_inclusion.api.code_officiel_geo import models
from data_inclusion.api.core import db

logger = logging.getLogger(__name__)

IGN_ADMIN_EXPRESS_FILE_URL = "http://files.opendatarchives.fr/professionnels.ign.fr/adminexpress/ADMIN-EXPRESS_3-1__SHP__FRA_WM_2022-09-20.7z"


# TODO(vmttn): use https://geo.api.gouv.fr/


def download(url: str, output_path: pathlib.Path):
    with httpx.stream("GET", url) as response:
        total = int(response.headers["Content-Length"])

        response.raise_for_status()

        with output_path.open("wb") as fp:
            with tqdm(
                total=total, unit_scale=True, unit_divisor=1024, unit="B"
            ) as progress:
                num_bytes_downloaded = response.num_bytes_downloaded
                for chunck in response.iter_bytes(chunk_size=32768):
                    fp.write(chunck)
                    progress.update(
                        response.num_bytes_downloaded - num_bytes_downloaded
                    )
                    num_bytes_downloaded = response.num_bytes_downloaded


def load_communes(file_path: pathlib.Path):
    for i in tqdm(range(100)):
        chunck_df = geopandas.read_file(file_path, rows=slice(1000 * i, 1000 * (i + 1)))

        if len(chunck_df) == 0:
            break

        chunck_df = chunck_df.rename(
            columns={
                "INSEE_COM": "code",
                "NOM": "nom",
                "INSEE_DEP": "departement",
                "INSEE_REG": "region",
                "SIREN_EPCI": "siren_epci",
            }
        )

        chunck_df = chunck_df.rename_geometry("geom")

        column_names = ["code", "nom", "departement", "region", "siren_epci", "geom"]
        chunck_df = chunck_df[column_names]

        chunck_df = chunck_df.to_wkt()

        # optimize future lookups
        chunck_df = chunck_df.sort_values(by="code")

        commune_data_list = chunck_df.to_dict(orient="records")

        stmt = insert(models.Commune).values(commune_data_list)

        stmt = stmt.on_conflict_do_update(
            index_elements=[models.Commune.code],
            set_={col: getattr(stmt.excluded, col) for col in column_names},
        )

        with db.SessionLocal() as session:
            session.execute(stmt)
            session.commit()


def import_admin_express():
    with tempfile.TemporaryDirectory() as tmp_dir_name:
        tmp_dir_path = pathlib.Path(tmp_dir_name)
        archive_path = tmp_dir_path / "ign_admin_express.7z"
        extract_dir_path = tmp_dir_path
        logger.info(f"Downloading to {archive_path}")

        download(IGN_ADMIN_EXPRESS_FILE_URL, archive_path)

        logger.info(f"Extracting to {extract_dir_path}")

        with py7zr.SevenZipFile(archive_path, "r") as archive:
            archive.extractall(extract_dir_path)

        logger.info("Loading communes")

        communes_file_path = next(tmp_dir_path.glob("**/COMMUNE.shp"))
        load_communes(communes_file_path)
