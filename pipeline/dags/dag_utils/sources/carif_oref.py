from pathlib import Path


def extract(id: str, url: str, **kwargs) -> bytes:
    # TODO(vmttn)
    with open(Path(__file__).resolve().parent / "carif_oref.xml", "rb") as file:
        return file.read()


def read(path: Path):
    from pathlib import Path

    import pandas as pd
    import xmlschema

    from . import utils

    schema_path = Path(__file__).resolve().parent / "lheo.xsd"
    schema = xmlschema.XMLSchema(schema_path)
    data = schema.to_dict(path)

    for formation_data in data["offres"]["formation"]:
        formation_data["objectif-formation"] = utils.html_to_markdown(
            formation_data["objectif-formation"]
        )

    df = pd.json_normalize(
        data=data,
        record_path=["offres", "formation"],
        max_level=0,
    )
    return utils.df_clear_nan(df)
