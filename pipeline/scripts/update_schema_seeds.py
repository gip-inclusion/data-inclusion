#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "data-inclusion-schema==1.0.0b7",
#     "pandas",
# ]
# ///

"""Generate dbt seeds in pipeline/seeds/schema"""

import shutil
from pathlib import Path

import pandas as pd

from data_inclusion.schema import v0, v1

BASE_OUTPUT_DIR = Path(__file__).parent.parent / "dbt" / "seeds" / "schema"

VERSIONS_REGISTRY = {
    "v0": {
        "frais": v0.Frais,
        "labels_nationaux": v0.LabelNational,
        "modes_accueil": v0.ModeAccueil,
        "modes_orientation_accompagnateur": v0.ModeOrientationAccompagnateur,
        "modes_orientation_beneficiaire": v0.ModeOrientationBeneficiaire,
        "profils": v0.Profil,
        "thematiques": v0.Thematique,
        "typologies_de_services": v0.TypologieService,
        "typologies_de_structures": v0.TypologieStructure,
        "zones_de_diffusion_types": v0.ZoneDiffusionType,
    },
    "v1": {
        "frais": v1.Frais,
        "modes_accueil": v1.ModeAccueil,
        "modes_mobilisation": v1.ModeMobilisation,
        "personne_mobilisatrice": v1.PersonneMobilisatrice,
        "publics": v1.Public,
        "thematiques": v1.Thematique,
        "types_de_services": v1.TypeService,
        "reseaux_porteurs": v1.ReseauPorteur,
    },
}


if __name__ == "__main__":
    shutil.rmtree(BASE_OUTPUT_DIR)

    for version_schema, enums in VERSIONS_REGISTRY.items():
        OUTPUT_DIR = BASE_OUTPUT_DIR / version_schema
        OUTPUT_DIR.mkdir(parents=True)

        for tablename, enum in enums.items():
            df = pd.DataFrame(
                [
                    {"value": v.value, "label": v.label, "description": v.description}
                    for v in enum
                ]
            )
            if version_schema == "v1":
                tablename = tablename + "_v1"
            df.sort_values(by="value").to_csv(
                (OUTPUT_DIR / tablename).with_suffix(".csv"), index=False
            )
