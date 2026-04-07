from pathlib import Path

# cf https://www.data.gouv.fr/datasets/finess-extraction-du-fichier-des-etablissements
FINESS_CSV_COLUMNS = [
    "typeet",
    "nofinesset",
    "nofinessej",
    "rs",
    "rslongue",
    "complrs",
    "compldistrib",
    "numvoie",
    "typvoie",
    "voie",
    "compvoie",
    "lieuditbp",
    "commune",
    "departement",
    "libdepartement",
    "ligneacheminement",
    "telephone",
    "telecopie",
    "categetab",
    "libcategetab",
    "categagretab",
    "libcategagretab",
    "siret",
    "codeape",
    "codemft",
    "libmft",
    "codesph",
    "libsph",
    "dateouv",
    "dateautor",
    "datemaj",
    "numuai",
]


def read(path: Path):
    import pandas as pd

    return (
        pd.read_csv(
            path,
            sep=";",
            header=None,
            index_col=False,
            skiprows=1,  # first row is like finess;etalab;96;2026-03-11
            names=FINESS_CSV_COLUMNS,
            dtype=str,
        )
        .fillna("")
        .replace({"": None})
    )
