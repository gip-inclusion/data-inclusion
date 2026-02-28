from pathlib import Path
from typing import Annotated

import pandas as pd

import fastapi
from fastapi import staticfiles, templating

from data_inclusion.api.valideur import readers, translations, utils, validate

MAX_FILE_SIZE_IN_BYTES = 500_000_000  # 500MB
TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_FILES_DIR = Path(__file__).parent / "static"

app = fastapi.FastAPI()
app.mount(
    path="/static",
    app=staticfiles.StaticFiles(directory=STATIC_FILES_DIR.resolve()),
    name="static",
)

templates = templating.Jinja2Templates(directory=TEMPLATES_DIR)


@app.post(path="/fragments/analyze")
def results(
    request: fastapi.Request,
    dataset: Annotated[fastapi.UploadFile, fastapi.File()],
):
    structures_df, services_df = readers.read_file(
        content=dataset.file,
        file_type=readers.FileTypes.XLSX,
    )
    errors_df = validate.validate_dataset(structures_df, services_df)
    errors_df = errors_df.assign(message=errors_df["message"].apply(translations.tr))

    summary_df = pd.DataFrame(
        [
            {
                "Schéma": "Structures",
                "Total de lignes": len(structures_df),
                "Invalides": errors_df[errors_df["schéma"] == "Structure"][
                    "id"
                ].nunique(),
                "Erreurs": len(errors_df[errors_df["schéma"] == "Structure"]),
            },
            {
                "Schéma": "Services",
                "Total de lignes": len(services_df),
                "Invalides": errors_df[errors_df["schéma"] == "Service"][
                    "id"
                ].nunique(),
                "Erreurs": len(errors_df[errors_df["schéma"] == "Service"]),
            },
        ]
    )

    errors_df = errors_df.groupby(
        list(errors_df.columns[~errors_df.columns.isin(["id", "ligne"])]),
        as_index=False,
    )["ligne"].agg(
        lambda lignes: utils.display_ranges(list(lignes)).replace("-", " à ")
    )
    errors_df = errors_df[["schéma", "ligne", "champ", "message"]]
    errors_df = errors_df.sort_values(
        by=["schéma", "ligne", "champ"], ascending=[False, True, True]
    )

    return templates.TemplateResponse(
        request=request,
        name="fragments/results.html",
        context={
            "request": request,
            "summary_df": summary_df,
            "errors_df": errors_df,
        },
    )


@app.get(path="/")
def valideur(
    request: fastapi.Request,
):
    return templates.TemplateResponse(
        request=request,
        name="valideur.html",
        context={
            "request": request,
        },
    )
