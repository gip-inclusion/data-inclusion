from pathlib import Path
from typing import Annotated

import numpy as np

import fastapi
from fastapi import templating

from data_inclusion.api.valideur import readers, translations, utils, validate

MAX_FILE_SIZE_IN_BYTES = 500_000_000  # 500MB
TEMPLATES_DIR = Path(__file__).parent / "templates"

app = fastapi.FastAPI()

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

    summary_dict = {
        "structures": {
            "total": len(structures_df),
            "invalid": errors_df[errors_df["schema"] == "Structure"]["id"].nunique(),
            "errors": len(errors_df[errors_df["schema"] == "Structure"]),
        },
        "services": {
            "total": len(services_df),
            "invalid": errors_df[errors_df["schema"] == "Service"]["id"].nunique(),
            "errors": len(errors_df[errors_df["schema"] == "Service"]),
        },
    }

    errors_df = errors_df.groupby(
        by=["schema", "field", "value", "message"], as_index=False, dropna=False
    )["line"].agg(
        lines=lambda lines: utils.display_ranges(list(lines)).replace("-", " à ")
    )
    errors_df = errors_df[["schema", "lines", "field", "value", "message"]]
    errors_df = errors_df.sort_values(
        by=["schema", "lines", "field"], ascending=[False, True, True]
    )
    errors_df = errors_df.replace(np.nan, "")
    errors_records = errors_df.to_dict("records")

    return templates.TemplateResponse(
        request=request,
        name="fragments/results.html",
        context={
            "request": request,
            "summary_dict": summary_dict,
            "errors_records": errors_records,
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
