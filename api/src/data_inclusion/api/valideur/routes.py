from pathlib import Path
from typing import Annotated

import numpy as np
import pandas as pd

import fastapi
from fastapi import templating

from data_inclusion.api.core import db
from data_inclusion.api.valideur import readers, services, utils

TEMPLATES_DIR = Path(__file__).parent / "templates"

app = fastapi.FastAPI()

templates = templating.Jinja2Templates(directory=TEMPLATES_DIR)

MAX_FILE_SIZE_BYTES = 5_000_000


@app.exception_handler(readers.ValideurException)
def valideur_exception_handler(
    request: fastapi.Request, exc: readers.ValideurException
) -> fastapi.responses.JSONResponse:
    return fastapi.responses.JSONResponse(
        status_code=422,
        content={"detail": str(exc)},
    )


@app.post(path="/fragments/analyze")
def results(
    request: fastapi.Request,
    dataset: Annotated[fastapi.UploadFile, fastapi.File()],
    db_session=fastapi.Depends(db.get_session),
):
    structures_df, services_df = readers.read_file(
        file=dataset.file, filename=dataset.filename
    )
    errors_df = pd.DataFrame(
        services.validate_dataset(
            db_session=db_session,
            structures_data=structures_df.to_dict(orient="records"),
            services_data=services_df.to_dict(orient="records"),
        ),
        columns=["schema", "line", "field", "value", "message"],
    )
    errors_df = errors_df.replace({np.nan: None})

    summary_dict = {
        "structures": {
            "total": len(structures_df),
            "invalid": errors_df[errors_df["schema"] == "Structure"]["line"].nunique(),
            "errors": len(errors_df[errors_df["schema"] == "Structure"]),
        },
        "services": {
            "total": len(services_df),
            "invalid": errors_df[errors_df["schema"] == "Service"]["line"].nunique(),
            "errors": len(errors_df[errors_df["schema"] == "Service"]),
        },
    }

    errors_df = (
        errors_df.assign(value=errors_df["value"].astype(str))
        .groupby(
            by=["schema", "field", "value", "message"], as_index=False, dropna=False
        )["line"]
        .agg(lines=lambda lines: utils.display_ranges(list(lines)).replace("-", " à "))
    )
    errors_df = errors_df[["schema", "lines", "field", "value", "message"]]
    errors_df = errors_df.sort_values(
        by=["schema", "lines", "field"],
        ascending=[False, True, True],
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
            "supported_file_types": readers.SupportedFileTypes,
            "max_file_size_bytes": MAX_FILE_SIZE_BYTES,
        },
    )
