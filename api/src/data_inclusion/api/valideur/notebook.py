import marimo

__generated_with = "0.20.2"

app = marimo.App(
    width="compact",
    css_file="/usr/local/_marimo/custom.css",
    html_head_file="",
    auto_download=["html"],
)


with app.setup():
    import marimo as mo
    import numpy as np
    import pandas as pd

    from data_inclusion.api.valideur import readers, translations, utils, validate
    from data_inclusion.schema import v1

    MAX_FILE_SIZE_IN_BYTES = 500_000_000  # 500MB


@app.cell
def _():
    LABEL = "Cliquez pour sélectionner un fichier ou glissez-déposez-le directement dans cet encart."  # noqa: E501

    input_file_widget = mo.ui.file(
        label=LABEL,
        kind="area",
        filetypes=readers.FileTypes.as_list(),
        max_size=MAX_FILE_SIZE_IN_BYTES,
        multiple=False,
    )
    input_file_widget
    return (input_file_widget,)


@app.cell
def _(input_file_widget):
    if len(input_file_widget.value) > 0:
        # file loaded
        with mo.status.spinner() as spinner:
            spinner.update(title="Chargement...")
            structures_df, services_df = readers.read_file(input_file_widget.value)

            spinner.update(title="Validation...")
            structures_errors_df = validate.list_errors(v1.Structure, structures_df)
            services_errors_df = validate.list_errors(v1.Service, services_df)
            errors_df = pd.concat([structures_errors_df, services_errors_df])
            errors_df = errors_df.assign(
                message=errors_df["message"].apply(translations.tr)
            )
            errors_df = errors_df.replace({np.nan: None})
    else:
        structures_df, services_df = None, None
        structures_errors_df, services_errors_df = pd.DataFrame(), pd.DataFrame()
        errors_df = pd.DataFrame()
    return (
        errors_df,
        services_df,
        services_errors_df,
        structures_df,
        structures_errors_df,
    )


@app.cell
def _(
    services_df,
    services_errors_df,
    structures_df,
    structures_errors_df,
):
    mo.stop(structures_df is None)

    mo.md(rf"""
    ## État du fichier

    | Schéma     | Total de lignes    | Invalides | Erreurs |
    | ---------- | ------------------ | --------- | ------- |
    | Structures | {len(structures_df)} | {structures_errors_df["id"].nunique()} | {len(structures_errors_df)} |
    | Services   | {len(services_df)}   | {services_errors_df["id"].nunique()} | {len(services_errors_df)} |
    """)  # noqa: E501
    return


@app.cell
def _(errors_df, structures_df):
    mo.stop(structures_df is None or len(errors_df) > 0)

    SUCCESS_MSG = (
        "Toutes vos données respectent intégralement notre schéma data·inclusion."
    )

    callout_success = mo.callout(value=SUCCESS_MSG, kind="success")

    mo.md(rf"""
    {callout_success}

    ## Votre fichier est parfaitement valide !

    Vous pouvez les publier en l'état sur la plateforme data.gouv.
    """)
    return


@app.cell
def _(errors_df, structures_df):
    mo.stop(structures_df is None or len(errors_df) == 0)

    _errors_df = errors_df.groupby(
        list(errors_df.columns[~errors_df.columns.isin(["id", "ligne"])]),
        as_index=False,
    )["ligne"].agg(lambda lignes: utils.display_ranges(lignes).replace("-", " à "))

    FAILURE_MSG = "Votre fichier comporte plusieurs erreurs qui nécessitent des corrections de votre part."  # noqa: E501

    callout_failure = mo.callout(value=FAILURE_MSG, kind="danger")

    mo.md(rf"""
    {callout_failure}

    ## Erreurs à corriger

    Voici la liste des anomalies détectées dans votre fichier par rapport au schéma data·inclusion.

    | Typologie | Ligne(s) | Champ | Valeur | Détail |
    | --------- | -------- | ----- | ------ | ------ |
    {
        "\n".join(
            "|".join(
                [
                    row["schéma"],
                    str(row["ligne"]),
                    row["champ"],
                    str(row["valeur"]) if row["valeur"] else "",
                    str(row["message"]) if row["message"] else "",
                ]
            )
            for _, row in _errors_df.iterrows()
        )
    }

    """)  # noqa: E501
    return


if __name__ == "__main__":
    app.run()
