from pathlib import Path

import marimo

app_factory = marimo.create_asgi_app(
    asset_url="https://cdn.jsdelivr.net/npm/@marimo-team/frontend@{version}/dist"
)

NOTEBOOK_PATH = Path(__file__).parent / "notebook.py"

app_factory.with_app(path="/", root=str(NOTEBOOK_PATH))

app = app_factory.build()
