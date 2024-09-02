from setuptools import find_namespace_packages, setup

setup(
    author="vmttn",
    name="data-inclusion-api",
    url="https://github.com/gip-inclusion/data-inclusion",
    version="0.1.0",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    package_data={"": ["*.md", "*.json"]},
    entry_points={"console_scripts": ["data-inclusion-api=data_inclusion.api.cli:cli"]},
    install_requires=[
        "alembic",
        "certifi",
        "click",
        "cryptography",
        "faker",
        "fastapi",
        "fastapi-pagination",
        "furl",
        "geopandas",
        "GeoAlchemy2",
        "gunicorn",
        "httpx",
        "jinja2",
        "minio",
        "numpy",
        "pandas",
        "psycopg2",
        "py7zr",
        "pyarrow",
        "pydantic[email]>=2.5.0",
        "pydantic-settings",
        "python-dotenv",
        "python-jose[cryptography]",
        "pytz",
        "requests",
        "sentry-sdk[fastapi]",
        "sqlalchemy",
        "tqdm",
        "uvicorn[standard]",
        "data-inclusion-schema==0.17.0",
    ],
    extras_require={
        "test": [
            "pytest",
            "faker",
            "factory_boy",
            "pytest-dotenv",
            "fastapi-debug-toolbar",
            "syrupy",
        ],
        "dev": [
            "fastapi-debug-toolbar",
            "pre-commit",
            "ruff",
            "tox",
            "uv",
        ],
    },
)
