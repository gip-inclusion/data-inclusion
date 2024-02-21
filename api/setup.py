from setuptools import find_namespace_packages, setup

setup(
    author="vmttn",
    name="data-inclusion-api",
    url="https://github.com/gip-inclusion/data-inclusion",
    version="0.1.0",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    entry_points={
        "console_scripts": ["data-inclusion-api=data_inclusion.api.entrypoints.cli:cli"]
    },
    install_requires=[
        "alembic",
        "certifi",
        "click",
        "cryptography",
        "faker",
        "fastapi",
        "fastapi-pagination",
        "GeoAlchemy2",
        "gunicorn",
        "httpx",
        "numpy",
        "pandas",
        "psycopg2",
        "pydantic[email]>=2.5.0",
        "python-dotenv",
        "python-jose[cryptography]",
        "pytz",
        "requests",
        "sentry-sdk[fastapi]",
        "sqlalchemy",
        "uvicorn[standard]",
        "data-inclusion-schema==0.14.0",
    ],
    extras_require={
        "test": [
            "pytest",
            "faker",
            "factory_boy",
            "pytest-dotenv",
            "geopandas",
        ],
        "dev": [
            "ruff",
            "tox",
            "pre-commit",
        ],
    },
)
