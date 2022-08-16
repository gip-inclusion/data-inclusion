from setuptools import find_namespace_packages, setup

setup(
    author="vmttn",
    name="data-inclusion-api",
    url="https://github.com/betagouv/data-inclusion",
    version="0.1.0",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    entry_points={
        "console_scripts": ["data-inclusion-api=data_inclusion.api.entrypoints.cli:cli"]
    },
    install_requires=[
        "alembic",
        "click",
        "faker",
        "fastapi",
        "fastapi-pagination",
        "gunicorn",
        "pandas",
        "psycopg2",
        "pydantic[email]",
        "python-dotenv",
        "python-jose[cryptography]",
        "requests",
        "sqlalchemy",
        "uvicorn[standard]",
    ],
    extras_require={
        "test": [
            "pytest",
            "faker",
            "factory_boy",
            "pytest-dotenv",
        ],
        "dev": [
            "black",
            "tox",
            "pre-commit",
        ],
    },
)
