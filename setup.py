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
    extras_require={
        "test": [
            "pytest==7.1.2",
            "faker==13.15.1",
            "factory_boy==3.2.1",
            "pytest-dotenv==0.5.2",
        ],
        "dev": [
            "black==22.6.0",
            "tox==3.25.1",
            "pre-commit==2.20.0",
        ],
    },
)
