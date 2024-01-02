from setuptools import find_namespace_packages, setup

setup(
    author="vmttn",
    name="data-inclusion-scripts",
    url="https://github.com/gip-inclusion/data-inclusion",
    version="0.0.1",
    packages=find_namespace_packages(where="dags"),
    package_dir={"": "dags"},
)
