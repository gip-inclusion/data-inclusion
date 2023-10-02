# `datawarehouse`

This subdirectory holds datawarehouse docker image.

The image:

* extends the official `postgres` image
* installs `postgis` and `plpython3u` extensions
* installs extra python requirements in a dedicated virtualenv
* makes this virtualenv available to `plpython3u`
* creates `pg_trgm`, `unaccent`, `postgis` and `plpython3u` extensions on the DB target by the `POSTGRES_DB` env variable

## compiling the python requirements

The base image is based on debian `bookworm` which ships with python3.11.

For the sake of simplicity, this will be the target python version.

The `requirements.txt` should therefore be compiled in a python3.11 environment.

```bash
pyenv shell 3.11
pipx run --spec pip-tools --python=$(which python) pip-compile requirements/requirements.in
```

## about `plpython3u`

`plpython3u`:

* enables the use of python in postgres
* can be leverage to do complex task (geocoding, validation) that can be orchestrated with dbt
* does not restrict what user can do with it in a postgres database
* is therefore not available in managed databases on cloud platforms
