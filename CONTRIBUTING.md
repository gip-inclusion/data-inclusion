# Contributing

## Local prerequisites

### os dependencies

```bash
# required for psycopg2 (postgres)
sudo apt-get update -y && sudo apt-get install -y libpq-dev python3-dev
```

### `vscode` (Recommended)

Support for vscode is provided, with a sensible configuration.

### `docker`

A `docker-compose.yml` file is provided for development.

```bash
# Copy the template .env
cp .template.env .env

# Edit the content of .env file to your needs
# It should work as is, except for the `AIRFLOW_UID=` variable that must be set
# to the value returned by `echo $(id -u)`

# Start the stack
docker compose up -d
```

After a few seconds, the services should be avaible as follow:

|                 | default location                                           | default credentials                     |
| --------------- | ---------------------------------------------------------- | --------------------------------------- |
| airflow UI      | [http://localhost:8080](http://localhost:8080)             | user: `airflow` pass: `airflow`         |
| data.inclusion  | [http://localhost:8000](http://localhost:8000/api/v0/docs) | token must be generated                 |
| siretisation UI | [http://localhost:8005](http://localhost:8005/admin)       | user: `admin@admin.admin` pass: `admin` |

### `minio` client

Optional, but it allows you to interact with the datalake from the commandline.

See installation instructions [here](https://min.io/docs/minio/linux/reference/minio-mc.html).

### `python`

Project uses `python3.10`

[`pyenv`](https://github.com/pyenv/pyenv) is a clean and easy way to manage multiple python versions on your computer. Installation instructions are available [here](https://github.com/pyenv/pyenv-installer).

### `pre-commit`

```bash
pipx install pre-commit
```

## Global setup

```bash
# Clone this repository
git clone git@github.com:betagouv/data-inclusion.git

# Setup code quality tools
pre-commit install

# Open workspace in vscode
code -n data-inclusion
```

Each subdirectory (`./pipeline`, `./api`, etc.) has its own contributing guidelines on how to setup an environment for development.

## Adding a public HTTP tabular source

To extract, read and store the source data in S3:

- add the URL in the `.template.env` file (use the format XXX_FILE_URL)
- add the environment variable in `docker-compose.yml`
- add the stream config in `pipeline/dags/dags/settings.py`
- add the source id + functions in `pipeline/dags/import_sources.py` (extract and read functions)

For transformations, create the relevant files in `dbt/sources`, `dbt/intermediate` and `dbt/marts`.

## Contribution

Issues and PRs are welcome.