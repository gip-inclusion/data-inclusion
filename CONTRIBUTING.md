# Contributing

## Common setup

```bash
# Clone this repository
git clone git@github.com:gip-inclusion/data-inclusion.git && cd data-inclusion

# Open workspace in vscode
code .vscode/data-inclusion.code-workspace

# Install precommit hooks
uvx pre-commit install

# Copy the template .env
cp .template.env .env

# Edit the content of .env file to your needs
# It should work as is, except for the `AIRFLOW_UID=` variable that must be set
# to the value returned by `echo $(id -u)`

# Start the stack (or just part of it)
docker compose up -d
```

After a few seconds, the services should be available as follows:

|                 | default location                                        | default credentials                               |
| --------------- | ------------------------------------------------------- | ------------------------------------------------- |
| airflow UI      | [http://localhost:8080](http://localhost:8080)          | none (all users are admins)                       |
| api             | [http://localhost:8000](http://localhost:8000/api/docs) | token must be generated                           |
| datawarehouse (pg)   | `localhost:5433`                                        | user: `data-inclusion` pass: `data-inclusion`     |
| datalake (minio)| `localhost:9000`                                        | user: `minioadmin` pass: `minioadmin`             |
| mlflow          | [http://localhost:5000](http://localhost:5000)          | none                                              |

The airflow, api and datawarehouse ports can be overridden in `.env` (`AIRFLOW_UI_PORT`, `API_PORT`, `TARGET_POSTGRES_PORT`).

## Subdir specific setups

Each subdirectory (`./api`, etc.) has its own contributing guidelines on how to setup an environment for development.

## Deployment

* The pipeline is deployed on Scaleway using terraform in a github workflow.
* The api is deployed on Scalingo, from sources, using the Scalingo github integration and [`PROJECT_DIR`](https://doc.scalingo.com/platform/getting-started/common-deployment-errors#project-in-a-subdirectory).
