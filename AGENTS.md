# AGENTS.md

## Project structure

- `./api`: user facing fastapi app
- `./pipeline`: internal airflow/dbt pipeline
- `./deployment`: terraform to deploy the pipeline
- `./datawarehouse`: custom postgres image
- `./deduplication`: notebooks for training dedup

## Stack

- python/uv based project
- monorepo (except for the data·inclusion schema)
- docker

## Convention

- git: branch naming pattern is `<username>/<type>/<scope>`
- language:
    - english for code, comments, git and github
    - french for user facing texts
- testing: use dependency injection, avoid mocks
- python: explicit kwargs in function calling
- config: use env vars when possible

## Commands

- run python commands and tools with `uv run`
- use `make diagrams` to generate and check docs diagrams
- use `uvx --from awscli aws s3` to interact with the datalake
- manage docker containers with `docker compose`
- avoid `cd`; use absolute paths or tool flags when possible
