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

## Convention

- language: english for code and comments, french for user facing texts
- testing: use dependency injection, avoid mocks
- python: explicit kwargs in function calling

## Commands

- run python commands and tools with `uv run`
- use `make diagrams` to generate and check docs diagrams
