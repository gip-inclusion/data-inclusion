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

### `python`

Project uses `python3.10`

[`pyenv`](https://github.com/pyenv/pyenv) is a clean and easy way to manage multiple python versions on your computer. Installation instructions are available [here](https://github.com/pyenv/pyenv-installer).

### `pre-commit`

```bash
pipx install pre-commit
```

## Setup

```bash
# Clone this repository
git clone git@github.com:betagouv/data-inclusion.git

# Setup code quality tools
pre-commit install

# Open workspace in vscode
code -n data-inclusion
```

## Contribution

Issues and PRs are welcome.