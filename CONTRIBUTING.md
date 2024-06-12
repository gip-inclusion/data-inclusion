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


### Scaleway

You will need to interact with Scaleway. Once you have your access with the right IAM configuration:

1. Install [Scaleway CLI](https://www.scaleway.com/en/docs/developer-tools/scaleway-cli/quickstart/#how-to-install-the-scaleway-cli-locally).
2. Generate an [SSH key](https://www.scaleway.com/en/docs/identity-and-access-management/organizations-and-projects/how-to/create-ssh-key/#how-to-upload-the-public-ssh-key-to-the-scaleway-interface) (if you don't already have one).
3. Upload it on [Scaleway](https://www.scaleway.com/en/docs/identity-and-access-management/organizations-and-projects/how-to/create-ssh-key/#how-to-upload-the-public-ssh-key-to-the-scaleway-interface).
4. Generate two API keys, one for the production bucket and one for the staging bucket.
5. You can then create two profiles for the Scaleway CLI with the following command:
    ```bash
    scw init -p staging \
      access-key={youraccesskey} \
      secret-key={yoursecretkey} \
      organization-id={organization} \
      project-id={projectid}
    ```

### `minio` Client

This is optional but allows you to interact with the datalake from the command line.

See installation instructions [here](https://min.io/docs/minio/linux/reference/minio-mc.html).

You can then create aliases for Scaleway S3 staging and production, as well as one for your local Minio server. For your local server, you need to first create your API key. After launching Docker Compose, go to the [console](http://localhost:9001), click on the `Access Keys` tab, and create an access key.

You can add aliases with the following command:
```bash
mc alias set dev http://localhost:9000 {youraccesskey} {yoursecretkey}
```

Do the same for staging and production (replace the access key and the secret key with the API key you created in Scaleway):
```bash
mc alias set prod https://s3.fr-par.scw.cloud {youraccesskey} {yoursecretkey} --api S3v4
mc alias set staging https://s3.fr-par.scw.cloud {youraccesskey} {yoursecretkey} --api S3v4
```

You can test it out, and you should have results that look like this:
```bash
$ mc ls prod
[2024-04-22 13:33:54 CEST]     0B data-inclusion-datalake-prod-grand-titmouse/
$ mc ls staging
[2024-04-10 19:45:43 CEST]     0B data-inclusion-datalake-staging-sincere-buzzard/
$ mc ls dev
[2024-06-11 10:08:06 CEST]     0B data-inclusion-lake/
```

You can now easily interact with all the buckets.

### `python`

Project uses `python3.11`

[`pyenv`](https://github.com/pyenv/pyenv) is a clean and easy way to manage multiple python versions on your computer. Installation instructions are available [here](https://github.com/pyenv/pyenv-installer).

### `pre-commit`

```bash
pipx install pre-commit
```

## Global setup

```bash
# Clone this repository
git clone git@github.com:gip-inclusion/data-inclusion.git

# Setup code quality tools
pre-commit install

# Open workspace in vscode
code -n data-inclusion
```

Each subdirectory (`./pipeline`, `./api`, etc.) has its own contributing guidelines on how to setup an environment for development.

## Contribution

Issues and PRs are welcome.
