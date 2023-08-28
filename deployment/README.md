# deployment

This documentation is structured as follow :

1. the provisioning of the infrastructure on which to deploy
2. the deployment of our services on top the infrastructure

## provisioning

### prerequisites

#### for the state backend

* A scaleway project
* A policy with access to object storage ([here](https://console.scaleway.com/iam/policies))
* An IAM application with this policy assigned ([here](https://console.scaleway.com/iam/applications))
* An API key for this application ([here](https://console.scaleway.com/iam/api-keys))

This is preferably shared by environments.

#### for provisioning an environment

* A scaleway project dedicated for that environment
* A policy ([here](https://console.scaleway.com/iam/policies)) with the following rules:
    * `InstancesFullAccess`, `ObjectStorageFullAccess`, `RelationalDatabasesFullAccess` in the target project scope
* An IAM application with this policy assigned ([here](https://console.scaleway.com/iam/applications))
* An API key for this application ([here](https://console.scaleway.com/iam/api-keys))

### targeting an environment

Use the `-chdir=` option to target a specific environment:

```bash
docker compose run --rm tf -chdir=environments/staging plan
```

### initializing the state backend

*Use the prerequisites for the state backend*

```bash
docker compose run --rm tf -chdir=environments/<ENVIRONMENT>/ init \
    -backend-config "bucket=data-inclusion-terraform" \
    -backend-config "key=stack_data/<ENVIRONMENT>" \
    -backend-config "region=fr-par" \
    -backend-config "endpoint=https://s3.fr-par.scw.cloud" \
    -backend-config "access_key=<ACCESS_KEY>" \
    -backend-config "secret_key=<SECRET_KEY>"
```

### configuring the provisioning

The deployment is configured through variables. The set of variables will be different for each environment.

To generate a configuration file for a specific environment:

```bash
USER_ID=$(id -u) docker compose run --rm tf-vars environments/<ENVIRONMENT>
```

The generated `terraform.tfvars.json` file can be filled with the appropriate values for that environment.

⚠️⚠️ `terraform.tfvars.json` FILES SHOULD NOT BE COMMITTED ⚠️⚠️

*Use the prerequisites for provisioning an environment*

### provisioning

```bash
# review changes
docker compose run --rm tf -chdir=environments/staging plan

# apply
docker compose run --rm tf -chdir=environments/staging apply
```

### updating auto generated documentation

```bash
USER_ID=$(id -u) docker compose run --rm tf-docs
```

### formatting `.tf` files

```bash
docker compose run --rm tf-fmt
```

### references

* [Scaleway Terraform Provider documentation](https://registry.terraform.io/providers/scaleway/scaleway/latest/docs)
* [Google's Best practices for using Terraform](https://cloud.google.com/docs/terraform/best-practices-for-terraform)
* [How To Create Reusable Infrastructure with Terraform Modules and Templates by Savic](https://www.digitalocean.com/community/tutorials/how-to-create-reusable-infrastructure-with-terraform-modules-and-templates)

## deployment

```bash
docker compose up -d [--env-file path/to/prod/.env]
```