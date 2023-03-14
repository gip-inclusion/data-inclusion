# deployment

## prerequisites

* A scaleway project.
* An IAM application ([here](https://console.scaleway.com/iam/applications)) with a policy to fully access all ressources in the target scaleway project.
* An API key for that application ([here](https://console.scaleway.com/iam/api-keys))

## targeting an environment

Use the `-chdir=` option to target a specific environment:

```bash
docker-compose run tf -chdir=environments/staging plan
```

## initializing the state backend

```bash
docker-compose run tf -chdir=environments/<ENVIRONMENT>/ init \
    -backend-config "bucket=data-inclusion-terraform" \
    -backend-config "key=stack_data/<ENVIRONMENT>" \
    -backend-config "region=fr-par" \
    -backend-config "endpoint=https://s3.fr-par.scw.cloud" \
    -backend-config "access_key=<ACCESS_KEY>" \
    -backend-config "secret_key=<SECRET_KEY>"
```

## configuring the deployment

The deployment is configured through variables. The set of variables will be different for each environment.

To generate a configuration file for a specific environment:

```bash
USER_ID=$(id -u) docker-compose run tf-vars environments/<ENVIRONMENT>
```

The generated `terraform.tfvars.json` file can be filled with the appropriate values for that environment.

⚠️⚠️ `terraform.tfvars.json` FILES SHOULD NOT BE COMMITTED ⚠️⚠️

## deployment

```bash
# review changes
docker-compose run tf -chdir=environments/staging plan

# apply
docker-compose run tf -chdir=environments/staging apply
```

## updating auto generated documentation

```bash
USER_ID=$(id -u) docker-compose run tf-docs
```

## formatting `.tf` files

```bash
docker-compose run tf-fmt
```

## references

* [Scaleway Terraform Provider documentation](https://registry.terraform.io/providers/scaleway/scaleway/latest/docs)
* [Google's Best practices for using Terraform](https://cloud.google.com/docs/terraform/best-practices-for-terraform)
* [How To Create Reusable Infrastructure with Terraform Modules and Templates by Savic](https://www.digitalocean.com/community/tutorials/how-to-create-reusable-infrastructure-with-terraform-modules-and-templates)