# deployment

Only the pipeline is currently managed by this terraform configuration.

The pipeline is deployed on scaleway, from compiled images, using terraform in a github workflow.

The api and metabase are deployed on scalingo, from sources, using the scalingo github integration.

## provisioning

### prerequisites

* `terraform==1.5.x` (can be installed with an `asdf` plugin)

#### for the state backend

The state backend is shared by environments.

| resource type         | resource name                      | description                                                                               | details                                                                                                                                                                                     |
|-----------------------|------------------------------------|-------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| scaleway project      | `data-inclusion-terraform`         | Common to all environments                                                                |                                                                                                                                                                                             |
| object storage bucket | `data-inclusion-tf-states`         | Store terraform states for all environments                                               |                                                                                                                                                                                             |
| IAM group             | `data-inclusion-terraform-editors` | Contains all apps or users who want to deploy                                             |  A team member who wants to deploy using the `terraform` cli locally must add himself to this group                                                                                                                                                                                       |
| IAM policy            | `data-inclusion-terraform-editors` | Assigned to the IAM group of the same name. Allow it to provision the resources required. | `IAMReadOnly`, `ProjectReadOnly` in the organization AND `InstancesFullAccess`, `ObjectStorageFullAccess`, `RelationalDatabasesFullAccess` and `DomainsDNSFullAccess` in the target project |
| IAM application       | `data-inclusion-terraform-github`  | Used by GH action to provision on SCW                                                   |  Must be in the `data-inclusion-terraform-editors` group |                                                                                                                                                                                         |
| API key               | -                                  | Creds for  `data-inclusion-terraform-github`                                            | Set secret `AWS_SECRET_ACCESS_KEY` and variable `AWS_ACCESS_KEY_ID` in the target GH environment ([here](https://github.com/gip-inclusion/data-inclusion/settings/environments))            |

#### to provision an environment

The following resources must be created manually **before** running `terraform apply`.

Replace `<ENVIRONMENT>` with the identifier for the target environment : `prod`, `staging`, etc.

| resource type    | resource name                         | description                                                                             | details                                                                                                                                                                                     |
|------------------|---------------------------------------|-----------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| scaleway project | `data-inclusion-<ENVIRONMENT>`        |                                                                                         |                                                                                                                                                                                             |
| IAM application  | `data-inclusion-<ENVIRONMENT>-github` | Used by GH action to provision on SCW                                                   |                                                                                                                                                                                             |
| IAM policy       | `data-inclusion-<ENVIRONMENT>-github` | Assigned to the IAM app of the same name. Allow it to provision the resources required. | `IAMReadOnly`, `ProjectReadOnly` in the organization AND `InstancesFullAccess`, `ObjectStorageFullAccess`, `RelationalDatabasesFullAccess` and `DomainsDNSFullAccess` in the target project |
| API key          | -                                     | Creds for  `data-inclusion-<ENVIRONMENT>-github`                                        | Set `scaleway_access_key` and `scaleway_secret_key` in the terraform config for that env stored in bitwarden |
| IAM application | `data-inclusion-<ENVIRONMENT>-airflow` | Used by airflow to read from & write to the datalake |
| IAM policy | `data-inclusion-<ENVIRONMENT>-airflow` | Assigned to the IAM app of the same name. Allow it to read from & write to object storage. | `ObjectStorageFullAccess` in the target project |
| API key | - | Creds for `data-inclusion-<ENVIRONMENT>-airflow` | Set `airflow_access_key` and `airflow_secret_key` in the terraform config for that env stored in bitwarden |
| IAM application | `data-inclusion-<ENVIRONMENT>-api` | Used by the api to read in the `data/marts` directory in the datalake |
| IAM policy | `data-inclusion-<ENVIRONMENT>-api` | Assigned to the IAM app of the same name. Allow it to read from object storage. | `ObjectStorageReadOnly` in the target project |
| API key | - | Creds for `data-inclusion-<ENVIRONMENT>-api` | Set `DATALAKE_SECRET_KEY` and `DATALAKE_ACCESS_KEY` in the scalingo app `data-inclusion-api-<ENVIRONMENT>` |
| SSH key | - | Used by GH action to connect to the server and deploy docker services | Generated with `ssh-keygen -t ed25519 -C <ENVIRONMENT> -f /tmp/<ENVIRONMENT> -N ''`. The public key must be uploaded to SCW. |
| External domain | `<ENVIRONMENT>.data.inclusion.gouv.fr` | Used to generate subdomains pointing to the server | The `data.inclusion.gouv.fr` domain is registered on Gandi |


💡 IAM applications must be created manually, because it would require the `data-inclusion-<ENVIRONMENT>-github` application to have full access to IAM management at the organization level.

#### links

* https://console.scaleway.com/iam/api-keys
* https://console.scaleway.com/iam/applications
* https://console.scaleway.com/iam/policies
* https://console.scaleway.com/project/ssh-keys
* https://console.scaleway.com/domains/external
* https://admin.alwaysdata.com/record/?domain=84883

### initializing the state backend

*Use the prerequisites for the state backend*

```bash
set +o history
terraform init \
    -backend-config "bucket=data-inclusion-tf-states" \
    -backend-config "key=<ENVIRONMENT>" \
    -backend-config "access_key=<ACCESS_KEY>" \
    -backend-config "secret_key=<SECRET_KEY>"
set -o history
```

where:

* `<ENVIRONMENT>` is an identifier for the target environment (`prod`, `staging`, etc.)
* `<ACCESS_KEY>` and `<SECRET_KEY>` are the api key pair associated to the state backend

### configuring the provisioning

The deployment is configured through variables. The set of variables will be different for each environment.

Variables must be defined in a `terraform.tfvars.json` in the deployment directory.

```bash
# Copy and fill the template
cp template.terraform.tfvars.json terraform.tfvars.json
```

### provisioning

```bash
# review changes
terraform plan

# apply
terraform apply
```

### prod and staging on github

Each environment has a dedicated github environment. In each of these gh environment, a `TF_VARS_BASE64` secret contains a base64 encoded version of `terraform.tfvars.json`, with all the variables for a given environment.

The files are stored on bitwarden as `<ENVIRONMENT>.terraform.tfvars.json`.

Use the following gh cli command to update this file on github:

```bash
# using gh cli
# replace <ENVIRONMENT>
base64 -w0 <ENVIRONMENT>.terraform.tfvars.json | gh secret --repo gip-inclusion/data-inclusion --env <ENVIRONMENT> set TF_VARS_BASE64
```

Make sure to update the config on bitwarden.

### formatting `.tf` files

A pre-commit hook is provided, make sure to `pre-commit install`!

### references

* [Scaleway Terraform Provider documentation](https://registry.terraform.io/providers/scaleway/scaleway/latest/docs)
* [Google's Best practices for using Terraform](https://cloud.google.com/docs/terraform/best-practices-for-terraform)
* [How To Create Reusable Infrastructure with Terraform Modules and Templates by Savic](https://www.digitalocean.com/community/tutorials/how-to-create-reusable-infrastructure-with-terraform-modules-and-templates)
