# deployment

## provisioning

### prerequisites

* `terraform==1.5.x` (can be installed with an `asdf` plugin)

#### for the state backend

* A scaleway project `terraform`
* A policy `terraform--manual--edit-tf-states` allowing edits to object storage
* An IAM application `terraform--manual--github-ci` with the `terraform--manual--edit-tf-states` policy assigned
* An API key for this application

The state backend is shared by environments. There should already be a project `terraform`.

#### to provision an environment

1. A scaleway project dedicated for that environment (`prod`, `staging`, etc.)
2. An IAM application `<ENVIRONMENT>--manual--github-ci` that will be used to provision scaleway resources in the project, together with the API key (access key + secret key) for this application. This application needs a policy `<ENVIRONMENT>--manual--edit-stack-data-resources` attached that gives it:
    * `IAMReadOnly`, `ProjectReadOnly` in the organization;
    * `InstancesFullAccess`, `ObjectStorageFullAccess`, `RelationalDatabasesFullAccess` and `DomainsDNSFullAccess` in the target project scope
3. Another IAM application `<ENVIRONMENT>--manual--airflow` that will be used for object storage by Airflow, together with the API key (access key + secret key) for this application. This application needs a policy `<ENVIRONMENT>--manual--airflow-object-storage-access` attached that gives it `ObjectStorageFullAccess` in the target project.
4. A SSH key pair:
    * generated with `ssh-keygen -t ed25519 -C <ENVIRONMENT> -f /tmp/<ENVIRONMENT> -N ''`)
    * the public key must have been uploaded to scaleway
5. A domain (`<ENVIRONMENT>.data.inclusion.beta.gouv.fr`) registered on AlwaysData and declared on scaleway as an external domain in the environment project.

💡 IAM applications are not created with terraform, because it would require the `<ENVIRONMENT>--manual--github-ci` application to have full access to IAM management at the organization level.

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
    -backend-config "bucket=data-inclusion-terraform-states" \
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

### formatting `.tf` files

A pre-commit hook is provided, make sure to `pre-commit install`!

### references

* [Scaleway Terraform Provider documentation](https://registry.terraform.io/providers/scaleway/scaleway/latest/docs)
* [Google's Best practices for using Terraform](https://cloud.google.com/docs/terraform/best-practices-for-terraform)
* [How To Create Reusable Infrastructure with Terraform Modules and Templates by Savic](https://www.digitalocean.com/community/tutorials/how-to-create-reusable-infrastructure-with-terraform-modules-and-templates)
