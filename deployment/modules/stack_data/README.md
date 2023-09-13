# `stack_data`

Provision a compute instance with docker in the given scaleway project.

<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_null"></a> [null](#requirement\_null) | 3.2.1 |
| <a name="requirement_scaleway"></a> [scaleway](#requirement\_scaleway) | 2.26.0 |
| <a name="requirement_time"></a> [time](#requirement\_time) | 0.9.1 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_null"></a> [null](#provider\_null) | 3.2.1 |
| <a name="provider_scaleway"></a> [scaleway](#provider\_scaleway) | 2.26.0 |
| <a name="provider_time"></a> [time](#provider\_time) | 0.9.1 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [null_resource.up](https://registry.terraform.io/providers/hashicorp/null/3.2.1/docs/resources/resource) | resource |
| [scaleway_iam_api_key.main](https://registry.terraform.io/providers/scaleway/scaleway/2.26.0/docs/resources/iam_api_key) | resource |
| [scaleway_iam_application.main](https://registry.terraform.io/providers/scaleway/scaleway/2.26.0/docs/resources/iam_application) | resource |
| [scaleway_instance_ip.main](https://registry.terraform.io/providers/scaleway/scaleway/2.26.0/docs/resources/instance_ip) | resource |
| [scaleway_instance_security_group.main](https://registry.terraform.io/providers/scaleway/scaleway/2.26.0/docs/resources/instance_security_group) | resource |
| [scaleway_instance_server.main](https://registry.terraform.io/providers/scaleway/scaleway/2.26.0/docs/resources/instance_server) | resource |
| [scaleway_object_bucket.main](https://registry.terraform.io/providers/scaleway/scaleway/2.26.0/docs/resources/object_bucket) | resource |
| [scaleway_object_bucket_policy.main](https://registry.terraform.io/providers/scaleway/scaleway/2.26.0/docs/resources/object_bucket_policy) | resource |
| [time_rotating.api_key_rotation](https://registry.terraform.io/providers/hashicorp/time/0.9.1/docs/resources/rotating) | resource |
| [scaleway_account_project.main](https://registry.terraform.io/providers/scaleway/scaleway/2.26.0/docs/data-sources/account_project) | data source |
| [scaleway_iam_group.editors](https://registry.terraform.io/providers/scaleway/scaleway/2.26.0/docs/data-sources/iam_group) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_airflow_admin_password"></a> [airflow\_admin\_password](#input\_airflow\_admin\_password) | Password for airflow admin panel | `string` | n/a | yes |
| <a name="input_api_secret_key"></a> [api\_secret\_key](#input\_api\_secret\_key) | Secret key used for cryptographic signing by the api | `string` | n/a | yes |
| <a name="input_api_version"></a> [api\_version](#input\_api\_version) | Version (e.g. sha or semver) of the api to deploy | `string` | n/a | yes |
| <a name="input_datawarehouse_admin_password"></a> [datawarehouse\_admin\_password](#input\_datawarehouse\_admin\_password) | Password for the first user of the postgres datawarehouse | `string` | n/a | yes |
| <a name="input_datawarehouse_admin_username"></a> [datawarehouse\_admin\_username](#input\_datawarehouse\_admin\_username) | Identifier for the first user of the postgres datawarehouse | `string` | n/a | yes |
| <a name="input_datawarehouse_di_database"></a> [datawarehouse\_di\_database](#input\_datawarehouse\_di\_database) | Identifier for the data inclusion database | `string` | n/a | yes |
| <a name="input_datawarehouse_di_password"></a> [datawarehouse\_di\_password](#input\_datawarehouse\_di\_password) | Password for the main user of the postgres datawarehouse | `string` | n/a | yes |
| <a name="input_datawarehouse_di_username"></a> [datawarehouse\_di\_username](#input\_datawarehouse\_di\_username) | Identifier for the main user of the postgres datawarehouse | `string` | n/a | yes |
| <a name="input_environment"></a> [environment](#input\_environment) | Identifier of the target environment | `string` | n/a | yes |
| <a name="input_scaleway_access_key"></a> [scaleway\_access\_key](#input\_scaleway\_access\_key) | Scaleway access key (https://console.scaleway.com/iam/api-keys) | `string` | n/a | yes |
| <a name="input_scaleway_project_id"></a> [scaleway\_project\_id](#input\_scaleway\_project\_id) | Scaleway project id (https://console.scaleway.com/project/settings) | `string` | n/a | yes |
| <a name="input_scaleway_secret_key"></a> [scaleway\_secret\_key](#input\_scaleway\_secret\_key) | Scaleway secret key (https://console.scaleway.com/iam/api-keys) | `string` | n/a | yes |
| <a name="input_ssh_private_key"></a> [ssh\_private\_key](#input\_ssh\_private\_key) | The associated public key will be deployed to the instance | `string` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_airflow_conn_pg"></a> [airflow\_conn\_pg](#output\_airflow\_conn\_pg) | Connection string to the datawarehouse for airflow |
| <a name="output_airflow_conn_s3"></a> [airflow\_conn\_s3](#output\_airflow\_conn\_s3) | Connection string to the datalake for airflow |
| <a name="output_object_storage_access_key"></a> [object\_storage\_access\_key](#output\_object\_storage\_access\_key) | Access key for the datalake object storage |
| <a name="output_object_storage_secret_key"></a> [object\_storage\_secret\_key](#output\_object\_storage\_secret\_key) | Secret key for the datalake object storage |
| <a name="output_public_ip"></a> [public\_ip](#output\_public\_ip) | Publicly reachable IP (with `ssh root@<public_ip>`) |
<!-- END_TF_DOCS -->