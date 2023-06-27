# `stack_data`

Provision a compute instance with docker in the given scaleway project.

<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_scaleway"></a> [scaleway](#requirement\_scaleway) | 2.13.1 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_scaleway"></a> [scaleway](#provider\_scaleway) | 2.13.1 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [scaleway_iam_api_key.main](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/iam_api_key) | resource |
| [scaleway_iam_application.main](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/iam_application) | resource |
| [scaleway_iam_policy.object_full_access](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/iam_policy) | resource |
| [scaleway_instance_ip.main](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/instance_ip) | resource |
| [scaleway_instance_server.main](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/instance_server) | resource |
| [scaleway_object_bucket.main](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/object_bucket) | resource |
| [scaleway_rdb_acl.main](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/rdb_acl) | resource |
| [scaleway_rdb_database.main](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/rdb_database) | resource |
| [scaleway_rdb_instance.main](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/rdb_instance) | resource |
| [scaleway_rdb_privilege.main](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/rdb_privilege) | resource |
| [scaleway_rdb_user.main](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/rdb_user) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_datawarehouse_admin_password"></a> [datawarehouse\_admin\_password](#input\_datawarehouse\_admin\_password) | Password for the first user of the postgres datawarehouse | `string` | n/a | yes |
| <a name="input_datawarehouse_admin_username"></a> [datawarehouse\_admin\_username](#input\_datawarehouse\_admin\_username) | Identifier for the first user of the postgres datawarehouse | `string` | n/a | yes |
| <a name="input_datawarehouse_di_database"></a> [datawarehouse\_di\_database](#input\_datawarehouse\_di\_database) | Identifier for the data inclusion database | `string` | n/a | yes |
| <a name="input_datawarehouse_di_password"></a> [datawarehouse\_di\_password](#input\_datawarehouse\_di\_password) | Password for the main user of the postgres datawarehouse | `string` | n/a | yes |
| <a name="input_datawarehouse_di_username"></a> [datawarehouse\_di\_username](#input\_datawarehouse\_di\_username) | Identifier for the main user of the postgres datawarehouse | `string` | n/a | yes |
| <a name="input_scaleway_access_key"></a> [scaleway\_access\_key](#input\_scaleway\_access\_key) | Scaleway access key (https://console.scaleway.com/iam/api-keys) | `string` | n/a | yes |
| <a name="input_scaleway_project_id"></a> [scaleway\_project\_id](#input\_scaleway\_project\_id) | Scaleway project id (https://console.scaleway.com/project/settings) | `string` | n/a | yes |
| <a name="input_scaleway_secret_key"></a> [scaleway\_secret\_key](#input\_scaleway\_secret\_key) | Scaleway secret key (https://console.scaleway.com/iam/api-keys) | `string` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_object_storage_access_key"></a> [object\_storage\_access\_key](#output\_object\_storage\_access\_key) | Access key for the datalake object storage |
| <a name="output_object_storage_secret_key"></a> [object\_storage\_secret\_key](#output\_object\_storage\_secret\_key) | Secret key for the datalake object storage |
| <a name="output_public_ip"></a> [public\_ip](#output\_public\_ip) | Publicly reachable IP (with `ssh root@<public_ip>`) |
<!-- END_TF_DOCS -->