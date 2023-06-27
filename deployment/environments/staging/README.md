<!-- BEGIN_TF_DOCS -->
## Requirements

No requirements.

## Providers

No providers.

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_stack_data"></a> [stack\_data](#module\_stack\_data) | ../../modules/stack_data | n/a |

## Resources

No resources.

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