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
| [scaleway_instance_ip.main](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/instance_ip) | resource |
| [scaleway_instance_server.main](https://registry.terraform.io/providers/scaleway/scaleway/2.13.1/docs/resources/instance_server) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_scaleway_access_key"></a> [scaleway\_access\_key](#input\_scaleway\_access\_key) | Scaleway access key (https://console.scaleway.com/iam/api-keys) | `string` | n/a | yes |
| <a name="input_scaleway_project_id"></a> [scaleway\_project\_id](#input\_scaleway\_project\_id) | Scaleway project id (https://console.scaleway.com/project/settings) | `string` | n/a | yes |
| <a name="input_scaleway_secret_key"></a> [scaleway\_secret\_key](#input\_scaleway\_secret\_key) | Scaleway secret key (https://console.scaleway.com/iam/api-keys) | `string` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_public_ip"></a> [public\_ip](#output\_public\_ip) | Publicly reachable IP (with `ssh root@<public_ip>`) |
<!-- END_TF_DOCS -->