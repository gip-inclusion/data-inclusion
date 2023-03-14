variable "scaleway_access_key" {
  description = "Scaleway access key (https://console.scaleway.com/iam/api-keys)"
  type        = string
}

variable "scaleway_secret_key" {
  description = "Scaleway secret key (https://console.scaleway.com/iam/api-keys)"
  type        = string
  sensitive   = true
}

variable "scaleway_project_id" {
  description = "Scaleway project id (https://console.scaleway.com/project/settings)"
  type        = string
}

module "stack_data" {
  source = "../../modules/stack_data"

  scaleway_access_key = var.scaleway_access_key
  scaleway_secret_key = var.scaleway_secret_key
  scaleway_project_id = var.scaleway_project_id
}

output "stack_data_public_ip" {
  value       = module.stack_data.public_ip
  description = "Publicly reachable IP (with `ssh root@<public_ip>`)"
}
