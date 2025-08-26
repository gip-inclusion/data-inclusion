variable "airflow_access_key" {
  description = "Scaleway access key for Airflow"
  type        = string
  sensitive   = false
}

variable "airflow_application_id" {
  description = "Scaleway app ID for Airflow"
  type        = string
  sensitive   = false
}

variable "api_scw_application_id" {
  description = "Used to manage access rights on the datalake"
  type        = string
  sensitive   = false
}

variable "dns_zone" {
  description = "DNS zone where the public hostnames will be created"
  type        = string
}

variable "environment" {
  description = "Identifier of the target environment"
  type        = string
}

variable "scaleway_access_key" {
  description = "the key having access to the terraform states"
  type        = string
}

variable "scaleway_secret_key" {
  description = "the secret key having access to the terraform states"
  type        = string
}

variable "scaleway_application_id" {
  description = "ID of the application owning the api keys"
  type        = string
}

variable "scaleway_project_id" {
  description = "Scaleway project id (https://console.scaleway.com/project/settings)"
  type        = string
}

variable "scaleway_organization_id" {
  description = "Scaleway organization id (https://console.scaleway.com/organization)"
  type        = string
}

variable "app_secrets_access_key" {
  description = "Access key for the app secrets"
  type        = string
}

variable "app_secrets_secret_key" {
  description = "Secret key for the app secrets"
  type        = string
  sensitive   = true
}

variable "ssh_private_key" {
  description = "The associated public key will be deployed to the instance"
  type        = string
  sensitive   = true
}

variable "stack_version" {
  description = "Version (e.g. sha or semver) of the stack services to deploy"
  type        = string
}
