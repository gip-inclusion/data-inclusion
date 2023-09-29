variable "scaleway_access_key" {
  description = "Scaleway access key (https://console.scaleway.com/iam/api-keys)"
  type        = string
  sensitive   = true
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

variable "airflow_application_id" {
  description = "Scaleway app ID for Airflow"
  type        = string
  sensitive   = false
}

variable "airflow_access_key" {
  description = "Scaleway access key for Airflow"
  type        = string
  sensitive   = false
}

variable "airflow_secret_key" {
  description = "Scaleway secret key for Airflow"
  type        = string
  sensitive   = true
}

variable "datawarehouse_admin_username" {
  description = "Identifier for the first user of the postgres datawarehouse"
  type        = string
}

variable "datawarehouse_admin_password" {
  description = "Password for the first user of the postgres datawarehouse"
  type        = string
  sensitive   = true
}

variable "datawarehouse_di_username" {
  description = "Identifier for the main user of the postgres datawarehouse"
  type        = string
}

variable "datawarehouse_di_password" {
  description = "Password for the main user of the postgres datawarehouse"
  type        = string
  sensitive   = true
}

variable "datawarehouse_di_database" {
  description = "Identifier for the data inclusion database"
  type        = string
}

variable "environment" {
  description = "Identifier of the target environment"
  type        = string
}

variable "airflow_admin_password" {
  description = "Password for airflow admin panel"
  type        = string
  sensitive   = true
}

variable "api_secret_key" {
  description = "Secret key used for cryptographic signing by the api"
  type        = string
  sensitive   = true
}

variable "api_version" {
  description = "Version (e.g. sha or semver) of the api to deploy"
  type        = string
}

variable "ssh_private_key" {
  description = "The associated public key will be deployed to the instance"
  type        = string
  sensitive   = true
}

variable "public_hostname" {
  description = "Public hostname that will be used to reach the instance"
  type        = string
}

variable "airflow__core__fernet_key" {
  description = "Secret key to save connection passwords in the db"
  type        = string
  sensitive   = true
}

variable "dora_api_token" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
}

module "stack_data" {
  source = "./modules/stack_data"

  scaleway_access_key          = var.scaleway_access_key
  scaleway_secret_key          = var.scaleway_secret_key
  scaleway_project_id          = var.scaleway_project_id
  airflow_application_id       = var.airflow_application_id
  airflow_access_key           = var.airflow_access_key
  airflow_secret_key           = var.airflow_secret_key
  datawarehouse_admin_username = var.datawarehouse_admin_username
  datawarehouse_admin_password = var.datawarehouse_admin_password
  datawarehouse_di_username    = var.datawarehouse_di_username
  datawarehouse_di_password    = var.datawarehouse_di_password
  datawarehouse_di_database    = var.datawarehouse_di_database
  environment                  = var.environment
  airflow_admin_password       = var.airflow_admin_password
  api_secret_key               = var.api_secret_key
  api_version                  = var.api_version
  ssh_private_key              = var.ssh_private_key
  public_hostname              = var.public_hostname
  airflow__core__fernet_key    = var.airflow__core__fernet_key
  dora_api_token               = var.dora_api_token
}

output "public_ip" {
  description = "Publicly reachable IP (with `ssh root@<public_ip>`)"
  value       = module.stack_data.public_ip
}

output "airflow_conn_pg" {
  description = "Connection string to the datawarehouse for airflow"
  value       = module.stack_data.airflow_conn_pg
  sensitive   = true
}

output "airflow_conn_s3" {
  description = "Connection string to the datalake for airflow"
  value       = module.stack_data.airflow_conn_s3
  sensitive   = true
}
