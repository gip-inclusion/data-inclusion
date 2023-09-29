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
