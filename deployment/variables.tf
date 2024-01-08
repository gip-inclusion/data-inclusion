variable "airflow__core__fernet_key" {
  description = "Secret key to save connection passwords in the db"
  type        = string
  sensitive   = true
}

variable "airflow_access_key" {
  description = "Scaleway access key for Airflow"
  type        = string
  sensitive   = false
}

variable "airflow_admin_password" {
  description = "Password for airflow admin panel"
  type        = string
  sensitive   = true
}

variable "airflow_application_id" {
  description = "Scaleway app ID for Airflow"
  type        = string
  sensitive   = false
}

variable "airflow_conn_s3_sources" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
}

variable "airflow_secret_key" {
  description = "Scaleway secret key for Airflow"
  type        = string
  sensitive   = true
}

variable "api_secret_key" {
  description = "Secret key used for cryptographic signing by the api"
  type        = string
  sensitive   = true
}

variable "api_token_enabled" {
  description = "Whether to enable the api token auth or not"
  type        = string
}

variable "datagouv_api_key" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
}

variable "brevo_api_key" {
  description = "Used in notifications for RGPD users"
  type        = string
  sensitive   = true
  default     = ""
}

variable "datawarehouse_admin_password" {
  description = "Password for the first user of the postgres datawarehouse"
  type        = string
  sensitive   = true
}

variable "datawarehouse_admin_username" {
  description = "Identifier for the first user of the postgres datawarehouse"
  type        = string
}

variable "datawarehouse_di_database" {
  description = "Identifier for the data inclusion database"
  type        = string
}

variable "datawarehouse_di_password" {
  description = "Password for the main user of the postgres datawarehouse"
  type        = string
  sensitive   = true
}

variable "datawarehouse_di_username" {
  description = "Identifier for the main user of the postgres datawarehouse"
  type        = string
}

variable "dns_subdomain" {
  description = "DNS subdomain where the public hostnames will be created within dns_zone (optional)"
  type        = string
  default     = ""
}

variable "dns_zone" {
  description = "DNS zone where the public hostnames will be created"
  type        = string
}

variable "dora_api_token" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
}

variable "dora_preprod_api_token" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
}

variable "dora_api_url" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
}

variable "ft_api_token" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
}

variable "emplois_api_token" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
}

variable "environment" {
  description = "Identifier of the target environment"
  type        = string
}

variable "grist_api_token" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
}

variable "mes_aides_airtable_key" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
}

variable "metabase_secret_key" {
  description = "Secret key to save connection passwords in the db"
  type        = string
  sensitive   = true
}

variable "scaleway_access_key" {
  description = "Scaleway access key (https://console.scaleway.com/iam/api-keys)"
  type        = string
  sensitive   = true
}

variable "scaleway_application_id" {
  description = "ID of the application owning the api keys"
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

variable "scaleway_instance_type" {
  description = "Scaleway instance type (ex. GP1-XS, see https://www.scaleway.com/en/pricing/?tags=compute)"
  type        = string
  default     = "GP1-XS"
}

variable "soliguide_api_token" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
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
