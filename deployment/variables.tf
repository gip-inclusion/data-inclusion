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

variable "api_scw_application_id" {
  description = "Used to manage access rights on the datalake"
  type        = string
  sensitive   = false
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

variable "mes_aides_airtable_key" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
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

variable "airflow__sentry__sentry_dsn" {
  description = "Sentry DSN for airflow monitoring"
  type        = string
  default     = ""
}

variable "airflow_conn_pg_api" {
  description = "Postgres URI similar to the api scalingo app SCALINGO_POSTGRESQL_URL, but with a dedicated read-only credentials"
  type        = string
  sensitive   = true
  default     = ""
}

variable "airflow_conn_ssh_api" {
  description = "SSH connection string used to open a tunnel to scalingo. The associated private_key must have been uploaded to scalingo"
  type        = string
  sensitive   = true
  default     = ""
}

variable "airflow_conn_mattermost" {
  description = "Mattermost webhook used by airflow to notifications"
  type        = string
  sensitive   = true
  default     = ""
}

variable "siao_file_url" {
  description = "Public URL to the siao export on our s3 bucket"
  type        = string
  default     = ""
}

variable "twocaptcha_api_key" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
}

variable "fredo_api_token" {
  description = "Used in extraction tasks orchestrated by airflow"
  type        = string
  sensitive   = true
  default     = ""
}
