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