output "public_ip" {
  description = "Publicly reachable IP (with `ssh root@<public_ip>`)"
  value       = scaleway_instance_server.main.public_ip
}

output "object_storage_access_key" {
  description = "Access key for the datalake object storage"
  value       = scaleway_iam_api_key.main.access_key
  sensitive   = true
}

output "object_storage_secret_key" {
  description = "Secret key for the datalake object storage"
  value       = scaleway_iam_api_key.main.secret_key
  sensitive   = true
}

output "airflow_conn_pg" {
  description = "Connection string to the datawarehouse for airflow"
  value       = "postgresql://${var.datawarehouse_di_username}:${var.datawarehouse_di_password}@${scaleway_instance_server.main.public_ip}/${var.datawarehouse_di_database}"
  sensitive   = true
}

output "airflow_conn_s3" {
  description = "Connection string to the datalake for airflow"
  value       = "aws://@/${scaleway_object_bucket.main.name}?endpoint_url=${scaleway_object_bucket.main.endpoint}&region_name=${scaleway_object_bucket.main.region}&aws_access_key_id=${scaleway_iam_api_key.main.access_key}&aws_secret_access_key=${scaleway_iam_api_key.main.secret_key}"
  sensitive   = true
}