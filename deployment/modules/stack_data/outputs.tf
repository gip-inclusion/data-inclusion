output "public_ip" {
  description = "Publicly reachable IP (with `ssh root@<public_ip>`)"
  value       = scaleway_instance_server.main.public_ip
}

output "airflow_conn_pg" {
  description = "Connection string to the datawarehouse for airflow"
  value       = local.airflow_conn_pg
  sensitive   = true
}

output "airflow_conn_s3" {
  description = "Connection string to the datalake for airflow"
  value       = local.airflow_conn_s3
  sensitive   = true
}
