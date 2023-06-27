output "public_ip" {
  description = "Publicly reachable IP (with `ssh root@<public_ip>`)"
  value       = scaleway_instance_server.main.public_ip
}

output "object_storage_access_key" {
  description = "Access key for the datalake object storage"
  value       = scaleway_iam_api_key.main.access_key
}

output "object_storage_secret_key" {
  description = "Secret key for the datalake object storage"
  value       = scaleway_iam_api_key.main.secret_key
  sensitive   = true
}