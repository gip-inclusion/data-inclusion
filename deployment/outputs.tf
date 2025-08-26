output "public_ip" {
  description = "Publicly reachable IP (with `ssh root@<public_ip>`)"
  value       = scaleway_instance_server.main.public_ips[0].address
}
