resource "scaleway_instance_ip" "main" {}

resource "scaleway_instance_server" "main" {
  type  = "DEV1-L"
  image = "docker"
  ip_id = scaleway_instance_ip.main.id
}