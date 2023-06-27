resource "scaleway_instance_ip" "main" {}

resource "scaleway_instance_server" "main" {
  type  = "DEV1-L"
  image = "docker"
  ip_id = scaleway_instance_ip.main.id
}

resource "scaleway_rdb_instance" "main" {
  name           = "datawarehouse"
  node_type      = "DB-DEV-S"
  engine         = "PostgreSQL-14"
  is_ha_cluster  = false
  disable_backup = true
  user_name      = var.datawarehouse_admin_username
  password       = var.datawarehouse_admin_password
}

resource "scaleway_rdb_acl" "main" {
  instance_id = scaleway_rdb_instance.main.id
  acl_rules {
    ip = "${scaleway_instance_ip.main.address}/32"
  }
}

resource "scaleway_rdb_user" "main" {
  instance_id = scaleway_rdb_instance.main.id
  name        = var.datawarehouse_di_username
  password    = var.datawarehouse_di_password
  is_admin    = false
}

resource "scaleway_rdb_database" "main" {
  instance_id = scaleway_rdb_instance.main.id
  name        = var.datawarehouse_di_database
}

resource "scaleway_rdb_privilege" "main" {
  instance_id   = scaleway_rdb_instance.main.id
  user_name     = scaleway_rdb_user.main.name
  database_name = scaleway_rdb_database.main.name
  permission    = "all"
}

resource "scaleway_object_bucket" "main" {
  name = "datalake"
}

resource "scaleway_iam_application" "main" {
  name = "airflow"
}

resource "scaleway_iam_policy" "object_full_access" {
  application_id = scaleway_iam_application.main.id
  rule {
    project_ids          = [var.scaleway_project_id]
    permission_set_names = ["ObjectStorageObjectsRead", "ObjectStorageObjectsWrite"]
  }
}

resource "scaleway_iam_api_key" "main" {
  application_id = scaleway_iam_application.main.id
}