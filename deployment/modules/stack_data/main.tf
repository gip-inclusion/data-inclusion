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

resource "scaleway_object_bucket_policy" "policy" {
  bucket = scaleway_object_bucket.main.name
  policy = jsonencode(
    {
      Version = "2023-04-17",
      Statement = [
        {
          Effect = "Allow",
          Principal = {
            SCW = "application_id:${scaleway_iam_application.main.name}"
          },
          Action = [
            "s3:GetObject",
            "s3:PutObject"
          ],
          Resources = [
            "${scaleway_object_bucket.main.name}/data/*",
          ]
        },
        {
          Effect = "Allow",
          Principal = {
            SCW = "application_id:${scaleway_iam_application.main.name}"
          },
          Action = [
            "s3:GetObject"
          ],
          Resources = [
            "${scaleway_object_bucket.main.name}/sources/*"
          ]
        }
      ]
    }
  )
}

resource "scaleway_iam_api_key" "main" {
  application_id = scaleway_iam_application.main.id
}