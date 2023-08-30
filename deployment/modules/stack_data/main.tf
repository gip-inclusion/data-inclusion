resource "scaleway_instance_ip" "main" {}

resource "scaleway_instance_security_group" "main" {
  inbound_default_policy  = "drop"
  outbound_default_policy = "accept"
  stateful                = true

  inbound_rule {
    action = "accept"
    port   = 22
  }
  inbound_rule {
    action = "accept"
    port   = 80
  }
  inbound_rule {
    action = "accept"
    port   = 443
  }
}

resource "scaleway_instance_server" "main" {
  type              = "DEV1-L"
  image             = "docker"
  ip_id             = scaleway_instance_ip.main.id
  security_group_id = scaleway_instance_security_group.main.id
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
  name = "data-inclusion-datalake-${var.environment_name}"
}

resource "scaleway_iam_application" "main" {
  organization_id = data.scaleway_account_project.main.organization_id
  name            = "${var.environment_name}--airflow--tf"
}

data "scaleway_account_project" "main" {
  project_id = var.scaleway_project_id
}

data "scaleway_iam_group" "editors" {
  organization_id = data.scaleway_account_project.main.organization_id
  name            = "Editors"
}

resource "scaleway_object_bucket_policy" "main" {
  # disable resource. TODO: find a way to retrieve the user/app associated to the access key
  count = 0

  bucket = scaleway_object_bucket.main.name
  policy = jsonencode(
    {
      Version = "2023-04-17",
      Statement = [
        {
          Effect = "Allow",
          Principal = {
            SCW = concat(
              [for user_id in data.scaleway_iam_group.editors.user_ids : "user_id:${user_id}"],
              [
                "application_id:${scaleway_iam_application.main.id}"
              ]
            )
          },
          Action = [
            "s3:GetObject",
            "s3:PutObject"
          ],
          Resource = [
            "${scaleway_object_bucket.main.name}/data/*",
          ]
        },
        {
          Effect = "Allow",
          Principal = {
            SCW = concat(
              [for user_id in data.scaleway_iam_group.editors.user_ids : "user_id:${user_id}"],
              [
                "application_id:${scaleway_iam_application.main.id}"
              ]
            )
          },
          Action = [
            "s3:GetObject"
          ],
          Resource = [
            "${scaleway_object_bucket.main.name}/sources/*"
          ]
        },
        {
          Effect = "Allow",
          Principal = {
            SCW = [for user_id in data.scaleway_iam_group.editors.user_ids : "user_id:${user_id}"]
          },
          Action = "*",
          Resource = [
            "${scaleway_object_bucket.main.name}",
            "${scaleway_object_bucket.main.name}/*"
          ]
        }
      ]
    }
  )
}

resource "time_rotating" "api_key_rotation" {
  rfc3339        = "2024-06-01T00:00:00Z"
  rotation_years = 1
}

resource "scaleway_iam_api_key" "main" {
  application_id = scaleway_iam_application.main.id
  expires_at     = time_rotating.api_key_rotation.id
}