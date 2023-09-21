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

# TODO: the postgis extension must be enabled manually
# Until we figure out a good enough way of doing that,
# the resources linked to the managed database are commented out
# and the datawarehouse is deployed as a container on the VM
#
# resource "scaleway_rdb_instance" "main" {
#   name           = "datawarehouse"
#   node_type      = "DB-DEV-S"
#   engine         = "PostgreSQL-14"
#   is_ha_cluster  = false
#   disable_backup = true
#   user_name      = var.datawarehouse_admin_username
#   password       = var.datawarehouse_admin_password
# }
#
# resource "scaleway_rdb_acl" "main" {
#   instance_id = scaleway_rdb_instance.main.id
#   acl_rules {
#     ip = "${scaleway_instance_ip.main.address}/32"
#   }
# }
#
# resource "scaleway_rdb_user" "main" {
#   instance_id = scaleway_rdb_instance.main.id
#   name        = var.datawarehouse_di_username
#   password    = var.datawarehouse_di_password
#   is_admin    = false
# }
#
# resource "scaleway_rdb_database" "main" {
#   instance_id = scaleway_rdb_instance.main.id
#   name        = var.datawarehouse_di_database
# }
#
# resource "scaleway_rdb_privilege" "main" {
#   instance_id   = scaleway_rdb_instance.main.id
#   user_name     = scaleway_rdb_user.main.name
#   database_name = scaleway_rdb_database.main.name
#   permission    = "all"
# }

resource "scaleway_object_bucket" "main" {
  name = "data-inclusion-datalake-${var.environment}"
}

resource "scaleway_iam_application" "main" {
  organization_id = data.scaleway_account_project.main.organization_id
  name            = "${var.environment}--airflow--tf"
}

data "scaleway_account_project" "main" {
  project_id = var.scaleway_project_id
}

data "scaleway_iam_group" "editors" {
  organization_id = data.scaleway_account_project.main.organization_id
  name            = "Editors"
}

resource "scaleway_object_bucket_policy" "main" {
  # TODO: find a way to retrieve the user/app associated to the access key
  count = 0 # disable resource

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

locals {
  airflow_conn_pg = "postgresql://${var.datawarehouse_di_username}:${var.datawarehouse_di_password}@datawarehouse:5432/${var.datawarehouse_di_database}"
  airflow_conn_s3 = "aws://@/${scaleway_object_bucket.main.name}?endpoint_url=${scaleway_object_bucket.main.endpoint}&region_name=${scaleway_object_bucket.main.region}&aws_access_key_id=${scaleway_iam_api_key.main.access_key}&aws_secret_access_key=${scaleway_iam_api_key.main.secret_key}"
}

resource "null_resource" "up" {
  triggers = {
    always_run = timestamp()
  }

  connection {
    type        = "ssh"
    user        = "root"
    host        = scaleway_instance_server.main.public_ip
    private_key = var.ssh_private_key
  }

  provisioner "file" {
    content = sensitive(<<-EOT
    API_VERSION=${var.api_version}
    AIRFLOW_CONN_PG=${local.airflow_conn_pg}
    AIRFLOW_CONN_S3=${local.airflow_conn_s3}
    AIRFLOW_WWW_USER_PASSWORD=${var.airflow_admin_password}
    DATAWAREHOUSE_DI_DATABASE=${var.datawarehouse_di_database}
    DATAWAREHOUSE_DI_USERNAME=${var.datawarehouse_di_username}
    DATAWAREHOUSE_DI_PASSWORD=${var.datawarehouse_di_password}
    API_SECRET_KEY=${var.api_secret_key}
    BAN_API_URL=https://api-adresse.data.gouv.fr
    DORA_API_URL=https://api.dora.incubateur.net/api/v2/
    IGN_ADMIN_EXPRESS_FILE_URL=http://files.opendatarchives.fr/professionnels.ign.fr/adminexpress/ADMIN-EXPRESS-COG_3-0__SHP__FRA_WM_2021-05-19.7z
    INSEE_FIRSTNAME_FILE_URL=https://www.insee.fr/fr/statistiques/fichier/2540004/nat2021_csv.zip
    INSEE_COG_DATASET_URL=https://www.insee.fr/fr/statistiques/fichier/6800675
    SIRENE_STOCK_ETAB_GEOCODE_FILE_URL=https://data.cquest.org/geo_sirene/v2019/last/StockEtablissementActif_utf8_geo.csv.gz
    SIRENE_STOCK_ETAB_HIST_FILE_URL=https://www.data.gouv.fr/fr/datasets/r/88fbb6b4-0320-443e-b739-b4376a012c32
    SIRENE_STOCK_ETAB_LIENS_SUCCESSION_URL=https://www.data.gouv.fr/fr/datasets/r/9c4d5d9c-4bbb-4b9c-837a-6155cb589e26
    SIRENE_STOCK_UNITE_LEGALE_FILE_URL=https://www.data.gouv.fr/fr/datasets/r/825f4199-cadd-486c-ac46-a65a8ea1a047
    EOT
    )
    destination = ".env"
  }

  provisioner "remote-exec" {
    inline = [
      "rm -rf data-inclusion",
      "git clone https://github.com/betagouv/data-inclusion 2>&1 | cat",
      "cd data-inclusion",
      "git checkout vmttn/feat/provision-terraform-scaleway", # TODO: use the commit sha
      "docker compose -f deployment/docker/docker-compose.yml --env-file ../.env up --quiet-pull -d 2>&1 | cat"
    ]
  }
}
