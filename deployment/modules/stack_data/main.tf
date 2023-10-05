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

resource "scaleway_object_bucket" "main" {
  name = "data-inclusion-datalake-${var.environment}"
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
                "application_id:${var.airflow_application_id}"
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
                "application_id:${var.airflow_application_id}"
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

resource "scaleway_domain_record" "dns" {
  for_each = toset(["airflow", "api"])

  dns_zone = var.public_hostname
  name     = each.key
  type     = "A"
  data     = scaleway_instance_server.main.public_ip
  ttl      = 60
}

locals {
  airflow_conn_pg = format(
    "postgresql://%s:%s@%s:%s/%s",
    var.datawarehouse_di_username,
    var.datawarehouse_di_password,
    "datawarehouse",
    "5432",
    var.datawarehouse_di_database
  )

  airflow_conn_s3 = format(
    "aws://@/%s?endpoint_url=%s&region_name=%s&aws_access_key_id=%s&aws_secret_access_key=%s",
    scaleway_object_bucket.main.name,
    urlencode(replace(scaleway_object_bucket.main.endpoint, "${scaleway_object_bucket.main.name}.", "")),
    scaleway_object_bucket.main.region,
    var.airflow_access_key,
    var.airflow_secret_key
  )

  work_dir = "/root/data-inclusion"
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

  provisioner "remote-exec" {
    inline = [
      "rm -rf ${local.work_dir}",
      "mkdir -p ${local.work_dir}",
    ]
  }

  provisioner "file" {
    content = sensitive(<<-EOT
    STACK_VERSION=${var.stack_version}
    AIRFLOW_CONN_PG=${local.airflow_conn_pg}
    AIRFLOW_CONN_S3=${local.airflow_conn_s3}
    AIRFLOW__CORE__FERNET_KEY=${var.airflow__core__fernet_key}
    AIRFLOW_WWW_USER_PASSWORD=${var.airflow_admin_password}
    DATAWAREHOUSE_DI_DATABASE=${var.datawarehouse_di_database}
    DATAWAREHOUSE_DI_USERNAME=${var.datawarehouse_di_username}
    DATAWAREHOUSE_DI_PASSWORD=${var.datawarehouse_di_password}
    API_SECRET_KEY=${var.api_secret_key}
    BAN_API_URL=https://api-adresse.data.gouv.fr
    DORA_API_URL=https://api.dora.incubateur.net/api/v2/
    DORA_API_TOKEN=${var.dora_api_token}
    IGN_ADMIN_EXPRESS_FILE_URL=http://files.opendatarchives.fr/professionnels.ign.fr/adminexpress/ADMIN-EXPRESS-COG_3-0__SHP__FRA_WM_2021-05-19.7z
    INSEE_FIRSTNAME_FILE_URL=https://www.insee.fr/fr/statistiques/fichier/2540004/nat2021_csv.zip
    INSEE_COG_DATASET_URL=https://www.insee.fr/fr/statistiques/fichier/6800675
    SIRENE_STOCK_ETAB_GEOCODE_FILE_URL=https://data.cquest.org/geo_sirene/v2019/last/StockEtablissementActif_utf8_geo.csv.gz
    SIRENE_STOCK_ETAB_HIST_FILE_URL=https://www.data.gouv.fr/fr/datasets/r/88fbb6b4-0320-443e-b739-b4376a012c32
    SIRENE_STOCK_ETAB_LIENS_SUCCESSION_URL=https://www.data.gouv.fr/fr/datasets/r/9c4d5d9c-4bbb-4b9c-837a-6155cb589e26
    SIRENE_STOCK_UNITE_LEGALE_FILE_URL=https://www.data.gouv.fr/fr/datasets/r/825f4199-cadd-486c-ac46-a65a8ea1a047
    UN_JEUNE_UNE_SOLUTION_API_URL=https://mes-aides.1jeune1solution.beta.gouv.fr/api/
    PUBLIC_HOSTNAME=${var.public_hostname}
    API_TOKEN_ENABLED=${var.api_token_enabled}
    EOT
    )
    destination = "${local.work_dir}/.env"
  }

  provisioner "file" {
    source      = "${path.module}/docker-compose.yml"
    destination = "${local.work_dir}/docker-compose.yml"
  }

  provisioner "remote-exec" {
    inline = [
      "cd ${local.work_dir}",
      "docker compose --progress=plain up --pull=always --force-recreate --wait --wait-timeout 1200 --quiet-pull --detach",
      # FIXME: ideally this file should be removed
      # "rm -f ${local.work_dir}/.env",
    ]
  }
}
