resource "scaleway_instance_ip" "main" {
  type = "routed_ipv4"
}

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
  type              = var.environment == "prod" ? "POP2-HC-8C-16G" : "GP1-XS"
  image             = "docker"
  ip_id             = scaleway_instance_ip.main.id
  security_group_id = scaleway_instance_security_group.main.id

  root_volume {
    size_in_gb            = var.environment == "prod" ? 200 : 100
    delete_on_termination = false
  }
}

resource "random_pet" "datalake_bucket_suffix" {}

resource "scaleway_object_bucket" "main" {
  name = "data-inclusion-datalake-${var.environment}-${random_pet.datalake_bucket_suffix.id}"

  lifecycle_rule {
    id      = "archive-raw-data-after-30-days"
    prefix  = "data/raw"
    enabled = true

    transition {
      days          = 30
      storage_class = "GLACIER"
    }
  }

  lifecycle_rule {
    id      = "archive-marts-data-after-7-days"
    prefix  = "data/marts"
    enabled = true

    transition {
      days          = 7
      storage_class = "GLACIER"
    }
  }

  lifecycle_rule {
    id      = "expire-marts-data-after-30-days"
    prefix  = "data/marts"
    enabled = true

    expiration {
      days = 30
    }
  }
}

data "scaleway_account_project" "main" {
  project_id = var.scaleway_project_id
}

data "scaleway_iam_group" "editors" {
  organization_id = data.scaleway_account_project.main.organization_id
  name            = "data-inclusion-terraform-editors"
}

resource "scaleway_object_bucket_policy" "main" {
  bucket = scaleway_object_bucket.main.name
  policy = jsonencode(
    {
      Version = "2023-04-17",
      Statement = [
        {
          Effect = "Allow",
          Sid    = "Grant list, read & write in data/* to airflow",
          Principal = {
            SCW = ["application_id:${var.airflow_application_id}"]
          },
          Action = [
            "s3:ListBucket",
            "s3:GetObject",
            "s3:PutObject"
          ],
          Resource = [
            "${scaleway_object_bucket.main.name}",
            "${scaleway_object_bucket.main.name}/data/*",
          ]
        },
        {
          Effect = "Allow",
          Sid    = "Grant list, read & write in tests/* to airflow",
          Principal = {
            SCW = ["application_id:${var.airflow_application_id}"]
          },
          Action = [
            "s3:ListBucket",
            "s3:GetObject",
            "s3:PutObject"
          ],
          Resource = [
            "${scaleway_object_bucket.main.name}",
            "${scaleway_object_bucket.main.name}/tests/*",
          ]
        },
        {
          Effect = "Allow",
          Sid    = "Grant list & read in sources/* to airflow",
          Principal = {
            SCW = ["application_id:${var.airflow_application_id}"]
          },
          Action = [
            "s3:ListBucket",
            "s3:GetObject"
          ],
          Resource = [
            "${scaleway_object_bucket.main.name}",
            "${scaleway_object_bucket.main.name}/sources/*",
          ]
        },
        {
          Effect = "Allow",
          Sid    = "Grant list & read in data/marts/* to the api",
          Principal = {
            SCW = ["application_id:${var.api_scw_application_id}"]
          },
          Action = [
            "s3:ListBucket",
            "s3:GetObject",
          ],
          Resource = [
            "${scaleway_object_bucket.main.name}",
            "${scaleway_object_bucket.main.name}/data/marts/*",
          ]
        },
        {
          Effect = "Allow",
          Sid    = "Grant full access to apps and users that must manage this bucket",
          Principal = {
            SCW = concat(
              [for user_id in data.scaleway_iam_group.editors.user_ids : "user_id:${user_id}"],
              ["application_id:${var.scaleway_application_id}"]
            )
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

locals {
  airflow_conn_pg = format(
    "postgresql://%s:%s@%s:%s/%s",
    var.datawarehouse_di_username,
    var.datawarehouse_di_password,
    "datawarehouse",
    "5432",
    var.datawarehouse_di_database
  )

  s3_endpoint = replace(scaleway_object_bucket.main.endpoint, "${scaleway_object_bucket.main.name}.", "")

  airflow_conn_s3 = format(
    "aws://@/%s?endpoint_url=%s&region_name=%s&aws_access_key_id=%s&aws_secret_access_key=%s",
    scaleway_object_bucket.main.name,
    urlencode(local.s3_endpoint),
    scaleway_object_bucket.main.region,
    var.airflow_access_key,
    var.airflow_secret_key
  )

  base_hostname = "${var.dns_subdomain != "" ? "${var.dns_subdomain}." : ""}${var.dns_zone}"

  airflow_hostname = "airflow.${local.base_hostname}"

  work_dir = "/root/data-inclusion"
}

resource "scaleway_domain_record" "dns" {
  for_each = toset(
    [
      "",
      local.airflow_hostname
    ]
  )

  dns_zone = var.dns_zone
  name     = replace(each.key, ".${var.dns_zone}", "")
  type     = "A"
  data     = scaleway_instance_server.main.public_ips[0].address
  ttl      = 3600
}

resource "null_resource" "up" {
  triggers = {
    always_run = timestamp()
  }

  connection {
    type        = "ssh"
    user        = "root"
    host        = scaleway_instance_server.main.public_ips[0].address
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
    STACK_VERSION='${var.stack_version}'
    AIRFLOW_HOSTNAME='${local.airflow_hostname}'

    # Datawarehouse
    DATAWAREHOUSE_DI_DATABASE='${var.datawarehouse_di_database}'
    DATAWAREHOUSE_DI_PASSWORD='${var.datawarehouse_di_password}'
    DATAWAREHOUSE_DI_USERNAME='${var.datawarehouse_di_username}'

    # Airflow settings
    AIRFLOW_WWW_USER_PASSWORD='${var.airflow_admin_password}'
    AIRFLOW__CORE__FERNET_KEY='${var.airflow__core__fernet_key}'
    AIRFLOW__SENTRY__SENTRY_DSN='${var.airflow__sentry__sentry_dsn}'
    AIRFLOW__SENTRY__RELEASE='${var.stack_version}'
    AIRFLOW__WEBSERVER__BASE_URL='https://${local.airflow_hostname}'

    # Airflow connections
    AIRFLOW_CONN_MATTERMOST='${var.airflow_conn_mattermost}'
    AIRFLOW_CONN_PG_API='${var.airflow_conn_pg_api}'
    AIRFLOW_CONN_PG='${local.airflow_conn_pg}'
    AIRFLOW_CONN_S3_SOURCES='${var.airflow_conn_s3_sources}'
    AIRFLOW_CONN_S3='${local.airflow_conn_s3}'
    AIRFLOW_CONN_SSH_API='${var.airflow_conn_ssh_api}'

    # Airflow variables
    AIRFLOW_VAR_BREVO_API_KEY='${var.brevo_api_key}'
    AIRFLOW_VAR_CARIF_OREF_URL='${var.carif_oref_url}'
    AIRFLOW_VAR_DATA_INCLUSION_API_PROBE_TOKEN='${var.data_inclusion_api_probe_token}'
    AIRFLOW_VAR_DATAGOUV_API_KEY='${var.datagouv_api_key}'
    AIRFLOW_VAR_DORA_API_TOKEN='${var.dora_api_token}'
    AIRFLOW_VAR_DORA_API_URL='${var.dora_api_url}'
    AIRFLOW_VAR_EMPLOIS_API_TOKEN='${var.emplois_api_token}'
    AIRFLOW_VAR_ENVIRONMENT='${var.environment}'
    AIRFLOW_VAR_FREDO_API_TOKEN='${var.fredo_api_token}'
    AIRFLOW_VAR_MISSION_LOCALE_API_SECRET='${var.mission_locale_api_secret}'
    AIRFLOW_VAR_FT_API_TOKEN='${var.ft_api_token}'
    AIRFLOW_VAR_MES_AIDES_AIRTABLE_KEY='${var.mes_aides_airtable_key}'
    AIRFLOW_VAR_SOLIGUIDE_API_TOKEN='${var.soliguide_api_token}'
    AIRFLOW_VAR_TWOCAPTCHA_API_KEY='${var.twocaptcha_api_key}'
    EOT
    )
    destination = "${local.work_dir}/.env"
  }

  provisioner "file" {
    source      = "${path.root}/docker-compose.yml"
    destination = "${local.work_dir}/docker-compose.yml"
  }

  provisioner "file" {
    source      = "${path.root}/../pipeline/defaults.env"
    destination = "${local.work_dir}/defaults.env"
  }

  provisioner "file" {
    source      = "${path.root}/systemd/system/"
    destination = "/etc/systemd/system"
  }

  provisioner "remote-exec" {
    inline = [
      "systemctl enable cleanup.service",
      "systemctl enable cleanup.timer",
      "systemctl start cleanup.timer",
      "cd ${local.work_dir}",
      "docker compose --progress=plain up --pull=always --force-recreate --remove-orphans --wait --wait-timeout 1200 --quiet-pull --detach",
      # FIXME: ideally this file should be removed
      # "rm -f ${local.work_dir}/.env",
    ]
  }

}
