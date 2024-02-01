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
  inbound_rule {
    action = "accept"
    port   = 5432
  }
}

resource "scaleway_instance_server" "main" {
  type              = var.scaleway_instance_type
  image             = "docker"
  ip_id             = scaleway_instance_ip.main.id
  routed_ip_enabled = true
  security_group_id = scaleway_instance_security_group.main.id

  root_volume {
    delete_on_termination = false
  }
}

resource "random_pet" "datalake_bucket_suffix" {}

resource "scaleway_object_bucket" "main" {
  name = "data-inclusion-datalake-${var.environment}-${random_pet.datalake_bucket_suffix.id}"
}

data "scaleway_account_project" "main" {
  project_id = var.scaleway_project_id
}

data "scaleway_iam_group" "editors" {
  organization_id = data.scaleway_account_project.main.organization_id
  name            = "Editors"
}

resource "scaleway_object_bucket_policy" "main" {
  bucket = scaleway_object_bucket.main.name
  policy = jsonencode(
    {
      Version = "2023-04-17",
      Statement = [
        {
          Effect = "Allow",
          Principal = {
            SCW = ["application_id:${var.airflow_application_id}"]
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

  airflow_conn_s3 = format(
    "aws://@/%s?endpoint_url=%s&region_name=%s&aws_access_key_id=%s&aws_secret_access_key=%s",
    scaleway_object_bucket.main.name,
    urlencode(replace(scaleway_object_bucket.main.endpoint, "${scaleway_object_bucket.main.name}.", "")),
    scaleway_object_bucket.main.region,
    var.airflow_access_key,
    var.airflow_secret_key
  )

  base_hostname = "${var.dns_subdomain != "" ? "${var.dns_subdomain}." : ""}${var.dns_zone}"

  airflow_hostname  = "airflow.${local.base_hostname}"
  api_hostname      = "api.${local.base_hostname}"
  metabase_hostname = "metabase.${local.base_hostname}"

  work_dir = "/root/data-inclusion"
}

resource "scaleway_domain_record" "dns" {
  for_each = toset([local.airflow_hostname, local.api_hostname, local.metabase_hostname])

  dns_zone = var.dns_zone
  name     = replace(each.key, ".${var.dns_zone}", "")
  type     = "A"
  data     = scaleway_instance_server.main.public_ip
  ttl      = 60
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
    API_HOSTNAME=${local.api_hostname}
    API_SECRET_KEY=${var.api_secret_key}
    API_TOKEN_ENABLED=${var.api_token_enabled}

    METABASE_HOSTNAME=${local.metabase_hostname}
    METABASE_SECRET_KEY=${var.metabase_secret_key}

    # common configuration
    AIRFLOW__CORE__FERNET_KEY=${var.airflow__core__fernet_key}
    AIRFLOW_CONN_PG=${local.airflow_conn_pg}
    AIRFLOW_CONN_S3=${local.airflow_conn_s3}
    AIRFLOW_HOSTNAME=${local.airflow_hostname}
    AIRFLOW_WWW_USER_PASSWORD=${var.airflow_admin_password}
    DATAWAREHOUSE_DI_DATABASE=${var.datawarehouse_di_database}
    DATAWAREHOUSE_DI_PASSWORD=${var.datawarehouse_di_password}
    DATAWAREHOUSE_DI_USERNAME=${var.datawarehouse_di_username}
    STACK_VERSION=${var.stack_version}

    # pipeline secrets
    AIRFLOW_CONN_S3_SOURCES=${var.airflow_conn_s3_sources}
    AIRFLOW_VAR_BREVO_API_KEY=${var.brevo_api_key}
    AIRFLOW_VAR_DATAGOUV_API_KEY=${var.datagouv_api_key}
    AIRFLOW_VAR_DORA_API_TOKEN=${var.dora_api_token}
    AIRFLOW_VAR_FT_API_TOKEN=${var.ft_api_token}
    AIRFLOW_VAR_DORA_PREPROD_API_TOKEN=${var.dora_preprod_api_token}
    AIRFLOW_VAR_EMPLOIS_API_TOKEN=${var.emplois_api_token}
    AIRFLOW_VAR_GRIST_API_TOKEN=${var.grist_api_token}
    AIRFLOW_VAR_MES_AIDES_AIRTABLE_KEY=${var.mes_aides_airtable_key}
    AIRFLOW_VAR_SOLIGUIDE_API_TOKEN=${var.soliguide_api_token}

    # overrides
    AIRFLOW_VAR_DORA_API_URL=${var.dora_api_url}
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

  provisioner "remote-exec" {
    inline = [
      "cd ${local.work_dir}",
      "docker compose --progress=plain up --pull=always --force-recreate --remove-orphans --wait --wait-timeout 1200 --quiet-pull --detach",
      # FIXME: ideally this file should be removed
      # "rm -f ${local.work_dir}/.env",
    ]
  }
}
