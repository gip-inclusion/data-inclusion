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
  image             = "ubuntu_noble"
  ip_id             = scaleway_instance_ip.main.id
  security_group_id = scaleway_instance_security_group.main.id

  root_volume {
    size_in_gb            = 300
    delete_on_termination = false
  }

  cloud_init = <<-EOF
    #cloud-config
    package_update: true
    package_upgrade: true
    package_reboot_if_required: true
    packages:
      - apt-transport-https
      - ca-certificates
      - curl
      - gnupg
      - lsb-release

    runcmd:
      - install -m 0755 -d /etc/apt/keyrings
      - curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
      - chmod a+r /etc/apt/keyrings/docker.asc
      - echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
      - apt-get update
      - apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
      - curl -s https://raw.githubusercontent.com/scaleway/scaleway-cli/master/scripts/get.sh | sh
  EOF
}

# TODO(vperron): some day, replace the bucket with a fixed name one
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

  lifecycle_rule {
    id      = "expire-api-data-after-1-day"
    prefix  = "data/api"
    enabled = true

    expiration {
      days = 1
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

data "scaleway_iam_group" "marts_consumers" {
  organization_id = data.scaleway_account_project.main.organization_id
  name            = "data-inclusion-${var.environment}-marts-consumers"
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
          Sid    = "Grant write in data/api/* to the api",
          Principal = {
            SCW = ["application_id:${var.api_scw_application_id}"]
          },
          Action = [
            "s3:PutObject",
          ],
          Resource = [
            "${scaleway_object_bucket.main.name}/data/api/*",
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
        },
        {
          Effect = "Allow",
          Sid    = "Grant list, read & write in data/marts/* to consumers",
          Principal = {
            SCW = [for app_id in data.scaleway_iam_group.marts_consumers.application_ids : "application_id:${app_id}"]
          },
          Action = [
            "s3:ListBucket",
            "s3:GetObject"
          ],
          Resource = [
            "${scaleway_object_bucket.main.name}",
            "${scaleway_object_bucket.main.name}/data/marts/*",
          ]
        }
      ]
    }
  )
}

locals {
  airflow_hostname = "airflow.${var.dns_zone}"
  work_dir         = "/root/data-inclusion"
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

resource "terraform_data" "up" {
  triggers_replace = timestamp()

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
    AIRFLOW__SENTRY__RELEASE='${var.stack_version}'
    AIRFLOW_HOSTNAME='${local.airflow_hostname}'
    AIRFLOW__API__BASE_URL='https://${local.airflow_hostname}'
    AIRFLOW_VAR_ENVIRONMENT='${var.environment}'
    EOT
    )
    destination = "${local.work_dir}/.env"
  }

  provisioner "file" {
    source      = "${path.root}/docker-compose.yml"
    destination = "${local.work_dir}/docker-compose.yml"
  }

  provisioner "file" {
    content = sensitive(<<-EOT
    access_key: ${var.app_secrets_access_key}
    secret_key: ${var.app_secrets_secret_key}
    default_organization_id: ${var.scaleway_organization_id}
    default_project_id: ${var.scaleway_project_id}
    default_region: fr-par
    default_zone: fr-par-1
    EOT
    )
    destination = "${path.root}/.config/scw/config.yaml"
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
      "scw secret version access-by-path secret-name=pipeline secret-path=/ revision=latest -o template=\"{{ printf \\\"%s\\\" .Data }}\" >> .env",
      "docker compose --progress=plain up --pull=always --force-recreate --remove-orphans --wait --wait-timeout 1200 --quiet-pull --detach",
      "shred -u --force --zero .env",
    ]
  }
}
