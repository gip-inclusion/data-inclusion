terraform {
  required_providers {
    scaleway = {
      source  = "scaleway/scaleway"
      version = "2.13.1"
    }
  }
}

provider "scaleway" {
  access_key = var.scaleway_access_key
  secret_key = var.scaleway_secret_key
  project_id = var.scaleway_project_id
  zone       = "fr-par-1"
  region     = "fr-par"
}