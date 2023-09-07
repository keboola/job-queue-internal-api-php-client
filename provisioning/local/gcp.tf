locals {
  gcp_project = "kbc-dev-platform-services"
  gcp_region  = "europe-west3"
}

provider "google" {
  project = local.gcp_project
  region  = local.gcp_region
}
