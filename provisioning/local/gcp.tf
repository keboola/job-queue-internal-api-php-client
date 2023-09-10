locals {
  gcp_project = "kbc-dev-platform-services"
  gcp_region  = "europe-west3"
}

provider "google" {
  project = local.gcp_project
  region  = local.gcp_region
}

resource "google_kms_key_ring" "object_encryptor_keyring" {
  name     = "${var.name_prefix}-job-queue-internal-api-php-client"
  location = "europe-west1"
}

resource "google_kms_crypto_key" "object_encryptor_key" {
  name     = "${var.name_prefix}-job-queue-internal-api-php-client"
  key_ring = google_kms_key_ring.object_encryptor_keyring.id
  purpose  = "ENCRYPT_DECRYPT"

  lifecycle {
    prevent_destroy = false
  }
}

resource "google_service_account" "object_encryptor_service_account" {
  account_id   = substr("${var.name_prefix}-job-queue-internal-api-php-client", 0, 28)
  display_name = "${var.name_prefix} Job Queue Internal API PHP Client"
}

resource "google_kms_crypto_key_iam_binding" "object_encryptor_iam" {
  crypto_key_id = google_kms_crypto_key.object_encryptor_key.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"

  members = [
    google_service_account.object_encryptor_service_account.member,
  ]
}

resource "google_service_account_key" "object_encryptor_key" {
  service_account_id = google_service_account.object_encryptor_service_account.name
  public_key_type    = "TYPE_X509_PEM_FILE"
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}

output "gcp_private_key" {
  value     = google_service_account_key.object_encryptor_key.private_key
  sensitive = true
}

output "gcp_kms_key_id" {
  value = google_kms_crypto_key.object_encryptor_key.id
}
