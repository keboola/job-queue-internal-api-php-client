locals {
  aws_region = "eu-central-1"
}

provider "aws" {
  region              = local.aws_region
  allowed_account_ids = ["025303414634"]

  default_tags {
    tags = {
      KebolaStack = "${var.name_prefix}-job-queue-internal-api-php-client"
      KeboolaRole = "job-queue-internal-api-php-client"
    }
  }
}

// user
resource "aws_iam_user" "job_queue_internal_api_php_client" {
  name = "${var.name_prefix}-job-queue-internal-api-php-client"
}

resource "aws_iam_access_key" "job_queue_internal_api_php_client" {
  user = aws_iam_user.job_queue_internal_api_php_client.name
}

// KMS
resource "aws_kms_key" "job_queue_internal_api_php_client" {
  description             = "Job Queue Internal API PHP Client Encryption Key"
  deletion_window_in_days = 10
}

resource "aws_iam_user_policy" "job_queue_internal_api_php_client_kms_access" {
  user        = aws_iam_user.job_queue_internal_api_php_client.name
  name_prefix = "kms_access_"

  policy = jsonencode({
    "Version" = "2012-10-17",
    "Statement" = [
      {
        "Sid"    = "UseKMS",
        "Effect" = "Allow",
        "Action" = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey",
        ],
        "Resource" = aws_kms_key.job_queue_internal_api_php_client.arn,
      },
    ]
  })
}

// ==== outputs ====
output "aws_region" {
  value = local.aws_region
}

output "aws_kms_key_id" {
  value = aws_kms_key.job_queue_internal_api_php_client.id
}

output "aws_access_key_id" {
  value = aws_iam_access_key.job_queue_internal_api_php_client.id
}

output "aws_access_key_secret" {
  value     = aws_iam_access_key.job_queue_internal_api_php_client.secret
  sensitive = true
}
