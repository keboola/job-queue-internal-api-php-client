#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ./functions.sh

# AWS
output_var 'TEST_KMS_REGION' "$(terraform_output 'aws_region')"
output_var 'TEST_KMS_KEY_ID' "$(terraform_output 'aws_kms_key_id')"
output_var 'TEST_AWS_ACCESS_KEY_ID' "$(terraform_output 'aws_access_key_id')"
output_var 'TEST_AWS_SECRET_ACCESS_KEY' "$(terraform_output 'aws_access_key_secret')"
echo ""

# Azure
output_var 'TEST_AZURE_CLIENT_ID' "$(terraform_output 'azure_application_id')"
output_var 'TEST_AZURE_CLIENT_SECRET' "$(terraform_output 'azure_application_secret')"
output_var 'TEST_AZURE_TENANT_ID' "$(terraform_output 'azure_tenant_id')"
output_var 'TEST_AZURE_KEY_VAULT_URL' "$(terraform_output 'azure_key_vault_url')"
echo ""

# GCP
output_var 'TEST_GCP_KMS_KEY_ID' "$(terraform_output 'gcp_kms_key_id')"
PRIVATE_KEY_ENCODED="$(terraform_output 'gcp_private_key')"
PRIVATE_KEY=$(printf "%s" "$PRIVATE_KEY_ENCODED" | base64 --decode)

output_file 'var/gcp-private-key.json' "$PRIVATE_KEY"
output_var 'TEST_GOOGLE_APPLICATION_CREDENTIALS' "var/gcp-private-key.json"
echo ""
