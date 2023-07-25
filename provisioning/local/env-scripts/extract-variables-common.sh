#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ./functions.sh

# output variables
output_var 'TEST_KMS_REGION' "$(terraform_output 'aws_region')"
output_var 'TEST_KMS_KEY_ID' "$(terraform_output 'aws_kms_key_id')"
output_var 'TEST_AWS_ACCESS_KEY_ID' "$(terraform_output 'aws_access_key_id')"
output_var 'TEST_AWS_SECRET_ACCESS_KEY' "$(terraform_output 'aws_access_key_secret')"
echo ""

output_var 'TEST_AZURE_CLIENT_ID' "$(terraform_output 'azure_application_id')"
output_var 'TEST_AZURE_CLIENT_SECRET' "$(terraform_output 'azure_application_secret')"
output_var 'TEST_AZURE_TENANT_ID' "$(terraform_output 'azure_tenant_id')"
output_var 'TEST_AZURE_KEY_VAULT_URL' "$(terraform_output 'azure_key_vault_url')"
echo ""
