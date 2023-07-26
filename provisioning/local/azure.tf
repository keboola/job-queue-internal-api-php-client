locals {
  azure_tenant_id       = "9b85ee6f-4fb0-4a46-8cb7-4dcc6b262a89" // Keboola
  azure_subscription_id = "c5182964-8dca-42c8-a77a-fa2a3c6946ea" // Keboola DEV Platform Services Team
  azure_location        = "West Europe"
}

provider "azurerm" {
  tenant_id       = local.azure_tenant_id
  subscription_id = local.azure_subscription_id
  features {}
}

provider "azuread" {
  tenant_id = local.azure_tenant_id
}

data "azurerm_client_config" "current" {}
data "azuread_client_config" "current" {}

// service principal
resource "azuread_application" "job_queue_internal_api_php_client" {
  display_name = "${var.name_prefix}-job-queue-internal-api-php-client"
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal" "job_queue_internal_api_php_client" {
  application_id = azuread_application.job_queue_internal_api_php_client.application_id
  owners         = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal_password" "job_queue_internal_api_php_client" {
  service_principal_id = azuread_service_principal.job_queue_internal_api_php_client.id
}

// resource group
resource "azurerm_resource_group" "job_queue_internal_api_php_client" {
  name     = "${var.name_prefix}-job-queue-internal-api-php-client"
  location = local.azure_location
}

resource "azurerm_role_assignment" "job_queue_daemon" {
  scope                = azurerm_resource_group.job_queue_internal_api_php_client.id
  principal_id         = azuread_service_principal.job_queue_internal_api_php_client.id
  role_definition_name = "Contributor"
}

// key vault
resource "azurerm_key_vault" "job_queue_internal_api_php_client" {
  name                = "${var.name_prefix}-jqiapc" # max 24 chars
  tenant_id           = data.azurerm_client_config.current.tenant_id
  resource_group_name = azurerm_resource_group.job_queue_internal_api_php_client.name
  location            = azurerm_resource_group.job_queue_internal_api_php_client.location
  sku_name            = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = azuread_service_principal.job_queue_internal_api_php_client.id

    key_permissions = [
      "Decrypt",
      "Encrypt",
      "Get",
      "List",
      "Create"
    ]

    secret_permissions = [
      "Get",
      "List",
      "Set"
    ]
  }
}

// ==== outputs ====
output "azure_tenant_id" {
  value = local.azure_tenant_id
}

output "azure_application_id" {
  value = azuread_application.job_queue_internal_api_php_client.application_id
}

output "azure_application_secret" {
  value     = azuread_service_principal_password.job_queue_internal_api_php_client.value
  sensitive = true
}

output "azure_key_vault_url" {
  value = azurerm_key_vault.job_queue_internal_api_php_client.vault_uri
}
