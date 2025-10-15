output "databricks_workspace_id" {
  description = "The numeric ID of the Databricks workspace."
  value       = azurerm_databricks_workspace.workspace.id
}

output "databricks_workspace_numeric_id" {
  description = "The numeric ID of the Databricks workspace."
  value       = azurerm_databricks_workspace.workspace.workspace_id
}

output "databricks_workspace_url" {
  description = "The URL of the Databricks workspace."
  value       = azurerm_databricks_workspace.workspace.workspace_url
}

output "location" {
  description = "The location of the resource group."
  value       = azurerm_resource_group.rg.location
}

output "storage_account_name" {
  description = "The name of the storage account."
  value       = azurerm_storage_account.sa.name
}

output "access_connector_id" {
  description = "The ID of the Databricks access connector."
  value       = azurerm_databricks_access_connector.connector.id
}