resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_storage_account" "sa" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_account_replication_type
  is_hns_enabled           = true
}


resource "azurerm_storage_container" "catalog_storage" {
  name                  = "unity-storage"
  storage_account_id    = azurerm_storage_account.sa.id
  container_access_type = "private"
}

resource "azurerm_storage_data_lake_gen2_filesystem" "filesystem" {
  name               = "raw"
  storage_account_id = azurerm_storage_account.sa.id
  depends_on         = [azurerm_storage_account.sa]
}

resource "azurerm_storage_data_lake_gen2_path" "path" {
  for_each           = local.data_folders
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.filesystem.name
  storage_account_id = azurerm_storage_account.sa.id
  path               = each.value
  resource           = "directory"
}

resource "azurerm_databricks_workspace" "workspace" {
  name                = var.databricks_ws_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"
  custom_parameters {
    storage_account_sku_name = "Standard_LRS"
  }
  depends_on = [azurerm_storage_account.sa]
}


resource "azurerm_databricks_access_connector" "connector" {
  name                = "iot-access-connector"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "role_assignment" {
  principal_id         = azurerm_databricks_access_connector.connector.identity[0].principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope                = azurerm_storage_account.sa.id
}


locals {
  data_folders = toset(["telemetry", "failures", "maintenance", "device_master"])
}