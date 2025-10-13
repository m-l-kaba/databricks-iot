resource "azurerm_resource_group" "rg" {
  name     = "databricks-iot-rg"
  location = "Germany West Central"
}

resource "azurerm_storage_account" "sa" {
  name                     = "databricksiotstorage"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}