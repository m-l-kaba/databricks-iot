

module "azure_resources" {
  source               = "./modules/azure"
  resource_group_name  = "databricks-iot-rg"
  storage_account_name = "databricksiotsa"
  databricks_ws_name   = "databricks-iot-ws"
}

module "databricks_resources" {
  source                  = "./modules/databricks"
  databricks_catalog      = local.databricks_catalog
  databricks_schemas      = local.databricks_schemas
  access_connector_id     = module.azure_resources.access_connector_id
  databricks_workspace_id = module.azure_resources.databricks_workspace_id
  azure_client_id         = var.azure_client_id
  azure_client_secret     = var.azure_client_secret
  azure_tenant_id         = var.azure_tenant_id


  providers = {
    databricks.workspace = databricks.workspace
    databricks.admin     = databricks.admin
  }

  depends_on = [module.azure_resources]
}

locals {
  databricks_catalog = "production"
  databricks_schemas = ["raw", "bronze", "silver", "gold"]
}