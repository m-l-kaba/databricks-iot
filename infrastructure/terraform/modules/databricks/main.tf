
# ============================================================================
# Databricks Unity Catalog Resources
# ============================================================================

# Storage credential for Azure Data Lake access using managed identity
resource "databricks_storage_credential" "storage_credential" {
  name     = "iot-sc"
  provider = databricks.workspace

  azure_managed_identity {
    access_connector_id = "/subscriptions/ecb989f5-517b-4269-8395-1da237c7293c/resourceGroups/databricks-iot-rg/providers/Microsoft.Databricks/accessConnectors/iot-access-connector"
  }
}

# External location pointing to the Unity Catalog storage root
resource "databricks_external_location" "external_location" {
  name            = "iot-el"
  url             = "abfss://unity-storage@databricksiotsa.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.storage_credential.name
  provider        = databricks.workspace

  depends_on = [databricks_storage_credential.storage_credential]
}

resource "databricks_external_location" "external_location_raw" {
  name            = "iot-raw-el"
  url             = "abfss://raw@databricksiotsa.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.storage_credential.name
  provider        = databricks.workspace

  depends_on = [databricks_storage_credential.storage_credential]
}

# Unity Catalog for IoT data management
resource "databricks_catalog" "catalog" {
  name         = var.databricks_catalog
  provider     = databricks.workspace
  storage_root = "abfss://unity-storage@databricksiotsa.dfs.core.windows.net"

  depends_on = [
    databricks_storage_credential.storage_credential,
    databricks_external_location.external_location
  ]
}

# Schemas within the IoT catalog
resource "databricks_schema" "schema" {
  for_each     = toset(var.databricks_schemas)
  name         = each.value
  catalog_name = databricks_catalog.catalog.name
  provider     = databricks.workspace

  depends_on = [databricks_catalog.catalog]
}

resource "databricks_volume" "landing_volume" {
  name             = "landing-volume"
  catalog_name     = databricks_catalog.catalog.name
  schema_name      = "raw"
  volume_type      = "EXTERNAL"
  storage_location = databricks_external_location.external_location_raw.url
  depends_on       = [databricks_schema.schema, databricks_external_location.external_location_raw]
  provider         = databricks.workspace
}