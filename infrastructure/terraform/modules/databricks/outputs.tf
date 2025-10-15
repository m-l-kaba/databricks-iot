# ============================================================================
# Databricks Module Outputs
# ============================================================================

output "catalog_name" {
  description = "Name of the created Unity Catalog"
  value       = databricks_catalog.catalog.name
}

output "catalog_id" {
  description = "ID of the created Unity Catalog"
  value       = databricks_catalog.catalog.id
}

output "storage_credential_name" {
  description = "Name of the storage credential"
  value       = databricks_storage_credential.storage_credential.name
}

output "storage_credential_id" {
  description = "ID of the storage credential"
  value       = databricks_storage_credential.storage_credential.id
}

output "external_location_name" {
  description = "Name of the external location"
  value       = databricks_external_location.external_location.name
}

output "external_location_url" {
  description = "URL of the external location"
  value       = databricks_external_location.external_location.url
}

output "schema_names" {
  description = "List of created schema names"
  value       = [for schema in databricks_schema.schema : schema.name]
}

output "schema_full_names" {
  description = "List of fully qualified schema names (catalog.schema)"
  value       = [for schema in databricks_schema.schema : "${databricks_catalog.catalog.name}.${schema.name}"]
}