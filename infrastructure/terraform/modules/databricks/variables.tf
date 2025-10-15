# ============================================================================
# Databricks Module Variables
# ============================================================================

variable "databricks_catalog" {
  description = "Name of the Unity Catalog to create in Databricks"
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z0-9_]+$", var.databricks_catalog))
    error_message = "Catalog name must contain only alphanumeric characters and underscores."
  }
}

variable "databricks_schemas" {
  description = "List of schema names to create within the Databricks catalog"
  type        = list(string)
  default     = []

  validation {
    condition     = length(var.databricks_schemas) > 0
    error_message = "At least one schema must be specified."
  }
}

# ============================================================================
# Azure Integration Variables
# ============================================================================

variable "access_connector_id" {
  description = "The resource ID of the Azure Databricks access connector for managed identity authentication"
  type        = string
}

variable "databricks_workspace_id" {
  description = "The numeric workspace ID of the Databricks workspace"
  type        = string
}

# ============================================================================
# Service Principal Variables (Legacy - kept for compatibility)
# ============================================================================

variable "azure_client_id" {
  description = "The application (client) ID of the Azure AD service principal"
  type        = string
  default     = ""
}

variable "azure_client_secret" {
  description = "The client secret of the Azure AD service principal"
  type        = string
  sensitive   = true
  default     = ""
}

variable "azure_tenant_id" {
  description = "The directory (tenant) ID of the Azure AD tenant"
  type        = string
  default     = ""
}


