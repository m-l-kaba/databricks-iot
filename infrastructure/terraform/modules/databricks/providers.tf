
# ============================================================================
# Terraform and Provider Configuration
# ============================================================================

terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
      configuration_aliases = [
        databricks.workspace,
        databricks.admin
      ]
    }
  }
}