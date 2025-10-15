terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
    }
    databricks = {
      source = "databricks/databricks"
    }
  }

  backend "local" {
    path = "terraform.tfstate"
  }
}


provider "azurerm" {
  features {}
}

provider "databricks" {
  alias                       = "workspace"
  azure_workspace_resource_id = module.azure_resources.databricks_workspace_id
  host                        = module.azure_resources.databricks_workspace_url
  profile                     = "DEFAULT"
}

provider "databricks" {
  alias      = "admin"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.account_id
  profile    = "DEFAULT"


}