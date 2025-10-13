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
#   backend "azurerm" {
#     container_name      = "tfstate"
#     key                 = "terraform.tfstate"
#     resource_group_name = "faers-lakehouse-rg"
#     storage_account_name = "faerslakehouse"
#   }
}


provider "azurerm" {
  features {}
}