variable "resource_group_name" {
  description = "The name of the resource group in which to create the resources."
  type        = string
}

variable "location" {
  description = "The Azure region in which to create the resources."
  type        = string
  default     = "West Europe"
}

variable "storage_account_name" {
  description = "The name of the storage account to create."
  type        = string
}

variable "storage_account_tier" {
  description = "The performance tier of the storage account. Valid options are 'Standard' and 'Premium'."
  type        = string
  default     = "Standard"
}

variable "storage_account_replication_type" {
  description = "The replication type of the storage account. Valid options are 'LRS', 'GRS', 'RAGRS', and 'ZRS'."
  type        = string
  default     = "LRS"
}

variable "container_name" {
  description = "The name of the storage container to create."
  type        = string
  default     = "iot-container"
}


variable "databricks_ws_name" {
  description = "The name of the Databricks workspace to create."
  type        = string
}