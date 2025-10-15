variable "azure_client_id" {
  description = "The Client ID of the Service Principal."
  type        = string
}

variable "azure_client_secret" {
  description = "The Client Secret of the Service Principal."
  type        = string
  sensitive   = true
}

variable "azure_tenant_id" {
  description = "The Tenant ID of the Service Principal."
  type        = string
}

variable "account_id" {
  description = "The Account ID for the Databricks account."
  type        = string
}