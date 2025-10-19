<!-- BEGIN_TF_DOCS -->
## Requirements

No requirements.

## Providers

No providers.

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_azure_resources"></a> [azure\_resources](#module\_azure\_resources) | ./modules/azure | n/a |
| <a name="module_databricks_resources"></a> [databricks\_resources](#module\_databricks\_resources) | ./modules/databricks | n/a |

## Resources

No resources.

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_account_id"></a> [account\_id](#input\_account\_id) | The Account ID for the Databricks account. | `string` | n/a | yes |
| <a name="input_azure_client_id"></a> [azure\_client\_id](#input\_azure\_client\_id) | The Client ID of the Service Principal. | `string` | n/a | yes |
| <a name="input_azure_client_secret"></a> [azure\_client\_secret](#input\_azure\_client\_secret) | The Client Secret of the Service Principal. | `string` | n/a | yes |
| <a name="input_azure_tenant_id"></a> [azure\_tenant\_id](#input\_azure\_tenant\_id) | The Tenant ID of the Service Principal. | `string` | n/a | yes |

## Outputs

No outputs.
<!-- END_TF_DOCS -->