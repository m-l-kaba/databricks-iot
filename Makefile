generate_data:
	uv run python run_generator.py --devices 100 --telemetry-hours 168 --failure-days 90 --maintenance-days 180  --upload-to-azure --storage-account databricksiotsa

deploy_infra:
	cd infra/terraform &&
		source .env &&
		terraform init &&
		terraform apply -auto-approve
destroy_infra:
	cd infra/terraform &&
		source .env &&
		terraform destroy -auto-approve