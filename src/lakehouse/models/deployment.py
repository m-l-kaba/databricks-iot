"""Deployment module for managing model versions in MLflow."""

from mlflow.tracking import MlflowClient


def main():
    client = MlflowClient()

    model_version_challenger = client.get_model_version_by_alias(
        "production.gold.predictive_maintenance_pipeline", "Challenger"
    )

    model_version_champion = client.get_model_version_by_alias(
        "production.gold.predictive_maintenance_pipeline", "Champion"
    )

    client.set_registered_model_alias(
        "production.gold.predictive_maintenance_pipeline",
        "Retired",
        model_version_champion.version,
    )

    # Promote challenger to champion

    client.delete_registered_model_alias(
        "production.gold.predictive_maintenance_pipeline", "Champion"
    )

    client.delete_registered_model_alias(
        "production.gold.predictive_maintenance_pipeline", "Challenger"
    )

    client.set_registered_model_alias(
        "production.gold.predictive_maintenance_pipeline",
        "Champion",
        model_version_challenger.version,
    )
