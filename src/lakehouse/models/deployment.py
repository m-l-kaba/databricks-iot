from mlflow.tracking import MlflowClient


def main():
    client = MlflowClient()

    model_version_challenger = client.get_model_version_by_alias(
        "production.gold.predictive_maintenance_pipeline", "Challenger"
    )

    # Promote challenger to champion

    client.delete_registered_model_alias("predictive_maintenance_pipeline", "Champion")

    client.set_registered_model_alias(
        "predictive_maintenance_pipeline", "Champion", model_version_challenger.version
    )
