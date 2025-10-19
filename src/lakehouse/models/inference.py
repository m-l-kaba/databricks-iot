from mlflow.tracking import MlflowClient
import pyspark.sql.functions as F


def run_inference(model_name: str, input_data):
    """
    Run inference using a registered MLflow model.

    Args:
        model_name (str): Name of the registered model.
        model_version (str): Version of the registered model.
        input_data: Input data for prediction.

    Returns:
        Predictions from the model.
    """
    client = MlflowClient()
    latest_model_version = client.get_latest_versions(model_name)[0].version
    model_uri = f"models:/{model_name}/{latest_model_version}"
    model = mlflow.sklearn.load_model(model_uri)
    predictions = model.predict(input_data)
    return predictions


def main():

    model_name = "predictive_maintenance_pipeline"
    input_data = spark.read.table(
        "production.gold.predictive_maintenance_features"
    ).filter(F.col("hour_bucket") > ).toPandas()
