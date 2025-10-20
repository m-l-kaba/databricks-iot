"""Inference module for predictive maintenance model."""

from mlflow.tracking import MlflowClient
import pyspark.sql.functions as F
import pandas as pd
import numpy as np


def run_inference(model_name: str, input_data: pd.DataFrame) -> np.ndarray:
    """
    Run inference using a registered MLflow model.

    Args:
        model_name (str): Name of the registered model.
        input_data (pd.DataFrame): Input data for prediction.

    Returns:
        Predictions from the model.
    """
    model = mlflow.sklearn.load_model(f"models:/{model_name}@Champion")
    predictions = model.predict(input_data)
    return predictions


def main():
    client = MlflowClient()

    model_version = client.get_model_version_by_alias(
        "production.gold.predictive_maintenance_pipeline", "Champion"
    )
    latest_training_hour_bucket = [
        param.value
        for param in model_version.params
        if param.key == "latest_training_hour_bucket"
    ][0]

    model_name = "predictive_maintenance_pipeline"
    input_data = spark.read.table(
        "production.gold.predictive_maintenance_features"
    ).filter(F.col("hour_bucket") > F.to_timestamp(F.lit(latest_training_hour_bucket)))

    input_data_pd = input_data.toPandas()

    predictions = run_inference(model_name, input_data_pd)

    input_data_pd["will_fail_prediction"] = predictions.to_list()

    output_df = spark.createDataFrame(input_data_pd)

    output_df.write.format("delta").mode("append").saveAsTable(
        "production.gold.predictive_maintenance_predictions"
    )


if __name__ == "__main__":
    main()
