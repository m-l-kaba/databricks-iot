# Databricks notebook source

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="gold.device_features_hourly",
    comment="Hourly aggregated features for predictive maintenance ML model",
    table_properties={
        "quality": "gold",
        "delta.feature.allowColumnDefaults": "supported",
    },
)
def device_features_hourly():
    """Creates hourly aggregated features for machine learning."""
    health_metrics = spark.readStream.table("silver.device_health_metrics")

    return (
        health_metrics.withColumn("hour_bucket", F.date_trunc("hour", "timestamp"))
        .groupBy("device_id", "hour_bucket", "device_type", "location")
        .agg(
            F.avg("temperature").alias("avg_temperature"),
            F.avg("vibration").alias("avg_vibration"),
            F.avg("pressure").alias("avg_pressure"),
            F.avg("power_consumption").alias("avg_power"),
            F.count("*").alias("reading_count"),
            F.first("days_since_last_maintenance").alias("days_since_maintenance"),
        )
        .withColumn(
            "high_temperature", F.when(F.col("avg_temperature") > 75, 1).otherwise(0)
        )
        .withColumn(
            "high_vibration", F.when(F.col("avg_vibration") > 8, 1).otherwise(0)
        )
        .withColumn(
            "needs_maintenance_soon",
            F.when(F.col("days_since_maintenance") > 60, 1).otherwise(0),
        )
        # Health score calculation
        .withColumn(
            "health_score",
            F.when(
                (F.col("high_temperature") == 0)
                & (F.col("high_vibration") == 0)
                & (F.col("needs_maintenance_soon") == 0),
                1.0,
            ).otherwise(0.0),
        )
        .select(
            "device_id",
            "hour_bucket",
            "device_type",
            "location",
            "reading_count",
            "avg_temperature",
            "avg_vibration",
            "avg_pressure",
            "avg_power",
            "high_temperature",
            "high_vibration",
            "needs_maintenance_soon",
            "health_score",
            "days_since_maintenance",
        )
    )


# COMMAND ----------


@dp.table(
    name="gold.failure_labels",
    comment="Failure labels for supervised learning with extended prediction window",
    table_properties={"quality": "gold"},
)
def failure_labels():
    """Creates failure labels for supervised ML - devices that will fail within 7 days."""
    failures = spark.readStream.table("silver.failures_clean")

    return (
        failures.filter(F.col("severity").isin(["minor", "major", "critical"]))
        .select("device_id", "failure_timestamp", "failure_type", "severity")
        .withColumn("prediction_hours", F.array(*[F.lit(h) for h in range(6, 169, 6)]))
        .select("*", F.explode("prediction_hours").alias("hours_before_failure"))
        .withColumn(
            "prediction_time",
            F.col("failure_timestamp")
            - F.expr("INTERVAL 1 HOUR") * F.col("hours_before_failure"),
        )
        .withColumn("prediction_hour", F.date_trunc("hour", "prediction_time"))
        .withColumn(
            "days_before_failure", F.round(F.col("hours_before_failure") / 24.0, 1)
        )
        .select(
            "device_id",
            "prediction_hour",
            F.lit(1).alias("will_fail"),
            "failure_type",
            "severity",
            "days_before_failure",
            "hours_before_failure",
        )
    )


# COMMAND ----------


@dp.table(
    name="gold.predictive_maintenance_features",
    comment="Final ML dataset with features and labels for 7-day failure prediction",
    table_properties={"quality": "gold"},
    schema="""
        device_id STRING,
        hour_bucket TIMESTAMP,
        device_type STRING,
        location STRING,
        avg_temperature DOUBLE,
        avg_vibration DOUBLE,
        avg_pressure DOUBLE,
        avg_power DOUBLE,
        high_temperature INT,
        high_vibration INT,
        needs_maintenance_soon INT,
        health_score DOUBLE,
        days_since_maintenance INT,
        will_fail INT,
        failure_type STRING,
        severity STRING,
        days_before_failure DOUBLE,
        reading_count LONG,
        has_sufficient_data INT,
        CONSTRAINT device_id_hour_pk PRIMARY KEY (device_id, hour_bucket)
    """,
)
def predictive_maintenance_features():
    """
    Creates the final ML training dataset by joining features with failure labels.
    This is the primary table used for training and inference of the predictive maintenance model.
    """
    features = spark.readStream.table("gold.device_features_hourly").withWatermark(
        "hour_bucket", "1 hour"
    )

    labels = spark.readStream.table("gold.failure_labels").withWatermark(
        "prediction_hour", "1 hour"
    )

    return (
        features.join(
            labels,
            (features.device_id == labels.device_id)
            & (features.hour_bucket == labels.prediction_hour),
            "left",
        )
        .drop(labels.device_id)  # Remove duplicate column
        # Fill missing labels (no failure = 0)
        .fillna(0, ["will_fail", "days_before_failure"])
        .fillna("Normal", ["failure_type", "severity"])
        # Add data quality indicators
        .withColumn(
            "has_sufficient_data", F.when(F.col("reading_count") >= 10, 1).otherwise(0)
        )
        # Select only essential columns for ML
        .select(
            # Identifiers
            "device_id",
            "hour_bucket",
            "device_type",
            "location",
            # Core features (sensor aggregations)
            "avg_temperature",
            "avg_vibration",
            "avg_pressure",
            "avg_power",
            # Engineered features
            "high_temperature",
            "high_vibration",
            "needs_maintenance_soon",
            "health_score",
            "days_since_maintenance",
            # Target variable and metadata
            "will_fail",
            "failure_type",
            "severity",
            "days_before_failure",
            # Data quality indicators
            "reading_count",
            "has_sufficient_data",
        )
    )


# COMMAND ----------


@dp.table(
    name="gold.device_health_dashboard",
    comment="Device health dashboard for operational monitoring",
    table_properties={"quality": "gold"},
)
def device_health_dashboard():
    """
    Creates a dashboard view with current device health status.
    Used by operations teams for monitoring and alerting.
    """
    features = spark.readStream.table("gold.device_features_hourly")

    return (
        features.withColumn(
            "status",
            F.when(F.col("health_score") == 1, "Healthy")
            .when(
                (F.col("high_temperature") == 1) | (F.col("high_vibration") == 1),
                "Warning",
            )
            .when(F.col("needs_maintenance_soon") == 1, "Maintenance Due")
            .otherwise("Unknown"),
        )
        .withColumn("last_updated", F.current_timestamp())
        .select(
            "device_id",
            "device_type",
            "location",
            "hour_bucket",
            "last_updated",
            "status",
            "health_score",
            "avg_temperature",
            "avg_vibration",
            "avg_pressure",
            "days_since_maintenance",
            "reading_count",
        )
    )


# COMMAND ----------


@dp.table(
    name="gold.maintenance_analytics",
    comment="Maintenance analytics for planning and cost optimization",
    table_properties={"quality": "gold"},
)
def maintenance_analytics():
    """
    Creates analytics for maintenance planning and cost optimization.
    """
    maintenance = spark.readStream.table("silver.maintenance_clean")

    return (
        maintenance.withColumn(
            "maintenance_month", F.date_trunc("month", "maintenance_date")
        )
        .withColumn("is_expensive", F.when(F.col("cost") > 1000, 1).otherwise(0))
        .select(
            "device_id",
            "maintenance_id",
            "maintenance_type",
            "maintenance_date",
            "maintenance_month",
            "duration_hours",
            "cost",
            "is_expensive",
            "technician",
        )
    )
