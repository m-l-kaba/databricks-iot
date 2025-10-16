# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - ML Features and Analytics
# MAGIC
# MAGIC This notebook contains the gold layer tables for the IoT predictive maintenance pipeline.
# MAGIC It creates ML-ready features, training datasets, and business intelligence views.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Tables - ML Features and Business Intelligence

# COMMAND ----------


@dlt.table(
    name="gold.device_features_hourly",
    comment="Hourly aggregated features for predictive maintenance ML model",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported",
    },
)
def device_features_hourly():
    """
    Creates hourly aggregated features for machine learning.
    This table serves as the primary feature store for predictive maintenance models.
    """
    health_metrics = dlt.read_stream("silver.device_health_metrics")

    return (
        health_metrics.withColumn("hour_bucket", F.date_trunc("hour", "timestamp"))
        .groupBy("device_id", "hour_bucket", "device_type", "location", "facility")
        .agg(
            # Count of readings (data quality indicator)
            F.count("*").alias("reading_count"),
            # Temperature features - using actual sensor readings and trends
            F.avg("temperature").alias("avg_temp_1h"),
            F.stddev("temperature").alias("temp_std_1h"),
            F.min("temperature").alias("min_temp_1h"),
            F.max("temperature").alias("max_temp_1h"),
            F.avg("temp_1h_trend").alias("avg_temp_1h_trend"),
            F.avg("temp_short_trend").alias("avg_temp_short_trend"),
            # Vibration features
            F.avg("vibration").alias("avg_vibration_1h"),
            F.max("vibration").alias("max_vibration_1h"),
            F.avg("vibration_trend").alias("avg_vibration_trend"),
            # Pressure features
            F.avg("pressure").alias("avg_pressure_1h"),
            F.stddev("pressure").alias("pressure_std_1h"),
            F.avg("pressure_trend").alias("avg_pressure_trend"),
            # Power consumption features
            F.avg("power_consumption").alias("avg_power_1h"),
            F.avg("power_trend").alias("avg_power_trend"),
            # Anomaly scores
            F.max("temp_anomaly_score").alias("max_temp_anomaly_score"),
            F.max("vibration_anomaly_score").alias("max_vibration_anomaly_score"),
            F.max("pressure_anomaly_score").alias("max_pressure_anomaly_score"),
            # Maintenance timing features
            F.first("days_since_last_maintenance").alias("days_since_last_maintenance"),
            F.first("days_to_next_maintenance").alias("days_to_next_maintenance"),
        )
        # Feature engineering - enhanced anomaly scoring
        .withColumn(
            "temp_anomaly_score",
            F.when(F.col("max_temp_anomaly_score") > 0, 1.0)
            .when(F.col("temp_std_1h") > 5.0, 1.0)
            .otherwise(0.0),
        )
        .withColumn(
            "vibration_anomaly_score",
            F.when(F.col("max_vibration_anomaly_score") > 0, 1.0)
            .when(F.col("max_vibration_1h") > 10.0, 1.0)
            .otherwise(0.0),
        )
        .withColumn(
            "pressure_anomaly_score",
            F.when(F.col("max_pressure_anomaly_score") > 0, 1.0)
            .when(F.col("pressure_std_1h") > 10.0, 1.0)
            .otherwise(0.0),
        )
        # Maintenance urgency scoring
        .withColumn(
            "maintenance_urgency_score",
            F.when(F.col("days_to_next_maintenance") < 7, 1.0)
            .when(F.col("days_to_next_maintenance") < 30, 0.5)
            .otherwise(0.0),
        )
        # Overall health score (composite metric)
        .withColumn(
            "overall_health_score",
            (1.0 - F.col("temp_anomaly_score"))
            * (1.0 - F.col("vibration_anomaly_score"))
            * (1.0 - F.col("pressure_anomaly_score"))
            * (1.0 - F.col("maintenance_urgency_score")),
        )
        # Device age at time of reading
        .withColumn("device_age_weeks", F.col("days_since_last_maintenance") / 7.0)
        # Time-based features
        .withColumn("hour_of_day", F.hour("hour_bucket"))
        .withColumn("day_of_week", F.dayofweek("hour_bucket"))
        .withColumn(
            "is_weekend", F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0)
        )
        # Select final feature columns
        .select(
            "device_id",
            "hour_bucket",
            "device_type",
            "location",
            "facility",
            "reading_count",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            # Temperature features
            "avg_temp_1h",
            "temp_std_1h",
            "min_temp_1h",
            "max_temp_1h",
            "avg_temp_1h_trend",
            "avg_temp_short_trend",
            # Vibration features
            "avg_vibration_1h",
            "max_vibration_1h",
            "avg_vibration_trend",
            # Pressure features
            "avg_pressure_1h",
            "pressure_std_1h",
            "avg_pressure_trend",
            # Power features
            "avg_power_1h",
            "avg_power_trend",
            # Derived features
            "temp_anomaly_score",
            "vibration_anomaly_score",
            "pressure_anomaly_score",
            "maintenance_urgency_score",
            "overall_health_score",
            "device_age_weeks",
            "days_since_last_maintenance",
            "days_to_next_maintenance",
        )
    )


# COMMAND ----------


@dlt.table(
    name="gold.failure_labels",
    comment="Failure labels for supervised learning with 7-day prediction window",
    table_properties={"quality": "gold"},
)
def failure_labels():
    """
    Creates failure labels for supervised machine learning.
    Generates labels for devices that will fail within the next 7 days.
    """
    failures = dlt.read_stream("silver.failures_clean")

    return (
        failures
        # Filter only significant failures (major and critical)
        .filter(F.col("severity").isin(["major", "critical"]))
        .select(
            "device_id",
            "failure_timestamp",
            "failure_type",
            "severity",
            "failure_severity_score",
            F.lit(1).alias("failure_occurred"),
        )
        # Create 7-day prediction windows (168 hours)
        .withColumn(
            "prediction_windows", F.expr("sequence(0, 167)")
        )  # 0 to 167 hours (7 days)
        .select("*", F.explode("prediction_windows").alias("hours_before_failure"))
        .withColumn(
            "prediction_timestamp",
            F.col("failure_timestamp")
            - F.expr("INTERVAL 1 HOUR") * F.col("hours_before_failure"),
        )
        .withColumn("prediction_hour", F.date_trunc("hour", "prediction_timestamp"))
        # Add time until failure feature
        .withColumn("hours_to_failure", F.col("hours_before_failure"))
        .withColumn("days_to_failure", F.col("hours_before_failure") / 24.0)
        # Select final columns for labeling
        .select(
            "device_id",
            "prediction_hour",
            "failure_occurred",
            "failure_type",
            "severity",
            "failure_severity_score",
            "hours_to_failure",
            "days_to_failure",
        )
    )


# COMMAND ----------


@dlt.table(
    name="gold.predictive_maintenance_features",
    comment="Final ML training dataset with features and labels for 7-day failure prediction",
    table_properties={"quality": "gold"},
)
def predictive_maintenance_features():
    """
    Creates the final ML training dataset by joining features with failure labels.
    This is the primary table used for training and inference of the predictive maintenance model.
    Uses batch processing for complex joins required in ML training.
    """
    features = dlt.read("gold.device_features_hourly")
    labels = dlt.read("gold.failure_labels")

    return (
        features.join(
            labels,
            (features.device_id == labels.device_id)
            & (features.hour_bucket == labels.prediction_hour),
            "left",
        )
        # Drop duplicate device_id column from labels to avoid ambiguity
        .drop(labels.device_id)
        # Fill null values for non-failure cases
        .fillna(
            0,
            [
                "failure_occurred",
                "failure_severity_score",
                "hours_to_failure",
                "days_to_failure",
            ],
        )
        .fillna("Normal", ["failure_type", "severity"])
        # Feature interactions
        .withColumn(
            "temp_vibration_interaction",
            F.col("avg_temp_1h") * F.col("avg_vibration_1h"),
        )
        .withColumn(
            "maintenance_health_interaction",
            F.col("maintenance_urgency_score") * (1 - F.col("overall_health_score")),
        )
        # Streaming-compatible trend features (using existing trend columns)
        .withColumn(
            "temp_change_rate",
            F.col("avg_temp_1h_trend"),
        )
        .withColumn(
            "vibration_change_rate",
            F.col("avg_vibration_trend"),
        )
        # Select final columns for ML
        .select(
            # Identifiers and timestamps
            "device_id",
            "hour_bucket",
            "device_type",
            "location",
            "facility",
            # Basic features
            "reading_count",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            # Sensor features
            "avg_temp_1h",
            "temp_std_1h",
            "min_temp_1h",
            "max_temp_1h",
            "avg_temp_1h_trend",
            "avg_temp_short_trend",
            "avg_vibration_1h",
            "max_vibration_1h",
            "avg_vibration_trend",
            "avg_pressure_1h",
            "pressure_std_1h",
            "avg_pressure_trend",
            "avg_power_1h",
            "avg_power_trend",
            # Anomaly scores
            "temp_anomaly_score",
            "vibration_anomaly_score",
            "pressure_anomaly_score",
            # Maintenance features
            "maintenance_urgency_score",
            "days_since_last_maintenance",
            "days_to_next_maintenance",
            "device_age_weeks",
            "overall_health_score",
            # Interaction features
            "temp_vibration_interaction",
            "maintenance_health_interaction",
            # Trend features
            "temp_change_rate",
            "vibration_change_rate",
            # Target labels
            "failure_occurred",
            "failure_type",
            "severity",
            "failure_severity_score",
            "hours_to_failure",
            "days_to_failure",
        )
    )


# COMMAND ----------


@dlt.table(
    name="gold.device_health_dashboard",
    comment="Real-time device health metrics for operational dashboards and monitoring",
    table_properties={"quality": "gold"},
)
def device_health_dashboard():
    """
    Creates a real-time dashboard view with latest device health status.
    Used by operations teams for monitoring and alerting.
    Streaming compatible - shows all records for real-time monitoring.
    """
    features = dlt.read_stream("gold.device_features_hourly")

    return (
        features
        # Risk categorization
        .withColumn(
            "risk_level",
            F.when(F.col("overall_health_score") > 0.8, "Low")
            .when(F.col("overall_health_score") > 0.6, "Medium")
            .when(F.col("overall_health_score") > 0.4, "High")
            .otherwise("Critical"),
        )
        # Alert flags
        .withColumn(
            "temperature_alert",
            F.when(F.col("temp_anomaly_score") > 0, "ALERT").otherwise("OK"),
        )
        .withColumn(
            "vibration_alert",
            F.when(F.col("vibration_anomaly_score") > 0, "ALERT").otherwise("OK"),
        )
        .withColumn(
            "maintenance_alert",
            F.when(F.col("maintenance_urgency_score") > 0.5, "ALERT").otherwise("OK"),
        )
        # Last updated timestamp
        .withColumn("last_updated", F.current_timestamp())
        .select(
            "device_id",
            "device_type",
            "location",
            "facility",
            "hour_bucket",
            "last_updated",
            "overall_health_score",
            "risk_level",
            "temperature_alert",
            "vibration_alert",
            "maintenance_alert",
            "days_to_next_maintenance",
            "days_since_last_maintenance",
            "avg_temp_1h",
            "avg_pressure_1h",
            "avg_vibration_1h",
            "avg_power_1h",
            "temp_anomaly_score",
            "vibration_anomaly_score",
            "pressure_anomaly_score",
        )
    )


# COMMAND ----------


@dlt.table(
    name="gold.maintenance_analytics",
    comment="Analytics table for maintenance planning and cost optimization - streaming compatible",
    table_properties={"quality": "gold"},
)
def maintenance_analytics():
    """
    Creates analytics for maintenance planning and cost optimization.
    Simplified streaming-compatible approach without complex aggregations.
    """
    maintenance = dlt.read_stream("silver.maintenance_clean")

    return (
        maintenance
        # Add derived metrics for each maintenance event
        .withColumn(
            "is_overdue", F.when(F.col("maintenance_overdue") == 1, 1).otherwise(0)
        )
        .withColumn("is_expensive", F.when(F.col("cost") > 1000, 1).otherwise(0))
        .withColumn(
            "is_long_duration", F.when(F.col("duration_hours") > 4, 1).otherwise(0)
        )
        .withColumn("maintenance_month", F.date_trunc("month", "maintenance_date"))
        .withColumn("days_overdue", F.greatest(F.lit(0), F.col("delay_days")))
        # Select relevant columns for analytics
        .select(
            "device_id",
            "maintenance_id",
            "maintenance_type",
            "maintenance_date",
            "maintenance_month",
            "duration_hours",
            "cost",
            "is_overdue",
            "is_expensive",
            "is_long_duration",
            "days_overdue",
            "technician",
            "ingestion_time",
        )
    )
