# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Cleansing and Validation
# MAGIC
# MAGIC This notebook contains the silver layer tables for the IoT predictive maintenance pipeline.
# MAGIC It cleanses and validates raw data with quality expectations and type conversions.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Cleaned and Validated Data

# COMMAND ----------


@dp.table(
    name="silver.telemetry_clean",
    comment="Cleaned telemetry data with quality checks and device enrichment",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
@dp.expect_all(
    {
        "valid_device_id": "device_id IS NOT NULL AND device_id != ''",
        "valid_timestamp": "timestamp IS NOT NULL",
        "reasonable_temperature": "temperature IS NULL OR (temperature BETWEEN -50 AND 150)",
        "reasonable_pressure": "pressure IS NULL OR (pressure BETWEEN 0 AND 2000)",
        "reasonable_vibration": "vibration IS NULL OR (vibration BETWEEN 0 AND 100)",
        "reasonable_humidity": "humidity IS NULL OR (humidity BETWEEN 0 AND 100)",
        "reasonable_power": "power_consumption IS NULL OR (power_consumption >= 0)",
        "reasonable_flow_rate": "flow_rate IS NULL OR (flow_rate >= 0)",
        "reasonable_motor_speed": "motor_speed IS NULL OR (motor_speed >= 0)",
    }
)
def telemetry_clean():
    """
    Cleans and validates telemetry data with device enrichment.
    Applies data quality expectations and filters out invalid readings.
    """
    telemetry = spark.readStream.table("bronze.telemetry_raw")

    # Use read() for device master - it's reference data that changes infrequently
    device_master = spark.read.table("bronze.device_master_raw")

    return (
        telemetry
        # Clean and convert timestamp string to proper timestamp
        # Remove the extra ".000000" suffix from timestamps like "2025-10-16T16:32:31.590002.000000"
        .withColumn(
            "timestamp_clean",
            F.regexp_replace(F.col("timestamp"), r"\.(\d{6})\.000000$", ".$1"),
        )
        .withColumn(
            "timestamp",
            F.to_timestamp("timestamp_clean", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        )
        .drop("timestamp_clean")  # Remove the temporary column
        # Drop location and facility from telemetry since device master has authoritative data
        .drop("location", "facility")
        # Join with device master for enrichment (using raw device master data)
        .join(
            device_master.select(
                F.col("device_id").alias("dm_device_id"),
                F.col("device_type").alias("device_type"),
                F.col("location").alias("location"),
                F.col("facility").alias("facility"),
                F.col("manufacturer").alias("manufacturer"),
                F.col("model").alias("model"),
                F.col("status").alias("status"),
                # Convert timestamps on the fly for join with cleaning
                F.to_timestamp(
                    F.regexp_replace(
                        F.col("installation_date"), r"\.(\d{6})\.000000$", ".$1"
                    ),
                    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
                ).alias("installation_date"),
                F.to_timestamp(
                    F.regexp_replace(
                        F.col("last_maintenance"), r"\.(\d{6})\.000000$", ".$1"
                    ),
                    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
                ).alias("last_maintenance"),
                F.to_timestamp(
                    F.regexp_replace(
                        F.col("next_scheduled_maintenance"),
                        r"\.(\d{6})\.000000$",
                        ".$1",
                    ),
                    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
                ).alias("next_scheduled_maintenance"),
            ),
            F.col("device_id") == F.col("dm_device_id"),
            "inner",
        )
        .drop("dm_device_id")  # Remove the aliased device_id column
        # Filter only active devices
        .filter(F.col("status").isin(["active", "maintenance"]))
        # Add time-based features
        .withColumn("reading_hour", F.date_trunc("hour", "timestamp"))
        .withColumn("reading_date", F.to_date("timestamp"))
        .withColumn("hour_of_day", F.hour("timestamp"))
        .withColumn("day_of_week", F.dayofweek("timestamp"))
        # Add derived metrics
        .withColumn(
            "days_since_installation",
            F.datediff(F.col("timestamp"), F.col("installation_date")),
        )
        .withColumn(
            "days_since_last_maintenance",
            F.datediff(F.col("timestamp"), F.col("last_maintenance")),
        )
        .withColumn(
            "days_to_next_maintenance",
            F.datediff(F.col("next_scheduled_maintenance"), F.col("timestamp")),
        )
        # Select final columns
        .select(
            "device_id",
            "timestamp",
            "reading_hour",
            "reading_date",
            "hour_of_day",
            "day_of_week",
            "device_type",
            "location",
            "facility",
            "manufacturer",
            "model",
            "temperature",
            "pressure",
            "vibration",
            "humidity",
            "power_consumption",
            "flow_rate",
            "valve_position",
            "motor_speed",
            "days_since_installation",
            "days_since_last_maintenance",
            "days_to_next_maintenance",
            "ingestion_time",
        )
    )


# COMMAND ----------


@dp.table(
    name="silver.device_master_clean",
    comment="Cleaned device master with proper data types and derived fields",
    table_properties={"quality": "silver"},
)
@dp.expect_all(
    {
        "valid_device_id": "device_id IS NOT NULL AND device_id != ''",
        "valid_device_type": "device_type IS NOT NULL",
        "valid_status": "status IN ('active', 'inactive', 'maintenance', 'failed')",
        "valid_facility": "facility IS NOT NULL",
        "valid_manufacturer": "manufacturer IS NOT NULL",
        "valid_installation_date": "installation_date IS NOT NULL",
    }
)
def device_master_clean():
    """
    Cleans device master data with proper type conversions and derived fields.
    """
    return (
        spark.readStream.table("bronze.device_master_raw")
        # Convert string timestamps to proper timestamp types with cleaning
        .withColumn(
            "installation_date",
            F.to_timestamp(
                F.regexp_replace(
                    F.col("installation_date"), r"\.(\d{6})\.000000$", ".$1"
                ),
                "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
            ),
        )
        .withColumn(
            "last_maintenance",
            F.to_timestamp(
                F.regexp_replace(
                    F.col("last_maintenance"), r"\.(\d{6})\.000000$", ".$1"
                ),
                "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
            ),
        )
        .withColumn(
            "next_scheduled_maintenance",
            F.to_timestamp(
                F.regexp_replace(
                    F.col("next_scheduled_maintenance"), r"\.(\d{6})\.000000$", ".$1"
                ),
                "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
            ),
        )
        # Add derived fields
        .withColumn(
            "device_age_days", F.datediff(F.current_date(), F.col("installation_date"))
        )
        .withColumn(
            "days_since_last_maintenance",
            F.datediff(F.current_date(), F.col("last_maintenance")),
        )
        .withColumn(
            "days_to_next_maintenance",
            F.datediff(F.col("next_scheduled_maintenance"), F.current_date()),
        )
        .withColumn(
            "maintenance_overdue",
            F.when(F.col("days_to_next_maintenance") < 0, 1).otherwise(0),
        )
        # Clean string fields
        .withColumn("device_type", F.trim(F.lower("device_type")))
        .withColumn("location", F.trim("location"))
        .withColumn("facility", F.trim("facility"))
        .withColumn("status", F.trim(F.lower("status")))
        # Select final columns
        .select(
            "device_id",
            "device_type",
            "location",
            "facility",
            "installation_date",
            "firmware_version",
            "model",
            "manufacturer",
            "status",
            "last_maintenance",
            "next_scheduled_maintenance",
            "device_age_days",
            "days_since_last_maintenance",
            "days_to_next_maintenance",
            "maintenance_overdue",
            "ingestion_time",
        )
    )


# COMMAND ----------


@dp.table(
    name="silver.maintenance_clean",
    comment="Cleaned maintenance events with proper typing and duration calculations",
    table_properties={"quality": "silver"},
)
@dp.expect_all(
    {
        "valid_device_id": "device_id IS NOT NULL",
        "valid_maintenance_id": "maintenance_id IS NOT NULL",
        "valid_maintenance_date": "maintenance_date IS NOT NULL",
        "valid_duration": "duration_minutes IS NULL OR duration_minutes > 0",
    }
)
def maintenance_clean():
    """
    Cleans maintenance event data with proper type conversions.
    """
    return (
        spark.readStream.table("bronze.maintenance_raw")
        # Convert timestamps with cleaning
        .withColumn(
            "maintenance_date",
            F.to_timestamp(
                F.regexp_replace(
                    F.col("maintenance_date"), r"\.(\d{6})\.000000$", ".$1"
                ),
                "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
            ),
        )
        .withColumn(
            "scheduled_date",
            F.to_timestamp(
                F.regexp_replace(F.col("scheduled_date"), r"\.(\d{6})\.000000$", ".$1"),
                "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
            ),
        )
        # Calculate derived fields
        .withColumn("duration_hours", F.col("duration_minutes") / 60.0)
        .withColumn(
            "maintenance_overdue",
            F.when(F.col("maintenance_date") > F.col("scheduled_date"), 1).otherwise(0),
        )
        .withColumn(
            "delay_days", F.datediff(F.col("maintenance_date"), F.col("scheduled_date"))
        )
        # Clean string fields
        .withColumn("maintenance_type", F.trim(F.lower("maintenance_type")))
        .withColumn("technician", F.trim("technician"))
        # Select final columns
        .select(
            "device_id",
            "maintenance_id",
            "maintenance_type",
            "maintenance_date",
            "scheduled_date",
            "duration_minutes",
            "duration_hours",
            "maintenance_overdue",
            "delay_days",
            "technician",
            "description",
            "parts_replaced",
            "cost",
            "ingestion_time",
        )
    )


# COMMAND ----------


@dp.table(
    name="silver.failures_clean",
    comment="Cleaned failure events with severity scoring and downtime calculations",
    table_properties={"quality": "silver"},
)
@dp.expect_all(
    {
        "valid_device_id": "device_id IS NOT NULL",
        "valid_failure_id": "failure_id IS NOT NULL",
        "valid_failure_timestamp": "failure_timestamp IS NOT NULL",
        "valid_severity": "severity IN ('minor', 'major', 'critical')",
    }
)
def failures_clean():
    """
    Cleans failure event data with severity scoring and downtime calculations.
    """
    return (
        spark.readStream.table("bronze.failures_raw")
        # Convert timestamps with cleaning
        .withColumn(
            "failure_timestamp",
            F.to_timestamp(
                F.regexp_replace(
                    F.col("failure_timestamp"), r"\.(\d{6})\.000000$", ".$1"
                ),
                "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
            ),
        )
        .withColumn(
            "repair_timestamp",
            F.to_timestamp(
                F.regexp_replace(
                    F.col("repair_timestamp"), r"\.(\d{6})\.000000$", ".$1"
                ),
                "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
            ),
        )
        # Calculate downtime
        .withColumn(
            "downtime_hours",
            (
                F.col("repair_timestamp").cast("long")
                - F.col("failure_timestamp").cast("long")
            )
            / 3600,
        )
        # Add severity scoring
        .withColumn(
            "failure_severity_score",
            F.when(F.col("severity") == "critical", 3)
            .when(F.col("severity") == "major", 2)
            .when(F.col("severity") == "minor", 1)
            .otherwise(0),
        )
        # Clean string fields
        .withColumn("failure_type", F.trim(F.lower("failure_type")))
        .withColumn("severity", F.trim(F.lower("severity")))
        # Add time features
        .withColumn("failure_hour", F.hour("failure_timestamp"))
        .withColumn("failure_day_of_week", F.dayofweek("failure_timestamp"))
        # Select final columns
        .select(
            "device_id",
            "failure_id",
            "failure_timestamp",
            "repair_timestamp",
            "failure_type",
            "severity",
            "failure_severity_score",
            "downtime_hours",
            "failure_hour",
            "failure_day_of_week",
            "root_cause",
            "repair_action",
            "cost",
            "ingestion_time",
        )
    )


# COMMAND ----------


@dp.table(
    name="silver.device_health_metrics",
    comment="Real-time device health indicators - streaming compatible without windowing",
    table_properties={"quality": "silver"},
)
def device_health_metrics():
    """
    Calculates real-time device health metrics without lag functions.
    Used as input for anomaly detection and predictive features.
    Fully streaming-compatible approach using statistical thresholds.
    """
    telemetry = spark.readStream.table("silver.telemetry_clean")

    return (
        telemetry
        # Simple trend indicators based on current readings vs normal ranges
        .withColumn(
            "temp_1h_trend",
            F.when(F.col("temperature") > 75.0, 1.0)
            .when(F.col("temperature") < 65.0, -1.0)
            .otherwise(0.0),
        )
        .withColumn(
            "temp_short_trend",
            F.when(F.col("temperature") > 80.0, 2.0)
            .when(F.col("temperature") < 60.0, -2.0)
            .otherwise(0.0),
        )
        .withColumn(
            "vibration_trend",
            F.when(F.col("vibration") > 8.0, 1.0)
            .when(F.col("vibration") < 2.0, -1.0)
            .otherwise(0.0),
        )
        .withColumn(
            "pressure_trend",
            F.when(F.col("pressure") > 1200.0, 1.0)
            .when(F.col("pressure") < 800.0, -1.0)
            .otherwise(0.0),
        )
        .withColumn(
            "power_trend",
            F.when(F.col("power_consumption") > 500.0, 1.0)
            .when(F.col("power_consumption") < 100.0, -1.0)
            .otherwise(0.0),
        )
        # Simple anomaly detection using threshold-based approach
        .withColumn(
            "temp_anomaly_score",
            F.when(
                (F.col("temperature") > 85.0) | (F.col("temperature") < 55.0), 1.0
            ).otherwise(0.0),
        )
        .withColumn(
            "vibration_anomaly_score",
            F.when(F.col("vibration") > 10.0, 1.0).otherwise(0.0),
        )
        .withColumn(
            "pressure_anomaly_score",
            F.when(
                (F.col("pressure") > 1400.0) | (F.col("pressure") < 600.0), 1.0
            ).otherwise(0.0),
        )
        # Select final columns
        .select(
            "device_id",
            "timestamp",
            "device_type",
            "location",
            "facility",
            "temperature",
            "pressure",
            "vibration",
            "power_consumption",
            "temp_1h_trend",
            "temp_short_trend",
            "vibration_trend",
            "pressure_trend",
            "power_trend",
            "temp_anomaly_score",
            "vibration_anomaly_score",
            "pressure_anomaly_score",
            "days_since_last_maintenance",
            "days_to_next_maintenance",
        )
    )
