# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC This notebook contains the bronze layer tables for the IoT predictive maintenance pipeline.
# MAGIC It ingests raw data from various sources using Auto Loader with proper schemas.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definitions

# COMMAND ----------

# Device master schema
device_master_schema = StructType(
    [
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("location", StringType(), False),
        StructField("facility", StringType(), False),
        StructField("installation_date", StringType(), False),
        StructField("firmware_version", StringType(), True),
        StructField("model", StringType(), False),
        StructField("manufacturer", StringType(), False),
        StructField("status", StringType(), False),
        StructField("last_maintenance", StringType(), True),
        StructField("next_scheduled_maintenance", StringType(), True),
    ]
)

# Telemetry data schema
telemetry_schema = StructType(
    [
        StructField("device_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("temperature", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("vibration", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("power_consumption", DoubleType(), True),
        StructField("flow_rate", DoubleType(), True),
        StructField("valve_position", DoubleType(), True),
        StructField("motor_speed", DoubleType(), True),
    ]
)

# Maintenance events schema
maintenance_schema = StructType(
    [
        StructField("device_id", StringType(), False),
        StructField("maintenance_id", StringType(), False),
        StructField("maintenance_type", StringType(), False),
        StructField("maintenance_date", StringType(), False),
        StructField("scheduled_date", StringType(), False),
        StructField("duration_minutes", IntegerType(), True),
        StructField("technician", StringType(), True),
        StructField("description", StringType(), True),
        StructField("parts_replaced", StringType(), True),
        StructField("cost", DoubleType(), True),
    ]
)

# Failure events schema
failures_schema = StructType(
    [
        StructField("device_id", StringType(), False),
        StructField("failure_id", StringType(), False),
        StructField("failure_timestamp", StringType(), False),
        StructField("repair_timestamp", StringType(), True),
        StructField("failure_type", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("root_cause", StringType(), True),
        StructField("repair_action", StringType(), True),
        StructField("cost", DoubleType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables

# COMMAND ----------


@dlt.table(
    name="bronze.telemetry_raw",
    comment="Raw IoT telemetry data from sensors",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def telemetry_raw():
    """
    Ingests streaming telemetry data using Auto Loader.
    Expected data format: JSON files in /Volumes/production/raw/landing-volume/telemetry/
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/production/raw/landing-volume/schemas/telemetry/",
        )
        .option("cloudFiles.maxFilesPerTrigger", "1000")
        .schema(telemetry_schema)
        .load("/Volumes/production/raw/landing-volume/telemetry/")
        .select(
            "*",
            F.current_timestamp().alias("ingestion_time"),
            F.col("_metadata.file_path").alias("source_file"),
        )
    )


# COMMAND ----------


@dlt.table(
    name="bronze.device_master_raw",
    comment="Raw device master data with metadata and configuration",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.managed": "true"},
)
def device_master_raw():
    """
    Ingests device master data using Auto Loader.
    Expected data format: JSON files in /Volumes/production/raw/landing-volume/device_master/
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/production/raw/landing-volume/schemas/device_master/",
        )
        .schema(device_master_schema)
        .load("/Volumes/production/raw/landing-volume/device_master/")
        .select(
            "*",
            F.current_timestamp().alias("ingestion_time"),
            F.col("_metadata.file_path").alias("source_file"),
        )
    )


# COMMAND ----------


@dlt.table(
    name="bronze.maintenance_raw",
    comment="Raw maintenance events and schedules",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.managed": "true"},
)
def maintenance_raw():
    """
    Ingests maintenance event data using Auto Loader.
    Expected data format: JSON files in /Volumes/production/raw/landing-volume/maintenance/
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/production/raw/landing-volume/schemas/maintenance/",
        )
        .schema(maintenance_schema)
        .load("/Volumes/production/raw/landing-volume/maintenance/")
        .select(
            "*",
            F.current_timestamp().alias("ingestion_time"),
            F.col("_metadata.file_path").alias("source_file"),
        )
    )


# COMMAND ----------


@dlt.table(
    name="bronze.failures_raw",
    comment="Raw failure events for model training",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.managed": "true"},
)
def failures_raw():
    """
    Ingests failure event data using Auto Loader.
    Expected data format: JSON files in /Volumes/production/raw/landing-volume/failures/
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/production/raw/landing-volume/schemas/failures/",
        )
        .schema(failures_schema)
        .load("/Volumes/production/raw/landing-volume/failures/")
        .select(
            "*",
            F.current_timestamp().alias("ingestion_time"),
            F.col("_metadata.file_path").alias("source_file"),
        )
    )
