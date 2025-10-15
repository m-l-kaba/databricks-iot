# Databricks IoT Data Pipeline

A Python-based synthetic IoT data generator designed for Azure Data Lake and Databricks analytics. This tool generates realistic telemetry, failure, and maintenance data for industrial IoT scenarios.

Built with modern Python tooling using [uv](https://docs.astral.sh/uv/) for fast dependency management.

## Overview

The project generates realistic synthetic data for four main categories:

- **Telemetry**: Real-time sensor readings from IoT devices
- **Failures**: Device failure and error events
- **Maintenance**: Scheduled and unscheduled maintenance records
- **Device Master**: Device metadata and configuration data

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager
- Azure Storage Account (if uploading to Azure)
- Azure Databricks (for data analysis)

### Installing uv

```bash
# macOS and Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or via pip
pip install uv
```

## Architecture

The Terraform infrastructure creates:

- Azure Data Lake Gen2 storage account with hierarchical namespace
- Four data folders: `telemetry`, `failures`, `maintenance`, `device_master`
- Databricks workspace for analytics

## Features

### IoT Device Types Supported

- Temperature sensors
- Humidity sensors
- Pressure sensors
- Vibration sensors
- Flow meters
- Power meters
- Smart valves
- Motor controllers

### Data Characteristics

- **Realistic sensor readings** with time-based patterns, seasonal drift, and noise
- **Device failures** with different severity levels and resolution times
- **Maintenance records** with cost tracking and parts replacement
- **Quality indicators** for telemetry data
- **Configurable time periods** and data volumes

## Quick Start

### 1. Install Dependencies

```bash
uv sync
```

### 2. Generate Demo Data

```bash
uv run python run_generator.py --demo --verbose
```

This creates sample data in the `./output` directory with:

- 20 devices
- 6 hours of telemetry data
- 7 days of failure events
- 30 days of maintenance records

### 3. Generate Full Dataset

```bash
uv run python run_generator.py --devices 100 --telemetry-hours 48 --failure-days 30 --maintenance-days 90
```

## Configuration

The IoT data generator uses a YAML configuration file (`config.yaml`) for all settings. This provides a centralized, readable way to configure the entire simulation.

### Main Configuration File

The `config.yaml` file contains comprehensive settings for:

- Device types and sensor configurations
- Data generation periods and intervals
- Simulation parameters (facilities, locations, manufacturers)
- Failure patterns and probabilities
- Maintenance scheduling and costs
- Data quality settings

Key configuration sections:

```yaml
# Device configuration
devices:
  num_devices: 100

# Data generation periods
data_periods:
  telemetry_hours: 24
  failure_days: 30
  maintenance_days: 90

# Sensor configurations for each device type
sensors:
  temperature_sensor:
    reading_type: "temperature"
    unit: "celsius"
    base_value: 25.0
    variance: 15.0
```

### Environment Variables Override

Environment variables can override configuration file settings:

```bash
# Azure Data Lake Configuration
AZURE_STORAGE_ACCOUNT_NAME=your_storage_account_name
AZURE_STORAGE_ACCOUNT_KEY=your_storage_account_key
AZURE_CONTAINER_NAME=raw

# Data Generation Settings
IOT_NUM_DEVICES=100
IOT_TELEMETRY_HOURS=24
IOT_FAILURE_DAYS=30
IOT_MAINTENANCE_DAYS=90
IOT_TELEMETRY_INTERVAL=5
```

Copy `.env.example` to `.env` for environment-specific overrides.

#### Customizing Configurations

You can easily customize the data generator by modifying `config.yaml`:

1. **Add new device types**: Extend the `sensors` section with new device configurations
2. **Modify failure patterns**: Adjust failure probabilities and resolution times
3. **Change facility layouts**: Update facilities and locations in the `simulation` section
4. **Tune data quality**: Modify missing data probabilities and quality distributions
5. **Adjust maintenance costs**: Change labor rates and parts costs

Example - Adding a new sensor type:

```yaml
sensors:
  gas_sensor:
    reading_type: "gas_concentration"
    unit: "ppm"
    base_value: 50.0
    variance: 25.0
    min_val: 0.0
    max_val: 1000.0
```

### Command Line Options

```bash
uv run python run_generator.py --help
```

Key options:

- `--devices N`: Number of IoT devices to simulate
- `--telemetry-hours N`: Hours of telemetry data to generate
- `--failure-days N`: Days of failure events to generate
- `--maintenance-days N`: Days of maintenance records to generate
- `--upload-to-azure`: Upload data directly to Azure Data Lake
- `--demo`: Quick demo with reduced data volumes

## Data Schemas

### Telemetry Data

```json
{
  "device_id": "DEV_TEMPERATURE_SENSOR_0001",
  "timestamp": "2025-10-13T10:30:00",
  "reading_type": "temperature",
  "value": 23.45,
  "unit": "celsius",
  "quality": "good",
  "location": "Production_Line_1",
  "facility": "Factory_A"
}
```

### Failure Events

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "device_id": "DEV_PRESSURE_SENSOR_0010",
  "timestamp": "2025-10-12T14:22:00",
  "failure_type": "sensor_malfunction",
  "severity": "medium",
  "description": "Sensor reading outside normal range",
  "location": "Quality_Control",
  "facility": "Factory_A",
  "resolved": true,
  "resolution_time": "2025-10-12T18:45:00"
}
```

### Maintenance Records

```json
{
  "maintenance_id": "660e8400-e29b-41d4-a716-446655440000",
  "device_id": "DEV_FLOW_METER_0025",
  "maintenance_type": "preventive",
  "scheduled_date": "2025-10-10T09:00:00",
  "actual_date": "2025-10-10T09:15:00",
  "duration_hours": 2.5,
  "technician_id": "TECH_1025",
  "description": "Routine preventive maintenance check",
  "parts_replaced": ["filter_replacement", "sensor_calibration"],
  "cost": 287.5,
  "location": "Storage_Zone_A",
  "facility": "Warehouse_1"
}
```

### Device Master Data

```json
{
  "device_id": "DEV_VIBRATION_SENSOR_0050",
  "device_type": "vibration_sensor",
  "location": "Assembly_1",
  "facility": "Factory_B",
  "installation_date": "2023-05-15T00:00:00",
  "firmware_version": "2.1.4",
  "model": "VIBRATION_SENSOR-456",
  "manufacturer": "SensorTech",
  "status": "active",
  "last_maintenance": "2025-09-20T00:00:00",
  "next_scheduled_maintenance": "2025-12-15T00:00:00"
}
```

## Azure Integration

### Deploy Infrastructure

```bash
cd infrastructure/terraform
terraform init
terraform plan
terraform apply
```

### Upload Data to Azure

```bash
# Set Azure credentials
export AZURE_STORAGE_ACCOUNT_NAME="your_storage_account"
export AZURE_STORAGE_ACCOUNT_KEY="your_key"

# Generate and upload data
uv run python run_generator.py --upload-to-azure --devices 500 --telemetry-hours 72
```

## Development

### Project Structure

```
databrick_iot/
├── src/data_simulation/
│   ├── iot_data_generator.py  # Main data generator
│   ├── config.py              # Configuration management
│   └── azure_uploader.py      # Azure Data Lake integration
├── infrastructure/terraform/   # Infrastructure as code
├── run_generator.py           # CLI script
└── pyproject.toml            # Project dependencies
```

### Extending the Generator

The data generator is modular and can be extended:

1. **Add new device types**: Extend the `DeviceType` enum and add sensor configurations
2. **Custom data patterns**: Modify the telemetry generation logic for specific use cases
3. **Additional data categories**: Create new data classes and generation methods
4. **Output formats**: Add support for Parquet, CSV, or other formats

### Testing

Run the generator in demo mode to validate output:

```bash
uv run python run_generator.py --demo --verbose --output-dir ./test_output
```

## Use Cases

This synthetic data is ideal for:

- **Databricks analytics development** without real IoT infrastructure
- **Machine learning model training** for predictive maintenance
- **Dashboard and visualization development**
- **Data pipeline testing** and validation
- **Performance testing** with large datasets

## License

This project is provided as-is for educational and development purposes.
