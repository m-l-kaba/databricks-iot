# Industrial IoT Predictive Maintenance Platform

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/)
[![Terraform](https://img.shields.io/badge/Terraform-623CE4?style=for-the-badge&logo=terraform&logoColor=white)](https://terraform.io/)
[![MLflow](https://img.shields.io/badge/MLflow-0194E2?style=for-the-badge&logo=mlflow&logoColor=white)](https://mlflow.org/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)

> **End-to-end MLOps platform** demonstrating production-ready machine learning pipelines on Databricks with Unity Catalog, automated deployment workflows, and comprehensive data engineering practices for industrial IoT predictive maintenance.

## Project Overview

This project showcases a **complete MLOps platform** built on Databricks, implementing predictive maintenance for industrial IoT devices. It demonstrates data engineering, machine learning, and MLOps best practices with automated CI/CD deployment.

### Key Capabilities

- **Medallion Architecture**: Bronze → Silver → Gold data layers with Delta Lake
- **Delta Live Tables**: Serverless declarative ETL pipelines
- **Unity Catalog**: Model registry with Challenger/Champion alias-based deployment
- **MLOps Automation**: Complete training, deployment, and inference pipelines
- **Synthetic Data Generation**: Realistic IoT device simulator with configurable failure patterns
- **CI/CD Integration**: GitHub Actions workflow for automated bundle deployment

## Technical Architecture

### Data Pipeline (Medallion Architecture)

```
Azure Data Lake Gen2 (Raw Data)
    ↓
Bronze Layer: Raw ingestion with Auto Loader
    ↓
Silver Layer: Data cleansing and validation
    ↓
Gold Layer: Feature engineering for ML
    ↓
ML Pipeline: Training → Deployment → Inference
```

### MLOps Workflow

```
1. Training Pipeline (training.py)
   - Loads gold features
   - Trains Gradient Boosting pipeline with custom preprocessing
   - Logs model to Unity Catalog with Challenger alias
   - Tracks training cutoff timestamp for inference filtering

2. Deployment Pipeline (deployment.py)
   - Retrieves Challenger model
   - Promotes current Champion to Retired
   - Promotes Challenger to Champion

3. Inference Pipeline (inference.py)
   - Loads Champion model from Unity Catalog
   - Filters new data based on training cutoff
   - Generates predictions
   - Saves to gold.predictive_maintenance_predictions
```

## Technology Stack

### Core Platform

- **Databricks**: Unified analytics platform with serverless compute
- **Delta Lake**: ACID transactions, time travel, schema evolution
- **Unity Catalog**: Centralized governance, model registry with aliases
- **Delta Live Tables**: Declarative ETL with data quality expectations

### Machine Learning

- **MLflow**: Experiment tracking, model registry with Unity Catalog integration
- **Scikit-learn**: Gradient Boosting with custom preprocessing pipeline
- **Python 3.13**: Modern type hints (PEP 685, PEP 604, PEP 673)
- **Feature Engineering**: Time-series aggregations, rolling windows, maintenance patterns

### Infrastructure & DevOps

- **Azure Data Lake Gen2**: Scalable cloud storage
- **Terraform**: Infrastructure as Code for Azure resources
- **GitHub Actions**: CI/CD with automated testing and deployment
- **Databricks CLI**: Bundle-based deployments
- **uv**: Fast Python package management

## Quick Start

### Prerequisites

- **Azure Subscription** with Databricks workspace
- **Python 3.13** or higher
- **uv** package manager
- **Azure CLI** configured
- **Databricks CLI** installed

### 1. Install Dependencies

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
uv sync
```

### 2. Generate Synthetic Data

```bash
# Generate 100 devices with 7 days of telemetry
make generate_data

# Or run directly with custom parameters
uv run python run_generator.py \
  --devices 100 \
  --telemetry-hours 168 \
  --failure-days 90 \
  --maintenance-days 180 \
  --upload-to-azure \
  --storage-account databricksiotsa

# Validate data coherence
uv run python validate_data_coherence.py
```

### 3. Deploy Infrastructure (Optional)

```bash
# Deploy Azure resources with Terraform
make deploy_infra

# Or manually
cd infrastructure/terraform
source .env
terraform init
terraform apply
```

### 4. Deploy Databricks Bundle

```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to production
databricks bundle deploy --target prod
```

### 5. Run the Pipeline

The deployment includes three automated jobs configured in `resources/`:

- **Delta Live Tables Pipeline** (`lakehouse_pipeline.yml`): Bronze → Silver → Gold ETL
- **Model Deployment** (`model_deployment.yml`): Promotes Challenger to Champion
- **Model Inference** (`model_inference.yml`): Runs predictions with Champion model

## Project Structure

```
databrick_iot/
├── src/
│   ├── data_simulation/          # IoT data generator
│   │   ├── iot_data_generator.py # Synthetic device/telemetry generator
│   │   ├── azure_uploader.py     # Upload to Azure Data Lake
│   │   └── config.py             # Data generation configuration
│   └── lakehouse/
│       ├── bronze/               # Raw data ingestion layer
│       │   └── bronze_ingestion.py
│       ├── silver/               # Data cleansing layer
│       │   └── silver_cleansing.py
│       ├── gold/                 # Feature engineering layer
│       │   └── gold_features.py
│       └── models/               # ML pipeline
│           ├── training.py       # Model training with MLflow
│           ├── deployment.py     # Challenger → Champion promotion
│           └── inference.py      # Champion model predictions
├── infrastructure/
│   └── terraform/                # IaC for Azure resources
│       ├── main.tf
│       ├── modules/
│       │   ├── azure/           # Azure Data Lake, Storage
│       │   └── databricks/      # Databricks workspace config
│       └── ...
├── resources/                    # Databricks Asset Bundles
│   ├── lakehouse_pipeline.yml   # DLT pipeline configuration
│   ├── model_deployment.yml     # Deployment job
│   └── model_inference.yml      # Inference job
├── tests/                        # Unit tests
│   ├── test_config.py
│   ├── test_iot_data_generator.py
│   └── conftest.py
├── .github/workflows/
│   └── deploy.yml               # CI/CD pipeline
├── databricks.yml               # Bundle configuration
├── config.yaml                  # Data generation config
├── Makefile                     # Common tasks
└── pyproject.toml               # Project dependencies
```

## Data Generation

The synthetic data generator creates realistic IoT scenarios:

### Device Types

- Temperature sensors, humidity sensors, pressure sensors
- Vibration sensors, flow meters, power meters
- Smart valves, motor controllers

### Generated Data

- **Device Master**: 100 devices across 5 facilities
- **Telemetry**: 5-minute intervals, 168 hours (7 days)
- **Failures**: Realistic MTBF patterns, multiple failure types
- **Maintenance**: Preventive, corrective, predictive events

### Configuration

All generation parameters are in `config.yaml`:

```yaml
devices:
  num_devices: 100

data_periods:
  telemetry_hours: 168
  failure_days: 90
  maintenance_days: 180

failures:
  mtbf_days: 45
```

## MLOps Pipeline Details

### Training Pipeline

**File**: `src/lakehouse/models/training.py`

**Key Features**:

- Custom `PredictiveMaintenancePreprocessor` class
- Complete scikit-learn pipeline (preprocessing + model)
- Python 3.13 type hints throughout
- MLflow logging with Unity Catalog signatures
- Tracks `latest_training_hour_bucket` for inference filtering

**Model Registration**:

```python
# Logs model to Unity Catalog with Challenger alias
mlflow.sklearn.log_model(
    pipeline,
    "model",
    registered_model_name="production.gold.predictive_maintenance_pipeline",
    signature=signature
)
client.set_registered_model_alias(model_name, "Challenger", version)
```

### Deployment Pipeline

**File**: `src/lakehouse/models/deployment.py`

**Deployment Strategy**:

1. Retrieve current Challenger model version
2. Retire existing Champion (move to Retired alias)
3. Promote Challenger to Champion
4. Clean up Challenger alias

**Unity Catalog Aliases**:

- **Challenger**: Newly trained model awaiting promotion
- **Champion**: Current production model
- **Retired**: Previous production model (rollback option)

### Inference Pipeline

**File**: `src/lakehouse/models/inference.py`

**Inference Flow**:

1. Load Champion model from Unity Catalog
2. Retrieve training cutoff timestamp from model metadata
3. Filter gold features for data after training cutoff
4. Generate predictions with Champion model
5. Save predictions to `production.gold.predictive_maintenance_predictions`

**Key Feature**: Prevents data leakage by only scoring data newer than training set

## CI/CD Workflow

**File**: `.github/workflows/deploy.yml`

### Automated Steps

1. **Code checkout**
2. **Python 3.13 setup** with uv package manager
3. **Unit tests** with pytest
4. **Databricks CLI setup**
5. **Bundle validation**
6. **Production deployment** using service principal authentication

### Secrets Required

- `DATABRICKS_HOST`
- `ARM_TENANT_ID`
- `ARM_CLIENT_ID`
- `ARM_CLIENT_SECRET`

## Testing

```bash
# Run all tests
uv run pytest tests/ -v

# Run with coverage
uv run pytest tests/ --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/test_iot_data_generator.py -v
```

## Professional Skills Demonstrated

### Databricks & Data Engineering

✅ Delta Live Tables with declarative ETL  
✅ Unity Catalog for governance and model registry  
✅ Medallion architecture (Bronze/Silver/Gold)  
✅ Serverless compute optimization  
✅ Delta Lake for ACID transactions

### MLOps & Machine Learning

✅ End-to-end ML pipeline (training → deployment → inference)  
✅ Unity Catalog alias-based deployment strategy  
✅ Custom scikit-learn transformers and pipelines  
✅ MLflow experiment tracking and model versioning  
✅ Training metadata tracking for inference filtering  
✅ Data leakage prevention in inference pipeline

### Software Engineering

✅ Modern Python 3.13 with type hints (PEP 685, 604, 673)  
✅ Comprehensive unit testing with pytest  
✅ Clean, maintainable code architecture  
✅ Configuration-driven design (YAML configs)  
✅ CI/CD automation with GitHub Actions

### Cloud & DevOps

✅ Infrastructure as Code with Terraform  
✅ Azure Data Lake Gen2 integration  
✅ Databricks Asset Bundles for deployment  
✅ Service principal authentication  
✅ Automated testing and deployment pipelines

## Contact & Collaboration

**Available for freelance data engineering and MLOps projects**

- **LinkedIn**: https://www.linkedin.com/in/mory-kaba-80b5641a0/
- **GitHub**: https://github.com/m-l-kaba

### Expertise Areas

- Databricks Platform Engineering
- MLOps Pipeline Development
- Azure Cloud Architecture
- Data Engineering & ETL
- Machine Learning in Production

---
