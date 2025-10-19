# Databricks notebook source

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from sklearn.preprocessing import LabelEncoder, StandardScaler, RobustScaler
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    classification_report,
    confusion_matrix,
)
from sklearn.utils.class_weight import compute_class_weight
import mlflow
import mlflow.sklearn
from datetime import datetime
import warnings
import logging

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

mlflow.set_registry_uri("databricks-uc")


def load_and_explore_data():
    """Load data from gold layer and perform initial exploration."""
    logger.info("Loading data from gold layer...")

    df = (
        spark.read.table("production.gold.predictive_maintenance_features")
        .filter(F.col("has_sufficient_data") == 1)
        .filter(F.col("reading_count") >= 10)
        .toPandas()
    )

    logger.info(f"Dataset shape: {df.shape}")
    logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

    target_dist = df["will_fail"].value_counts()
    logger.info(f"Target variable distribution: {target_dist.to_dict()}")
    logger.info(f"Class imbalance ratio: {target_dist[0]/target_dist[1]:.2f}:1")

    missing_data = df.isnull().sum()
    if missing_data.any():
        logger.warning("Missing values found:")
        for col, count in missing_data[missing_data > 0].items():
            logger.warning(f"  {col}: {count}")
    else:
        logger.info("No missing values found.")

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    logger.info(f"Dataset has {len(numeric_cols)} numerical features")
    logger.debug(f"Numerical features summary:\n{df[numeric_cols].describe()}")

    return df


def preprocess_features(df):
    """Comprehensive feature preprocessing for Gradient Boosting."""
    logger.info("Starting feature preprocessing...")

    processed_df = df.copy()

    initial_missing = processed_df.isnull().sum()
    if initial_missing.any():
        logger.warning("Initial missing values found:")
        for col, count in initial_missing[initial_missing > 0].items():
            logger.warning(f"  {col}: {count}")

    if "hour_bucket" in processed_df.columns:
        logger.info("Processing datetime features from hour_bucket column")
        processed_df["hour_bucket"] = pd.to_datetime(processed_df["hour_bucket"])
        processed_df["hour_of_day"] = processed_df["hour_bucket"].dt.hour
        processed_df["day_of_week"] = processed_df["hour_bucket"].dt.dayofweek
        processed_df["month"] = processed_df["hour_bucket"].dt.month
        processed_df.drop("hour_bucket", axis=1, inplace=True)

    # CRITICAL: Fill missing values before any operations to prevent NaN propagation
    numerical_cols = processed_df.select_dtypes(include=[np.number]).columns
    for col in numerical_cols:
        if processed_df[col].isnull().any():
            median_value = processed_df[col].median()
            processed_df[col] = processed_df[col].fillna(median_value)
            logger.info(f"Filled {col} NaN values with median: {median_value}")

    categorical_cols = ["device_type", "location", "failure_type", "severity"]
    label_encoders = {}

    for col in categorical_cols:
        if col in processed_df.columns:
            le = LabelEncoder()
            processed_df[col] = processed_df[col].fillna("Unknown")
            processed_df[f"{col}_encoded"] = le.fit_transform(
                processed_df[col].astype(str)
            )
            label_encoders[col] = le
            if col == "device_type":
                # Create binary features for each device type
                device_types = processed_df[col].unique()
                for dtype in device_types:
                    processed_df[f"is_{dtype}"] = (processed_df[col] == dtype).astype(
                        int
                    )

            processed_df.drop(col, axis=1, inplace=True)

    if "avg_temperature" in processed_df.columns:
        processed_df["temp_deviation"] = abs(processed_df["avg_temperature"] - 70)
        processed_df["temp_risk"] = (
            (processed_df["avg_temperature"] > 75)
            | (processed_df["avg_temperature"] < 65)
        ).astype(int)
    if "avg_vibration" in processed_df.columns:
        processed_df["vibration_risk"] = (processed_df["avg_vibration"] > 8).astype(int)
        processed_df["vibration_squared"] = processed_df["avg_vibration"] ** 2

    # Maintenance-based features
    if "days_since_maintenance" in processed_df.columns:
        processed_df["maintenance_overdue"] = (
            processed_df["days_since_maintenance"] > 180
        ).astype(int)
        processed_df["maintenance_urgent"] = (
            processed_df["days_since_maintenance"] > 365
        ).astype(int)
        processed_df["log_days_maintenance"] = np.log1p(
            processed_df["days_since_maintenance"].fillna(0)
        )

    # Composite health indicators
    health_features = ["high_temperature", "high_vibration", "needs_maintenance_soon"]
    if all(col in processed_df.columns for col in health_features):
        # Fill any missing values in health features with 0
        for col in health_features:
            processed_df[col] = processed_df[col].fillna(0)

        processed_df["total_risk_factors"] = sum(
            processed_df[col] for col in health_features
        )
        processed_df["any_risk_factor"] = (
            processed_df["total_risk_factors"] > 0
        ).astype(int)

    # Power consumption features (safe division)
    if (
        "avg_power" in processed_df.columns
        and "avg_temperature" in processed_df.columns
    ):
        # Avoid division by zero
        temp_safe = processed_df["avg_temperature"].replace(0, 1)
        processed_df["power_efficiency"] = processed_df["avg_power"] / temp_safe
        processed_df["high_power_consumption"] = (
            processed_df["avg_power"] > processed_df["avg_power"].quantile(0.75)
        ).astype(int)

    # Data quality features
    if "reading_count" in processed_df.columns:
        processed_df["data_quality_score"] = np.minimum(
            processed_df["reading_count"] / 50, 1.0
        )  # Normalize to [0,1]
        processed_df["low_data_quality"] = (processed_df["reading_count"] < 20).astype(
            int
        )

    # 5. Handle outliers using IQR method for numerical features
    numerical_features = processed_df.select_dtypes(include=[np.number]).columns
    numerical_features = [
        col for col in numerical_features if col not in ["will_fail", "device_id"]
    ]

    for col in numerical_features:
        if processed_df[col].nunique() > 10:  # Skip binary/categorical encoded features
            # Check for any remaining NaN values
            if processed_df[col].isnull().any():
                processed_df[col] = processed_df[col].fillna(processed_df[col].median())

            Q1 = processed_df[col].quantile(0.25)
            Q3 = processed_df[col].quantile(0.75)
            IQR = Q3 - Q1

            # Handle case where IQR is 0 (all values are the same)
            if IQR == 0:
                continue

            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            # Cap outliers instead of removing them
            processed_df[col] = np.clip(processed_df[col], lower_bound, upper_bound)

    # 6. Final NaN check and cleanup
    final_missing = processed_df.isnull().sum()
    if final_missing.any():
        logger.warning("Still have missing values after preprocessing:")
        for col, count in final_missing[final_missing > 0].items():
            logger.warning(f"  {col}: {count}")
            # Fill any remaining NaN with 0 or column mean
            if processed_df[col].dtype in ["float64", "int64"]:
                fill_value = (
                    processed_df[col].mean()
                    if not processed_df[col].mean() != processed_df[col].mean()
                    else 0
                )
                processed_df[col] = processed_df[col].fillna(fill_value)
            else:
                processed_df[col] = processed_df[col].fillna(0)

    # 7. Remove columns that won't be used for training
    columns_to_remove = [
        "device_id",
        "failure_type_encoded",
        "severity_encoded",
        "days_before_failure",
    ]
    for col in columns_to_remove:
        if col in processed_df.columns:
            processed_df.drop(col, axis=1, inplace=True)

    logger.info(
        f"Removed {len([col for col in columns_to_remove if col in df.columns])} columns not needed for training"
    )

    # 8. Final validation - ensure no NaN values remain
    final_check = processed_df.isnull().sum().sum()
    if final_check > 0:
        logger.error(f"{final_check} NaN values still present!")
        # Force fill all remaining NaN with 0
        processed_df = processed_df.fillna(0)
        logger.warning("Filled all remaining NaN values with 0")
    else:
        logger.info("✓ No NaN values found after preprocessing")

    # 9. Ensure all columns are numeric (except target)
    for col in processed_df.columns:
        if col != "will_fail":
            processed_df[col] = pd.to_numeric(processed_df[col], errors="coerce")

    # Final NaN fill after conversion
    processed_df = processed_df.fillna(0)

    logger.info(f"Preprocessing completed. Final shape: {processed_df.shape}")
    logger.info(
        f"Number of features: {processed_df.shape[1] - 1}"
    )  # Exclude target column
    logger.debug(f"Feature columns: {list(processed_df.columns)}")
    logger.debug(f"Data types: {processed_df.dtypes.value_counts().to_dict()}")

    return processed_df, label_encoders


# COMMAND ----------


def prepare_train_test_split(df, test_size=0.2, random_state=42):
    """
    Prepare training and testing datasets with proper stratification
    """
    logger.info("Preparing train-test split...")

    # Separate features and target
    X = df.drop("will_fail", axis=1)
    y = df["will_fail"]

    logger.info(f"Features shape: {X.shape}")
    logger.info(f"Target shape: {y.shape}")

    # Stratified split to maintain class distribution
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    logger.info(f"Training set: {X_train.shape[0]} samples")
    logger.info(f"Test set: {X_test.shape[0]} samples")
    logger.info(f"Training set class distribution: {y_train.value_counts().to_dict()}")
    logger.info(f"Test set class distribution: {y_test.value_counts().to_dict()}")

    return X_train, X_test, y_train, y_test


# COMMAND ----------


def validate_data_for_training(X_train, X_test, y_train, y_test):
    """
    Validate data before training to catch common issues
    """
    logger.info("Validating data for training...")

    # Check for NaN values
    train_nan = X_train.isnull().sum().sum()
    test_nan = X_test.isnull().sum().sum()
    target_train_nan = y_train.isnull().sum()
    target_test_nan = y_test.isnull().sum()

    if train_nan > 0:
        logger.error(f"Training features contain {train_nan} NaN values!")
        return False
    if test_nan > 0:
        logger.error(f"Test features contain {test_nan} NaN values!")
        return False
    if target_train_nan > 0:
        logger.error(f"Training target contains {target_train_nan} NaN values!")
        return False
    if target_test_nan > 0:
        logger.error(f"Test target contains {target_test_nan} NaN values!")
        return False

    # Check for infinite values
    train_inf = np.isinf(X_train.select_dtypes(include=[np.number])).sum().sum()
    test_inf = np.isinf(X_test.select_dtypes(include=[np.number])).sum().sum()

    if train_inf > 0:
        logger.error(f"Training features contain {train_inf} infinite values!")
        return False
    if test_inf > 0:
        logger.error(f"Test features contain {test_inf} infinite values!")
        return False

    # Check data types
    non_numeric_train = X_train.select_dtypes(exclude=[np.number]).columns
    non_numeric_test = X_test.select_dtypes(exclude=[np.number]).columns

    if len(non_numeric_train) > 0:
        logger.warning(
            f"Non-numeric columns in training data: {list(non_numeric_train)}"
        )
    if len(non_numeric_test) > 0:
        logger.warning(f"Non-numeric columns in test data: {list(non_numeric_test)}")

    # Check for constant features
    constant_features = [col for col in X_train.columns if X_train[col].nunique() <= 1]
    if constant_features:
        logger.warning(f"Constant features found: {constant_features}")

    logger.info("✓ Data validation passed!")
    return True


def scale_features(X_train, X_test, scaler_type="robust"):
    """
    Scale numerical features using RobustScaler (better for outliers) or StandardScaler
    """
    logger.info(f"Scaling features using {scaler_type} scaler...")

    if scaler_type == "robust":
        scaler = RobustScaler()
    else:
        scaler = StandardScaler()

    # Identify numerical columns that need scaling
    numerical_cols = X_train.select_dtypes(include=[np.number]).columns
    binary_cols = [col for col in numerical_cols if X_train[col].nunique() <= 2]
    cols_to_scale = [col for col in numerical_cols if col not in binary_cols]

    logger.info(f"Scaling {len(cols_to_scale)} numerical features")
    logger.debug(f"Features to scale: {list(cols_to_scale)}")

    X_train_scaled = X_train.copy()
    X_test_scaled = X_test.copy()

    # Fit scaler on training data and transform both sets
    if cols_to_scale:
        # Check for NaN before scaling
        if X_train[cols_to_scale].isnull().any().any():
            logger.warning(
                "NaN values found in features to scale. Filling with median..."
            )
            for col in cols_to_scale:
                median_val = X_train[col].median()
                X_train_scaled[col] = X_train[col].fillna(median_val)
                X_test_scaled[col] = X_test[col].fillna(median_val)

        X_train_scaled[cols_to_scale] = scaler.fit_transform(
            X_train_scaled[cols_to_scale]
        )
        X_test_scaled[cols_to_scale] = scaler.transform(X_test_scaled[cols_to_scale])
        logger.info("Feature scaling completed successfully")

    return X_train_scaled, X_test_scaled, scaler


# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Training and Hyperparameter Tuning

# COMMAND ----------


def train_gradient_boosting_model(
    X_train, y_train, X_test, y_test, use_grid_search=True
):
    """
    Train Gradient Boosting classifier with optional hyperparameter tuning
    """
    logger.info("Training Gradient Boosting classifier...")

    # Calculate class weights to handle imbalance
    class_weights = compute_class_weight(
        "balanced", classes=np.unique(y_train), y=y_train
    )
    class_weight_dict = dict(zip(np.unique(y_train), class_weights))
    logger.info(f"Calculated class weights: {class_weight_dict}")

    # Start MLflow run
    with mlflow.start_run(
        run_name=f"gradient_boost_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    ):

        if use_grid_search:
            logger.info("Performing hyperparameter tuning...")

            # Define simplified parameter grid for initial testing
            param_grid = {
                "n_estimators": [50, 100],
                "max_depth": [3, 5],
                "learning_rate": [0.1, 0.2],
                "min_samples_split": [5, 10],
            }

            # Create base model
            gb_model = GradientBoostingClassifier(random_state=42, verbose=0)

            # Use StratifiedKFold for cross-validation
            cv = StratifiedKFold(
                n_splits=3, shuffle=True, random_state=42
            )  # Reduced folds

            # Grid search with cross-validation
            try:
                grid_search = GridSearchCV(
                    estimator=gb_model,
                    param_grid=param_grid,
                    cv=cv,
                    scoring="roc_auc",  # Use AUC for imbalanced classification
                    n_jobs=1,  # Use single job to avoid issues
                    verbose=0,  # Reduce verbosity for cleaner logs
                    error_score="raise",  # This will help debug issues
                )

                # Fit the model
                grid_search.fit(X_train, y_train)

                # Get best model
                best_model = grid_search.best_estimator_

                # Log best parameters
                mlflow.log_params(grid_search.best_params_)
                mlflow.log_param("cv_score", grid_search.best_score_)

                logger.info(f"Best parameters: {grid_search.best_params_}")
                logger.info(f"Best CV score: {grid_search.best_score_:.4f}")

            except Exception as e:
                logger.error(f"Grid search failed: {e}")
                logger.info("Falling back to default parameters...")
                use_grid_search = False

        if not use_grid_search:
            logger.info("Using default parameters...")
            # Train with default parameters
            best_model = GradientBoostingClassifier(
                n_estimators=100,
                max_depth=5,
                learning_rate=0.1,
                min_samples_split=10,
                random_state=42,
                verbose=0,
            )

            best_model.fit(X_train, y_train)

            # Log default parameters
            mlflow.log_param("n_estimators", 100)
            mlflow.log_param("max_depth", 5)
            mlflow.log_param("learning_rate", 0.1)
            mlflow.log_param("min_samples_split", 10)
            mlflow.log_param("tuning_method", "default_params")

        # Make predictions
        y_train_pred = best_model.predict(X_train)
        y_test_pred = best_model.predict(X_test)
        y_test_proba = best_model.predict_proba(X_test)[:, 1]

        # Calculate metrics
        metrics = {}

        # Training metrics
        metrics["train_accuracy"] = accuracy_score(y_train, y_train_pred)
        metrics["train_precision"] = precision_score(
            y_train, y_train_pred, zero_division=0
        )
        metrics["train_recall"] = recall_score(y_train, y_train_pred, zero_division=0)
        metrics["train_f1"] = f1_score(y_train, y_train_pred, zero_division=0)

        # Test metrics
        metrics["test_accuracy"] = accuracy_score(y_test, y_test_pred)
        metrics["test_precision"] = precision_score(
            y_test, y_test_pred, zero_division=0
        )
        metrics["test_recall"] = recall_score(y_test, y_test_pred, zero_division=0)
        metrics["test_f1"] = f1_score(y_test, y_test_pred, zero_division=0)
        metrics["test_auc"] = roc_auc_score(y_test, y_test_proba)

        # Log all metrics
        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, metric_value)

        # Feature importance
        feature_importance = pd.DataFrame(
            {"feature": X_train.columns, "importance": best_model.feature_importances_}
        ).sort_values("importance", ascending=False)

        logger.info("Top 10 Feature Importances:")
        for _, row in feature_importance.head(10).iterrows():
            logger.info(f"  {row['feature']}: {row['importance']:.4f}")

        # Log model
        mlflow.sklearn.log_model(
            sk_model=best_model,
            registered_model_name="production.gold.predictive_maintenance_gb",
        )
        # Log results
        logger.info("=== Model Performance ===")
        logger.info(f"Test Accuracy: {metrics['test_accuracy']:.4f}")
        logger.info(f"Test Precision: {metrics['test_precision']:.4f}")
        logger.info(f"Test Recall: {metrics['test_recall']:.4f}")
        logger.info(f"Test F1 Score: {metrics['test_f1']:.4f}")
        logger.info(f"Test AUC: {metrics['test_auc']:.4f}")

        logger.debug("Classification Report:")
        logger.debug(f"\n{classification_report(y_test, y_test_pred)}")

        logger.debug("Confusion Matrix:")
        logger.debug(f"\n{confusion_matrix(y_test, y_test_pred)}")

        return best_model, metrics, feature_importance


# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution Pipeline

# COMMAND ----------


def main():
    """
    Main execution pipeline
    """
    logger.info("=== Starting Predictive Maintenance Model Training ===")

    try:
        # 1. Load and explore data
        logger.info("STEP 1: Loading and exploring data")
        df = load_and_explore_data()

        # 2. Preprocess features
        logger.info("STEP 2: Preprocessing features")
        processed_df, label_encoders = preprocess_features(df)

        # 3. Prepare train-test split
        logger.info("STEP 3: Preparing train-test split")
        X_train, X_test, y_train, y_test = prepare_train_test_split(processed_df)

        # 4. Validate data before scaling
        logger.info("STEP 4: Validating data quality")
        if not validate_data_for_training(X_train, X_test, y_train, y_test):
            raise ValueError(
                "Data validation failed. Please check the preprocessing steps."
            )

        # 5. Scale features
        logger.info("STEP 5: Scaling features")
        X_train_scaled, X_test_scaled, scaler = scale_features(X_train, X_test)

        # 6. Final validation after scaling
        logger.info("STEP 6: Final validation after scaling")
        if not validate_data_for_training(
            X_train_scaled, X_test_scaled, y_train, y_test
        ):
            raise ValueError("Data validation failed after scaling.")

        # 7. Train model
        logger.info("STEP 7: Training Gradient Boosting model")
        model, metrics, feature_importance = train_gradient_boosting_model(
            X_train_scaled, y_train, X_test_scaled, y_test
        )

        logger.info("=== Training completed successfully! ===")

        return model, metrics, feature_importance, scaler, label_encoders

    except Exception as e:
        logger.error(f"ERROR in training pipeline: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback

        logger.error(traceback.format_exc())
        raise e


# Execute the main pipeline
model, metrics, feature_importance, scaler, label_encoders = main()
