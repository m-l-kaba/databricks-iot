import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from sklearn.preprocessing import LabelEncoder, StandardScaler, RobustScaler
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.base import BaseEstimator, TransformerMixin
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
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient

from datetime import datetime
import warnings
import logging
from typing import Self


warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

mlflow.set_registry_uri("databricks-uc")


class PredictiveMaintenancePreprocessor(BaseEstimator, TransformerMixin):

    def __init__(self) -> None:
        self.label_encoders: dict[str, LabelEncoder] = {}
        self.numerical_medians: dict[str, float] = {}
        self.fitted: bool = False

    def fit(self, X: pd.DataFrame, y: pd.Series | None = None) -> Self:
        logger.info("Fitting preprocessor...")

        X_copy = X.copy()

        # Handle datetime features
        if "hour_bucket" in X_copy.columns:
            X_copy["hour_bucket"] = pd.to_datetime(X_copy["hour_bucket"])
            X_copy["hour_of_day"] = X_copy["hour_bucket"].dt.hour
            X_copy["day_of_week"] = X_copy["hour_bucket"].dt.dayofweek
            X_copy["month"] = X_copy["hour_bucket"].dt.month
            X_copy.drop("hour_bucket", axis=1, inplace=True)

        numerical_cols = X_copy.select_dtypes(include=[np.number]).columns
        for col in numerical_cols:
            if X_copy[col].isnull().any():
                self.numerical_medians[col] = X_copy[col].median()

        categorical_cols = ["device_type", "location", "failure_type", "severity"]
        for col in categorical_cols:
            if col in X_copy.columns:
                le = LabelEncoder()
                X_copy[col] = X_copy[col].fillna("Unknown")
                le.fit(X_copy[col].astype(str))
                self.label_encoders[col] = le

        self.fitted = True
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if not self.fitted:
            raise ValueError("Preprocessor must be fitted before transform")

        logger.info("Applying preprocessing transformation...")
        processed_df = X.copy()

        # Handle datetime features
        if "hour_bucket" in processed_df.columns:
            processed_df["hour_bucket"] = pd.to_datetime(processed_df["hour_bucket"])
            processed_df["hour_of_day"] = processed_df["hour_bucket"].dt.hour
            processed_df["day_of_week"] = processed_df["hour_bucket"].dt.dayofweek
            processed_df["month"] = processed_df["hour_bucket"].dt.month
            processed_df.drop("hour_bucket", axis=1, inplace=True)

        for col, median_val in self.numerical_medians.items():
            if col in processed_df.columns and processed_df[col].isnull().any():
                processed_df[col] = processed_df[col].fillna(median_val)

        for col, le in self.label_encoders.items():
            if col in processed_df.columns:
                processed_df[col] = processed_df[col].fillna("Unknown")
                processed_df[f"{col}_encoded"] = le.transform(
                    processed_df[col].astype(str)
                )

                if col == "device_type":
                    device_types = le.classes_
                    for dtype in device_types:
                        processed_df[f"is_{dtype}"] = (
                            processed_df[col] == dtype
                        ).astype(int)

                processed_df.drop(col, axis=1, inplace=True)

        if "avg_temperature" in processed_df.columns:
            processed_df["temp_deviation"] = abs(processed_df["avg_temperature"] - 70)
            processed_df["temp_risk"] = (
                (processed_df["avg_temperature"] > 75)
                | (processed_df["avg_temperature"] < 65)
            ).astype(int)

        if "avg_vibration" in processed_df.columns:
            processed_df["vibration_risk"] = (processed_df["avg_vibration"] > 8).astype(
                int
            )
            processed_df["vibration_squared"] = processed_df["avg_vibration"] ** 2

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

        health_features = [
            "high_temperature",
            "high_vibration",
            "needs_maintenance_soon",
        ]
        if all(col in processed_df.columns for col in health_features):
            for col in health_features:
                processed_df[col] = processed_df[col].fillna(0)

            processed_df["total_risk_factors"] = sum(
                processed_df[col] for col in health_features
            )
            processed_df["any_risk_factor"] = (
                processed_df["total_risk_factors"] > 0
            ).astype(int)

        if (
            "avg_power" in processed_df.columns
            and "avg_temperature" in processed_df.columns
        ):
            temp_safe = processed_df["avg_temperature"].replace(0, 1)
            processed_df["power_efficiency"] = processed_df["avg_power"] / temp_safe
            processed_df["high_power_consumption"] = (
                processed_df["avg_power"] > processed_df["avg_power"].quantile(0.75)
            ).astype(int)

        if "reading_count" in processed_df.columns:
            processed_df["data_quality_score"] = np.minimum(
                processed_df["reading_count"] / 50, 1.0
            )
            processed_df["low_data_quality"] = (
                processed_df["reading_count"] < 20
            ).astype(int)

        numerical_features = processed_df.select_dtypes(include=[np.number]).columns
        numerical_features = [
            col for col in numerical_features if col not in ["will_fail", "device_id"]
        ]

        for col in numerical_features:
            if processed_df[col].nunique() > 10:
                if processed_df[col].isnull().any():
                    processed_df[col] = processed_df[col].fillna(
                        processed_df[col].median()
                    )

                Q1 = processed_df[col].quantile(0.25)
                Q3 = processed_df[col].quantile(0.75)
                IQR = Q3 - Q1

                if IQR == 0:
                    continue

                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                processed_df[col] = np.clip(processed_df[col], lower_bound, upper_bound)

        final_missing = processed_df.isnull().sum()
        if final_missing.any():
            for col, count in final_missing[final_missing > 0].items():
                if count > 0:
                    if processed_df[col].dtype in ["float64", "int64"]:
                        fill_value = (
                            processed_df[col].mean()
                            if not pd.isna(processed_df[col].mean())
                            else 0
                        )
                        processed_df[col] = processed_df[col].fillna(fill_value)
                    else:
                        processed_df[col] = processed_df[col].fillna(0)

        columns_to_remove = [
            "device_id",
            "failure_type_encoded",
            "severity_encoded",
            "days_before_failure",
            "failure_type",  # Remove original too
            "severity",  # Remove original too
        ]
        for col in columns_to_remove:
            if col in processed_df.columns:
                processed_df.drop(col, axis=1, inplace=True)

        for col in processed_df.columns:
            if col != "will_fail":
                processed_df[col] = pd.to_numeric(processed_df[col], errors="coerce")

        processed_df = processed_df.fillna(0)

        return processed_df


def load_and_explore_data() -> pd.DataFrame:
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


def preprocess_features(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, LabelEncoder]]:
    logger.info("Starting feature preprocessing...")

    processed_df: pd.DataFrame = df.copy()

    initial_missing: pd.Series = processed_df.isnull().sum()
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

    numerical_cols: pd.Index = processed_df.select_dtypes(include=[np.number]).columns
    for col in numerical_cols:
        if processed_df[col].isnull().any():
            median_value: float = processed_df[col].median()
            processed_df[col] = processed_df[col].fillna(median_value)
            logger.info(f"Filled {col} NaN values with median: {median_value}")

    categorical_cols: list[str] = [
        "device_type",
        "location",
        "failure_type",
        "severity",
    ]
    label_encoders: dict[str, LabelEncoder] = {}

    for col in categorical_cols:
        if col in processed_df.columns:
            le = LabelEncoder()
            processed_df[col] = processed_df[col].fillna("Unknown")
            processed_df[f"{col}_encoded"] = le.fit_transform(
                processed_df[col].astype(str)
            )
            label_encoders[col] = le
            if col == "device_type":
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

    health_features = ["high_temperature", "high_vibration", "needs_maintenance_soon"]
    if all(col in processed_df.columns for col in health_features):
        for col in health_features:
            processed_df[col] = processed_df[col].fillna(0)

        processed_df["total_risk_factors"] = sum(
            processed_df[col] for col in health_features
        )
        processed_df["any_risk_factor"] = (
            processed_df["total_risk_factors"] > 0
        ).astype(int)

    if (
        "avg_power" in processed_df.columns
        and "avg_temperature" in processed_df.columns
    ):
        temp_safe = processed_df["avg_temperature"].replace(0, 1)
        processed_df["power_efficiency"] = processed_df["avg_power"] / temp_safe
        processed_df["high_power_consumption"] = (
            processed_df["avg_power"] > processed_df["avg_power"].quantile(0.75)
        ).astype(int)

    if "reading_count" in processed_df.columns:
        processed_df["data_quality_score"] = np.minimum(
            processed_df["reading_count"] / 50, 1.0
        )
        processed_df["low_data_quality"] = (processed_df["reading_count"] < 20).astype(
            int
        )

    numerical_features = processed_df.select_dtypes(include=[np.number]).columns
    numerical_features = [
        col for col in numerical_features if col not in ["will_fail", "device_id"]
    ]

    for col in numerical_features:
        if processed_df[col].nunique() > 10:
            if processed_df[col].isnull().any():
                processed_df[col] = processed_df[col].fillna(processed_df[col].median())

            Q1 = processed_df[col].quantile(0.25)
            Q3 = processed_df[col].quantile(0.75)
            IQR = Q3 - Q1

            if IQR == 0:
                continue

            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            processed_df[col] = np.clip(processed_df[col], lower_bound, upper_bound)

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
        "failure_type",  # Remove original too
        "severity",  # Remove original too
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


def prepare_train_test_split(
    df: pd.DataFrame, test_size: float = 0.2, random_state: int = 42
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    logger.info("Preparing train-test split...")

    X = df.drop("will_fail", axis=1)
    y = df["will_fail"]

    logger.info(f"Features shape: {X.shape}")
    logger.info(f"Target shape: {y.shape}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    logger.info(f"Training set: {X_train.shape[0]} samples")
    logger.info(f"Test set: {X_test.shape[0]} samples")
    logger.info(f"Training set class distribution: {y_train.value_counts().to_dict()}")
    logger.info(f"Test set class distribution: {y_test.value_counts().to_dict()}")

    return X_train, X_test, y_train, y_test


def validate_data_for_training(
    X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series, y_test: pd.Series
) -> bool:
    logger.info("Validating data for training...")

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

    train_inf = np.isinf(X_train.select_dtypes(include=[np.number])).sum().sum()
    test_inf = np.isinf(X_test.select_dtypes(include=[np.number])).sum().sum()

    if train_inf > 0:
        logger.error(f"Training features contain {train_inf} infinite values!")
        return False
    if test_inf > 0:
        logger.error(f"Test features contain {test_inf} infinite values!")
        return False

    non_numeric_train = X_train.select_dtypes(exclude=[np.number]).columns
    non_numeric_test = X_test.select_dtypes(exclude=[np.number]).columns

    if len(non_numeric_train) > 0:
        logger.warning(
            f"Non-numeric columns in training data: {list(non_numeric_train)}"
        )
    if len(non_numeric_test) > 0:
        logger.warning(f"Non-numeric columns in test data: {list(non_numeric_test)}")

    constant_features = [col for col in X_train.columns if X_train[col].nunique() <= 1]
    if constant_features:
        logger.warning(f"Constant features found: {constant_features}")

    logger.info("✓ Data validation passed!")
    return True


def scale_features(
    X_train: pd.DataFrame, X_test: pd.DataFrame, scaler_type: str = "robust"
) -> tuple[pd.DataFrame, pd.DataFrame, RobustScaler | StandardScaler]:
    logger.info(f"Scaling features using {scaler_type} scaler...")

    if scaler_type == "robust":
        scaler = RobustScaler()
    else:
        scaler = StandardScaler()

    numerical_cols = X_train.select_dtypes(include=[np.number]).columns
    binary_cols = [col for col in numerical_cols if X_train[col].nunique() <= 2]
    cols_to_scale = [col for col in numerical_cols if col not in binary_cols]

    logger.info(f"Scaling {len(cols_to_scale)} numerical features")
    logger.debug(f"Features to scale: {list(cols_to_scale)}")

    X_train_scaled = X_train.copy()
    X_test_scaled = X_test.copy()

    if cols_to_scale:
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


def create_complete_pipeline() -> Pipeline:
    logger.info("Creating complete ML pipeline...")

    preprocessor = PredictiveMaintenancePreprocessor()

    scaler = RobustScaler()

    complete_pipeline = Pipeline(
        [
            ("preprocessor", preprocessor),
            ("scaler", scaler),
            ("classifier", GradientBoostingClassifier(random_state=42, verbose=0)),
        ]
    )

    return complete_pipeline


def train_gradient_boosting_model_with_pipeline(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    original_df: pd.DataFrame,
    use_grid_search: bool = True,
) -> tuple[Pipeline, dict[str, float], pd.DataFrame]:
    logger.info("Training complete Gradient Boosting pipeline...")

    class_weights: np.ndarray = compute_class_weight(
        "balanced", classes=np.unique(y_train), y=y_train
    )
    class_weight_dict: dict[int, float] = dict(zip(np.unique(y_train), class_weights))
    logger.info(f"Calculated class weights: {class_weight_dict}")

    client = MlflowClient()

    # Get latest timestamp from training data
    latest_training_timestamp: pd.Timestamp = original_df["hour_bucket"].max()
    logger.info(f"Latest training timestamp: {latest_training_timestamp}")

    with mlflow.start_run(
        run_name=f"pipeline_gradient_boost_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    ):
        # Log the latest training timestamp for inference filtering
        mlflow.log_param(
            "latest_training_hour_bucket", latest_training_timestamp.isoformat()
        )
        logger.info(f"Logged latest_training_hour_bucket: {latest_training_timestamp}")

        if use_grid_search:
            logger.info("Performing hyperparameter tuning...")

            pipeline: Pipeline = create_complete_pipeline()

            param_grid: dict[str, list[int | float]] = {
                "classifier__n_estimators": [50, 100],
                "classifier__max_depth": [3, 5],
                "classifier__learning_rate": [0.1, 0.2],
                "classifier__min_samples_split": [5, 10],
            }

            cv: StratifiedKFold = StratifiedKFold(
                n_splits=3, shuffle=True, random_state=42
            )

            try:
                grid_search = GridSearchCV(
                    estimator=pipeline,
                    param_grid=param_grid,
                    cv=cv,
                    scoring="roc_auc",
                    n_jobs=1,
                    verbose=0,
                    error_score="raise",
                )

                grid_search.fit(X_train, y_train)

                best_pipeline: Pipeline = grid_search.best_estimator_

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
            best_pipeline: Pipeline = create_complete_pipeline()

            best_pipeline.fit(X_train, y_train)

            mlflow.log_param("classifier__n_estimators", 100)
            mlflow.log_param("classifier__max_depth", 5)
            mlflow.log_param("classifier__learning_rate", 0.1)
            mlflow.log_param("classifier__min_samples_split", 10)
            mlflow.log_param("tuning_method", "default_params")

        y_train_pred: np.ndarray = best_pipeline.predict(X_train)
        y_test_pred: np.ndarray = best_pipeline.predict(X_test)
        y_test_proba: np.ndarray = best_pipeline.predict_proba(X_test)[:, 1]

        metrics: dict[str, float] = {}

        metrics["train_accuracy"] = accuracy_score(y_train, y_train_pred)
        metrics["train_precision"] = precision_score(
            y_train, y_train_pred, zero_division=0
        )
        metrics["train_recall"] = recall_score(y_train, y_train_pred, zero_division=0)
        metrics["train_f1"] = f1_score(y_train, y_train_pred, zero_division=0)

        metrics["test_accuracy"] = accuracy_score(y_test, y_test_pred)
        metrics["test_precision"] = precision_score(
            y_test, y_test_pred, zero_division=0
        )
        metrics["test_recall"] = recall_score(y_test, y_test_pred, zero_division=0)
        metrics["test_f1"] = f1_score(y_test, y_test_pred, zero_division=0)
        metrics["test_auc"] = roc_auc_score(y_test, y_test_proba)

        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, metric_value)

        classifier: GradientBoostingClassifier = best_pipeline.named_steps["classifier"]

        preprocessor: PredictiveMaintenancePreprocessor = best_pipeline.named_steps[
            "preprocessor"
        ]

        X_train_preprocessed: pd.DataFrame = preprocessor.transform(X_train)
        feature_names: list[str] = X_train_preprocessed.columns.tolist()

        feature_importance = pd.DataFrame(
            {"feature": feature_names, "importance": classifier.feature_importances_}
        ).sort_values("importance", ascending=False)

        logger.info("Top 10 Feature Importances:")
        for _, row in feature_importance.head(10).iterrows():
            logger.info(f"  {row['feature']}: {row['importance']:.4f}")

        signature = infer_signature(X_train, y_test_pred)

        model_info = mlflow.sklearn.log_model(
            sk_model=best_pipeline,
            registered_model_name="production.gold.predictive_maintenance_pipeline",
            signature=signature,
        )

        client.set_registered_model_alias(
            "production.gold.predictive_maintenance_pipeline",
            "Challenger",
            model_info.registered_model_version,
        )

        logger.info(
            "Complete pipeline logged to MLflow with all preprocessing included!"
        )

        logger.info("=== Pipeline Performance ===")
        logger.info(f"Test Accuracy: {metrics['test_accuracy']:.4f}")
        logger.info(f"Test Precision: {metrics['test_precision']:.4f}")
        logger.info(f"Test Recall: {metrics['test_recall']:.4f}")
        logger.info(f"Test F1 Score: {metrics['test_f1']:.4f}")
        logger.info(f"Test AUC: {metrics['test_auc']:.4f}")

        logger.debug("Classification Report:")
        logger.debug(f"\n{classification_report(y_test, y_test_pred)}")

        logger.debug("Confusion Matrix:")
        logger.debug(f"\n{confusion_matrix(y_test, y_test_pred)}")

        return best_pipeline, metrics, feature_importance


def main() -> tuple[Pipeline, dict[str, float], pd.DataFrame]:
    logger.info("=== Starting Predictive Maintenance Pipeline Training ===")

    try:
        logger.info("STEP 1: Loading and exploring data")
        df: pd.DataFrame = load_and_explore_data()

        logger.info("STEP 2: Preparing train-test split with raw data")
        X: pd.DataFrame = df.drop("will_fail", axis=1)
        y: pd.Series = df["will_fail"]

        logger.info(f"Features shape: {X.shape}")
        logger.info(f"Target shape: {y.shape}")

        X_train: pd.DataFrame
        X_test: pd.DataFrame
        y_train: pd.Series
        y_test: pd.Series
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        logger.info(f"Training set: {X_train.shape[0]} samples")
        logger.info(f"Test set: {X_test.shape[0]} samples")
        logger.info(
            f"Training set class distribution: {y_train.value_counts().to_dict()}"
        )
        logger.info(f"Test set class distribution: {y_test.value_counts().to_dict()}")

        logger.info("STEP 3: Training complete ML pipeline")
        pipeline: Pipeline
        metrics: dict[str, float]
        feature_importance: pd.DataFrame
        pipeline, metrics, feature_importance = (
            train_gradient_boosting_model_with_pipeline(
                X_train, y_train, X_test, y_test, df
            )
        )

        logger.info("=== Pipeline training completed successfully! ===")
        logger.info(
            "Complete pipeline logged to MLflow with all preprocessing included"
        )
        logger.info(" Model can now be deployed directly for inference")

        return pipeline, metrics, feature_importance

    except Exception as e:
        logger.error(f"ERROR in training pipeline: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback

        logger.error(traceback.format_exc())
        raise e


if __name__ == "__main__":
    pipeline, metrics, feature_importance = main()
