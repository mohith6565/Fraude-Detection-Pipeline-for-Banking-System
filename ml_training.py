"""
ML Training Pipeline for Fraud Detection
Downloads Credit Card Fraud Detection dataset from Kaggle and trains XGBoost model
"""

import os
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score,
    roc_curve,
    precision_recall_curve,
    f1_score,
)
import xgboost as xgb
import joblib
from pathlib import Path
import warnings

warnings.filterwarnings("ignore")

# Create models directory
MODELS_DIR = Path(__file__).parent / "models"
MODELS_DIR.mkdir(exist_ok=True)

MODEL_PATH = MODELS_DIR / "fraud_detector_xgboost.pkl"
SCALER_PATH = MODELS_DIR / "feature_scaler.pkl"
FEATURE_COLUMNS_PATH = MODELS_DIR / "feature_columns.pkl"


def download_kaggle_dataset():
    """
    Download Credit Card Fraud Detection dataset from Kaggle
    Requires: kaggle API key configured at ~/.kaggle/kaggle.json
    """
    print("[INFO] Attempting to download dataset from Kaggle...")
    try:
        import kaggle

        kaggle.api.dataset_download_files(
            "mlg-ulb/creditcardfraud", path="datasets", unzip=True
        )
        print("[OK] Dataset downloaded successfully")
        return "datasets/creditcard.csv"
    except Exception as e:
        print(f"[WARN] Could not download from Kaggle: {e}")
        print("[INFO] Using synthetic dataset generation instead...")
        return None


def generate_synthetic_dataset(n_samples=284807):
    """Generate a synthetic credit card fraud dataset for demo purposes"""
    print(f"[INFO] Generating synthetic dataset with {n_samples} samples...")

    np.random.seed(42)

    # Features: normalized transaction amounts, time, and derived statistics
    features = np.random.randn(n_samples, 28)

    # Create fraud patterns
    fraud_indices = np.random.choice(n_samples, size=int(n_samples * 0.0017), replace=False)
    labels = np.zeros(n_samples)
    labels[fraud_indices] = 1

    # Make fraud transactions have distinct patterns
    for idx in fraud_indices:
        features[idx, :5] = np.random.uniform(-3, 3, 5)  # Different amount distribution
        features[idx, 5:10] = np.random.uniform(1, 5, 5)  # Different time patterns

    df = pd.DataFrame(
        features, columns=[f"V{i}" for i in range(1, 29)]
    )
    df["Amount"] = np.random.exponential(50, n_samples)
    df["Amount"][fraud_indices] = np.random.exponential(200, len(fraud_indices))
    df["Class"] = labels

    print(f"[OK] Synthetic dataset created: {df.shape}")
    print(f"    Fraud ratio: {labels.sum() / len(labels) * 100:.2f}%")
    return df


def load_dataset(csv_path=None):
    """Load and preprocess the dataset"""
    if csv_path and os.path.exists(csv_path):
        print(f"[INFO] Loading dataset from {csv_path}...")
        df = pd.read_csv(csv_path)
    else:
        df = generate_synthetic_dataset()

    print(f"[INFO] Dataset shape: {df.shape}")
    print(f"[INFO] Fraud samples: {(df['Class'] == 1).sum()} ({(df['Class'] == 1).sum() / len(df) * 100:.2f}%)")
    print(f"[INFO] Legitimate samples: {(df['Class'] == 0).sum()}")

    return df


def preprocess_data(df):
    """Preprocess and split the dataset"""
    print("[INFO] Preprocessing data...")

    # Separate features and target
    X = df.drop("Class", axis=1)
    y = df["Class"]

    # Store feature columns for later use
    feature_columns = X.columns.tolist()

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    print(f"[OK] Training set: {X_train_scaled.shape}")
    print(f"[OK] Test set: {X_test_scaled.shape}")

    # Save scaler and feature columns
    joblib.dump(scaler, SCALER_PATH)
    joblib.dump(feature_columns, FEATURE_COLUMNS_PATH)

    return X_train_scaled, X_test_scaled, y_train, y_test, feature_columns


def train_model(X_train, y_train, X_test, y_test):
    """Train XGBoost model with fraud detection optimizations"""
    print("[INFO] Training XGBoost model...")

    # Calculate scale_pos_weight to handle class imbalance
    scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=7,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,  # Handle fraud imbalance
        random_state=42,
        tree_method="hist",
        eval_metric="logloss",
        early_stopping_rounds=20,
    )

    # Train with early stopping
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    print("[OK] Model training complete")
    return model


def evaluate_model(model, X_train, X_test, y_train, y_test, feature_columns):
    """Comprehensive model evaluation"""
    print("\n" + "=" * 60)
    print("MODEL EVALUATION REPORT")
    print("=" * 60)

    # Predictions
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    y_test_proba = model.predict_proba(X_test)[:, 1]

    # Metrics
    train_f1 = f1_score(y_train, y_train_pred)
    test_f1 = f1_score(y_test, y_test_pred)
    train_auc = roc_auc_score(y_train, model.predict_proba(X_train)[:, 1])
    test_auc = roc_auc_score(y_test, y_test_proba)

    print(f"\nTraining F1-Score:   {train_f1:.4f}")
    print(f"Test F1-Score:       {test_f1:.4f}")
    print(f"Training AUC-ROC:    {train_auc:.4f}")
    print(f"Test AUC-ROC:        {test_auc:.4f}")

    print("\nTest Set Classification Report:")
    print(classification_report(y_test, y_test_pred, target_names=["Legitimate", "Fraud"]))

    print("\nConfusion Matrix (Test Set):")
    cm = confusion_matrix(y_test, y_test_pred)
    print(cm)
    print(f"True Negatives:  {cm[0, 0]}")
    print(f"False Positives: {cm[0, 1]}")
    print(f"False Negatives: {cm[1, 0]}")
    print(f"True Positives:  {cm[1, 1]}")

    # Feature importance
    print("\nTop 10 Most Important Features:")
    feature_importance = pd.DataFrame(
        {
            "feature": feature_columns,
            "importance": model.feature_importances_,
        }
    ).sort_values("importance", ascending=False)

    for idx, row in feature_importance.head(10).iterrows():
        print(f"  {row['feature']}: {row['importance']:.4f}")

    print("\n" + "=" * 60)


def save_model(model):
    """Save the trained model"""
    joblib.dump(model, MODEL_PATH)
    print(f"[OK] Model saved to {MODEL_PATH}")


def main():
    """Main training pipeline"""
    print("[START] Fraud Detection Model Training Pipeline\n")

    # Load data
    csv_path = download_kaggle_dataset()
    df = load_dataset(csv_path)

    # Preprocess
    X_train, X_test, y_train, y_test, feature_columns = preprocess_data(df)

    # Train
    model = train_model(X_train, y_train, X_test, y_test)

    # Evaluate
    evaluate_model(model, X_train, X_test, y_train, y_test, feature_columns)

    # Save
    save_model(model)

    print("\n[COMPLETE] Training pipeline finished")
    print(f"[INFO] Model location: {MODEL_PATH}")
    print(f"[INFO] Scaler location: {SCALER_PATH}")
    print(f"[INFO] Feature columns: {FEATURE_COLUMNS_PATH}")


if __name__ == "__main__":
    main()
