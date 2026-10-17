"""
ML Model Serving Microservice
Dedicated service for fraud detection model inference with sub-100ms latency
"""

import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pathlib import Path
from typing import List
import time

# Paths
MODELS_DIR = Path(__file__).resolve().parent / "models"
MODEL_PATH = MODELS_DIR / "fraud_detector_xgboost.pkl"
SCALER_PATH = MODELS_DIR / "feature_scaler.pkl"
FEATURE_COLUMNS_PATH = MODELS_DIR / "feature_columns.pkl"

# Prometheus metrics
model_predictions = Counter(
    "ml_predictions_total",
    "Total number of fraud detection predictions",
    ["prediction"]
)
prediction_latency = Histogram(
    "ml_prediction_latency_seconds",
    "Latency of fraud detection predictions in seconds",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1)
)
model_score_histogram = Histogram(
    "ml_fraud_score",
    "Distribution of fraud probability scores",
    buckets=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
)

app = FastAPI(
    title="ML Model Service",
    description="Fraud Detection Model Inference Engine",
    version="1.0.0"
)

# Global model storage
model = None
scaler = None
feature_columns = None


@app.on_event("startup")
async def load_model():
    """Load model, scaler, and feature columns on startup"""
    global model, scaler, feature_columns

    if not MODEL_PATH.exists():
        raise RuntimeError(f"Model not found at {MODEL_PATH}. Run ml_training.py first.")
    if not SCALER_PATH.exists():
        raise RuntimeError(f"Scaler not found at {SCALER_PATH}. Run ml_training.py first.")
    if not FEATURE_COLUMNS_PATH.exists():
        raise RuntimeError(f"Feature columns not found at {FEATURE_COLUMNS_PATH}. Run ml_training.py first.")

    print("[INFO] Loading ML model, scaler, and feature columns...")
    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    feature_columns = joblib.load(FEATURE_COLUMNS_PATH)
    print("[OK] ML model loaded successfully")
    print(f"[OK] Model expects {len(feature_columns)} features")


class TransactionFeatures(BaseModel):
    """Transaction features for fraud detection"""
    features: List[float]  # Raw features (V1-V28, Amount)
    transaction_id: str = None


class FraudScoreResponse(BaseModel):
    """Fraud detection response"""
    transaction_id: str
    fraud_probability: float
    is_fraud: bool
    fraud_score: int  # 0-100 scale
    confidence: float
    latency_ms: float


class BulkScoringRequest(BaseModel):
    """Bulk fraud detection request"""
    transactions: List[TransactionFeatures]


class BulkScoringResponse(BaseModel):
    """Bulk fraud detection response"""
    scores: List[FraudScoreResponse]
    total_latency_ms: float


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "expected_features": len(feature_columns)
    }


@app.post("/predict", response_model=FraudScoreResponse)
async def predict_fraud(request: TransactionFeatures):
    """
    Single transaction fraud detection
    
    Input features should be in order: V1, V2, ..., V28, Amount
    (matching the training data feature order)
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    if len(request.features) != len(feature_columns):
        raise HTTPException(
            status_code=400,
            detail=f"Expected {len(feature_columns)} features, got {len(request.features)}"
        )

    start_time = time.time()

    try:
        # Convert to numpy array and scale
        X = np.array(request.features).reshape(1, -1)
        X_scaled = scaler.transform(X)

        # Get prediction and probability
        prediction = model.predict(X_scaled)[0]
        probability = model.predict_proba(X_scaled)[0, 1]

        # Convert probability to 0-100 scale
        fraud_score = int(probability * 100)

        # Record metrics
        prediction_label = "fraud" if prediction == 1 else "legitimate"
        model_predictions.labels(prediction=prediction_label).inc()
        model_score_histogram.observe(probability)

        latency_ms = (time.time() - start_time) * 1000

        return FraudScoreResponse(
            transaction_id=request.transaction_id or "unknown",
            fraud_probability=float(probability),
            is_fraud=bool(prediction),
            fraud_score=fraud_score,
            confidence=float(max(model.predict_proba(X_scaled)[0])),
            latency_ms=latency_ms
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")


@app.post("/predict_bulk", response_model=BulkScoringResponse)
async def predict_fraud_bulk(request: BulkScoringRequest):
    """
    Bulk fraud detection for multiple transactions
    Returns predictions for all transactions with overall latency
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    start_time = time.time()
    scores = []

    try:
        for transaction in request.transactions:
            tx_start = time.time()

            if len(transaction.features) != len(feature_columns):
                raise HTTPException(
                    status_code=400,
                    detail=f"Expected {len(feature_columns)} features, got {len(transaction.features)}"
                )

            # Predict
            X = np.array(transaction.features).reshape(1, -1)
            X_scaled = scaler.transform(X)
            prediction = model.predict(X_scaled)[0]
            probability = model.predict_proba(X_scaled)[0, 1]

            fraud_score = int(probability * 100)
            latency_ms = (time.time() - tx_start) * 1000

            # Record metrics
            prediction_label = "fraud" if prediction == 1 else "legitimate"
            model_predictions.labels(prediction=prediction_label).inc()
            model_score_histogram.observe(probability)

            scores.append(
                FraudScoreResponse(
                    transaction_id=transaction.transaction_id or f"tx_{len(scores)}",
                    fraud_probability=float(probability),
                    is_fraud=bool(prediction),
                    fraud_score=fraud_score,
                    confidence=float(max(model.predict_proba(X_scaled)[0])),
                    latency_ms=latency_ms
                )
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bulk prediction error: {str(e)}")

    total_latency_ms = (time.time() - start_time) * 1000

    return BulkScoringResponse(scores=scores, total_latency_ms=total_latency_ms)


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return generate_latest()


@app.get("/model_info")
async def model_info():
    """Return model metadata"""
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    return {
        "model_type": type(model).__name__,
        "n_features": len(feature_columns),
        "feature_columns": feature_columns,
        "model_path": str(MODEL_PATH),
        "feature_order": "V1-V28 (normalized transaction features) + Amount"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
