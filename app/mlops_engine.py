"""
MLOps Engine with Shadow Model Deployment
Implements A/B testing of models without impacting production decisions
Logs predictions from both active and shadow models for analysis
"""

import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import logging
import json

logger = logging.getLogger(__name__)

# ============ Prometheus Metrics ============

model_predictions = Counter(
    'mlops_predictions_total',
    'Total predictions by model version',
    ['model_version', 'prediction_type']
)

model_latency = Histogram(
    'mlops_model_latency_seconds',
    'Model inference latency',
    ['model_version'],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1)
)

model_score_distribution = Histogram(
    'mlops_fraud_score_distribution',
    'Distribution of fraud scores',
    ['model_version'],
    buckets=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
)

prediction_confidence = Gauge(
    'mlops_prediction_confidence',
    'Average prediction confidence',
    ['model_version']
)

model_drift = Gauge(
    'mlops_model_drift_score',
    'Detected model drift score (0-1)',
    ['model_version']
)


@dataclass
class ModelPrediction:
    """Structured prediction with metadata"""
    fraud_probability: float
    is_fraud: bool
    confidence: float
    fraud_score: int
    model_version: str
    latency_ms: float
    timestamp: str


class ShadowModelResponse(BaseModel):
    """Response including both active and shadow model predictions"""
    transaction_id: str
    active_model: Dict  # Active model prediction (used for decision)
    shadow_model: Optional[Dict]  # Shadow model prediction (logged only)
    shadow_mode_enabled: bool
    decision: str  # "approve", "flag", "block"


class ModelEnsemble:
    """
    MLOps ensemble managing multiple model versions
    Handles shadow deployments and A/B testing
    """

    def __init__(self, models_dir: Path = None):
        """Initialize model ensemble"""
        if models_dir is None:
            models_dir = Path(__file__).parent.parent / "models"
        
        self.models_dir = models_dir
        self.active_model = None
        self.active_version = "1.0.0"
        self.shadow_model = None
        self.shadow_version = "2.0.0-beta"
        self.shadow_enabled = False
        self.scaler = None
        self.feature_columns = None
        
        self._load_models()

    def _load_models(self):
        """Load active and shadow models from disk"""
        try:
            # Load active model
            active_model_path = self.models_dir / "fraud_detector_xgboost.pkl"
            if active_model_path.exists():
                self.active_model = joblib.load(active_model_path)
                logger.info(f"[OK] Loaded active model v{self.active_version}")
            else:
                raise FileNotFoundError(f"Active model not found: {active_model_path}")
            
            # Load scaler and features
            scaler_path = self.models_dir / "feature_scaler.pkl"
            features_path = self.models_dir / "feature_columns.pkl"
            
            if scaler_path.exists():
                self.scaler = joblib.load(scaler_path)
            if features_path.exists():
                self.feature_columns = joblib.load(features_path)
            
            # Try to load shadow model (optional)
            shadow_model_path = self.models_dir / "fraud_detector_xgboost_shadow.pkl"
            if shadow_model_path.exists():
                self.shadow_model = joblib.load(shadow_model_path)
                self.shadow_enabled = True
                logger.info(f"[OK] Loaded shadow model v{self.shadow_version}")
            else:
                logger.info("[INFO] Shadow model not available (will skip shadow predictions)")
                self.shadow_enabled = False

        except Exception as e:
            logger.error(f"[ERROR] Failed to load models: {e}")
            raise

    def predict_active(self, features: np.ndarray) -> ModelPrediction:
        """Predict with active production model"""
        if self.active_model is None:
            raise RuntimeError("Active model not loaded")
        
        try:
            import time
            start = time.time()
            
            # Scale features
            if self.scaler:
                features_scaled = self.scaler.transform(features.reshape(1, -1))
            else:
                features_scaled = features.reshape(1, -1)
            
            # Get prediction
            prediction = self.active_model.predict(features_scaled)[0]
            probability = self.active_model.predict_proba(features_scaled)[0, 1]
            
            latency_ms = (time.time() - start) * 1000
            
            # Record metrics
            model_predictions.labels(
                model_version=self.active_version,
                prediction_type="fraud" if prediction == 1 else "legitimate"
            ).inc()
            model_latency.labels(model_version=self.active_version).observe(latency_ms / 1000.0)
            model_score_distribution.labels(model_version=self.active_version).observe(probability)
            
            return ModelPrediction(
                fraud_probability=float(probability),
                is_fraud=bool(prediction),
                confidence=float(max(self.active_model.predict_proba(features_scaled)[0])),
                fraud_score=int(probability * 100),
                model_version=self.active_version,
                latency_ms=latency_ms,
                timestamp=datetime.utcnow().isoformat()
            )
        
        except Exception as e:
            logger.error(f"[ERROR] Active model prediction failed: {e}")
            raise

    def predict_shadow(self, features: np.ndarray) -> Optional[ModelPrediction]:
        """
        Predict with shadow model (if enabled)
        Shadow predictions are logged but NOT used for decisions
        Used for A/B testing and model validation
        """
        if not self.shadow_enabled or self.shadow_model is None:
            return None
        
        try:
            import time
            start = time.time()
            
            # Scale features
            if self.scaler:
                features_scaled = self.scaler.transform(features.reshape(1, -1))
            else:
                features_scaled = features.reshape(1, -1)
            
            # Get prediction
            prediction = self.shadow_model.predict(features_scaled)[0]
            probability = self.shadow_model.predict_proba(features_scaled)[0, 1]
            
            latency_ms = (time.time() - start) * 1000
            
            # Record metrics (separate namespace)
            model_predictions.labels(
                model_version=self.shadow_version,
                prediction_type="fraud" if prediction == 1 else "legitimate"
            ).inc()
            model_latency.labels(model_version=self.shadow_version).observe(latency_ms / 1000.0)
            model_score_distribution.labels(model_version=self.shadow_version).observe(probability)
            
            logger.debug(f"[SHADOW] Prediction: {probability:.3f} (active would be X)")
            
            return ModelPrediction(
                fraud_probability=float(probability),
                is_fraud=bool(prediction),
                confidence=float(max(self.shadow_model.predict_proba(features_scaled)[0])),
                fraud_score=int(probability * 100),
                model_version=self.shadow_version,
                latency_ms=latency_ms,
                timestamp=datetime.utcnow().isoformat()
            )
        
        except Exception as e:
            logger.error(f"[ERROR] Shadow model prediction failed: {e}")
            return None

    def predict_with_shadow(
        self,
        features: List[float],
        transaction_id: str = "unknown"
    ) -> Dict:
        """
        Make prediction with both active and shadow models
        
        Args:
            features: Transaction features
            transaction_id: For logging purposes
            
        Returns:
            Dict with both predictions and shadow comparison
        """
        try:
            features_array = np.array(features)
            
            # Get active prediction (used for decision)
            active_pred = self.predict_active(features_array)
            
            # Get shadow prediction (logged for analysis)
            shadow_pred = self.predict_shadow(features_array) if self.shadow_enabled else None
            
            # Compare predictions (if shadow enabled)
            comparison = None
            if shadow_pred:
                prediction_agreement = (
                    active_pred.is_fraud == shadow_pred.is_fraud
                )
                score_diff = abs(active_pred.fraud_probability - shadow_pred.fraud_probability)
                
                comparison = {
                    "agreement": prediction_agreement,
                    "score_diff": float(score_diff),
                    "active_fraud_score": active_pred.fraud_score,
                    "shadow_fraud_score": shadow_pred.fraud_score,
                }
                
                # Log discrepancies for model drift analysis
                if score_diff > 0.3:  # Significant difference
                    logger.warning(
                        f"[DRIFT] TXN {transaction_id}: "
                        f"Active={active_pred.fraud_score}, "
                        f"Shadow={shadow_pred.fraud_score}"
                    )
            
            return {
                "transaction_id": transaction_id,
                "active_model": {
                    "version": active_pred.model_version,
                    "fraud_probability": active_pred.fraud_probability,
                    "is_fraud": active_pred.is_fraud,
                    "fraud_score": active_pred.fraud_score,
                    "confidence": active_pred.confidence,
                    "latency_ms": active_pred.latency_ms,
                },
                "shadow_model": {
                    "version": shadow_pred.model_version,
                    "fraud_probability": shadow_pred.fraud_probability,
                    "is_fraud": shadow_pred.is_fraud,
                    "fraud_score": shadow_pred.fraud_score,
                    "confidence": shadow_pred.confidence,
                    "latency_ms": shadow_pred.latency_ms,
                } if shadow_pred else None,
                "shadow_enabled": self.shadow_enabled,
                "comparison": comparison,
                "decision": self._make_decision(active_pred, shadow_pred),
                "timestamp": datetime.utcnow().isoformat(),
            }
        
        except Exception as e:
            logger.error(f"[ERROR] Prediction with shadow failed: {e}")
            raise

    @staticmethod
    def _make_decision(
        active_pred: ModelPrediction,
        shadow_pred: Optional[ModelPrediction] = None
    ) -> str:
        """
        Make fraud decision based on active model only
        (shadow predictions are logged but don't affect decision)
        
        Args:
            active_pred: Active model prediction
            shadow_pred: Shadow model prediction (for logging only)
            
        Returns:
            Decision: "approve", "flag", or "block"
        """
        # Decision thresholds (configurable)
        BLOCK_THRESHOLD = 0.9  # >90% fraud probability = block
        FLAG_THRESHOLD = 0.5   # >50% fraud probability = flag for review
        
        if active_pred.fraud_probability >= BLOCK_THRESHOLD:
            return "block"
        elif active_pred.fraud_probability >= FLAG_THRESHOLD:
            return "flag"
        else:
            return "approve"

    def enable_shadow_mode(self, enable: bool = True):
        """Enable or disable shadow mode"""
        if enable and not self.shadow_enabled:
            logger.warning("[WARN] Cannot enable shadow mode - shadow model not loaded")
            return False
        
        self.shadow_enabled = enable
        logger.info(f"[INFO] Shadow mode {'enabled' if enable else 'disabled'}")
        return True

    def get_model_stats(self) -> Dict:
        """Get stats about loaded models"""
        return {
            "active_model": {
                "version": self.active_version,
                "type": type(self.active_model).__name__ if self.active_model else None,
                "loaded": self.active_model is not None,
            },
            "shadow_model": {
                "version": self.shadow_version,
                "type": type(self.shadow_model).__name__ if self.shadow_model else None,
                "loaded": self.shadow_model is not None,
                "enabled": self.shadow_enabled,
            },
            "scaler_loaded": self.scaler is not None,
            "feature_columns_count": len(self.feature_columns) if self.feature_columns else 0,
        }


# Global ensemble instance
_model_ensemble = None


def get_model_ensemble(models_dir: Path = None) -> ModelEnsemble:
    """Get or create global model ensemble"""
    global _model_ensemble
    if _model_ensemble is None:
        _model_ensemble = ModelEnsemble(models_dir)
    return _model_ensemble
