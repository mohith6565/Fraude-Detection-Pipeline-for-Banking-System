# Advanced Features Deployment & Integration Guide

**Version**: 2.0.0-enterprise  
**Phase**: 10 - Advanced Features  
**Status**: ✅ Ready for Integration

---

## Summary of Changes

### New Files Created

| File | Lines | Purpose | Dependencies |
|------|-------|---------|--------------|
| `app/velocity_engine.py` | 400 | Redis sliding window velocity metrics | redis, math |
| `app/ingestion_gateway.py` | 300 | 202 Accepted streaming ingestion | fastapi, pydantic, kafka |
| `app/mlops_engine.py` | 300 | Shadow model deployment | joblib, prometheus, numpy |
| `app/observability_tracing.py` | 400 | Distributed tracing | opentelemetry, prometheus |
| `ADVANCED_FEATURES.md` | 1200+ | Complete feature documentation | - |
| `QUICK_REFERENCE.md` | 300+ | Developer quick reference | - |

### Updated Files

| File | Changes |
|------|---------|
| `app/main.py` | Added imports, initialized all services in lifespan |
| `requirements.txt` | Added OpenTelemetry packages |

### Total Lines of Code
- **New Application Code**: 1400+ lines
- **New Documentation**: 1500+ lines
- **Total**: 2900+ lines of enterprise-grade code

---

## Pre-Deployment Checklist

### ✅ Code Quality
- [x] All imports included
- [x] Type hints on all functions
- [x] Error handling with logging
- [x] Docstrings on all classes/methods
- [x] Prometheus metrics defined
- [x] Thread-safe operations

### ✅ Dependencies
- [x] `redis>=5.0` for velocity engine
- [x] `opentelemetry-api>=1.20` for tracing
- [x] `opentelemetry-exporter-prometheus>=0.41b0`
- [x] `opentelemetry-exporter-jaeger-thrift>=1.20`
- [x] All instrumentation packages

### ✅ Architecture
- [x] Decoupled modules (no circular imports)
- [x] Global singletons for engines
- [x] Middleware for trace injection
- [x] Router isolation for ingestion
- [x] Feature composition in main

### ✅ Database
- [x] PostgreSQL 16+ ready
- [x] Connection pooling configured
- [x] Audit logging for trace IDs
- [x] Transaction model updated

### ✅ Monitoring
- [x] Prometheus metrics comprehensive
- [x] Grafana dashboard support
- [x] Trace logging available
- [x] Error tracking enabled

---

## Step-by-Step Integration

### Step 1: Install Dependencies
```bash
# Update requirements
pip install -r requirements.txt

# Key new packages:
pip install opentelemetry-api opentelemetry-sdk
pip install opentelemetry-exporter-prometheus
pip install opentelemetry-exporter-jaeger-thrift
pip install opentelemetry-instrumentation-fastapi
pip install opentelemetry-instrumentation-sqlalchemy
pip install opentelemetry-instrumentation-redis
pip install opentelemetry-instrumentation-kafka
```

### Step 2: Verify Redis Connection
```bash
# Check Redis is running and accessible
redis-cli ping
# Expected: PONG

# Verify DB 1 is available (for velocity engine)
redis-cli -n 1 ping
# Expected: PONG

# Check available memory
redis-cli info memory
# Should show available_memory > 100MB
```

### Step 3: Load ML Models
```bash
# Ensure models directory exists
mkdir -p models/

# Run model training if needed
python ml_training.py

# Verify models are present
ls -la models/
# Should show: fraud_detector_xgboost.pkl
#             feature_scaler.pkl
#             feature_columns.pkl
```

### Step 4: Initialize Application
```bash
# Start with tracing enabled
export JAEGER_ENABLED=false  # Start with false, enable after Jaeger setup

# Run app
uvicorn app.main:app --host 0.0.0.0 --port 8000

# Expected logs:
# [INFO] Database initialized successfully
# [INFO] Enterprise services initialized
# [INFO] Velocity engine initialized
# [OK] OpenTelemetry tracing initialized
# [OK] ML model ensemble loaded
```

### Step 5: Test API Endpoints
```bash
# Test velocity engine indirectly (via health)
curl http://localhost:8000/health

# Test ingestion endpoint
curl -X POST http://localhost:8000/api/transactions/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "test_user",
    "card_number": "4242",
    "amount": 100.0,
    "merchant": "Test Merchant",
    "merchant_category": "RETAIL",
    "ip_address": "192.168.1.1",
    "device_id": "test_device",
    "location": "Delhi"
  }'

# Expected response (202 Accepted):
# {
#   "transaction_id": "TXN-...",
#   "status": "accepted",
#   "message": "Transaction queued for processing",
#   "timestamp": "2024-01-15T10:30:45Z"
# }

# Check transaction status
curl http://localhost:8000/api/transactions/status/TXN-...
```

### Step 6: Configure Shadow Model (Optional)
```python
# In Python REPL or script
from app.mlops_engine import get_model_ensemble

ensemble = get_model_ensemble()
print(ensemble.get_model_stats())

# Enable shadow mode
ensemble.enable_shadow_mode(enable=True)
print("Shadow model enabled")
```

### Step 7: Start Kafka Workers
```bash
# In separate terminals

# Worker 1
python kafka_worker.py --worker-id 1

# Worker 2
python kafka_worker.py --worker-id 2

# Expected logs:
# [INFO] Connected to Kafka
# [OK] Starting transaction consumer
# [INFO] Processing transaction: TXN-...
```

### Step 8: Start Prometheus & Grafana
```bash
# Using docker-compose
docker-compose -f docker-compose-new.yml up prometheus grafana

# Or manually
prometheus --config.file=prometheus.yml
grafana-server

# Access:
# Prometheus: http://localhost:9090
# Grafana: http://localhost:3000 (admin/admin)
```

### Step 9: Enable Jaeger (For Full Tracing)
```bash
# Start Jaeger container
docker run -d \
  -p 6831:6831/udp \
  -p 16686:16686 \
  jaegertracing/all-in-one:latest

# Enable in config
# Set JAEGER_ENABLED=True in app/observability_tracing.py

# Access UI: http://localhost:16686
```

---

## Integration Examples

### Example 1: Use Velocity Engine in Custom Endpoint

```python
from fastapi import FastAPI
from app.velocity_engine import get_velocity_engine
from app.observability_tracing import TimedOperation

app = FastAPI()

@app.post("/check-velocity")
async def check_velocity(user_id: str, card: str, ip: str, location: str):
    velocity_engine = get_velocity_engine()
    
    with TimedOperation("velocity_check") as timer:
        metrics = velocity_engine.get_velocity_metrics(
            user_id=user_id,
            card_number=card,
            ip_address=ip,
            location=location
        )
    
    anomaly_score = velocity_engine.get_anomaly_score(metrics)
    
    return {
        "user_id": user_id,
        "metrics": {
            "txn_60s": metrics.user_txn_count_60s,
            "txn_24h": metrics.user_txn_count_24h,
            "distinct_cards": metrics.distinct_cards_per_ip_10m,
            "distinct_ips": metrics.distinct_ips_per_user_24h,
            "impossible_travel": metrics.impossible_travel_risk,
        },
        "anomaly_score": anomaly_score,
        "latency_ms": timer.duration_ms
    }
```

### Example 2: Custom ML Pipeline with Shadow Model

```python
from app.mlops_engine import get_model_ensemble
from app.observability_tracing import record_ml_prediction

async def predict_fraud(features, transaction_id):
    ensemble = get_model_ensemble()
    
    # Get both model predictions
    result = ensemble.predict_with_shadow(features, transaction_id)
    
    # Record metrics
    active_pred = result["active_model"]
    record_ml_prediction(
        model_version=active_pred["version"],
        fraud_probability=active_pred["fraud_probability"],
        is_fraud=active_pred["is_fraud"],
        latency_ms=active_pred["latency_ms"]
    )
    
    # Use only active model for decision
    if active_pred["fraud_probability"] > 0.9:
        return {"decision": "block", "reason": "High fraud score"}
    elif active_pred["fraud_probability"] > 0.5:
        return {"decision": "flag", "reason": "Requires review"}
    else:
        return {"decision": "approve"}
```

### Example 3: Complete Transaction Processing Flow

```python
import uuid
from app.velocity_engine import get_velocity_engine
from app.mlops_engine import get_model_ensemble
from app.observability_tracing import (
    TraceContext,
    get_trace_logger,
    record_transaction_end_to_end
)

async def process_transaction_complete(transaction_data):
    # Generate trace ID
    trace_id = str(uuid.uuid4())
    trace_logger = get_trace_logger()
    
    with TraceContext(trace_id=trace_id, span_name="complete_transaction"):
        try:
            # 1. Velocity check
            velocity_engine = get_velocity_engine()
            trace_logger.log_event(trace_id, "velocity_check_start")
            
            metrics = velocity_engine.get_velocity_metrics(
                user_id=transaction_data["user_id"],
                card_number=transaction_data["card"],
                ip_address=transaction_data["ip"],
                location=transaction_data["location"]
            )
            velocity_score = velocity_engine.get_anomaly_score(metrics)
            trace_logger.log_event(trace_id, "velocity_check_complete", 
                                 {"score": velocity_score})
            
            # 2. ML prediction
            ensemble = get_model_ensemble()
            trace_logger.log_event(trace_id, "ml_prediction_start")
            
            ml_result = ensemble.predict_with_shadow(
                features=transaction_data["features"],
                transaction_id=transaction_data["id"]
            )
            trace_logger.log_event(trace_id, "ml_prediction_complete",
                                 {"fraud_score": ml_result["active_model"]["fraud_score"]})
            
            # 3. Combine scores
            ml_score = ml_result["active_model"]["fraud_probability"]
            combined_score = (ml_score * 0.7 + velocity_score * 0.3)
            
            # 4. Make decision
            if combined_score > 0.8:
                decision = "block"
            elif combined_score > 0.5:
                decision = "flag"
            else:
                decision = "approve"
            
            trace_logger.log_event(trace_id, "decision_made", {"decision": decision})
            
            # 5. Record metrics
            record_transaction_end_to_end(
                transaction_id=transaction_data["id"],
                user_id=transaction_data["user_id"],
                decision=decision,
                total_latency_ms=5.0,  # Your measurement
                amount=transaction_data["amount"],
                is_fraud=(combined_score > 0.5)
            )
            
            return {
                "transaction_id": transaction_data["id"],
                "decision": decision,
                "fraud_score": int(combined_score * 100),
                "trace_id": trace_id,
                "velocity_score": velocity_score,
                "ml_score": ml_score
            }
        
        except Exception as e:
            trace_logger.log_event(trace_id, "error", {"error": str(e)})
            raise
```

---

## Configuration Examples

### docker-compose-new.yml - Environment Variables
```yaml
services:
  app:
    environment:
      - REDIS_URL=redis://redis:6379/1
      - KAFKA_BROKERS=kafka:9092
      - JAEGER_ENABLED=false
      - JAEGER_AGENT_HOST=jaeger
      - DATABASE_URL=postgresql://user:pass@postgres:5432/fraud_db
```

### Prometheus Scrape Config
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'app'
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/metrics'
    
  - job_name: 'ml_service'
    static_configs:
      - targets: ['localhost:8001']
    metrics_path: '/metrics'
```

### Grafana Dashboard JSON
```json
{
  "dashboard": {
    "title": "Fraud Detection Advanced Features",
    "panels": [
      {
        "title": "Transaction Latency (p99)",
        "targets": [
          {
            "expr": "histogram_quantile(0.99, fraud_detection_transaction_latency_seconds)"
          }
        ]
      },
      {
        "title": "Velocity Check Latency (p99)",
        "targets": [
          {
            "expr": "histogram_quantile(0.99, fraud_detection_velocity_check_latency_seconds)"
          }
        ]
      }
    ]
  }
}
```

---

## Performance Validation

### Load Testing Script
```python
import asyncio
import time
from httpx import AsyncClient

async def load_test():
    client = AsyncClient()
    
    start = time.time()
    tasks = []
    
    for i in range(100):
        task = client.post(
            "http://localhost:8000/api/transactions/ingest",
            json={
                "user_id": f"user_{i}",
                "card_number": "4242",
                "amount": 100.0 + i,
                "merchant": "Test",
                "merchant_category": "RETAIL",
                "ip_address": f"192.168.1.{i % 256}",
                "device_id": f"device_{i}",
                "location": "Mumbai"
            }
        )
        tasks.append(task)
    
    responses = await asyncio.gather(*tasks)
    elapsed = time.time() - start
    
    # Should complete 100 requests in <1 second
    assert elapsed < 1.0, f"Took {elapsed}s, expected <1s"
    assert all(r.status_code == 202 for r in responses)
    
    print(f"✅ Load test passed: 100 requests in {elapsed:.2f}s")
```

### Metrics Validation
```python
import requests

def validate_metrics():
    response = requests.get("http://localhost:8000/metrics")
    metrics = response.text
    
    # Check velocity metrics
    assert "fraud_detection_velocity_check_latency_seconds" in metrics
    
    # Check transaction metrics
    assert "fraud_detection_transactions_processed_total" in metrics
    
    # Check ML metrics
    assert "mlops_predictions_total" in metrics
    assert "mlops_model_drift_score" in metrics
    
    print("✅ All metrics registered correctly")
```

---

## Troubleshooting

### Issue: Velocity Engine Not Recording
```python
# Debug
engine = get_velocity_engine()
if not engine.health_check():
    print("Redis not available")
    # Check Redis connection
    redis-cli ping
```

### Issue: Trace IDs Not Propagating
```python
# Check middleware is registered
# In app/main.py, verify:
app.middleware("http")(trace_middleware)

# Check Kafka headers
from app.observability_tracing import extract_trace_id_from_kafka_headers
trace_id = extract_trace_id_from_kafka_headers(message.headers)
assert trace_id is not None
```

### Issue: Shadow Model Not Loading
```python
# Check model files exist
import os
assert os.path.exists("models/fraud_detector_xgboost.pkl")
assert os.path.exists("models/feature_scaler.pkl")
assert os.path.exists("models/feature_columns.pkl")

# Verify pickle format
import joblib
model = joblib.load("models/fraud_detector_xgboost.pkl")
assert model is not None
```

---

## Next Steps

1. **Integration**: Copy new files to your project
2. **Dependencies**: Install OpenTelemetry packages
3. **Testing**: Run load tests to validate performance
4. **Deployment**: Deploy with docker-compose-new.yml
5. **Monitoring**: Configure Prometheus & Grafana dashboards
6. **Production**: Enable Jaeger for distributed tracing

---

## Support & Documentation

- **Quick Reference**: [QUICK_REFERENCE.md](QUICK_REFERENCE.md)
- **Full Features**: [ADVANCED_FEATURES.md](ADVANCED_FEATURES.md)
- **Deployment**: [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md)
- **Setup Guide**: [ENTERPRISE_SETUP.md](ENTERPRISE_SETUP.md)

---

**Version**: 2.0.0-enterprise  
**Last Updated**: January 2024  
**Status**: ✅ Production Ready  
**Ready for Integration**: YES
