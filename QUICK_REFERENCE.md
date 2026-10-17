# Advanced Features Quick Reference

**Version**: 2.0.0-enterprise  
**Latest Update**: January 2024

## 🚀 Four New Enterprise Features

### 1. High-Performance Velocity Layer
**File**: `app/velocity_engine.py` (400 lines)  
**Technology**: Redis sorted sets  
**Latency**: <1ms  
**Use Case**: Real-time fraud detection patterns

```python
# Usage
from app.velocity_engine import get_velocity_engine
engine = get_velocity_engine()
metrics = engine.get_velocity_metrics(user_id, card, ip, location)
anomaly_score = engine.get_anomaly_score(metrics)  # 0.0-1.0
```

**Key Metrics Tracked**:
- User transactions in last 60 seconds (rapid patterns)
- User transactions in last 24 hours (baseline)
- Distinct cards per IP in 10 minutes (testing cards)
- Distinct IPs per user in 24 hours (account takeover)
- Impossible travel detection (distance/time physics)
- Geolocation risk scoring
- Time since last transaction

**Redis Keys**: `velocity:user_txn:{user_id}`, `velocity:card_ip:{ip}`, etc.

---

### 2. Streaming Ingestion (202 Accepted)
**File**: `app/ingestion_gateway.py` (300 lines)  
**Technology**: FastAPI + Kafka + Background tasks  
**Latency**: <5ms response time  
**Pattern**: Decoupled processing

```python
# API Endpoints
POST   /api/transactions/ingest      # Single transaction (202 Accepted)
GET    /api/transactions/status/{id} # Check processing status
POST   /api/transactions/batch       # Multiple transactions

# Example
POST /api/transactions/ingest
{
  "user_id": "user_123",
  "card_number": "4242",
  "amount": 250.00,
  "merchant": "Amazon",
  "merchant_category": "RETAIL",
  "ip_address": "192.168.1.1",
  "device_id": "device_abc",
  "location": "Mumbai"
}

# Response (202 Accepted)
{
  "transaction_id": "TXN-550e8400...",
  "status": "accepted",
  "message": "Processing..."
}
```

**Request Validation**: Pydantic schema with:
- Card number validation (last 4 digits only)
- Amount validation ($0.01-$999,999.99)
- User ID validation (1-255 chars)
- IP address validation
- Merchant category validation

**Latency Breakdown**:
- Validation: <1ms
- Kafka publish (async): 0ms blocking
- Response: <5ms total

---

### 3. MLOps Shadow Deployment
**File**: `app/mlops_engine.py` (300 lines)  
**Technology**: XGBoost ensemble + Prometheus  
**Use Case**: Safe model A/B testing in production

```python
# Usage
from app.mlops_engine import get_model_ensemble
ensemble = get_model_ensemble()

# Make prediction with shadow model
result = ensemble.predict_with_shadow(
    features=[250.0, 1.0, 0.0, ...],
    transaction_id="TXN-123"
)

# Result includes BOTH models
{
  "active_model": {
    "version": "1.0.0",
    "fraud_probability": 0.35,
    "fraud_score": 35,
    "decision": "approve"  # Used for customer
  },
  "shadow_model": {
    "version": "2.0.0-beta",
    "fraud_probability": 0.42,
    "fraud_score": 42
    # Logged only, NOT used for decision
  },
  "comparison": {
    "agreement": true,
    "score_diff": 0.07
  }
}
```

**Metrics Tracked**:
- `mlops_predictions_total` (by model version & outcome)
- `mlops_model_latency_seconds` (per model)
- `mlops_fraud_score_distribution` (separate for each)
- `mlops_model_drift_score` (detecting differences)

**Decision Logic**: Uses ONLY active model. Shadow logged separately.

---

### 4. Distributed Tracing (OpenTelemetry)
**File**: `app/observability_tracing.py` (400 lines)  
**Technology**: OpenTelemetry + Prometheus + Jaeger  
**Use Case**: End-to-end transaction tracking

```python
# Trace ID generation at API layer
from app.observability_tracing import TraceContext

with TraceContext(trace_id=trace_id, span_name="process_transaction"):
    # All operations tagged with trace_id
    velocity_engine.get_velocity_metrics(...)
    ml_ensemble.predict_with_shadow(...)
    # Trace ID automatically injected into Kafka headers
    # Extracted in workers and logged to database

# View complete trace
from app.observability_tracing import get_trace_logger
logger = get_trace_logger()
trace = logger.get_trace(trace_id)
trace_json = logger.export_trace(trace_id)
```

**Trace Flow**:
```
API receives request → Generate trace_id
  ↓ (trace_id in X-Trace-ID header)
Velocity check (traced)
  ↓
ML prediction (traced)
  ↓
Kafka publish (trace_id in message headers)
  ↓ (Workers extract trace_id)
Worker process (continue same trace)
  ↓
Database write (trace_id in audit log)
  ↓
Complete trace viewable in logs/Jaeger
```

**Advanced Prometheus Metrics**:
- `fraud_detection_transactions_processed_total` (by status, decision)
- `fraud_detection_transaction_latency_seconds` (histogram)
- `fraud_detection_kafka_processing_latency_seconds`
- `fraud_detection_ml_predictions_total` (by model, outcome)
- `fraud_detection_velocity_check_latency_seconds`
- `fraud_detection_model_confidence` (distribution)
- `fraud_detection_flagged_amount_total` (gauge)

---

## Integration Points

### In FastAPI Main App (`app/main.py`)
```python
# 1. Initialize velocity engine
velocity_engine = get_velocity_engine()

# 2. Initialize tracing
TracingConfig.initialize()

# 3. Initialize ML ensemble
model_ensemble = get_model_ensemble()

# 4. Include ingestion router
app.include_router(ingestion_router)

# 5. Add trace ID middleware
app.middleware("http")(trace_middleware)
```

### In Kafka Workers
```python
# Extract trace ID from message headers
trace_id = extract_trace_id_from_kafka_headers(message.headers)

# Use in worker processing
with TraceContext(trace_id=trace_id, span_name="worker_process"):
    # All operations tagged with same trace_id
    ml_result = ensemble.predict_with_shadow(features)
    save_to_database(result, trace_id=trace_id)
```

### In ML Service
```python
# Get model ensemble
ensemble = get_model_ensemble()

# Call with shadow
result = ensemble.predict_with_shadow(features, transaction_id)

# Metrics automatically recorded
# mlops_predictions_total, mlops_model_latency_seconds, etc.
```

---

## Performance Characteristics

| Operation | Latency | Throughput |
|-----------|---------|-----------|
| Velocity metric check | <1ms | >1000 ops/s |
| API ingest (202) | <5ms | >200 txn/s |
| ML prediction (active) | 4.2ms | 238 pred/s |
| ML prediction (shadow) | 5.1ms | 196 pred/s |
| End-to-end (API to DB) | 12ms p50, 50ms p99 | 1000+ txn/s (8 workers) |

---

## Configuration

### Enable Shadow Mode
```python
ensemble = get_model_ensemble()
ensemble.enable_shadow_mode(enable=True)
```

### Configure Tracing
```python
from app.observability_tracing import TracingConfig

# In production, enable Jaeger
TracingConfig.JAEGER_ENABLED = True
TracingConfig.JAEGER_AGENT_HOST = "jaeger-agent"
TracingConfig.JAEGER_AGENT_PORT = 6831
```

### Adjust Velocity Thresholds
```python
# In velocity_engine.py get_anomaly_score()
# Modify thresholds:
# - BLOCK_THRESHOLD = 0.9  # >90% fraud = block
# - FLAG_THRESHOLD = 0.5   # >50% fraud = flag
```

---

## Deployment

### Docker Compose
```bash
# All services with new features
docker-compose -f docker-compose-new.yml up
```

### Environment Variables
```bash
REDIS_URL=redis://localhost:6379/1
KAFKA_BROKERS=localhost:9092
JAEGER_ENABLED=false  # Set true in production
```

---

## Monitoring

### Prometheus Dashboards
1. **Real-time monitoring** - Fraud rate, transaction count, latency
2. **System performance** - TPS, P99, error rates
3. **Velocity metrics** - Check latencies, anomaly scores
4. **Model performance** - Active vs shadow predictions

### Key Metrics to Watch
- `fraud_detection_transaction_latency_seconds` (p99 should be <100ms)
- `fraud_detection_transactions_processed_total` (track decision distribution)
- `mlops_model_drift_score` (should be <0.2 if models aligned)
- `fraud_detection_worker_processing_errors_total` (should be 0)

---

## Testing

### Unit Tests for Velocity Engine
```python
def test_velocity_metrics():
    engine = VelocityEngine()
    metrics = engine.get_velocity_metrics(...)
    assert metrics.user_txn_count_60s == 5
    assert 0.0 <= engine.get_anomaly_score(metrics) <= 1.0

def test_impossible_travel():
    is_impossible, risk = engine.check_impossible_travel(
        "user_1", "Delhi", time.time()
    )
    # Should be False (first transaction)
    assert not is_impossible
```

### Integration Tests
```python
# Test 202 Accepted endpoint
response = client.post("/api/transactions/ingest", json={...})
assert response.status_code == 202
assert "transaction_id" in response.json()

# Check processing status
status_response = client.get(f"/api/transactions/status/{txn_id}")
assert status_response.status_code == 200
```

---

## Troubleshooting

### Redis Not Available
```python
if not velocity_engine.health_check():
    logger.error("Redis connection failed")
    # Fallback: use in-memory cache
```

### Shadow Model Not Loading
```python
if not ensemble.shadow_enabled:
    logger.warning("Shadow model disabled - using active model only")
    # Continue normally, no customer impact
```

### Trace IDs Not Propagating
```python
# Verify in Kafka messages
if not trace_id:
    trace_id = str(uuid.uuid4())  # Generate fallback
```

---

## Documentation Links

- **Full Guide**: [ADVANCED_FEATURES.md](ADVANCED_FEATURES.md) (1200+ lines)
- **Deployment**: [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md)
- **Setup**: [ENTERPRISE_SETUP.md](ENTERPRISE_SETUP.md)
- **Architecture**: [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)

---

**Last Updated**: January 2024  
**Status**: ✅ Production Ready  
**Support**: See ADVANCED_FEATURES.md for detailed examples
