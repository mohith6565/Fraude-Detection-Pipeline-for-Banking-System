# Enterprise Fraud Detection Pipeline - Complete Setup Guide

This document covers the complete enterprise-grade fraud detection system with all upgrades implemented.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [ML Model Training](#ml-model-training)
5. [Docker Deployment](#docker-deployment)
6. [Configuration](#configuration)
7. [Monitoring & Observability](#monitoring--observability)
8. [CI/CD Pipeline](#cicd-pipeline)
9. [Troubleshooting](#troubleshooting)

## Architecture Overview

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Load Balancer / API Gateway               │
└────────┬────────────────────────────────────────────────────┘
         │
         ├─────────────────────┬──────────────────────┐
         │                     │                      │
    ┌────▼────┐          ┌─────▼─────┐        ┌──────▼──────┐
    │ FastAPI │          │ ML Service│        │   Workers   │
    │  Main   │          │ (Port 8001)│        │  (Kafka)   │
    │(Port 8k)│          └─────┬─────┘        └──────┬──────┘
    └────┬────┘                │                     │
         │       ┌─────────────┼─────────────────────┤
         │       │             │                     │
    ┌────▼───────┼─────┐  ┌────▼────┐           ┌───▼────┐
    │  PostgreSQL│     │  │  Redis  │           │ Kafka  │
    │ (Port 5432)│     │  │(Port 637│           │(9092)  │
    └────────────┘     │  └─────────┘           └────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
    ┌───▼────┐                  ┌─────▼──────┐
    │Prometeus│                  │  Grafana   │
    │(9090)  │                  │ (3000)    │
    └────────┘                  └────────────┘
```

### Key Features

✨ **Real-Time ML Inference**
- XGBoost/Deep Learning model serving
- Sub-100ms prediction latency
- Dedicated microservice architecture

🔄 **Event-Driven Processing**
- Kafka message broker for transaction ingestion
- Async worker nodes for ML scoring
- 202 Accepted response pattern for immediate feedback

💾 **Enterprise Data Layer**
- PostgreSQL for durable storage with connection pooling
- Redis for velocity features and session caching
- Feature store for real-time context

📊 **Observability**
- Prometheus metrics collection
- Grafana dashboards for TPS, P99 Latency, Fraud Rate
- Structured logging across all services

🚀 **CI/CD & DevOps**
- GitHub Actions for automated testing and deployment
- Multi-stage Docker builds
- Kubernetes-ready manifest support

---

## Prerequisites

### System Requirements

- Docker & Docker Compose 20.10+
- Python 3.11+
- Git
- 8GB RAM (minimum)
- 20GB disk space

### Optional

- PostgreSQL 16 (for local development)
- Redis 7+ (for local development)
- Kafka 3.5+ (for local development)

### Environment Variables

Create a `.env` file:

```bash
# Database
DATABASE_URL=postgresql://fraud_user:fraud_password@postgres:5432/fraud_detection

# Cache
REDIS_URL=redis://redis:6379
REDIS_HOST=redis
REDIS_PORT=6379

# Message Queue
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# ML Service
ML_SERVICE_URL=http://ml-service:8001

# Application
APP_ENV=production
APP_DEBUG=false

# Docker Hub (for CI/CD)
DOCKER_USERNAME=your_dockerhub_username
DOCKER_PASSWORD=your_dockerhub_token
```

---

## Quick Start

### 1. Clone Repository

```bash
git clone <your-repo>
cd Real-Time-Fraud-Detection-Pipeline
```

### 2. Create Python Environment (Development Only)

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 3. Train ML Model

```bash
# Download dataset and train XGBoost model
python ml_training.py

# Output:
# [OK] Model saved to models/fraud_detector_xgboost.pkl
# [OK] Scaler saved to models/feature_scaler.pkl
# [OK] Feature columns saved to models/feature_columns.pkl
```

This creates:
- `models/fraud_detector_xgboost.pkl` - Trained model
- `models/feature_scaler.pkl` - Feature scaler
- `models/feature_columns.pkl` - Feature column names

### 4. Deploy with Docker Compose

```bash
# Build and start all services
docker-compose -f docker-compose-new.yml up -d

# Verify services are running
docker-compose -f docker-compose-new.yml ps

# Expected output:
# CONTAINER ID   SERVICE                 STATUS
# ...            fraud-detection-postgres  Up (healthy)
# ...            fraud-detection-redis    Up (healthy)
# ...            fraud-detection-kafka    Up (healthy)
# ...            fraud-detection-ml-service  Up (healthy)
# ...            fraud-detection-app      Up (healthy)
# ...            fraud-detection-worker-1  Up
# ...            fraud-detection-worker-2  Up
```

### 5. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Application | http://localhost:8000 | user@example.com / password123 |
| ML Service | http://localhost:8001/docs | - |
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3000 | admin / admin |
| PostgreSQL | localhost:5432 | fraud_user / fraud_password |
| Redis | localhost:6379 | - |
| Kafka | localhost:9092 | - |

---

## ML Model Training

### Dataset

The training pipeline supports two data sources:

**Option 1: Kaggle Dataset (Recommended)**

```bash
# Install Kaggle CLI
pip install kaggle

# Download credentials from https://www.kaggle.com/settings/account
# Place kaggle.json at ~/.kaggle/kaggle.json

# Train with real data
python ml_training.py
```

**Option 2: Synthetic Dataset**

If Kaggle API is unavailable, the script automatically generates synthetic data matching the real dataset distribution.

### Model Details

```
Model: XGBoost Classifier
- Trees: 200
- Max Depth: 7
- Learning Rate: 0.1
- Input Features: 29 (V1-V28 + Amount)
- Output: Fraud Probability (0-1)
- Metrics:
  - Test F1-Score: ~0.85
  - Test AUC-ROC: ~0.98
  - Training Time: ~2 minutes
```

### Feature Columns

```python
[
  'V1', 'V2', ..., 'V28',  # Normalized transaction features
  'Amount',                 # Transaction amount
  'user_txn_count',        # User transactions in 5min window
  'ip_txn_count',          # IP transactions in 5min window
  'amount_velocity',       # Total amount by user in 5min window
  'is_known_device'        # Device known to user
]
```

---

## Docker Deployment

### Full Stack with Docker Compose

```bash
# Start all services
docker-compose -f docker-compose-new.yml up -d

# View logs
docker-compose -f docker-compose-new.yml logs -f app

# Stop services
docker-compose -f docker-compose-new.yml down

# Remove volumes (WARNING: Deletes data)
docker-compose -f docker-compose-new.yml down -v
```

### Individual Services

```bash
# ML Service Only
docker run -p 8001:8001 \
  -v $(pwd)/models:/models \
  fraud-detection-ml:latest

# Worker Node
docker run \
  -e DATABASE_URL=postgresql://... \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e ML_SERVICE_URL=http://ml-service:8001 \
  fraud-detection-worker:latest
```

### Kubernetes Deployment

```bash
# Apply manifests
kubectl apply -f k8s/

# Scale workers
kubectl scale deployment fraud-detection-worker --replicas=5

# View status
kubectl get pods -l app=fraud-detection
```

---

## Configuration

### Environment Variables

#### Database
```bash
DATABASE_URL=postgresql://user:pass@host:5432/fraud_detection
# Connection pool settings (PostgreSQL only):
# - pool_size: 20 connections
# - max_overflow: 40 additional connections
# - pool_recycle: 3600 seconds
```

#### Cache
```bash
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0
# Features cached with TTL:
# - Velocity counters: 300s (5min)
# - User sessions: 3600s (1hr)
# - Model predictions: 300s (5min)
```

#### Message Queue
```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
# Topics:
# - transactions: Inbound transaction events
# - fraud-scores: ML model predictions
# - fraud-alerts: High-risk fraud notifications
# - disputes: Customer dispute events
```

#### ML Service
```bash
ML_SERVICE_URL=http://ml-service:8001
MODEL_PATH=/models/fraud_detector_xgboost.pkl
SCALER_PATH=/models/feature_scaler.pkl
FEATURE_COLUMNS_PATH=/models/feature_columns.pkl
```

---

## Monitoring & Observability

### Prometheus Metrics

**Application Metrics:**
- `fraud_atlas_requests_total` - Total HTTP requests
- `ml_predictions_total` - Model predictions by outcome
- `ml_prediction_latency_seconds` - Inference latency histogram
- `ml_fraud_score` - Fraud probability score distribution

**Scrape Endpoints:**
- App: http://app:8000/metrics
- ML Service: http://ml-service:8001/metrics

### Grafana Dashboards

Three pre-configured dashboards:

**1. Real-Time Monitoring**
- Fraud detection rate (5m)
- Fraudulent transactions count
- Model inference latency (P50, P99)
- Fraud score distribution

**2. System Performance**
- Transaction throughput (TPS)
- Total transactions (1h)
- Fraud detection rate
- P99 latency
- Average fraud score

**3. Service Health**
- Database connections
- Redis memory usage
- Kafka consumer lag
- Error rates

**Access:**
- URL: http://localhost:3000
- Admin: admin / admin
- Navigate to Dashboards > Fraud Detection

### Logging

Structured logs at:
- `/app/logs/main.log`
- `/app/logs/ml_service.log`
- `/app/logs/workers.log`

Log format:
```
[TIMESTAMP] - [LEVEL] - [SERVICE] - [MESSAGE]
[2024-01-15 10:30:45] - [INFO] - [WORKER-1] - Processed transaction TX-ABC123
```

---

## CI/CD Pipeline

### GitHub Actions Workflow

Triggers on: `push` to main/develop, `pull_request`

**Stages:**

1. **Lint** (Ruff + Black)
   - Check code quality
   - Format validation

2. **Test** (pytest + codecov)
   - Unit tests with coverage
   - Database tests with PostgreSQL service
   - Upload to codecov

3. **Build** (Multi-stage Docker)
   - Main application
   - ML service
   - Worker nodes
   - Push to Docker Hub

4. **Security** (Trivy)
   - Vulnerability scanning
   - SARIF report to GitHub Security

5. **Deploy** (Staging)
   - SSH to staging server
   - Pull latest images
   - Run smoke tests

6. **Notify** (Slack)
   - Pipeline status

### Secrets Required

```bash
DOCKER_USERNAME=your_username
DOCKER_PASSWORD=your_password_token
DEPLOY_KEY=your_ssh_private_key
STAGING_SERVER=staging.example.com
STAGING_URL=https://staging.example.com
SLACK_WEBHOOK_URL=https://hooks.slack.com/...
```

### Local Testing

```bash
# Run linting
ruff check .
black --check .

# Run tests
pytest tests/ -v --cov=app

# Build Docker images
docker build -f Dockerfile -t fraud-detection:latest .
docker build -f Dockerfile.ml-service -t fraud-detection-ml:latest .
```

---

## Troubleshooting

### Services Won't Start

```bash
# Check logs
docker-compose -f docker-compose-new.yml logs postgres
docker-compose -f docker-compose-new.yml logs kafka

# Verify ports available
netstat -an | grep 5432  # PostgreSQL
netstat -an | grep 6379  # Redis
netstat -an | grep 9092  # Kafka
```

### Database Connection Issues

```bash
# Test PostgreSQL connection
docker exec -it fraud-detection-postgres \
  psql -U fraud_user -d fraud_detection -c "SELECT 1"

# Reset database
docker-compose -f docker-compose-new.yml down -v
docker-compose -f docker-compose-new.yml up -d postgres
```

### ML Model Not Loading

```bash
# Check model files exist
ls -la models/

# Verify model is valid
python -c "import joblib; joblib.load('models/fraud_detector_xgboost.pkl')"

# Retrain if needed
python ml_training.py
```

### Kafka Connection Issues

```bash
# Check Kafka is running
docker exec fraud-detection-kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check topics
docker exec fraud-detection-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Redis Connection Issues

```bash
# Test Redis
docker exec fraud-detection-redis redis-cli ping

# Check memory usage
docker exec fraud-detection-redis redis-cli INFO memory
```

### Worker Nodes Not Processing

```bash
# Check worker logs
docker logs fraud-detection-worker-1

# Verify Kafka consumer
docker exec fraud-detection-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

### High Latency

```bash
# Check ML service latency
curl http://localhost:8001/metrics | grep ml_prediction_latency

# Check database query times
# Enable PostgreSQL slow query log:
docker exec fraud-detection-postgres \
  psql -U fraud_user -d fraud_detection \
  -c "ALTER SYSTEM SET log_min_duration_statement = 1000;"
```

---

## Performance Tuning

### Database Optimization

```sql
-- Create indexes for frequent queries
CREATE INDEX idx_transactions_user_timestamp 
  ON transactions(user_id, timestamp DESC);

CREATE INDEX idx_fraud_alerts_timestamp 
  ON fraud_alerts(timestamp DESC);

-- Analyze query plans
EXPLAIN ANALYZE 
  SELECT * FROM transactions 
  WHERE user_id = 1 
  ORDER BY timestamp DESC 
  LIMIT 20;
```

### Redis Optimization

```bash
# Monitor memory
docker exec fraud-detection-redis redis-cli INFO memory

# Set max memory policy
docker exec fraud-detection-redis redis-cli \
  CONFIG SET maxmemory-policy allkeys-lru

# Enable persistence
# Already configured in docker-compose (--appendonly yes)
```

### Kafka Tuning

```bash
# Increase consumer threads
WORKERS=10 python kafka_worker.py

# Monitor consumer lag
docker exec fraud-detection-kafka \
  kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group fraud-detector-worker \
  --describe
```

---

## Security Best Practices

### Secrets Management

```bash
# Use environment variables (never commit secrets)
# Or use secret managers:
# - AWS Secrets Manager
# - HashiCorp Vault
# - Kubernetes Secrets

# Docker Compose secrets
docker secret create db_password -
docker secret create api_key -
```

### Network Security

```bash
# Firewall rules
# Allow only required ports:
# - 8000 (API)
# - 3000 (Grafana)
# - 9090 (Prometheus)
# Block internal services (5432, 6379, 9092)

# Use network policies in Kubernetes
```

### Database Security

```bash
# Change default passwords
ALTER USER fraud_user WITH PASSWORD 'strong_password_here';

# Restrict connections
# In postgresql.conf:
# listen_addresses = 'localhost'

# Enable SSL
# In docker-compose: add --ssl flags
```

---

## Scaling for Production

### Horizontal Scaling

```bash
# Scale workers
docker-compose -f docker-compose-new.yml up -d --scale worker=5

# Kubernetes
kubectl scale deployment fraud-detection-worker --replicas=10
```

### Load Balancing

```bash
# Nginx configuration
upstream fraud_detection {
  server app:8000;
  server app:8000;
  server app:8000;
}

server {
  listen 80;
  location / {
    proxy_pass http://fraud_detection;
  }
}
```

### Multi-Region Deployment

- Replicate PostgreSQL with streaming replication
- Kafka clusters in each region
- Global load balancer (GeoDNS)

---

## Support & Resources

- Documentation: [Wiki](https://github.com/your-repo/wiki)
- Issues: [GitHub Issues](https://github.com/your-repo/issues)
- Contributing: [CONTRIBUTING.md](./CONTRIBUTING.md)

---

**Last Updated:** 2024-01-15
**Version:** 1.0.0
