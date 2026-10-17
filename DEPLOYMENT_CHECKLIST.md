# 🚀 Deployment Checklist

## Pre-Deployment

### Code Quality
- [ ] Run `ruff check .` - no errors
- [ ] Run `black --check .` - formatting compliant
- [ ] Run `pytest tests/ -v --cov=app` - all tests pass
- [ ] Review security scan results

### ML Model
- [ ] Dataset available (Kaggle or synthetic)
- [ ] Run `python ml_training.py` successfully
- [ ] Verify model files exist:
  - `models/fraud_detector_xgboost.pkl`
  - `models/feature_scaler.pkl`
  - `models/feature_columns.pkl`
- [ ] Model performance acceptable (F1 > 0.80, AUC > 0.95)

### Environment Configuration
- [ ] `.env` file created with all required variables
- [ ] Database URL configured
- [ ] Redis URL configured
- [ ] Kafka bootstrap servers configured
- [ ] ML Service URL configured
- [ ] No secrets in code/version control

### Docker Images
- [ ] Main app image builds: `docker build -f Dockerfile -t fraud-detection:latest .`
- [ ] ML service image builds: `docker build -f Dockerfile.ml-service -t fraud-detection-ml:latest .`
- [ ] Worker image builds: `docker build -f Dockerfile.worker -t fraud-detection-worker:latest .`
- [ ] Images pushed to registry (if using private registry)

---

## Local Testing

### Docker Compose Stack
- [ ] All services start: `docker-compose -f docker-compose-new.yml up -d`
- [ ] All services healthy: `docker-compose -f docker-compose-new.yml ps`

### Service Verification
- [ ] PostgreSQL accessible: `docker-compose -f docker-compose-new.yml exec postgres pg_isready`
- [ ] Redis accessible: `docker-compose -f docker-compose-new.yml exec redis redis-cli ping`
- [ ] Kafka accessible: Check broker health
- [ ] ML Service responds: `curl http://localhost:8001/health`
- [ ] Main app responds: `curl http://localhost:8000/health`

### Functional Testing
- [ ] Can login: Visit http://localhost:8000
- [ ] Can view dashboard: User dashboard loads
- [ ] Can view admin panel: Admin dashboard loads (as admin)
- [ ] ML model serving: `curl -X POST http://localhost:8001/predict`
- [ ] Metrics available: `curl http://localhost:9090`
- [ ] Grafana dashboards: http://localhost:3000

### Load Testing (Optional)
- [ ] Can handle 100+ concurrent transactions
- [ ] Model inference latency < 100ms (P99)
- [ ] No database connection pool exhaustion
- [ ] Redis doesn't run out of memory

---

## Staging Deployment

### Infrastructure
- [ ] Staging server provisioned
- [ ] PostgreSQL 16 installed and optimized
- [ ] Redis 7+ installed
- [ ] Kafka cluster running (3 brokers min)
- [ ] Prometheus/Grafana running
- [ ] SSL certificates installed

### Application Deployment
- [ ] Docker images pulled
- [ ] `.env` configured for staging
- [ ] Database migrations applied
- [ ] Initial data seeded
- [ ] Application starts without errors

### Integration Tests
- [ ] E2E tests pass against staging
- [ ] Smoke tests pass (health checks, basic flows)
- [ ] Performance tests pass (throughput, latency)
- [ ] Security tests pass (auth, RBAC, encryption)

### Monitoring Setup
- [ ] Prometheus scraping metrics
- [ ] Grafana dashboards displaying data
- [ ] Alerts configured
- [ ] Log aggregation working

---

## Production Deployment

### Pre-Production
- [ ] Load balancer configured
- [ ] DNS records updated
- [ ] SSL/TLS certificates installed
- [ ] Backup strategy in place
- [ ] Disaster recovery plan documented

### Database
- [ ] PostgreSQL replicated/clustered
- [ ] Backups automated and tested
- [ ] Connection pooling configured (pool_size=20, max_overflow=40)
- [ ] Indexes created on frequently queried columns
- [ ] Slow query logging enabled

### Cache
- [ ] Redis clustered/replicated
- [ ] Memory limits set
- [ ] Eviction policy configured
- [ ] Persistence enabled (RDB + AOF)

### Message Queue
- [ ] Kafka replicated (min 3 brokers)
- [ ] Topics configured with replication factor 3
- [ ] Consumer group offsets managed
- [ ] Dead letter queue setup for failed messages

### Application
- [ ] API rate limiting configured
- [ ] CORS configured
- [ ] Session timeout configured
- [ ] Error handling and logging comprehensive

### Security
- [ ] Firewall rules in place
- [ ] Network policies configured
- [ ] Secrets in secure vault (not in code)
- [ ] SSL/TLS enforced
- [ ] Authentication and authorization verified

### Observability
- [ ] Prometheus monitoring all services
- [ ] Grafana dashboards setup
- [ ] Alert rules configured
- [ ] Log aggregation setup (ELK, Splunk, etc.)
- [ ] APM instrumentation (optional)

### CI/CD
- [ ] GitHub Actions secrets configured
- [ ] Docker Hub credentials stored
- [ ] Deployment secrets configured
- [ ] Auto-deployment on main push
- [ ] Rollback procedure documented

---

## Post-Deployment

### Validation
- [ ] All services healthy: `kubectl get pods` (Kubernetes)
- [ ] Traffic flowing through load balancer
- [ ] Metrics being collected
- [ ] Alerts not firing (unless expected)
- [ ] No error spikes in logs

### Performance Baseline
- [ ] Establish baseline throughput (TPS)
- [ ] Establish baseline latency (P50, P99)
- [ ] Establish baseline fraud detection rate
- [ ] Document resource utilization (CPU, memory, disk)

### Monitoring
- [ ] Set up on-call rotation
- [ ] Escalation procedures in place
- [ ] War room procedure for incidents
- [ ] Post-incident review process

### Documentation
- [ ] Runbook created for common issues
- [ ] Architecture diagram updated
- [ ] API documentation current
- [ ] Deployment procedure documented

---

## Ongoing Maintenance

### Weekly
- [ ] Review error logs
- [ ] Check resource utilization
- [ ] Verify backups completed
- [ ] Check for security updates

### Monthly
- [ ] Review performance metrics
- [ ] Analyze model drift
- [ ] Retrain model if needed
- [ ] Update dependencies if necessary
- [ ] Security audit

### Quarterly
- [ ] Capacity planning review
- [ ] Disaster recovery drill
- [ ] Load testing validation
- [ ] Architecture review

### Yearly
- [ ] Full security assessment
- [ ] Compliance audit
- [ ] Cost optimization review
- [ ] Strategic planning update

---

## Troubleshooting During Deployment

### Service won't start
```bash
# Check logs
docker logs <container_id>
docker-compose -f docker-compose-new.yml logs <service_name>

# Verify configuration
echo $DATABASE_URL
echo $KAFKA_BOOTSTRAP_SERVERS
echo $REDIS_URL
```

### Database connection issues
```bash
# Test connection
psql -h localhost -U fraud_user -d fraud_detection -c "SELECT 1"

# Check pool status
SELECT datname, usename, count(*) FROM pg_stat_activity GROUP BY datname, usename;
```

### Kafka issues
```bash
# Check broker status
kafka-broker-api-versions --bootstrap-server localhost:9092

# Check topics
kafka-topics --bootstrap-server localhost:9092 --list

# Check consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 --group fraud-detector-worker --describe
```

### Redis issues
```bash
# Test connection
redis-cli ping

# Check memory
redis-cli INFO memory

# Check keys
redis-cli DBSIZE
```

### Model not loading
```bash
# Verify model exists
ls -la models/

# Test model loading
python -c "import joblib; joblib.load('models/fraud_detector_xgboost.pkl')"
```

---

## Rollback Procedure

If deployment fails or causes issues:

```bash
# 1. Stop new deployment
kubectl rollout undo deployment/fraud-detection-app

# 2. Or revert Docker Compose
docker-compose -f docker-compose-old.yml down
docker-compose -f docker-compose-old.yml up -d

# 3. Verify rollback
curl http://app-server/health

# 4. Post-incident review
# Document what went wrong and how to prevent it
```

---

## Success Criteria

Deployment is considered successful when:

✅ All services are healthy and responding
✅ Database migrations completed without errors
✅ Model is loaded and serving predictions
✅ Metrics are being collected and graphed
✅ At least one full transaction processed end-to-end
✅ Fraud alert triggered for test fraudulent transaction
✅ All dashboards displaying live data
✅ No error rates elevated compared to baseline
✅ Performance metrics within acceptable range
✅ Monitoring and alerting operational
✅ Team trained on new system
✅ Documentation updated and published

---

**Last Updated:** 2024-01-15
**Maintained By:** DevOps Team
