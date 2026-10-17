"""
Distributed Tracing with OpenTelemetry
Enables end-to-end tracing across API → Kafka → Workers → Database
Includes advanced Prometheus metrics for fraud detection domain
"""

import time
import uuid
from typing import Dict, Optional, Callable
from datetime import datetime
from functools import wraps
import logging
import json
import asyncio

from prometheus_client import Counter, Histogram, Gauge

logger = logging.getLogger(__name__)

# Optional OpenTelemetry imports (gracefully degrade if not available)
try:
    from opentelemetry import trace, metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    logger.warning("[WARN] OpenTelemetry not available - tracing will be limited")


# ============ Advanced Domain-Specific Prometheus Metrics ============

# Transaction metrics
transactions_processed_total = Counter(
    'fraud_detection_transactions_processed_total',
    'Total transactions processed',
    ['status', 'decision']  # status: success/failed, decision: approve/flag/block
)

transaction_latency = Histogram(
    'fraud_detection_transaction_latency_seconds',
    'End-to-end transaction processing latency',
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0)
)

kafka_processing_latency = Histogram(
    'fraud_detection_kafka_processing_latency_seconds',
    'Kafka consumer processing latency',
    buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1.0)
)

ml_model_predictions = Counter(
    'fraud_detection_ml_predictions_total',
    'ML model predictions by outcome',
    ['model_version', 'outcome']  # outcome: fraud/legitimate
)

velocity_check_latency = Histogram(
    'fraud_detection_velocity_check_latency_seconds',
    'Redis velocity check latency',
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01)
)

feature_engineering_latency = Histogram(
    'fraud_detection_feature_engineering_latency_seconds',
    'Feature engineering latency',
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1)
)

fraud_case_amount = Gauge(
    'fraud_detection_flagged_amount_total',
    'Total amount flagged as fraudulent'
)

model_prediction_confidence = Histogram(
    'fraud_detection_model_confidence',
    'Distribution of model confidence scores',
    buckets=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
)

# Database metrics
database_query_latency = Histogram(
    'fraud_detection_database_query_latency_seconds',
    'Database query latency by query type',
    ['query_type'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5)
)

redis_operation_latency = Histogram(
    'fraud_detection_redis_operation_latency_seconds',
    'Redis operation latency by operation',
    ['operation'],
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01)
)

# Worker metrics
worker_queue_depth = Gauge(
    'fraud_detection_worker_queue_depth',
    'Kafka consumer queue depth'
)

worker_processing_errors = Counter(
    'fraud_detection_worker_processing_errors_total',
    'Total worker processing errors'
)

# Alert metrics
fraud_alerts_created = Counter(
    'fraud_detection_alerts_created_total',
    'Fraud alerts created by severity',
    ['severity']  # severity: low/medium/high/critical
)


class TracingConfig:
    """OpenTelemetry tracing configuration"""
    
    SERVICE_NAME = "fraud-detection-pipeline"
    JAEGER_AGENT_HOST = "localhost"
    JAEGER_AGENT_PORT = 6831
    JAEGER_ENABLED = False  # Set to True to enable
    
    @classmethod
    def initialize(cls):
        """Initialize OpenTelemetry tracing"""
        if not OTEL_AVAILABLE:
            logger.info("[INFO] OpenTelemetry disabled (packages not installed)")
            return False
        
        try:
            # Resource identification
            resource = Resource.create({
                SERVICE_NAME: cls.SERVICE_NAME,
                "environment": "production",
            })
            
            # Create tracer provider
            tracer_provider = TracerProvider(resource=resource)
            
            # Set global tracer provider
            trace.set_tracer_provider(tracer_provider)
            
            logger.info("[OK] OpenTelemetry tracing initialized")
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to initialize tracing: {e}")
            return False


class TraceContext:
    """Context manager for managing trace IDs and spans"""
    
    def __init__(self, trace_id: Optional[str] = None, span_name: str = "transaction"):
        self.trace_id = trace_id or str(uuid.uuid4())
        self.span_name = span_name
        self.span = None
        self.start_time = None
        
        if OTEL_AVAILABLE:
            try:
                self.tracer = trace.get_tracer(__name__)
            except:
                self.tracer = None
        else:
            self.tracer = None
    
    def __enter__(self):
        """Start span and record start time"""
        if self.tracer:
            try:
                self.span = self.tracer.start_span(self.span_name)
                self.span.set_attribute("trace_id", self.trace_id)
            except:
                self.span = None
        
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """End span and record duration"""
        if self.span:
            try:
                duration_ms = (time.time() - self.start_time) * 1000
                self.span.set_attribute("duration_ms", duration_ms)
                
                if exc_type:
                    self.span.set_attribute("error", True)
                    self.span.set_attribute("error_type", exc_type.__name__)
                    self.span.set_attribute("error_message", str(exc_val))
                
                self.span.end()
            except:
                pass
    
    def add_event(self, name: str, attributes: Optional[Dict] = None):
        """Add event to current span"""
        if self.span:
            try:
                self.span.add_event(name, attributes or {})
            except:
                pass
    
    def set_attribute(self, key: str, value):
        """Set span attribute"""
        if self.span:
            try:
                self.span.set_attribute(key, value)
            except:
                pass


def trace_transaction(func: Callable) -> Callable:
    """
    Decorator to trace transaction processing
    Automatically captures timing, errors, and context
    """
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        trace_id = kwargs.get('trace_id') or str(uuid.uuid4())
        
        with TraceContext(trace_id=trace_id, span_name=func.__name__) as ctx:
            try:
                # Record function parameters
                ctx.set_attribute("function", func.__name__)
                
                # Extract transaction data if available
                if 'transaction_id' in kwargs:
                    ctx.set_attribute("transaction_id", kwargs['transaction_id'])
                if 'user_id' in kwargs:
                    ctx.set_attribute("user_id", kwargs['user_id'])
                
                # Call function
                result = await func(*args, **kwargs)
                
                # Record success
                ctx.add_event("function_completed", {"status": "success"})
                
                return result
            
            except Exception as e:
                ctx.add_event("function_failed", {
                    "error": str(e),
                    "error_type": type(e).__name__
                })
                raise
    
    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        trace_id = kwargs.get('trace_id') or str(uuid.uuid4())
        
        with TraceContext(trace_id=trace_id, span_name=func.__name__) as ctx:
            try:
                # Record function parameters
                ctx.set_attribute("function", func.__name__)
                
                if 'transaction_id' in kwargs:
                    ctx.set_attribute("transaction_id", kwargs['transaction_id'])
                if 'user_id' in kwargs:
                    ctx.set_attribute("user_id", kwargs['user_id'])
                
                result = func(*args, **kwargs)
                ctx.add_event("function_completed", {"status": "success"})
                return result
            
            except Exception as e:
                ctx.add_event("function_failed", {
                    "error": str(e),
                    "error_type": type(e).__name__
                })
                raise
    
    # Return appropriate wrapper based on function type
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper


def record_velocity_check(latency_ms: float, card_count: int, transaction_count: int):
    """Record velocity check metrics"""
    velocity_check_latency.observe(latency_ms / 1000.0)
    
    # Also track as Prometheus metric
    from prometheus_client import Histogram
    velocity_checks = Histogram(
        'fraud_detection_velocity_metrics',
        'Velocity metrics from checks',
        ['metric_type']
    )
    velocity_checks.labels(metric_type='card_count').observe(card_count)
    velocity_checks.labels(metric_type='txn_count').observe(transaction_count)


def record_ml_prediction(
    model_version: str,
    fraud_probability: float,
    is_fraud: bool,
    latency_ms: float,
):
    """Record ML model prediction metrics"""
    # Counter for prediction outcome
    outcome = "fraud" if is_fraud else "legitimate"
    ml_model_predictions.labels(
        model_version=model_version,
        outcome=outcome
    ).inc()
    
    # Histogram for confidence score
    model_prediction_confidence.observe(fraud_probability)


def record_transaction_end_to_end(
    transaction_id: str,
    user_id: str,
    decision: str,
    total_latency_ms: float,
    amount: float,
    is_fraud: bool,
):
    """Record complete transaction metrics"""
    # Transaction processed
    transactions_processed_total.labels(
        status="success",
        decision=decision
    ).inc()
    
    # Latency
    transaction_latency.observe(total_latency_ms / 1000.0)
    
    # Track fraudulent amount
    if is_fraud:
        fraud_case_amount.inc(amount)


def inject_trace_id_to_kafka_headers(trace_id: str) -> Dict[str, str]:
    """Convert trace ID to Kafka message headers format"""
    return {
        "x-trace-id": trace_id.encode('utf-8'),
        "x-timestamp": str(int(time.time() * 1000)).encode('utf-8'),
    }


def extract_trace_id_from_kafka_headers(headers: list) -> Optional[str]:
    """Extract trace ID from Kafka message headers"""
    if not headers:
        return None
    
    for key, value in headers:
        if key == b"x-trace-id":
            return value.decode('utf-8')
    
    return None


# Async context manager for precise timing
class TimedOperation:
    """Context manager for recording operation latency"""
    
    def __init__(self, operation_name: str, metric_callback: Optional[Callable] = None):
        self.operation_name = operation_name
        self.metric_callback = metric_callback
        self.start_time = None
        self.duration_ms = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.duration_ms = (time.time() - self.start_time) * 1000
        
        if self.metric_callback:
            self.metric_callback(self.duration_ms)
        
        if exc_type:
            logger.error(
                f"[ERROR] {self.operation_name} failed after {self.duration_ms:.2f}ms: {exc_val}"
            )
        else:
            logger.debug(f"[OK] {self.operation_name} completed in {self.duration_ms:.2f}ms")


class TraceLogger:
    """Centralized trace logging for debugging"""
    
    def __init__(self):
        self.traces: Dict[str, list] = {}
    
    def log_event(self, trace_id: str, event: str, details: Optional[Dict] = None):
        """Log an event for a trace"""
        if trace_id not in self.traces:
            self.traces[trace_id] = []
        
        event_log = {
            "timestamp": datetime.utcnow().isoformat(),
            "event": event,
            "details": details or {},
        }
        
        self.traces[trace_id].append(event_log)
    
    def get_trace(self, trace_id: str) -> list:
        """Get all events for a trace"""
        return self.traces.get(trace_id, [])
    
    def export_trace(self, trace_id: str) -> str:
        """Export trace as JSON"""
        return json.dumps(self.get_trace(trace_id), indent=2)
    
    def clear_old_traces(self, max_age_hours: int = 24):
        """Clear traces older than max_age_hours"""
        now = datetime.utcnow()
        to_delete = []
        
        for trace_id, events in self.traces.items():
            if events:
                last_event_time = datetime.fromisoformat(events[-1]["timestamp"])
                age_hours = (now - last_event_time).total_seconds() / 3600
                if age_hours > max_age_hours:
                    to_delete.append(trace_id)
        
        for trace_id in to_delete:
            del self.traces[trace_id]
        
        return len(to_delete)


# Global trace logger
_trace_logger = TraceLogger()


def get_trace_logger() -> TraceLogger:
    """Get global trace logger"""
    return _trace_logger
