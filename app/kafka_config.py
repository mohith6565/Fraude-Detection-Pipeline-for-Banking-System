"""
Kafka Configuration and Utilities
Event streaming setup for asynchronous transaction processing
"""

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
from typing import Dict, Optional, Callable
from dataclasses import dataclass, asdict
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Topic names
TRANSACTIONS_TOPIC = "transactions"
FRAUD_SCORES_TOPIC = "fraud-scores"
ALERTS_TOPIC = "fraud-alerts"
DISPUTES_TOPIC = "disputes"

@dataclass
class TransactionEvent:
    """Kafka transaction event"""
    transaction_id: str
    user_id: str
    amount: float
    timestamp: str
    merchant: str
    category: str
    ip_address: str
    device_id: str
    location: str
    features: list  # Raw transaction features for ML model

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(data: str) -> "TransactionEvent":
        d = json.loads(data)
        return TransactionEvent(**d)


@dataclass
class FraudScoreEvent:
    """Fraud detection result event"""
    transaction_id: str
    fraud_probability: float
    is_fraud: bool
    fraud_score: int
    confidence: float
    latency_ms: float
    timestamp: str
    model_version: str = "1.0.0"

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(data: str) -> "FraudScoreEvent":
        d = json.loads(data)
        return FraudScoreEvent(**d)


@dataclass
class FraudAlertEvent:
    """Fraud alert for user notification"""
    alert_id: str
    transaction_id: str
    user_id: str
    fraud_score: int
    reason: str
    timestamp: str
    action_required: bool = True

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(data: str) -> "FraudAlertEvent":
        d = json.loads(data)
        return FraudAlertEvent(**d)


class KafkaProducerService:
    """Produces events to Kafka"""

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """Initialize Kafka producer"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._connect()

    def _connect(self):
        """Establish connection to Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: v.encode("utf-8"),
                acks="all",  # Wait for all replicas
                retries=3,
            )
            logger.info(f"[OK] Connected to Kafka at {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"[ERROR] Failed to connect to Kafka: {e}")
            self.producer = None

    def is_connected(self) -> bool:
        """Check if producer is connected"""
        return self.producer is not None

    def publish_transaction(self, event: TransactionEvent) -> bool:
        """Publish transaction event to Kafka"""
        if not self.is_connected():
            logger.warning("[WARN] Producer not connected, buffering event locally")
            return False

        try:
            future = self.producer.send(
                TRANSACTIONS_TOPIC, value=event.to_json()
            )
            record_metadata = future.get(timeout=10)
            logger.info(
                f"[OK] Published transaction {event.transaction_id} to partition {record_metadata.partition}"
            )
            return True
        except KafkaError as e:
            logger.error(f"[ERROR] Failed to publish transaction: {e}")
            return False

    def publish_fraud_score(self, event: FraudScoreEvent) -> bool:
        """Publish fraud score result"""
        if not self.is_connected():
            return False

        try:
            future = self.producer.send(FRAUD_SCORES_TOPIC, value=event.to_json())
            future.get(timeout=10)
            logger.info(f"[OK] Published fraud score for {event.transaction_id}")
            return True
        except KafkaError as e:
            logger.error(f"[ERROR] Failed to publish fraud score: {e}")
            return False

    def publish_fraud_alert(self, event: FraudAlertEvent) -> bool:
        """Publish fraud alert"""
        if not self.is_connected():
            return False

        try:
            future = self.producer.send(ALERTS_TOPIC, value=event.to_json())
            future.get(timeout=10)
            logger.info(f"[OK] Published fraud alert {event.alert_id}")
            return True
        except KafkaError as e:
            logger.error(f"[ERROR] Failed to publish fraud alert: {e}")
            return False

    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.close()


class KafkaConsumerService:
    """Consumes events from Kafka"""

    def __init__(
        self,
        group_id: str,
        bootstrap_servers: str = "localhost:9092",
        topics: list = None,
    ):
        """Initialize Kafka consumer"""
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics or [TRANSACTIONS_TOPIC]
        self.consumer = None
        self._connect()

    def _connect(self):
        """Establish connection to Kafka"""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: m.decode("utf-8"),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                max_poll_records=100,
            )
            logger.info(
                f"[OK] Consumer group '{self.group_id}' connected to {self.bootstrap_servers}"
            )
        except Exception as e:
            logger.error(f"[ERROR] Failed to connect consumer: {e}")
            self.consumer = None

    def is_connected(self) -> bool:
        """Check if consumer is connected"""
        return self.consumer is not None

    def consume_transactions(
        self, callback: Callable[[TransactionEvent], None], timeout_ms: int = 1000
    ):
        """
        Consume transaction events
        
        Args:
            callback: Function to call for each transaction event
            timeout_ms: Consumer timeout in milliseconds
        """
        if not self.is_connected():
            logger.error("[ERROR] Consumer not connected")
            return

        try:
            for message in self.consumer:
                try:
                    event = TransactionEvent.from_json(message.value)
                    callback(event)
                except Exception as e:
                    logger.error(f"[ERROR] Failed to process message: {e}")
        except Exception as e:
            logger.error(f"[ERROR] Consumer error: {e}")

    def close(self):
        """Close consumer connection"""
        if self.consumer:
            self.consumer.close()


# Global producer instance
_producer = None


def get_kafka_producer(bootstrap_servers: str = "localhost:9092") -> KafkaProducerService:
    """Get or create global Kafka producer"""
    global _producer
    if _producer is None:
        _producer = KafkaProducerService(bootstrap_servers)
    return _producer
