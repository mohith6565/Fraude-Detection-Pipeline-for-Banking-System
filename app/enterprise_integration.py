"""
Enterprise Integration Module
Integrates Kafka, Redis, and ML services with the main application
"""

import os
import json
import uuid
from datetime import datetime
from typing import Optional, Dict, List
import logging
import aiohttp
import asyncio

from app.kafka_config import (
    KafkaProducerService,
    TransactionEvent,
    FraudScoreEvent,
    FraudAlertEvent,
    TRANSACTIONS_TOPIC,
)
from app.feature_store import get_feature_store

logger = logging.getLogger(__name__)

# Configuration from environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ML_SERVICE_URL = os.getenv("ML_SERVICE_URL", "http://localhost:8001")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Global instances
kafka_producer = None
feature_store = None


def init_enterprise_services():
    """Initialize all enterprise services"""
    global kafka_producer, feature_store
    
    try:
        # Initialize Kafka producer
        kafka_producer = KafkaProducerService(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        logger.info("[OK] Kafka producer initialized")
    except Exception as e:
        logger.error(f"[ERROR] Failed to initialize Kafka: {e}")
        kafka_producer = None
    
    try:
        # Initialize feature store
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        feature_store = get_feature_store(host=redis_host, port=redis_port)
        if feature_store.health_check():
            logger.info("[OK] Redis feature store initialized")
        else:
            logger.warning("[WARN] Redis connection failed, feature store disabled")
            feature_store = None
    except Exception as e:
        logger.error(f"[ERROR] Failed to initialize feature store: {e}")
        feature_store = None


async def call_ml_service(features: List[float]) -> Optional[Dict]:
    """
    Call ML microservice for fraud prediction
    
    Args:
        features: List of transaction features
        
    Returns:
        Fraud score response or None if error
    """
    try:
        async with aiohttp.ClientSession() as session:
            payload = {"features": features}
            async with session.post(
                f"{ML_SERVICE_URL}/predict",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.error(f"[ERROR] ML service error: {resp.status}")
                    return None
    except Exception as e:
        logger.error(f"[ERROR] Failed to call ML service: {e}")
        return None


async def publish_transaction_to_kafka(
    transaction_id: str,
    user_id: str,
    amount: float,
    merchant: str,
    category: str,
    ip_address: str,
    device_id: str,
    location: str,
    features: List[float],
) -> bool:
    """
    Publish transaction to Kafka for async processing
    
    Args:
        transaction_id: Unique transaction ID
        user_id: User identifier
        amount: Transaction amount
        merchant: Merchant name
        category: Merchant category
        ip_address: IP address of transaction
        device_id: Device fingerprint
        location: Geographic location
        features: Raw features for ML model
        
    Returns:
        True if published successfully
    """
    if not kafka_producer or not kafka_producer.is_connected():
        logger.warning("[WARN] Kafka producer not connected, skipping event publishing")
        return False
    
    try:
        event = TransactionEvent(
            transaction_id=transaction_id,
            user_id=user_id,
            amount=amount,
            timestamp=datetime.utcnow().isoformat(),
            merchant=merchant,
            category=category,
            ip_address=ip_address,
            device_id=device_id,
            location=location,
            features=features,
        )
        
        success = kafka_producer.publish_transaction(event)
        if success:
            logger.info(f"[OK] Published transaction {transaction_id} to Kafka")
        else:
            logger.error(f"[ERROR] Failed to publish transaction {transaction_id}")
        return success
        
    except Exception as e:
        logger.error(f"[ERROR] Exception publishing transaction: {e}")
        return False


async def publish_fraud_alert_to_kafka(
    transaction_id: str,
    user_id: str,
    fraud_score: int,
    reason: str,
) -> bool:
    """Publish fraud alert to Kafka"""
    if not kafka_producer or not kafka_producer.is_connected():
        return False
    
    try:
        event = FraudAlertEvent(
            alert_id=str(uuid.uuid4()),
            transaction_id=transaction_id,
            user_id=user_id,
            fraud_score=fraud_score,
            reason=reason,
            timestamp=datetime.utcnow().isoformat(),
            action_required=True,
        )
        
        success = kafka_producer.publish_fraud_alert(event)
        if success:
            logger.info(f"[OK] Published fraud alert for {transaction_id}")
        return success
        
    except Exception as e:
        logger.error(f"[ERROR] Exception publishing fraud alert: {e}")
        return False


def get_velocity_features(user_id: str, ip_address: str) -> Dict:
    """
    Get velocity features from Redis feature store
    
    Returns dict with velocity metrics
    """
    if not feature_store:
        return {
            "user_txn_count": 0,
            "ip_txn_count": 0,
            "amount_velocity": 0.0,
        }
    
    try:
        return {
            "user_txn_count": feature_store.get_transaction_count(user_id),
            "ip_txn_count": feature_store.get_ip_transaction_count(ip_address),
            "amount_velocity": feature_store.get_amount_velocity(user_id),
        }
    except Exception as e:
        logger.error(f"[ERROR] Failed to get velocity features: {e}")
        return {
            "user_txn_count": 0,
            "ip_txn_count": 0,
            "amount_velocity": 0.0,
        }


def cache_user_session(user_id: str, session_data: Dict):
    """Cache user session in Redis"""
    if not feature_store:
        return
    
    try:
        feature_store.cache_user_session(user_id, session_data, ttl=3600)
    except Exception as e:
        logger.error(f"[ERROR] Failed to cache session: {e}")


def get_user_session(user_id: str) -> Optional[Dict]:
    """Get cached user session from Redis"""
    if not feature_store:
        return None
    
    try:
        return feature_store.get_user_session(user_id)
    except Exception as e:
        logger.error(f"[ERROR] Failed to get session: {e}")
        return None


def track_device(user_id: str, device_id: str):
    """Track device for user"""
    if not feature_store:
        return
    
    try:
        feature_store.track_device(user_id, device_id)
    except Exception as e:
        logger.error(f"[ERROR] Failed to track device: {e}")


def track_location(user_id: str, location: str):
    """Track location for user"""
    if not feature_store:
        return
    
    try:
        feature_store.track_location(user_id, location)
    except Exception as e:
        logger.error(f"[ERROR] Failed to track location: {e}")


def get_enterprise_stats() -> Dict:
    """Get stats from all enterprise services"""
    stats = {
        "kafka": {
            "connected": kafka_producer.is_connected() if kafka_producer else False,
        },
        "redis": {
            "connected": feature_store.health_check() if feature_store else False,
        },
    }
    
    if feature_store and feature_store.health_check():
        try:
            stats["redis"]["stats"] = feature_store.get_stats()
        except Exception as e:
            logger.error(f"[ERROR] Failed to get Redis stats: {e}")
    
    return stats
