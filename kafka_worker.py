"""
Kafka Consumer Worker Node
Asynchronously processes transaction events from Kafka,
runs ML model inference, and updates database
"""

import asyncio
import json
import logging
import sys
import time
import uuid
from datetime import datetime
from typing import Optional

import aiohttp
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.kafka_config import (
    KafkaConsumerService,
    TransactionEvent,
    FraudScoreEvent,
    FraudAlertEvent,
    TRANSACTIONS_TOPIC,
)
from app.database import Transaction, FraudAlert, init_db
from app.feature_store import get_feature_store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
ML_SERVICE_URL = "http://localhost:8001"
DB_URL = "postgresql://fraud_user:fraud_password@localhost:5432/fraud_detection"

# Global instances
kafka_consumer = None
feature_store = None
db_session = None
ml_session = None


def get_db_session():
    """Get database session"""
    global db_session
    if db_session is None:
        engine = create_engine(DB_URL)
        SessionLocal = sessionmaker(bind=engine)
        db_session = SessionLocal()
    return db_session


async def call_ml_service(features: list) -> Optional[dict]:
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
                f"{ML_SERVICE_URL}/predict", json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.error(f"[ERROR] ML service error: {resp.status}")
                    return None
    except Exception as e:
        logger.error(f"[ERROR] Failed to call ML service: {e}")
        return None


def compute_transaction_features(event: TransactionEvent) -> list:
    """
    Compute or retrieve transaction features for ML model
    
    This can fetch from feature store or compute real-time velocity features
    """
    # Get velocity features from feature store
    feature_store = get_feature_store()
    
    # Velocity features
    user_txn_count = feature_store.get_transaction_count(event.user_id)
    ip_txn_count = feature_store.get_ip_transaction_count(event.ip_address)
    amount_velocity = feature_store.get_amount_velocity(event.user_id)
    
    # Device risk
    is_known_device = feature_store.is_known_device(event.user_id, event.device_id)
    
    # Construct full feature vector
    # This should match the model's expected input features
    # For now, using event features + computed velocity features
    features = event.features + [
        float(user_txn_count),
        float(ip_txn_count),
        float(amount_velocity),
        float(1 if is_known_device else 0),
    ]
    
    return features


async def process_transaction(event: TransactionEvent):
    """
    Process a single transaction event:
    1. Compute features
    2. Call ML service
    3. Save result to database
    4. Create alerts if fraudulent
    """
    logger.info(f"[PROCESS] Transaction {event.transaction_id} from {event.user_id}")
    
    start_time = time.time()
    
    try:
        # Get feature store
        feature_store = get_feature_store()
        
        # Update velocity counters
        feature_store.increment_transaction_count(event.user_id, time_window=300)
        feature_store.increment_ip_transaction_count(event.ip_address, time_window=300)
        feature_store.track_amount_velocity(event.user_id, event.amount, time_window=300)
        feature_store.track_device(event.user_id, event.device_id)
        feature_store.track_location(event.user_id, event.location)
        
        # Compute features
        features = compute_transaction_features(event)
        
        # Call ML service
        ml_result = await call_ml_service(features)
        
        if not ml_result:
            logger.error(f"[ERROR] Failed to get ML prediction for {event.transaction_id}")
            return
        
        # Extract results
        fraud_probability = ml_result.get("fraud_probability", 0.0)
        is_fraud = ml_result.get("is_fraud", False)
        fraud_score = ml_result.get("fraud_score", 0)
        
        # Save to database
        db = get_db_session()
        
        transaction = Transaction(
            id=event.transaction_id,
            user_id=event.user_id,
            amount=event.amount,
            merchant=event.merchant,
            category=event.category,
            status="flagged" if is_fraud else "approved",
            fraud_score=fraud_score,
            timestamp=datetime.fromisoformat(event.timestamp),
            ip_address=event.ip_address,
            device_id=event.device_id,
            location=event.location,
        )
        
        db.add(transaction)
        
        # Create fraud alert if needed
        if is_fraud:
            alert_id = str(uuid.uuid4())
            alert = FraudAlert(
                id=alert_id,
                transaction_id=event.transaction_id,
                user_id=event.user_id,
                fraud_score=fraud_score,
                reason=f"High fraud probability: {fraud_probability:.2%}",
                status="new",
                timestamp=datetime.utcnow(),
            )
            db.add(alert)
            logger.warning(f"[ALERT] Fraud detected: {alert_id} (score: {fraud_score})")
        
        db.commit()
        
        # Cache the prediction
        prediction_data = {
            "fraud_probability": fraud_probability,
            "is_fraud": is_fraud,
            "fraud_score": fraud_score,
            "timestamp": datetime.utcnow().isoformat(),
        }
        feature_store.cache_model_prediction(event.transaction_id, prediction_data)
        
        latency = time.time() - start_time
        logger.info(f"[OK] Processed {event.transaction_id} in {latency:.3f}s (fraud: {is_fraud})")
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to process transaction: {e}")
        import traceback
        traceback.print_exc()


async def consume_transactions_async(num_workers: int = 5):
    """
    Main consumer loop with async processing
    
    Args:
        num_workers: Number of concurrent tasks
    """
    global kafka_consumer
    
    kafka_consumer = KafkaConsumerService(
        group_id="fraud-detector-worker",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topics=[TRANSACTIONS_TOPIC],
    )
    
    if not kafka_consumer.is_connected():
        logger.error("[FATAL] Could not connect to Kafka")
        sys.exit(1)
    
    logger.info(f"[START] Kafka consumer worker started (workers: {num_workers})")
    
    # Create task queue
    task_queue = asyncio.Queue(maxsize=num_workers * 2)
    
    # Producer tasks
    async def producer():
        """Read from Kafka and queue tasks"""
        try:
            for message in kafka_consumer.consumer:
                try:
                    event = TransactionEvent.from_json(message.value)
                    await task_queue.put(event)
                except Exception as e:
                    logger.error(f"[ERROR] Failed to parse Kafka message: {e}")
        except Exception as e:
            logger.error(f"[ERROR] Kafka producer error: {e}")
    
    # Consumer tasks
    async def consumer(worker_id: int):
        """Process queued transactions"""
        while True:
            try:
                event = await asyncio.wait_for(task_queue.get(), timeout=30)
                await process_transaction(event)
                task_queue.task_done()
            except asyncio.TimeoutError:
                logger.debug(f"[WORKER {worker_id}] Idle")
                continue
            except Exception as e:
                logger.error(f"[WORKER {worker_id}] Error: {e}")
    
    # Create tasks
    producer_task = asyncio.create_task(producer())
    consumer_tasks = [
        asyncio.create_task(consumer(i)) for i in range(num_workers)
    ]
    
    # Run forever
    try:
        await asyncio.gather(producer_task, *consumer_tasks)
    except KeyboardInterrupt:
        logger.info("[STOP] Shutting down...")
        producer_task.cancel()
        for task in consumer_tasks:
            task.cancel()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Consumer Worker")
    parser.add_argument(
        "--workers", type=int, default=5, help="Number of async workers"
    )
    parser.add_argument(
        "--kafka", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--db", default=DB_URL, help="Database URL"
    )
    parser.add_argument(
        "--ml-service", default=ML_SERVICE_URL, help="ML service URL"
    )
    
    args = parser.parse_args()
    
    # Update globals
    globals()["KAFKA_BOOTSTRAP_SERVERS"] = args.kafka
    globals()["DB_URL"] = args.db
    globals()["ML_SERVICE_URL"] = args.ml_service
    
    # Initialize database
    init_db()
    
    # Run consumer
    asyncio.run(consume_transactions_async(num_workers=args.workers))


if __name__ == "__main__":
    main()
