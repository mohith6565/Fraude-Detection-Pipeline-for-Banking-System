"""
Streaming Ingestion Gateway
Implements decoupled transaction ingestion with 202 Accepted pattern
Validates and immediately publishes to Kafka without blocking
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, Request
from pydantic import BaseModel, Field, validator
from typing import Optional
import uuid
from datetime import datetime
import logging
import json

from app.kafka_config import (
    TransactionEvent,
    get_kafka_producer,
)
from app.velocity_engine import get_velocity_engine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/transactions", tags=["transactions"])


class TransactionRequest(BaseModel):
    """Incoming transaction request - validated by Pydantic"""
    user_id: str = Field(..., description="User identifier")
    card_number: str = Field(..., description="Last 4 digits of card")
    amount: float = Field(..., gt=0, description="Transaction amount")
    merchant: str = Field(..., description="Merchant name")
    merchant_category: str = Field(..., description="Merchant category code")
    ip_address: str = Field(..., description="Source IP address")
    device_id: str = Field(..., description="Device fingerprint")
    location: str = Field(..., description="Geographic location")
    timestamp: Optional[float] = Field(None, description="Unix timestamp")

    @validator('amount')
    def validate_amount(cls, v):
        if v < 0 or v > 999999.99:
            raise ValueError("Amount must be between 0 and 999,999.99")
        return v

    @validator('card_number')
    def validate_card_number(cls, v):
        if len(v) < 4 or len(v) > 4:
            raise ValueError("Card number must be last 4 digits")
        if not v.isdigit():
            raise ValueError("Card number must be digits")
        return v

    @validator('user_id')
    def validate_user_id(cls, v):
        if len(v) < 1 or len(v) > 255:
            raise ValueError("Invalid user_id")
        return v


class TransactionResponse(BaseModel):
    """Response for 202 Accepted"""
    transaction_id: str = Field(..., description="Unique transaction ID")
    status: str = Field(default="accepted", description="Status code")
    message: str = Field(..., description="Response message")
    timestamp: str = Field(..., description="Server timestamp")

    class Config:
        json_schema_extra = {
            "example": {
                "transaction_id": "TXN-550e8400-e29b-41d4-a716-446655440000",
                "status": "accepted",
                "message": "Transaction accepted for processing. Check status via transaction_id",
                "timestamp": "2024-01-15T10:30:45Z"
            }
        }


@router.post("/ingest", response_model=TransactionResponse, status_code=202)
async def ingest_transaction(
    request: TransactionRequest,
    background_tasks: BackgroundTasks,
    raw_request: Request,
) -> TransactionResponse:
    """
    Ingest transaction with 202 Accepted pattern
    
    Validation: <1ms (Pydantic schema validation)
    Response: <5ms (immediate 202 response)
    
    The actual fraud detection happens asynchronously in Kafka consumers.
    
    Args:
        request: Transaction data (validated by Pydantic)
        background_tasks: FastAPI background tasks
        raw_request: Raw request for trace ID extraction
        
    Returns:
        202 Accepted with transaction_id for status tracking
    """
    
    try:
        # 1. Generate unique transaction ID (milliseconds precision)
        transaction_id = f"TXN-{uuid.uuid4()}"
        
        # 2. Get trace ID from request headers (for distributed tracing)
        trace_id = raw_request.headers.get("X-Trace-ID", str(uuid.uuid4()))
        
        # 3. Prepare transaction event
        timestamp = request.timestamp or datetime.utcnow().timestamp()
        
        # Create ML features list (will be enhanced by feature store)
        # These are simplified features - real system would have 20-100
        transaction_event = TransactionEvent(
            transaction_id=transaction_id,
            user_id=request.user_id,
            amount=request.amount,
            timestamp=datetime.fromtimestamp(timestamp).isoformat(),
            merchant=request.merchant,
            category=request.merchant_category,
            ip_address=request.ip_address,
            device_id=request.device_id,
            location=request.location,
            features=[
                float(request.amount),
                1.0 if request.amount > 500 else 0.0,  # High amount flag
                1.0 if request.merchant_category in ["AIRLINE", "HOTEL", "LUXURY"] else 0.0,  # Travel category
                # More features would be added
            ],
        )
        
        # 4. Publish to Kafka in background
        def publish_to_kafka():
            """Publish transaction to Kafka (runs in thread pool, doesn't block)"""
            try:
                kafka_producer = get_kafka_producer()
                if kafka_producer.is_connected():
                    success = kafka_producer.publish_transaction(transaction_event)
                    if success:
                        logger.info(
                            f"[OK] Published TXN {transaction_id} to Kafka "
                            f"(trace_id={trace_id})"
                        )
                    else:
                        logger.error(f"[ERROR] Failed to publish TXN {transaction_id}")
                else:
                    logger.warning(f"[WARN] Kafka not connected for TXN {transaction_id}")
            except Exception as e:
                logger.error(f"[ERROR] Exception publishing to Kafka: {e}")
        
        # Queue background task (doesn't block response)
        background_tasks.add_task(publish_to_kafka)
        
        # 5. Return 202 Accepted immediately
        return TransactionResponse(
            transaction_id=transaction_id,
            status="accepted",
            message="Transaction queued for processing. Use transaction_id to check status.",
            timestamp=datetime.utcnow().isoformat() + "Z",
        )

    except Exception as e:
        logger.error(f"[ERROR] Exception in ingest_transaction: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Invalid transaction: {str(e)}"
        )


@router.get("/status/{transaction_id}")
async def get_transaction_status(transaction_id: str) -> dict:
    """
    Check status of a transaction being processed
    
    Args:
        transaction_id: Transaction ID from ingest response
        
    Returns:
        Transaction status and fraud score if available
    """
    try:
        # In production, would query database for status
        # For now, return processing status
        return {
            "transaction_id": transaction_id,
            "status": "processing",
            "message": "Transaction is being processed by workers",
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    
    except Exception as e:
        logger.error(f"[ERROR] Exception getting transaction status: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve transaction status"
        )


@router.post("/batch")
async def batch_ingest_transactions(
    transactions: list[TransactionRequest],
    background_tasks: BackgroundTasks,
) -> dict:
    """
    Batch ingest multiple transactions (useful for high-volume scenarios)
    
    Args:
        transactions: List of transaction requests
        background_tasks: FastAPI background tasks
        
    Returns:
        List of transaction IDs and acceptance confirmations
    """
    
    if len(transactions) > 1000:
        raise HTTPException(
            status_code=400,
            detail="Batch size cannot exceed 1000 transactions"
        )
    
    transaction_ids = []
    
    try:
        # Generate IDs for all transactions
        for txn_req in transactions:
            transaction_id = f"TXN-{uuid.uuid4()}"
            transaction_ids.append(transaction_id)
            
            # Create event
            timestamp = txn_req.timestamp or datetime.utcnow().timestamp()
            event = TransactionEvent(
                transaction_id=transaction_id,
                user_id=txn_req.user_id,
                amount=txn_req.amount,
                timestamp=datetime.fromtimestamp(timestamp).isoformat(),
                merchant=txn_req.merchant,
                category=txn_req.merchant_category,
                ip_address=txn_req.ip_address,
                device_id=txn_req.device_id,
                location=txn_req.location,
                features=[float(txn_req.amount)],
            )
            
            # Publish in background (batch)
            def publish_batch():
                try:
                    kafka_producer = get_kafka_producer()
                    if kafka_producer.is_connected():
                        for event in [event]:  # In real system, batch multiple
                            kafka_producer.publish_transaction(event)
                except Exception as e:
                    logger.error(f"[ERROR] Batch publish failed: {e}")
            
            background_tasks.add_task(publish_batch)
        
        return {
            "batch_size": len(transactions),
            "transaction_ids": transaction_ids,
            "status": "accepted",
            "message": f"{len(transactions)} transactions queued for processing",
        }
    
    except Exception as e:
        logger.error(f"[ERROR] Exception in batch ingest: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Batch ingestion failed: {str(e)}"
        )


# Middleware to inject trace ID
async def trace_middleware(request: Request, call_next):
    """
    Middleware to generate and propagate trace ID
    Adds to response headers for client tracking
    """
    trace_id = request.headers.get("X-Trace-ID", str(uuid.uuid4()))
    
    # Add to request state for use in endpoints
    request.state.trace_id = trace_id
    
    response = await call_next(request)
    
    # Add trace ID to response headers
    response.headers["X-Trace-ID"] = trace_id
    
    return response
