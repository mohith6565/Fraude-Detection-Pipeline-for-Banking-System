"""
Feature Store Implementation
Redis-based feature store for real-time velocity features and caching
"""

import redis
import json
from typing import Dict, Optional, List
from datetime import datetime, timedelta
from functools import wraps
import time

class FeatureStore:
    """Redis-based feature store for fraud detection features"""

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        """Initialize Redis connection"""
        self.redis_client = redis.Redis(
            host=host, port=port, db=db, decode_responses=True
        )
        self.ttl_seconds = 3600  # 1 hour default TTL

    def health_check(self) -> bool:
        """Check if Redis is available"""
        try:
            self.redis_client.ping()
            return True
        except Exception as e:
            print(f"[ERROR] Redis connection failed: {e}")
            return False

    # ============ Velocity Features ============

    def increment_transaction_count(self, user_id: str, time_window: int = 300) -> int:
        """
        Increment transaction count for a user within time window (seconds)
        Used for: detect rapid successive transactions
        
        Args:
            user_id: User identifier
            time_window: Time window in seconds (default 5 minutes)
        
        Returns:
            Current transaction count in the window
        """
        key = f"velocity:txn_count:{user_id}"
        pipe = self.redis_client.pipeline()
        pipe.incr(key)
        pipe.expire(key, time_window)
        result = pipe.execute()
        return result[0]

    def get_transaction_count(self, user_id: str) -> int:
        """Get current transaction count for a user"""
        key = f"velocity:txn_count:{user_id}"
        count = self.redis_client.get(key)
        return int(count) if count else 0

    def increment_ip_transaction_count(self, ip_address: str, time_window: int = 300) -> int:
        """
        Track transaction count by IP address
        Used for: detect fraud from specific IP
        """
        key = f"velocity:ip_txn_count:{ip_address}"
        pipe = self.redis_client.pipeline()
        pipe.incr(key)
        pipe.expire(key, time_window)
        result = pipe.execute()
        return result[0]

    def get_ip_transaction_count(self, ip_address: str) -> int:
        """Get transaction count for an IP address"""
        key = f"velocity:ip_txn_count:{ip_address}"
        count = self.redis_client.get(key)
        return int(count) if count else 0

    def track_amount_velocity(self, user_id: str, amount: float, time_window: int = 300):
        """
        Track total amount spent in time window
        Used for: detect unusual spending patterns
        """
        key = f"velocity:amount:{user_id}"
        pipe = self.redis_client.pipeline()
        pipe.incrbyfloat(key, amount)
        pipe.expire(key, time_window)
        pipe.execute()

    def get_amount_velocity(self, user_id: str) -> float:
        """Get total amount for user in current window"""
        key = f"velocity:amount:{user_id}"
        amount = self.redis_client.get(key)
        return float(amount) if amount else 0.0

    # ============ Geographic Features ============

    def track_location(self, user_id: str, country: str, time_window: int = 3600):
        """
        Track user location
        Used for: detect impossible travel
        """
        key = f"location:user:{user_id}"
        self.redis_client.setex(key, time_window, country)

    def get_last_location(self, user_id: str) -> Optional[str]:
        """Get last known location for user"""
        key = f"location:user:{user_id}"
        return self.redis_client.get(key)

    # ============ Device Fingerprinting ============

    def track_device(self, user_id: str, device_id: str, time_window: int = 7 * 24 * 3600):
        """
        Track devices used by user
        Used for: detect new/suspicious devices
        """
        key = f"devices:user:{user_id}"
        self.redis_client.sadd(key, device_id)
        self.redis_client.expire(key, time_window)

    def is_known_device(self, user_id: str, device_id: str) -> bool:
        """Check if device is known for user"""
        key = f"devices:user:{user_id}"
        return self.redis_client.sismember(key, device_id) > 0

    def get_user_devices(self, user_id: str) -> List[str]:
        """Get all known devices for user"""
        key = f"devices:user:{user_id}"
        devices = self.redis_client.smembers(key)
        return list(devices) if devices else []

    # ============ Session/JWT Caching ============

    def cache_user_session(self, user_id: str, session_data: Dict, ttl: int = 3600):
        """Cache user session data"""
        key = f"session:{user_id}"
        self.redis_client.setex(key, ttl, json.dumps(session_data))

    def get_user_session(self, user_id: str) -> Optional[Dict]:
        """Retrieve cached user session"""
        key = f"session:{user_id}"
        data = self.redis_client.get(key)
        return json.loads(data) if data else None

    def invalidate_session(self, user_id: str):
        """Invalidate user session"""
        key = f"session:{user_id}"
        self.redis_client.delete(key)

    # ============ Cache Patterns ============

    def cache_transaction_features(
        self, transaction_id: str, features: List[float], ttl: int = 300
    ):
        """Cache computed transaction features"""
        key = f"features:tx:{transaction_id}"
        self.redis_client.setex(key, ttl, json.dumps(features))

    def get_cached_features(self, transaction_id: str) -> Optional[List[float]]:
        """Retrieve cached transaction features"""
        key = f"features:tx:{transaction_id}"
        data = self.redis_client.get(key)
        return json.loads(data) if data else None

    def cache_model_prediction(
        self, transaction_id: str, prediction: Dict, ttl: int = 300
    ):
        """Cache model prediction result"""
        key = f"prediction:tx:{transaction_id}"
        self.redis_client.setex(key, ttl, json.dumps(prediction))

    def get_cached_prediction(self, transaction_id: str) -> Optional[Dict]:
        """Retrieve cached prediction"""
        key = f"prediction:tx:{transaction_id}"
        data = self.redis_client.get(key)
        return json.loads(data) if data else None

    # ============ Utility Functions ============

    def flush_all(self):
        """Flush all keys (use carefully!)"""
        self.redis_client.flushdb()

    def get_stats(self) -> Dict:
        """Get Redis stats for monitoring"""
        info = self.redis_client.info()
        return {
            "used_memory": info.get("used_memory_human"),
            "connected_clients": info.get("connected_clients"),
            "total_commands_processed": info.get("total_commands_processed"),
            "keyspace": self.redis_client.dbsize(),
        }


# Decorator for caching function results
def cache_result(ttl: int = 300):
    """Decorator to cache function results in Redis"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # This is a placeholder - in production, use proper key generation
            cache_key = f"cache:{func.__name__}:{str(args)}:{str(kwargs)}"
            result = redis.Redis(decode_responses=True).get(cache_key)

            if result:
                return json.loads(result)

            result = func(*args, **kwargs)
            redis.Redis(decode_responses=True).setex(cache_key, ttl, json.dumps(result))
            return result

        return wrapper
    return decorator


# Global feature store instance
_feature_store = None


def get_feature_store(host: str = "localhost", port: int = 6379) -> FeatureStore:
    """Get or create global feature store instance"""
    global _feature_store
    if _feature_store is None:
        _feature_store = FeatureStore(host=host, port=port)
    return _feature_store
