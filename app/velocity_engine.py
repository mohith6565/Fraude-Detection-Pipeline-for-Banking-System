"""
Advanced Velocity Layer with Sliding Window Counters
Uses Redis sorted sets for sub-millisecond velocity checks
Implements sophisticated fraud detection patterns
"""

import redis
import time
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class VelocityMetrics:
    """Velocity metrics for a transaction"""
    user_txn_count_60s: int  # Transactions in last 60 seconds
    user_txn_count_24h: int  # Transactions in last 24 hours
    distinct_cards_per_ip_10m: int  # Distinct cards from IP in 10min
    distinct_ips_per_user_24h: int  # Distinct IPs used by user in 24h
    impossible_travel_risk: bool  # Impossible travel detection
    geolocation_risk_score: float  # 0.0-1.0 based on location history
    time_since_last_txn: float  # Seconds since last transaction


class VelocityEngine:
    """
    Redis-based velocity engine using sorted sets for efficient sliding windows
    All operations are O(log N) or O(N) with N=window size
    """

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 1):
        """Initialize velocity engine with separate Redis DB"""
        self.redis_client = redis.Redis(
            host=host, port=port, db=db, decode_responses=True
        )
        self.pipe_batch_size = 100

    def health_check(self) -> bool:
        """Check Redis connection"""
        try:
            self.redis_client.ping()
            return True
        except Exception as e:
            logger.error(f"[ERROR] Redis health check failed: {e}")
            return False

    # ============ Sliding Window Counters ============

    def record_transaction(
        self,
        user_id: str,
        card_number: str,
        ip_address: str,
        location: str,
        timestamp: Optional[float] = None,
    ) -> bool:
        """
        Record a transaction in sliding window counters
        Uses sorted sets where score = timestamp, member = transaction identifier
        
        Args:
            user_id: User identifier
            card_number: Card used (last 4 digits)
            ip_address: Source IP
            location: Geographic location
            timestamp: Unix timestamp (default: now)
            
        Returns:
            True if recorded successfully
        """
        if timestamp is None:
            timestamp = time.time()

        try:
            # Use pipeline for atomic multi-write
            pipe = self.redis_client.pipeline()

            # 1. Record user transaction (60s window)
            user_txn_key = f"velocity:user_txn:{user_id}"
            pipe.zadd(user_txn_key, {f"{timestamp}:{card_number}": timestamp})
            pipe.expire(user_txn_key, 86400)  # 24h TTL

            # 2. Record card usage per IP (10m window)
            card_ip_key = f"velocity:card_ip:{ip_address}"
            pipe.zadd(
                card_ip_key,
                {f"{card_number}:{timestamp}": timestamp},
            )
            pipe.expire(card_ip_key, 600)  # 10m TTL

            # 3. Record user's IP usage (24h window)
            user_ip_key = f"velocity:user_ip:{user_id}"
            pipe.zadd(user_ip_key, {f"{ip_address}:{timestamp}": timestamp})
            pipe.expire(user_ip_key, 86400)  # 24h TTL

            # 4. Record user's location (for impossible travel)
            location_key = f"velocity:location:{user_id}"
            pipe.setex(
                location_key,
                300,  # 5 min window
                f"{location}:{timestamp}",
            )

            # 5. Record last transaction time
            last_txn_key = f"velocity:last_txn:{user_id}"
            pipe.setex(last_txn_key, 86400, str(timestamp))

            pipe.execute()
            return True

        except Exception as e:
            logger.error(f"[ERROR] Failed to record transaction: {e}")
            return False

    def get_user_transaction_count(self, user_id: str, window_seconds: int = 60) -> int:
        """
        Get number of transactions for user within time window using sorted set range
        Time complexity: O(log N) where N = total transactions
        
        Args:
            user_id: User identifier
            window_seconds: Look-back window in seconds
            
        Returns:
            Transaction count in window
        """
        try:
            key = f"velocity:user_txn:{user_id}"
            now = time.time()
            min_time = now - window_seconds

            # ZCOUNT is O(log N) - count members with scores in range
            count = self.redis_client.zcount(key, min_time, now)
            return int(count) if count else 0

        except Exception as e:
            logger.error(f"[ERROR] Failed to get user transaction count: {e}")
            return 0

    def get_distinct_cards_per_ip(self, ip_address: str, window_seconds: int = 600) -> int:
        """
        Get number of distinct card numbers used from an IP in time window
        
        Args:
            ip_address: Source IP address
            window_seconds: Look-back window (default 10min)
            
        Returns:
            Distinct card count
        """
        try:
            key = f"velocity:card_ip:{ip_address}"
            now = time.time()
            min_time = now - window_seconds

            # Get all transactions in window
            transactions = self.redis_client.zrangebyscore(key, min_time, now)

            # Extract unique card numbers
            cards = set()
            for txn in transactions:
                card_number = txn.split(":")[0]
                cards.add(card_number)

            return len(cards)

        except Exception as e:
            logger.error(f"[ERROR] Failed to get distinct cards per IP: {e}")
            return 0

    def get_distinct_ips_per_user(self, user_id: str, window_seconds: int = 86400) -> int:
        """
        Get number of distinct IPs used by user in time window
        
        Args:
            user_id: User identifier
            window_seconds: Look-back window (default 24h)
            
        Returns:
            Distinct IP count
        """
        try:
            key = f"velocity:user_ip:{user_id}"
            now = time.time()
            min_time = now - window_seconds

            transactions = self.redis_client.zrangebyscore(key, min_time, now)

            ips = set()
            for txn in transactions:
                ip = txn.split(":")[0]
                ips.add(ip)

            return len(ips)

        except Exception as e:
            logger.error(f"[ERROR] Failed to get distinct IPs per user: {e}")
            return 0

    # ============ Impossible Travel Detection ============

    def check_impossible_travel(
        self,
        user_id: str,
        current_location: str,
        current_timestamp: float,
        speed_threshold_kmh: float = 900,  # 900 km/h = max commercial flight speed
    ) -> Tuple[bool, float]:
        """
        Check if user could have physically traveled between locations
        Uses Haversine formula to calculate distance
        
        Args:
            user_id: User identifier
            current_location: Current transaction location
            current_timestamp: Current transaction time
            speed_threshold_kmh: Maximum realistic travel speed
            
        Returns:
            (is_impossible_travel: bool, risk_score: 0.0-1.0)
        """
        try:
            location_key = f"velocity:location:{user_id}"
            last_location_str = self.redis_client.get(location_key)

            if not last_location_str:
                # First transaction, no history
                return False, 0.0

            last_parts = last_location_str.split(":")
            last_location = last_parts[0]
            last_timestamp = float(last_parts[1])

            # Calculate distance (simplified - use city coordinates in production)
            distance_km = self._calculate_distance(last_location, current_location)
            time_diff_hours = (current_timestamp - last_timestamp) / 3600.0

            if time_diff_hours <= 0:
                # Same instant or backward time
                return True, 1.0

            required_speed = distance_km / time_diff_hours

            # Check if travel speed exceeds threshold
            if required_speed > speed_threshold_kmh:
                risk_score = min(1.0, required_speed / speed_threshold_kmh)
                logger.warning(
                    f"[WARN] Impossible travel: {user_id} "
                    f"{last_location}→{current_location}, "
                    f"required speed: {required_speed:.0f} km/h"
                )
                return True, risk_score
            else:
                # Possible but risky if close to threshold
                risk_score = max(0.0, required_speed / speed_threshold_kmh) * 0.5
                return False, risk_score

        except Exception as e:
            logger.error(f"[ERROR] Impossible travel check failed: {e}")
            return False, 0.0

    @staticmethod
    def _calculate_distance(loc1: str, loc2: str) -> float:
        """
        Simplified distance calculation between cities
        In production, use proper geocoding and Haversine formula
        """
        # Mock city coordinates
        coordinates = {
            "Delhi": (28.7041, 77.1025),
            "Mumbai": (19.0760, 72.8777),
            "Bangalore": (12.9716, 77.5946),
            "Chennai": (13.0827, 80.2707),
            "Kolkata": (22.5726, 88.3639),
            "New York": (40.7128, -74.0060),
            "London": (51.5074, -0.1278),
            "Singapore": (1.3521, 103.8198),
            "Tokyo": (35.6762, 139.6503),
        }

        if loc1 not in coordinates or loc2 not in coordinates:
            return 0.0  # Unknown location

        lat1, lon1 = coordinates[loc1]
        lat2, lon2 = coordinates[loc2]

        # Simplified Haversine distance
        from math import radians, cos, sin, asin, sqrt

        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371  # km
        return c * r

    # ============ Composite Velocity Metrics ============

    def get_velocity_metrics(
        self,
        user_id: str,
        card_number: str,
        ip_address: str,
        location: str,
        timestamp: Optional[float] = None,
    ) -> VelocityMetrics:
        """
        Get comprehensive velocity metrics for fraud scoring
        All sub-millisecond operations
        
        Returns:
            VelocityMetrics dataclass with all computed metrics
        """
        if timestamp is None:
            timestamp = time.time()

        try:
            # Record this transaction
            self.record_transaction(user_id, card_number, ip_address, location, timestamp)

            # Get all metrics (can be parallelized with pipeline)
            user_txn_60s = self.get_user_transaction_count(user_id, window_seconds=60)
            user_txn_24h = self.get_user_transaction_count(user_id, window_seconds=86400)
            distinct_cards = self.get_distinct_cards_per_ip(ip_address, window_seconds=600)
            distinct_ips = self.get_distinct_ips_per_user(user_id, window_seconds=86400)

            # Check impossible travel
            impossible_travel, geo_risk = self.check_impossible_travel(
                user_id, location, timestamp
            )

            # Time since last transaction
            last_txn_key = f"velocity:last_txn:{user_id}"
            last_txn_str = self.redis_client.get(last_txn_key)
            if last_txn_str:
                last_txn = float(last_txn_str)
                time_since_last = max(0, timestamp - last_txn)
            else:
                time_since_last = float("inf")

            return VelocityMetrics(
                user_txn_count_60s=user_txn_60s,
                user_txn_count_24h=user_txn_24h,
                distinct_cards_per_ip_10m=distinct_cards,
                distinct_ips_per_user_24h=distinct_ips,
                impossible_travel_risk=impossible_travel,
                geolocation_risk_score=geo_risk,
                time_since_last_txn=time_since_last,
            )

        except Exception as e:
            logger.error(f"[ERROR] Failed to get velocity metrics: {e}")
            # Return zero metrics on error
            return VelocityMetrics(
                user_txn_count_60s=0,
                user_txn_count_24h=0,
                distinct_cards_per_ip_10m=0,
                distinct_ips_per_user_24h=0,
                impossible_travel_risk=False,
                geolocation_risk_score=0.0,
                time_since_last_txn=float("inf"),
            )

    def get_anomaly_score(self, metrics: VelocityMetrics) -> float:
        """
        Calculate anomaly score (0.0-1.0) based on velocity metrics
        Higher score = more anomalous/risky
        
        Args:
            metrics: VelocityMetrics from get_velocity_metrics
            
        Returns:
            Anomaly score 0.0-1.0
        """
        score = 0.0

        # Rapid transactions
        if metrics.user_txn_count_60s > 10:
            score += min(0.3, metrics.user_txn_count_60s * 0.03)

        # Many distinct cards from same IP
        if metrics.distinct_cards_per_ip_10m > 3:
            score += min(0.2, metrics.distinct_cards_per_ip_10m * 0.05)

        # Many distinct IPs from same user (account takeover)
        if metrics.distinct_ips_per_user_24h > 5:
            score += min(0.25, metrics.distinct_ips_per_user_24h * 0.03)

        # Impossible travel
        if metrics.impossible_travel_risk:
            score += 0.4

        # Geolocation risk
        score += metrics.geolocation_risk_score * 0.15

        # Unusual time since last transaction (very rapid)
        if metrics.time_since_last_txn < 5:
            score += 0.2

        return min(1.0, score)

    def clear_user_data(self, user_id: str) -> bool:
        """Clear all velocity data for a user (for testing/cleanup)"""
        try:
            pipe = self.redis_client.pipeline()
            for pattern in [
                f"velocity:user_txn:{user_id}",
                f"velocity:user_ip:{user_id}",
                f"velocity:location:{user_id}",
                f"velocity:last_txn:{user_id}",
            ]:
                pipe.delete(pattern)
            pipe.execute()
            return True
        except Exception as e:
            logger.error(f"[ERROR] Failed to clear user data: {e}")
            return False


# Global instance
_velocity_engine = None


def get_velocity_engine(host: str = "localhost", port: int = 6379) -> VelocityEngine:
    """Get or create global velocity engine"""
    global _velocity_engine
    if _velocity_engine is None:
        _velocity_engine = VelocityEngine(host=host, port=port)
    return _velocity_engine
