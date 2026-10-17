import time
import requests
from typing import Dict, Optional

# Simple in-memory cache
_cache = {"rates": {}, "timestamp": 0}
CACHE_TTL = 60 * 30  # 30 minutes

API_URL = "https://api.exchangerate.host/latest"


def fetch_rates(base: str = "INR") -> Dict[str, float]:
    now = time.time()
    if _cache["rates"] and now - _cache["timestamp"] < CACHE_TTL and _cache.get("base") == base:
        return _cache["rates"]

    try:
        resp = requests.get(API_URL, params={"base": base})
        resp.raise_for_status()
        data = resp.json()
        rates = data.get("rates", {})
        _cache["rates"] = rates
        _cache["timestamp"] = now
        _cache["base"] = base
        return rates
    except Exception:
        # On failure, return previously cached rates or minimal fallback
        return _cache.get("rates", {})


def convert_amount(amount: float, from_currency: str, to_currency: str) -> float:
    if from_currency == to_currency:
        return amount
    rates = fetch_rates(base=from_currency)
    rate = rates.get(to_currency)
    if not rate:
        # fallback: return same amount
        return amount
    return round(amount * rate, 2)
