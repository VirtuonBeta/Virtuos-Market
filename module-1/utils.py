# utils.py
import time
import hmac
import hashlib
import pandas as pd
from typing import Dict, Callable, Any, Optional
from functools import wraps

def retry(max_attempts: int = 3, delay: float = 1.0, exceptions: tuple = (Exception,)):
    """Decorator for retrying function calls"""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        time.sleep(delay * attempt)  # Exponential backoff
            raise last_exception
        return wrapper
    return decorator

def generate_signature(params: Dict[str, Any], api_secret: str) -> str:
    """Generate signature for Binance API requests"""
    query_string = "&".join([f"{key}={value}" for key, value in sorted(params.items())])
    return hmac.new(
        api_secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

def compute_bid_ask_volumes(trades_df: pd.DataFrame) -> pd.DataFrame:
    """Compute bid and ask volumes from trade data"""
    # Create a copy to avoid SettingWithCopyWarning
    df = trades_df.copy()
    
    # Initialize columns
    df["bid_volume"] = 0.0
    df["ask_volume"] = 0.0
    
    # Calculate volumes based on is_buyer_maker
    # When is_buyer_maker is True, the trade was initiated by a seller (bid)
    # When is_buyer_maker is False, the trade was initiated by a buyer (ask)
    df.loc[df["is_buyer_maker"], "bid_volume"] = df.loc[df["is_buyer_maker"], "quantity"]
    df.loc[~df["is_buyer_maker"], "ask_volume"] = df.loc[~df["is_buyer_maker"], "quantity"]
    
    return df
