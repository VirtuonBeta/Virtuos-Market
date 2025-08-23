# utils.py
"""
Utility functions for Binance data fetching module.

This module provides helper functions for:
- Retry mechanism with configurable attempts and delays
- API request signature generation
- Bid/ask volume computation from trade data
- Input validation and error handling
"""
import time
import hmac
import hashlib
import logging
import pandas as pd
from typing import Dict, Callable, Any, Optional, Union
from functools import wraps

# Set up module logger
logger = logging.getLogger(__name__)

def retry(
    max_attempts: int = 3, 
    delay: float = 1.0, 
    exceptions: tuple = (Exception,),
    sleep_func: Optional[Callable] = None
) -> Callable:
    """Decorator for retrying function calls with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        exceptions: Tuple of exceptions to catch and retry on
        sleep_func: Optional function to use for sleeping (for testing)
    
    Returns:
        Decorated function with retry capability
    
    Example:
        @retry(max_attempts=5, delay=2.0)
        def api_call():
            # Make API request
            pass
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            sleep = sleep_func or time.sleep
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        wait_time = delay * attempt  # Exponential backoff
                        logger.warning(
                            f"Attempt {attempt}/{max_attempts} failed for {func.__name__}. "
                            f"Retrying in {wait_time:.2f}s. Error: {str(e)}"
                        )
                        sleep(wait_time)
            
            logger.error(
                f"All {max_attempts} attempts failed for {func.__name__}. "
                f"Final error: {str(last_exception)}"
            )
            raise last_exception
        return wrapper
    return decorator

def generate_signature(params: Dict[str, Any], api_secret: str) -> str:
    """Generate HMAC-SHA256 signature for Binance API requests.
    
    Args:
        params: Dictionary of request parameters
        api_secret: Binance API secret key
        
    Returns:
        Hexadecimal signature string
        
    Example:
        params = {"symbol": "BTCUSDT", "timestamp": 123456789}
        signature = generate_signature(params, "your_api_secret")
    """
    # Validate inputs
    if not isinstance(params, dict):
        raise TypeError("params must be a dictionary")
    if not api_secret or not isinstance(api_secret, str):
        raise ValueError("api_secret must be a non-empty string")
    
    # Create query string from sorted parameters
    query_string = "&".join([f"{key}={value}" for key, value in sorted(params.items())])
    
    # Generate HMAC-SHA256 signature
    return hmac.new(
        api_secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

def compute_bid_ask_volumes(
    trades_df: pd.DataFrame,
    quantity_col: str = "quantity",
    buyer_maker_col: str = "is_buyer_maker",
    bid_col: str = "bid_volume",
    ask_col: str = "ask_volume"
) -> pd.DataFrame:
    """Compute bid and ask volumes from trade data.
    
    Args:
        trades_df: DataFrame containing trade data
        quantity_col: Name of column containing trade quantity
        buyer_maker_col: Name of column indicating if buyer is maker
        bid_col: Name of column to store bid volume
        ask_col: Name of column to store ask volume
        
    Returns:
        DataFrame with added bid and ask volume columns
        
    Raises:
        ValueError: If required columns are missing
        TypeError: If input is not a DataFrame
        
    Example:
        trades = pd.DataFrame({
            "quantity": [0.1, 0.2, 0.3],
            "is_buyer_maker": [True, False, True]
        })
        result = compute_bid_ask_volumes(trades)
        # Result will have bid_volume and ask_volume columns
    """
    # Input validation
    if not isinstance(trades_df, pd.DataFrame):
        raise TypeError("trades_df must be a pandas DataFrame")
    
    # Check required columns
    required_cols = [quantity_col, buyer_maker_col]
    missing_cols = [col for col in required_cols if col not in trades_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Create a copy to avoid SettingWithCopyWarning
    df = trades_df.copy()
    
    # Initialize columns
    df[bid_col] = 0.0
    df[ask_col] = 0.0
    
    # Calculate volumes based on buyer_maker flag
    # When is_buyer_maker is True, the trade was initiated by a seller (bid)
    # When is_buyer_maker is False, the trade was initiated by a buyer (ask)
    df.loc[df[buyer_maker_col], bid_col] = df.loc[df[buyer_maker_col], quantity_col]
    df.loc[~df[buyer_maker_col], ask_col] = df.loc[~df[buyer_maker_col], quantity_col]
    
    return df
