"""
Utility functions for Module 1.

This module provides:
- Retry mechanisms
- API signature generation
- Data processing utilities
- Date/time utilities
- Math utilities
"""

import time
import hmac
import hashlib
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from functools import wraps
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None
):
    """
    Retry decorator with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff_factor: Factor to multiply delay by for each retry
        exceptions: Tuple of exceptions to catch and retry on
        on_retry: Optional callback function called on each retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt < max_attempts:
                        # Calculate delay with exponential backoff
                        current_delay = delay * (backoff_factor ** (attempt - 1))
                        current_delay = min(current_delay, 60)  # Cap at 60 seconds
                        
                        logger.warning(
                            f"Attempt {attempt}/{max_attempts} failed for {func.__name__}. "
                            f"Retrying in {current_delay:.2f}s. Error: {str(e)}"
                        )
                        
                        # Call on_retry callback if provided
                        if on_retry:
                            on_retry(e, attempt)
                        
                        time.sleep(current_delay)
            
            logger.error(
                f"All {max_attempts} attempts failed for {func.__name__}. "
                f"Final error: {str(last_exception)}"
            )
            raise last_exception
        return wrapper
    return decorator


def generate_signature(params: Dict[str, Any], api_secret: str) -> str:
    """
    Generate HMAC-SHA256 signature for Binance API requests.
    
    Args:
        params: Dictionary of request parameters
        api_secret: Binance API secret key
        
    Returns:
        Hexadecimal signature string
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
    """
    Compute bid and ask volumes from trade data.
    
    Args:
        trades_df: DataFrame containing trade data
        quantity_col: Name of column containing trade quantity
        buyer_maker_col: Name of column indicating if buyer is maker
        bid_col: Name of column to store bid volume
        ask_col: Name of column to store ask volume
        
    Returns:
        DataFrame with added bid and ask volume columns
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


def chunk_date_range(
    start_time: datetime,
    end_time: datetime,
    chunk_size: timedelta,
    align_to_boundary: bool = True
) -> List[Tuple[datetime, datetime]]:
    """
    Split a date range into chunks of specified size.
    
    Args:
        start_time: Start of the date range
        end_time: End of the date range
        chunk_size: Size of each chunk
        align_to_boundary: Whether to align chunks to time boundaries
        
    Returns:
        List of (chunk_start, chunk_end) tuples
    """
    if start_time >= end_time:
        raise ValueError("start_time must be before end_time")
    
    if chunk_size.total_seconds() <= 0:
        raise ValueError("chunk_size must be positive")
    
    chunks = []
    current_start = start_time
    
    # Align to boundary if requested
    if align_to_boundary:
        # For daily chunks, align to midnight
        if chunk_size >= timedelta(days=1):
            current_start = current_start.replace(hour=0, minute=0, second=0, microsecond=0)
        # For hourly chunks, align to hour
        elif chunk_size >= timedelta(hours=1):
            current_start = current_start.replace(minute=0, second=0, microsecond=0)
        # For minute chunks, align to minute
        else:
            current_start = current_start.replace(second=0, microsecond=0)
    
    while current_start < end_time:
        current_end = min(current_start + chunk_size, end_time)
        chunks.append((current_start, current_end))
        current_start = current_end
    
    return chunks


def resample_ohlc(
    df: pd.DataFrame,
    timestamp_col: str = 'timestamp',
    interval: str = '5T',
    price_cols: List[str] = ['open', 'high', 'low', 'close'],
    volume_col: str = 'volume'
) -> pd.DataFrame:
    """
    Resample OHLC data to a different interval.
    
    Args:
        df: DataFrame containing OHLC data
        timestamp_col: Name of the timestamp column
        interval: Resampling interval (e.g., '5T', '1H', '1D')
        price_cols: List of price column names
        volume_col: Name of the volume column
        
    Returns:
        Resampled DataFrame
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")
    
    if timestamp_col not in df.columns:
        raise ValueError(f"Column '{timestamp_col}' not found in DataFrame")
    
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Ensure timestamp is datetime and set as index
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    df.set_index(timestamp_col, inplace=True)
    
    # Define aggregation dictionary
    agg_dict = {}
    
    # Add price columns
    for col in price_cols:
        if col in df.columns:
            if col == 'open':
                agg_dict[col] = 'first'
            elif col == 'high':
                agg_dict[col] = 'max'
            elif col == 'low':
                agg_dict[col] = 'min'
            elif col == 'close':
                agg_dict[col] = 'last'
    
    # Add volume column
    if volume_col in df.columns:
        agg_dict[volume_col] = 'sum'
    
    # Add count of trades if available
    if 'number_of_trades' in df.columns:
        agg_dict['number_of_trades'] = 'sum'
    
    # Add bid/ask volumes if available
    if 'bid_volume' in df.columns:
        agg_dict['bid_volume'] = 'sum'
    if 'ask_volume' in df.columns:
        agg_dict['ask_volume'] = 'sum'
    
    # Resample
    resampled = df.resample(interval).agg(agg_dict)
    
    # Drop rows with NaN values (intervals with no data)
    resampled.dropna(inplace=True)
    
    # Reset index to make timestamp a column again
    resampled.reset_index(inplace=True)
    
    return resampled


def ensure_directory(path: Union[str, Path]) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Path to directory
        
    Returns:
        Path object for the directory
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def format_bytes(bytes_value: int) -> str:
    """
    Format bytes to human-readable string.
    
    Args:
        bytes_value: Number of bytes
        
    Returns:
        Formatted string
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string
    """
    if seconds < 0:
        return "0s"
    
    # Calculate time components
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    
    # Build result string
    components = []
    if days > 0:
        components.append(f"{int(days)}d")
    if hours > 0:
        components.append(f"{int(hours)}h")
    if minutes > 0:
        components.append(f"{int(minutes)}m")
    if seconds > 0 or not components:
        components.append(f"{int(seconds)}s")
    
    return " ".join(components)


def safe_divide(
    numerator: Union[float, int, pd.Series, np.ndarray],
    denominator: Union[float, int, pd.Series, np.ndarray],
    fill_value: Optional[float] = None
) -> Union[float, pd.Series, np.ndarray]:
    """
    Safely divide two values, handling division by zero.
    
    Args:
        numerator: Numerator
        denominator: Denominator
        fill_value: Value to use when denominator is zero
        
    Returns:
        Result of division
    """
    if isinstance(numerator, (pd.Series, np.ndarray)):
        if fill_value is not None:
            return np.divide(
                numerator, denominator,
                out=np.full_like(numerator, fill_value, dtype=float),
                where=denominator != 0
            )
        else:
            return np.divide(numerator, denominator)
    else:
        if denominator == 0:
            return fill_value if fill_value is not None else float('inf')
        return numerator / denominator
