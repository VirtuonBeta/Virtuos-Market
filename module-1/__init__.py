"""
Module 1: Data Ingestion and 1-Minute Aggregation

This module is responsible for:
- Fetching raw trade data from Binance API
- Validating data integrity
- Caching data for efficiency
- Aggregating to 1-minute OHLC bars
- Monitoring system performance and health
"""

from .fetcher import BinanceDataFetcher
from .cache_manager import CacheManager
from .rate_limiter import RateLimiter
from .validators import DataValidator
from .config import Config
from .monitoring.metrics import MetricsCollector
from .monitoring.alerts import AlertManager
from .monitoring.dashboard import Dashboard

__version__ = "1.0.0"
__all__ = [
    "BinanceDataFetcher",
    "CacheManager", 
    "RateLimiter",
    "DataValidator",
    "Config",
    "MetricsCollector",
    "AlertManager",
    "Dashboard"
]
