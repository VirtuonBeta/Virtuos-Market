# config.py
from dataclasses import dataclass
from typing import Optional

@dataclass
class Config:
    # API settings
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    
    # Cache settings
    cache_dir: str = "./cache"
    
    # Rate limiting
    max_requests_per_minute: int = 1200
    safety_margin: float = 0.9  # Use only 90% of limit
    
    # Request settings
    max_batch_size: int = 1000
    retry_attempts: int = 3
    retry_delay: float = 1.0  # seconds
    
    # Data validation
    max_volatility_threshold: float = 0.5  # 50% price change in one candle
    
    # Cache settings
    cache_version: str = "1.0"
