"""
Configuration management for Module 1.

This module handles:
- Centralized configuration
- Environment variable support
- Configuration validation
- Type safety
"""

import os
import json
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ApiConfig:
    """API configuration."""
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    base_url: str = "https://api.binance.com/api/v3"
    timeout: int = 30


@dataclass
class CacheConfig:
    """Cache configuration."""
    cache_dir: str = "./cache"
    max_memory_cache_size: int = 100
    cache_expiry_days: int = 7
    enable_cleanup: bool = True
    cleanup_interval_hours: int = 24


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    max_requests_per_minute: int = 1200
    safety_margin: float = 0.95
    max_concurrent_requests: int = 5
    enable_adaptive: bool = True


@dataclass
class ValidationConfig:
    """Validation configuration."""
    max_volatility_threshold: float = 0.5
    max_missing_percentage: float = 0.05
    max_duplicate_percentage: float = 0.01
    auto_fix: bool = True


@dataclass
class MonitoringConfig:
    """Monitoring configuration."""
    enable_metrics: bool = True
    metrics_interval: int = 60
    enable_alerts: bool = True
    alert_channels: List[str] = field(default_factory=lambda: ["log"])
    dashboard_port: int = 8080
    dashboard_host: str = "localhost"


@dataclass
class Config:
    """Main configuration class."""
    # API settings
    api: ApiConfig = field(default_factory=ApiConfig)
    
    # Cache settings
    cache: CacheConfig = field(default_factory=CacheConfig)
    
    # Rate limiting
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    
    # Validation
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    
    # Monitoring
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    
    # Request settings
    max_batch_size: int = 1000
    max_trade_batch_size: int = 1000
    retry_attempts: int = 3
    retry_delay: float = 1.0
    connection_pool_size: int = 20
    
    # Output settings
    output_dir: str = "./data/1min_bars"
    partition_by_date: bool = True
    compression: str = "snappy"
    
    # Logging
    log_level: str = "INFO"
    log_file: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables."""
        return cls(
            api=ApiConfig(
                api_key=os.getenv("BINANCE_API_KEY"),
                api_secret=os.getenv("BINANCE_API_SECRET"),
                base_url=os.getenv("BINANCE_BASE_URL", "https://api.binance.com/api/v3"),
                timeout=int(os.getenv("BINANCE_TIMEOUT", "30"))
            ),
            cache=CacheConfig(
                cache_dir=os.getenv("CACHE_DIR", "./cache"),
                max_memory_cache_size=int(os.getenv("MAX_MEMORY_CACHE_SIZE", "100")),
                cache_expiry_days=int(os.getenv("CACHE_EXPIRY_DAYS", "7")),
                enable_cleanup=os.getenv("ENABLE_CACHE_CLEANUP", "true").lower() == "true",
                cleanup_interval_hours=int(os.getenv("CACHE_CLEANUP_INTERVAL", "24"))
            ),
            rate_limit=RateLimitConfig(
                max_requests_per_minute=int(os.getenv("MAX_REQUESTS_PER_MINUTE", "1200")),
                safety_margin=float(os.getenv("SAFETY_MARGIN", "0.95")),
                max_concurrent_requests=int(os.getenv("MAX_CONCURRENT_REQUESTS", "5")),
                enable_adaptive=os.getenv("ENABLE_ADAPTIVE_RATE_LIMIT", "true").lower() == "true"
            ),
            validation=ValidationConfig(
                max_volatility_threshold=float(os.getenv("MAX_VOLATILITY_THRESHOLD", "0.5")),
                max_missing_percentage=float(os.getenv("MAX_MISSING_PERCENTAGE", "0.05")),
                max_duplicate_percentage=float(os.getenv("MAX_DUPLICATE_PERCENTAGE", "0.01")),
                auto_fix=os.getenv("AUTO_FIX_DATA", "true").lower() == "true"
            ),
            monitoring=MonitoringConfig(
                enable_metrics=os.getenv("ENABLE_METRICS", "true").lower() == "true",
                metrics_interval=int(os.getenv("METRICS_INTERVAL", "60")),
                enable_alerts=os.getenv("ENABLE_ALERTS", "true").lower() == "true",
                alert_channels=os.getenv("ALERT_CHANNELS", "log").split(","),
                dashboard_port=int(os.getenv("DASHBOARD_PORT", "8080")),
                dashboard_host=os.getenv("DASHBOARD_HOST", "localhost")
            ),
            max_batch_size=int(os.getenv("MAX_BATCH_SIZE", "1000")),
            max_trade_batch_size=int(os.getenv("MAX_TRADE_BATCH_SIZE", "1000")),
            retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "3")),
            retry_delay=float(os.getenv("RETRY_DELAY", "1.0")),
            connection_pool_size=int(os.getenv("CONNECTION_POOL_SIZE", "20")),
            output_dir=os.getenv("OUTPUT_DIR", "./data/1min_bars"),
            partition_by_date=os.getenv("PARTITION_BY_DATE", "true").lower() == "true",
            compression=os.getenv("COMPRESSION", "snappy"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_file=os.getenv("LOG_FILE")
        )
    
    @classmethod
    def from_file(cls, file_path: str) -> 'Config':
        """Create configuration from JSON file."""
        with open(file_path, 'r') as f:
            config_data = json.load(f)
        
        return cls(
            api=ApiConfig(**config_data.get('api', {})),
            cache=CacheConfig(**config_data.get('cache', {})),
            rate_limit=RateLimitConfig(**config_data.get('rate_limit', {})),
            validation=ValidationConfig(**config_data.get('validation', {})),
            monitoring=MonitoringConfig(**config_data.get('monitoring', {})),
            **{k: v for k, v in config_data.items() 
               if k not in ['api', 'cache', 'rate_limit', 'validation', 'monitoring']}
        )
    
    def to_file(self, file_path: str):
        """Save configuration to JSON file."""
        config_dict = {
            'api': self.api.__dict__,
            'cache': self.cache.__dict__,
            'rate_limit': self.rate_limit.__dict__,
            'validation': self.validation.__dict__,
            'monitoring': self.monitoring.__dict__,
            'max_batch_size': self.max_batch_size,
            'max_trade_batch_size': self.max_trade_batch_size,
            'retry_attempts': self.retry_attempts,
            'retry_delay': self.retry_delay,
            'connection_pool_size': self.connection_pool_size,
            'output_dir': self.output_dir,
            'partition_by_date': self.partition_by_date,
            'compression': self.compression,
            'log_level': self.log_level,
            'log_file': self.log_file
        }
        
        with open(file_path, 'w') as f:
            json.dump(config_dict, f, indent=2)
    
    def validate(self):
        """Validate configuration values."""
        errors = []
        
        # Validate API config
        if not self.api.base_url.startswith(('http://', 'https://')):
            errors.append("API base URL must start with http:// or https://")
        
        # Validate cache config
        if self.cache.max_memory_cache_size <= 0:
            errors.append("Max memory cache size must be positive")
        
        if self.cache.cache_expiry_days <= 0:
            errors.append("Cache expiry days must be positive")
        
        # Validate rate limit config
        if self.rate_limit.max_requests_per_minute <= 0:
            errors.append("Max requests per minute must be positive")
        
        if not 0 < self.rate_limit.safety_margin <= 1:
            errors.append("Safety margin must be between 0 and 1")
        
        if self.rate_limit.max_concurrent_requests <= 0:
            errors.append("Max concurrent requests must be positive")
        
        # Validate validation config
        if self.validation.max_volatility_threshold <= 0:
            errors.append("Max volatility threshold must be positive")
        
        if not 0 <= self.validation.max_missing_percentage <= 1:
            errors.append("Max missing percentage must be between 0 and 1")
        
        if not 0 <= self.validation.max_duplicate_percentage <= 1:
            errors.append("Max duplicate percentage must be between 0 and 1")
        
        # Validate request config
        if self.max_batch_size <= 0:
            errors.append("Max batch size must be positive")
        
        if self.max_trade_batch_size <= 0:
            errors.append("Max trade batch size must be positive")
        
        if self.retry_attempts <= 0:
            errors.append("Retry attempts must be positive")
        
        if self.retry_delay <= 0:
            errors.append("Retry delay must be positive")
        
        if self.connection_pool_size <= 0:
            errors.append("Connection pool size must be positive")
        
        # Validate monitoring config
        if self.monitoring.metrics_interval <= 0:
            errors.append("Metrics interval must be positive")
        
        if not (1 <= self.monitoring.dashboard_port <= 65535):
            errors.append("Dashboard port must be between 1 and 65535")
        
        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")
        
        logger.info("Configuration validation passed")
    
    def setup_logging(self):
        """Set up logging based on configuration."""
        import logging
        
        # Configure level
        level = getattr(logging, self.log_level.upper())
        logging.basicConfig(level=level)
        
        # Add file handler if specified
        if self.log_file:
            handler = logging.FileHandler(self.log_file)
            handler.setLevel(level)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logging.getLogger().addHandler(handler)
        
        logger.info(f"Logging configured with level {self.log_level}")
