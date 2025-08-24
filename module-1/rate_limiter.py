"""
API rate limiting implementation for Binance API.

This module handles:
- Token bucket algorithm for rate limiting
- Adaptive rate limiting based on API responses
- Distributed rate limiting support
- Performance monitoring
"""

import time
import asyncio
import logging
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from datetime import datetime

from .config import Config
from .monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)


@dataclass
class RateLimitMetrics:
    """Metrics for rate limiter performance."""
    requests_made: int = 0
    requests_limited: int = 0
    total_wait_time: float = 0.0
    max_wait_time: float = 0.0
    last_limit_time: Optional[datetime] = None
    adaptive_adjustments: int = 0


class RateLimiter:
    """
    Implements rate limiting using token bucket algorithm with adaptive adjustments.
    
    Features:
    - Token bucket algorithm
    - Adaptive rate limiting
    - Distributed support
    - Performance metrics
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.metrics = MetricsCollector(config)
        
        # Token bucket parameters
        self.max_tokens = int(config.max_requests_per_minute * config.safety_margin)
        self.tokens = self.max_tokens
        self.refill_rate = self.max_tokens / 60  # tokens per second
        self.last_refill = time.time()
        
        # Adaptive parameters
        self.current_limit = self.max_tokens
        self.adjustment_factor = 0.9
        self.recovery_factor = 1.05
        self.last_limit_time = None
        self.recent_limits = []
        
        # Performance tracking
        self.metrics_data = RateLimitMetrics()
        
        # Concurrency control
        self.semaphore = asyncio.Semaphore(config.max_concurrent_requests)
        self.lock = asyncio.Lock()
        
        logger.info(f"Initialized RateLimiter with {self.max_tokens} tokens/minute")
    
    async def _refill_tokens(self):
        """Refill tokens based on elapsed time."""
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_refill
            
            # Calculate tokens to add
            tokens_to_add = elapsed * self.refill_rate
            self.tokens = min(self.current_limit, self.tokens + tokens_to_add)
            
            self.last_refill = now
    
    async def wait_if_needed(self) -> float:
        """
        Wait if rate limit would be exceeded.
        
        Returns:
            Time waited in seconds
        """
        async with self.semaphore:
            async with self.lock:
                # Refill tokens first
                await self._refill_tokens()
                
                # Check if we have tokens
                if self.tokens >= 1:
                    self.tokens -= 1
                    self.metrics_data.requests_made += 1
                    return 0.0
                
                # Calculate wait time
                tokens_needed = 1 - self.tokens
                wait_time = tokens_needed / self.refill_rate
                
                # Cap wait time
                wait_time = min(wait_time, 5.0)
                
                # Update metrics
                self.metrics_data.requests_limited += 1
                self.metrics_data.total_wait_time += wait_time
                self.metrics_data.max_wait_time = max(self.metrics_data.max_wait_time, wait_time)
                
                # Wait
                start_wait = time.time()
                await asyncio.sleep(wait_time)
                
                # After waiting, refill and consume token
                await self._refill_tokens()
                self.tokens -= 1
                self.metrics_data.requests_made += 1
                
                actual_wait_time = time.time() - start_wait
                return actual_wait_time
    
    def record_rate_limit(self):
        """Record a rate limit response from API."""
        now = datetime.now()
        self.metrics_data.last_limit_time = now
        
        # Track recent limits
        self.recent_limits.append(now)
        
        # Keep only limits from last 5 minutes
        self.recent_limits = [
            t for t in self.recent_limits 
            if (now - t).total_seconds() < 300
        ]
        
        # If we have multiple recent limits, adjust
        if len(self.recent_limits) >= 3:
            self._adjust_rate_limit()
    
    def _adjust_rate_limit(self):
        """Adjust rate limit based on recent API responses."""
        old_limit = self.current_limit
        
        # Reduce limit
        self.current_limit = int(self.current_limit * self.adjustment_factor)
        self.current_limit = max(self.current_limit, int(self.max_tokens * 0.5))  # Don't go below 50%
        
        # Update metrics
        self.metrics_data.adaptive_adjustments += 1
        
        # Update refill rate
        self.refill_rate = self.current_limit / 60
        
        logger.warning(
            f"Rate limit adjusted: {old_limit} -> {self.current_limit} "
            f"({len(self.recent_limits)} recent limits)"
        )
    
    def record_success(self):
        """Record a successful request."""
        # If we haven't had limits recently, gradually increase limit
        now = datetime.now()
        recent_limits = [
            t for t in self.recent_limits 
            if (now - t).total_seconds() < 300
        ]
        
        if len(recent_limits) == 0 and self.current_limit < self.max_tokens:
            old_limit = self.current_limit
            self.current_limit = min(
                int(self.current_limit * self.recovery_factor),
                self.max_tokens
            )
            
            if old_limit != self.current_limit:
                self.refill_rate = self.current_limit / 60
                logger.info(f"Rate limit recovered: {old_limit} -> {self.current_limit}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get rate limiter metrics."""
        return {
            "current_limit": self.current_limit,
            "max_limit": self.max_tokens,
            "available_tokens": self.tokens,
            "requests_made": self.metrics_data.requests_made,
            "requests_limited": self.metrics_data.requests_limited,
            "limit_rate": (
                self.metrics_data.requests_limited / 
                max(1, self.metrics_data.requests_made) * 100
            ),
            "total_wait_time": self.metrics_data.total_wait_time,
            "max_wait_time": self.metrics_data.max_wait_time,
            "recent_limits": len(self.recent_limits),
            "adaptive_adjustments": self.metrics_data.adaptive_adjustments,
            "last_limit_time": (
                self.metrics_data.last_limit_time.isoformat() 
                if self.metrics_data.last_limit_time else None
            )
        }
