"""
Storage and caching management for Module 1.

This module handles:
- In-memory caching with LRU eviction
- Disk-based persistent caching
- Cache invalidation and cleanup
- Performance optimization
"""

import os
import json
import time
import asyncio
import aiofiles
import pandas as pd
import logging
from pathlib import Path
from typing import Any, Optional, Dict, List
from datetime import datetime, timedelta
from collections import OrderedDict

from .config import Config
from .monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class CacheManager:
    """
    Manages data caching for improved performance.
    
    Features:
    - In-memory LRU cache
    - Disk-based persistent cache
    - Automatic cleanup
    - Performance metrics
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.metrics = MetricsCollector(config)
        
        # Create cache directory
        os.makedirs(config.cache_dir, exist_ok=True)
        
        # In-memory cache with LRU eviction
        self.memory_cache = OrderedDict()
        self.max_memory_size = config.max_memory_cache_size
        
        # Cache metadata
        self.cache_index_file = os.path.join(config.cache_dir, "cache_index.json")
        self.cache_index = self._load_cache_index()
        
        # Performance tracking
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        
        logger.info(f"Initialized CacheManager with directory {config.cache_dir}")
    
    def _load_cache_index(self) -> Dict[str, Dict[str, Any]]:
        """Load cache index from disk."""
        if os.path.exists(self.cache_index_file):
            try:
                with open(self.cache_index_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading cache index: {str(e)}")
        
        return {}
    
    def _save_cache_index(self):
        """Save cache index to disk."""
        try:
            with open(self.cache_index_file, 'w') as f:
                json.dump(self.cache_index, f)
        except Exception as e:
            logger.error(f"Error saving cache index: {str(e)}")
    
    def _get_cache_path(self, key: str) -> Path:
        """Get file path for a cache key."""
        # Use hash to avoid filesystem issues
        hash_key = str(hash(key))
        return Path(self.config.cache_dir) / f"{hash_key}.parquet"
    
    def _evict_if_needed(self):
        """Evict items from memory cache if needed."""
        while len(self.memory_cache) > self.max_memory_size:
            self.memory_cache.popitem(last=False)
            self.evictions += 1
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get item from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached item or None if not found
        """
        # Check memory cache first
        if key in self.memory_cache:
            # Move to end (most recently used)
            item = self.memory_cache.pop(key)
            self.memory_cache[key] = item
            self.hits += 1
            self.metrics.record_cache_hit()
            return item
        
        # Check disk cache
        cache_info = self.cache_index.get(key)
        if cache_info:
            file_path = Path(cache_info['path'])
            
            if file_path.exists():
                try:
                    # Check if cache is expired
                    cached_at = datetime.fromisoformat(cache_info['cached_at'])
                    if datetime.now() - cached_at > timedelta(days=self.config.cache_expiry_days):
                        # Cache expired
                        await self.delete(key)
                        self.misses += 1
                        self.metrics.record_cache_miss()
                        return None
                    
                    # Load from disk
                    if cache_info['type'] == 'dataframe':
                        df = pd.read_parquet(file_path)
                        
                        # Add to memory cache
                        self.memory_cache[key] = df
                        self._evict_if_needed()
                        
                        self.hits += 1
                        self.metrics.record_cache_hit()
                        return df
                    
                except Exception as e:
                    logger.error(f"Error loading from cache: {str(e)}")
                    await self.delete(key)
        
        self.misses += 1
        self.metrics.record_cache_miss()
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """
        Set item in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds (optional)
        """
        # Add to memory cache
        self.memory_cache[key] = value
        self._evict_if_needed()
        
        # Save to disk
        try:
            file_path = self._get_cache_path(key)
            
            if isinstance(value, pd.DataFrame):
                value.to_parquet(file_path)
                
                # Update cache index
                self.cache_index[key] = {
                    'path': str(file_path),
                    'type': 'dataframe',
                    'cached_at': datetime.now().isoformat(),
                    'size': len(value),
                    'ttl': ttl
                }
                
                self._save_cache_index()
                
        except Exception as e:
            logger.error(f"Error saving to cache: {str(e)}")
    
    async def delete(self, key: str):
        """Delete item from cache."""
        # Remove from memory
        if key in self.memory_cache:
            del self.memory_cache[key]
        
        # Remove from disk
        cache_info = self.cache_index.get(key)
        if cache_info:
            file_path = Path(cache_info['path'])
            try:
                if file_path.exists():
                    file_path.unlink()
            except Exception as e:
                logger.error(f"Error deleting cache file: {str(e)}")
            
            # Remove from index
            del self.cache_index[key]
            self._save_cache_index()
    
    async def clear(self):
        """Clear all cache."""
        # Clear memory
        self.memory_cache.clear()
        
        # Clear disk
        try:
            for file_path in Path(self.config.cache_dir).glob("*.parquet"):
                file_path.unlink()
            
            # Clear index
            self.cache_index = {}
            self._save_cache_index()
            
            logger.info("Cleared all cache")
            
        except Exception as e:
            logger.error(f"Error clearing cache: {str(e)}")
    
    async def cleanup_expired(self):
        """Clean up expired cache entries."""
        expired_keys = []
        
        for key, info in self.cache_index.items():
            cached_at = datetime.fromisoformat(info['cached_at'])
            if datetime.now() - cached_at > timedelta(days=self.config.cache_expiry_days):
                expired_keys.append(key)
        
        for key in expired_keys:
            await self.delete(key)
        
        logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get cache performance metrics."""
        total_requests = self.hits + self.misses
        hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0
        
        return {
            "memory_usage": len(self.memory_cache),
            "memory_capacity": self.max_memory_size,
            "disk_entries": len(self.cache_index),
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate,
            "evictions": self.evictions
        }
