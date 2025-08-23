# cache_manager.py
import os
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
from datetime import datetime, timezone
from collections import OrderedDict
from config import Config

class CacheManager:
    def __init__(self, config: Config):
        self.config = config
        os.makedirs(config.cache_dir, exist_ok=True)
        
        # Initialize in-memory cache with LRU eviction (max 10 items)
        self._memory_cache = OrderedDict()
        self._max_cache_size = 10
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _get_cache_path(self, data_type: str, symbol: str, interval: Optional[str] = None, 
                       start_time: Optional[datetime] = None, 
                       end_time: Optional[datetime] = None) -> Path:
        """Construct cache file path based on parameters"""
        # Ensure datetime objects are timezone-aware (UTC)
        if start_time:
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            else:
                start_time = start_time.astimezone(timezone.utc)
                
        if end_time:
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            else:
                end_time = end_time.astimezone(timezone.utc)
        
        if data_type == "ohlc":
            if start_time and end_time:
                filename = f"{symbol}_{interval}_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}.parquet"
            else:
                filename = f"{symbol}_{interval}_latest.parquet"
        elif data_type == "trades":
            # For trades, we typically cache per candle
            candle_time = start_time.strftime('%Y%m%d_%H%M') if start_time else "latest"
            filename = f"{symbol}_trades_{candle_time}.parquet"
        else:
            raise ValueError(f"Unknown data type: {data_type}")
        
        return Path(self.config.cache_dir) / filename
    
    def _add_to_memory_cache(self, key: str, df: pd.DataFrame) -> None:
        """Add DataFrame to in-memory cache with LRU eviction"""
        if key in self._memory_cache:
            # Move to end (most recently used)
            self._memory_cache.pop(key)
        self._memory_cache[key] = df
        
        # Evict oldest if over size limit
        if len(self._memory_cache) > self._max_cache_size:
            self._memory_cache.popitem(last=False)
    
    def _get_from_memory_cache(self, key: str) -> Optional[pd.DataFrame]:
        """Get DataFrame from in-memory cache if available"""
        if key in self._memory_cache:
            # Move to end (most recently used)
            df = self._memory_cache.pop(key)
            self._memory_cache[key] = df
            return df
        return None
    
    def save_ohlc(self, df: pd.DataFrame, symbol: str, interval: str, 
                 start_time: datetime, end_time: datetime) -> None:
        """Save OHLC data to cache"""
        try:
            path = self._get_cache_path("ohlc", symbol, interval, start_time, end_time)
            
            # Add metadata
            metadata = {
                'symbol': symbol,
                'interval': interval,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'record_count': len(df),
                'cache_version': self.config.cache_version,
                'cached_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Save data and metadata
            df.to_parquet(path)
            pd.Series(metadata).to_json(path.with_suffix('.metadata.json'))
            self.logger.info(f"Saved OHLC data to {path}")
        except Exception as e:
            self.logger.error(f"Error saving OHLC data: {str(e)}")
            raise
    
    def load_ohlc(self, symbol: str, interval: str, 
                 start_time: datetime, end_time: datetime) -> Optional[pd.DataFrame]:
        """Load OHLC data from cache if available"""
        try:
            path = self._get_cache_path("ohlc", symbol, interval, start_time, end_time)
            cache_key = str(path)
            
            # Check in-memory cache first
            cached_df = self._get_from_memory_cache(cache_key)
            if cached_df is not None:
                return cached_df
            
            if not path.exists():
                return None
            
            # Check metadata
            metadata_path = path.with_suffix('.metadata.json')
            if not metadata_path.exists():
                return None
            
            metadata = pd.read_json(metadata_path, typ='series')
            
            # Verify cache version
            if metadata.get('cache_version') != self.config.cache_version:
                self.logger.warning(f"Cache version mismatch for {path}")
                return None
            
            # Verify date range
            cached_start = datetime.fromisoformat(metadata['start_time'])
            cached_end = datetime.fromisoformat(metadata['end_time'])
            
            if cached_start > start_time or cached_end < end_time:
                return None
            
            df = pd.read_parquet(path)
            # Add to in-memory cache
            self._add_to_memory_cache(cache_key, df)
            return df
        except Exception as e:
            self.logger.error(f"Error loading OHLC data: {str(e)}")
            return None
    
    def save_trades(self, df: pd.DataFrame, symbol: str, candle_time: datetime) -> None:
        """Save trades data to cache"""
        try:
            path = self._get_cache_path("trades", symbol, start_time=candle_time)
            
            # Add metadata
            metadata = {
                'symbol': symbol,
                'candle_time': candle_time.isoformat(),
                'record_count': len(df),
                'cache_version': self.config.cache_version,
                'cached_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Save data and metadata
            df.to_parquet(path)
            pd.Series(metadata).to_json(path.with_suffix('.metadata.json'))
            self.logger.info(f"Saved trades data to {path}")
        except Exception as e:
            self.logger.error(f"Error saving trades data: {str(e)}")
            raise
    
    def load_trades(self, symbol: str, candle_time: datetime) -> Optional[pd.DataFrame]:
        """Load trades data from cache if available"""
        try:
            path = self._get_cache_path("trades", symbol, start_time=candle_time)
            cache_key = str(path)
            
            # Check in-memory cache first
            cached_df = self._get_from_memory_cache(cache_key)
            if cached_df is not None:
                return cached_df
            
            if not path.exists():
                return None
            
            # Check metadata
            metadata_path = path.with_suffix('.metadata.json')
            if not metadata_path.exists():
                return None
            
            metadata = pd.read_json(metadata_path, typ='series')
            
            # Verify cache version
            if metadata.get('cache_version') != self.config.cache_version:
                self.logger.warning(f"Cache version mismatch for {path}")
                return None
            
            df = pd.read_parquet(path)
            # Add to in-memory cache
            self._add_to_memory_cache(cache_key, df)
            return df
        except Exception as e:
            self.logger.error(f"Error loading trades data: {str(e)}")
            return None
