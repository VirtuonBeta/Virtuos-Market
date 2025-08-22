# cache_manager.py
import os
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
from datetime import datetime
from config import Config

class CacheManager:
    def __init__(self, config: Config):
        self.config = config
        os.makedirs(config.cache_dir, exist_ok=True)
    
    def _get_cache_path(self, data_type: str, symbol: str, interval: str, 
                       start_time: Optional[datetime] = None, 
                       end_time: Optional[datetime] = None) -> Path:
        """Construct cache file path based on parameters"""
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
    
    def save_ohlc(self, df: pd.DataFrame, symbol: str, interval: str, 
                 start_time: datetime, end_time: datetime) -> None:
        """Save OHLC data to cache"""
        path = self._get_cache_path("ohlc", symbol, interval, start_time, end_time)
        
        # Add metadata
        metadata = {
            'symbol': symbol,
            'interval': interval,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'record_count': len(df),
            'cache_version': self.config.cache_version,
            'cached_at': datetime.now().isoformat()
        }
        
        # Save data and metadata
        df.to_parquet(path)
        pd.Series(metadata).to_json(path.with_suffix('.metadata.json'))
    
    def load_ohlc(self, symbol: str, interval: str, 
                 start_time: datetime, end_time: datetime) -> Optional[pd.DataFrame]:
        """Load OHLC data from cache if available"""
        path = self._get_cache_path("ohlc", symbol, interval, start_time, end_time)
        
        if not path.exists():
            return None
        
        # Check metadata
        metadata_path = path.with_suffix('.metadata.json')
        if not metadata_path.exists():
            return None
        
        try:
            metadata = pd.read_json(metadata_path, typ='series')
            
            # Verify cache version
            if metadata.get('cache_version') != self.config.cache_version:
                return None
            
            # Verify date range
            cached_start = datetime.fromisoformat(metadata['start_time'])
            cached_end = datetime.fromisoformat(metadata['end_time'])
            
            if cached_start > start_time or cached_end < end_time:
                return None
            
            return pd.read_parquet(path)
        except Exception:
            return None
    
    def save_trades(self, df: pd.DataFrame, symbol: str, candle_time: datetime) -> None:
        """Save trades data to cache"""
        path = self._get_cache_path("trades", symbol, "", candle_time)
        
        # Add metadata
        metadata = {
            'symbol': symbol,
            'candle_time': candle_time.isoformat(),
            'record_count': len(df),
            'cache_version': self.config.cache_version,
            'cached_at': datetime.now().isoformat()
        }
        
        # Save data and metadata
        df.to_parquet(path)
        pd.Series(metadata).to_json(path.with_suffix('.metadata.json'))
    
    def load_trades(self, symbol: str, candle_time: datetime) -> Optional[pd.DataFrame]:
        """Load trades data from cache if available"""
        path = self._get_cache_path("trades", symbol, "", candle_time)
        
        if not path.exists():
            return None
        
        # Check metadata
        metadata_path = path.with_suffix('.metadata.json')
        if not metadata_path.exists():
            return None
        
        try:
            metadata = pd.read_json(metadata_path, typ='series')
            
            # Verify cache version
            if metadata.get('cache_version') != self.config.cache_version:
                return None
            
            return pd.read_parquet(path)
        except Exception:
            return None
