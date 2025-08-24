"""
Main fetching logic for Binance API data retrieval.

This module handles:
- API communication with Binance
- Request batching and parallelization
- Error handling and retries
- Data format conversion
"""

import asyncio
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import aiohttp
import pandas as pd

from .config import Config
from .rate_limiter import RateLimiter
from .cache_manager import CacheManager
from .validators import DataValidator
from .monitoring.metrics import MetricsCollector
from .utils import retry, generate_signature, compute_bid_ask_volumes

logger = logging.getLogger(__name__)


class BinanceDataFetcher:
    """
    Main class for fetching data from Binance API.
    
    Responsibilities:
    - Manage API sessions and connections
    - Implement rate limiting
    - Handle retries and error recovery
    - Convert raw data to standardized format
    - Track performance metrics
    """
    
    BASE_URL = "https://api.binance.com/api/v3"
    
    # Mapping of Binance interval strings to minutes
    INTERVAL_MAP = {
        '1m': 1, '3m': 3, '5m': 5, '15m': 15, '30m': 30,
        '1h': 60, '2h': 120, '4h': 240, '6h': 360, '8h': 480,
        '12h': 720, '1d': 1440, '3d': 4320, '1w': 10080, '1M': 43200
    }
    
    def __init__(self, config: Config):
        self.config = config
        self.rate_limiter = RateLimiter(config)
        self.cache_manager = CacheManager(config)
        self.validator = DataValidator(config)
        self.metrics = MetricsCollector(config)
        
        # Session management
        self.session = None
        self.session_lock = asyncio.Lock()
        
        # Performance tracking
        self.request_count = 0
        self.error_count = 0
        self.bytes_received = 0
        
        logger.info("Initialized BinanceDataFetcher")
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session with proper configuration."""
        async with self.session_lock:
            if self.session is None or self.session.closed:
                # Configure connection pooling
                connector = aiohttp.TCPConnector(
                    limit=self.config.connection_pool_size,
                    limit_per_host=self.config.connection_pool_size,
                    ttl_dns_cache=300,
                    use_dns_cache=True
                )
                
                # Set timeout
                timeout = aiohttp.ClientTimeout(
                    total=self.config.request_timeout,
                    connect=10,
                    sock_read=30
                )
                
                # Set headers
                headers = {}
                if self.config.api_key:
                    headers["X-MBX-APIKEY"] = self.config.api_key
                
                self.session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers=headers,
                    trust_env=True
                )
                
                logger.debug("Created new aiohttp session")
            
            return self.session
    
    async def close(self):
        """Close the aiohttp session and cleanup resources."""
        async with self.session_lock:
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
                logger.debug("Closed aiohttp session")
    
    @retry(max_attempts=3, delay=1.0, backoff_factor=2.0)
    async def make_request(
        self, 
        endpoint: str, 
        params: Dict[str, Any], 
        signed: bool = False
    ) -> Dict[str, Any]:
        """
        Make an authenticated request to Binance API.
        
        Args:
            endpoint: API endpoint (e.g., 'klines', 'aggTrades')
            params: Request parameters
            signed: Whether to sign the request
            
        Returns:
            Response data as dictionary
        """
        session = await self.get_session()
        
        # Add timestamp and signature for signed requests
        if signed and self.config.api_secret:
            params["timestamp"] = int(time.time() * 1000)
            params["signature"] = generate_signature(params, self.config.api_secret)
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        # Track request start time
        start_time = time.time()
        
        try:
            # Wait for rate limiter
            await self.rate_limiter.wait_if_needed()
            
            # Make the request
            async with session.get(url, params=params) as response:
                # Update metrics
                self.request_count += 1
                request_time = time.time() - start_time
                
                # Handle rate limiting
                if response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', 5))
                    logger.warning(f"Rate limited. Retry after {retry_after} seconds")
                    self.rate_limiter.record_rate_limit()
                    await asyncio.sleep(retry_after)
                    raise Exception("Rate limit exceeded")
                
                # Handle other errors
                response.raise_for_status()
                
                # Parse response
                data = await response.json()
                
                # Update metrics
                self.bytes_received += len(str(data).encode('utf-8'))
                self.metrics.record_api_call(endpoint, request_time, len(data))
                
                return data
                
        except Exception as e:
            self.error_count += 1
            self.metrics.record_error(endpoint, str(e))
            logger.error(f"Request failed: {str(e)}")
            raise
    
    async def fetch_ohlc_data(
        self, 
        symbol: str, 
        interval: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> pd.DataFrame:
        """
        Fetch OHLC data for the specified symbol and time range.
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            interval: Kline interval (e.g., '1m', '5m', '1h')
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            DataFrame with OHLC data
        """
        logger.info(f"Fetching OHLC data for {symbol} {interval} from {start_time} to {end_time}")
        
        # Check cache first
        cache_key = f"ohlc_{symbol}_{interval}_{start_time}_{end_time}"
        cached_data = await self.cache_manager.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Using cached OHLC data for {symbol}")
            return cached_data
        
        # Calculate number of candles needed
        interval_minutes = self.INTERVAL_MAP.get(interval, 1)
        total_minutes = (end_time - start_time).total_seconds() / 60
        total_candles = int(total_minutes / interval_minutes)
        
        # Determine batch size
        batch_size = min(self.config.max_batch_size, total_candles)
        batch_duration = timedelta(minutes=batch_size * interval_minutes)
        
        # Create batches
        batches = []
        current_start = start_time
        
        while current_start < end_time:
            current_end = min(current_start + batch_duration, end_time)
            batches.append((current_start, current_end))
            current_start = current_end
        
        # Fetch data in parallel
        tasks = [
            self._fetch_ohlc_batch(symbol, interval, batch_start, batch_end)
            for batch_start, batch_end in batches
        ]
        
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        all_data = []
        for result in batch_results:
            if isinstance(result, Exception):
                logger.error(f"Batch fetch failed: {str(result)}")
            elif result:
                all_data.extend(result)
        
        # Convert to DataFrame
        df = self._process_ohlc_data(all_data)
        
        # Validate data
        validation_result = self.validator.validate_ohlc(df, interval)
        if not validation_result.valid:
            logger.warning(f"OHLC validation issues: {validation_result.issues}")
        
        # Cache the result
        await self.cache_manager.set(cache_key, df)
        
        logger.info(f"Fetched {len(df)} OHLC records for {symbol}")
        return df
    
    async def _fetch_ohlc_batch(
        self, 
        symbol: str, 
        interval: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[List]:
        """Fetch a batch of OHLC data."""
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "limit": self.config.max_batch_size
        }
        
        return await self.make_request("klines", params)
    
    def _process_ohlc_data(self, raw_data: List[List]) -> pd.DataFrame:
        """Convert raw OHLC data to DataFrame."""
        if not raw_data:
            return pd.DataFrame()
        
        # Define columns
        columns = [
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ]
        
        # Create DataFrame
        df = pd.DataFrame(raw_data, columns=columns)
        
        # Convert timestamp
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        
        # Convert numeric columns
        numeric_cols = [
            "open", "high", "low", "close", "volume",
            "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"
        ]
        
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], downcast="float")
        
        return df
    
    async def fetch_trades_data(
        self, 
        symbol: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> pd.DataFrame:
        """
        Fetch trades data for the specified symbol and time range.
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            DataFrame with trades data
        """
        logger.info(f"Fetching trades data for {symbol} from {start_time} to {end_time}")
        
        # Check cache first
        cache_key = f"trades_{symbol}_{start_time}_{end_time}"
        cached_data = await self.cache_manager.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Using cached trades data for {symbol}")
            return cached_data
        
        # Fetch data in batches
        all_trades = []
        from_id = None
        
        while True:
            # Fetch a batch
            batch = await self._fetch_trades_batch(symbol, start_time, end_time, from_id)
            
            if not batch:
                break
            
            all_trades.extend(batch)
            
            # Check if we need more batches
            if len(batch) < self.config.max_trade_batch_size:
                break
            
            # Set from_id for next batch
            from_id = batch[-1]["a"] + 1  # "a" is agg_trade_id
        
        # Convert to DataFrame
        df = self._process_trades_data(all_trades)
        
        # Validate data
        validation_result = self.validator.validate_trades(df)
        if not validation_result.valid:
            logger.warning(f"Trades validation issues: {validation_result.issues}")
        
        # Compute bid/ask volumes
        df = compute_bid_ask_volumes(df)
        
        # Cache the result
        await self.cache_manager.set(cache_key, df)
        
        logger.info(f"Fetched {len(df)} trade records for {symbol}")
        return df
    
    async def _fetch_trades_batch(
        self, 
        symbol: str, 
        start_time: datetime, 
        end_time: datetime, 
        from_id: Optional[int] = None
    ) -> List[Dict]:
        """Fetch a batch of trades data."""
        if from_id is None:
            params = {
                "symbol": symbol,
                "startTime": int(start_time.timestamp() * 1000),
                "endTime": int(end_time.timestamp() * 1000),
                "limit": self.config.max_trade_batch_size
            }
        else:
            params = {
                "symbol": symbol,
                "fromId": from_id,
                "limit": self.config.max_trade_batch_size
            }
        
        return await self.make_request("aggTrades", params)
    
    def _process_trades_data(self, raw_data: List[Dict]) -> pd.DataFrame:
        """Convert raw trades data to DataFrame."""
        if not raw_data:
            return pd.DataFrame()
        
        # Create DataFrame
        df = pd.DataFrame(raw_data)
        
        # Rename columns
        df.rename(columns={
            "a": "agg_trade_id",
            "p": "price",
            "q": "quantity",
            "f": "first_trade_id",
            "l": "last_trade_id",
            "T": "timestamp",
            "m": "is_buyer_maker",
            "M": "was_best_price"
        }, inplace=True)
        
        # Convert timestamp
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        
        # Convert numeric columns
        numeric_cols = ["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], downcast="float")
        
        # Convert boolean columns
        df["is_buyer_maker"] = df["is_buyer_maker"].astype(bool)
        df["was_best_price"] = df["was_best_price"].astype(bool)
        
        return df
    
    async def aggregate_to_minute_bars(self, trades_df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate raw trades to 1-minute OHLC bars.
        
        Args:
            trades_df: DataFrame with trades data
            
        Returns:
            DataFrame with 1-minute OHLC bars
        """
        logger.info(f"Aggregating {len(trades_df)} trades to 1-minute bars")
        
        if trades_df.empty:
            return pd.DataFrame()
        
        # Ensure timestamp is index
        df = trades_df.copy()
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        
        # Define aggregation
        agg_dict = {
            'price': {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last'
            },
            'quantity': 'sum',
            'bid_volume': 'sum',
            'ask_volume': 'sum',
            'agg_trade_id': 'count'
        }
        
        # Resample to 1-minute bars
        minute_bars = df.resample('1T').agg(agg_dict)
        
        # Flatten column names
        minute_bars.columns = ['_'.join(col).strip() for col in minute_bars.columns.values]
        
        # Rename columns
        minute_bars.rename(columns={
            'price_open': 'open',
            'price_high': 'high',
            'price_low': 'low',
            'price_close': 'close',
            'quantity_sum': 'volume',
            'bid_volume_sum': 'bid_volume',
            'ask_volume_sum': 'ask_volume',
            'agg_trade_id_count': 'trade_count'
        }, inplace=True)
        
        # Drop rows with no trades
        minute_bars.dropna(inplace=True)
        
        # Reset index
        minute_bars.reset_index(inplace=True)
        
        logger.info(f"Created {len(minute_bars)} 1-minute bars")
        return minute_bars
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics."""
        return {
            "requests": self.request_count,
            "errors": self.error_count,
            "bytes_received": self.bytes_received,
            "error_rate": self.error_count / max(1, self.request_count) * 100,
            "rate_limiter": self.rate_limiter.get_metrics(),
            "cache": self.cache_manager.get_metrics()
        }
