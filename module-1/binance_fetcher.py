# binance_fetcher.py
import time
import requests
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from config import Config
from rate_limiter import RateLimiter
from data_validator import DataValidator
from cache_manager import CacheManager
from progress_tracker import ProgressTracker
from utils import retry, generate_signature, compute_bid_ask_volumes

class BinanceDataFetcher:
    BASE_URL = "https://api.binance.com/api/v3"
    
    def __init__(self, config: Config):
        self.config = config
        self.rate_limiter = RateLimiter(config)
        self.validator = DataValidator(config)
        self.cache_manager = CacheManager(config)
        self.progress_tracker = ProgressTracker(config)
        self.session = requests.Session()
        
        if config.api_key:
            self.session.headers.update({"X-MBX-APIKEY": config.api_key})
    
    @retry(max_attempts=3, delay=1.0)
    def _make_request(self, endpoint: str, params: Dict[str, Any], signed: bool = False) -> Dict[str, Any]:
        """Make a request to Binance API with retries and rate limiting"""
        self.rate_limiter.wait_if_needed()
        
        if signed and self.config.api_secret:
            params["timestamp"] = int(time.time() * 1000)
            params["signature"] = generate_signature(params, self.config.api_secret)
        
        url = f"{self.BASE_URL}/{endpoint}"
        response = self.session.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        return response.json()
    
    def fetch_ohlc_batch(self, symbol: str, interval: str, start_time: datetime, 
                        end_time: datetime) -> pd.DataFrame:
        """Fetch a batch of OHLC data"""
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "limit": self.config.max_batch_size
        }
        
        data = self._make_request("klines", params)
        
        # Convert to DataFrame
        df = pd.DataFrame(data, columns=[
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ])
        
        # Convert data types
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        numeric_cols = ["open", "high", "low", "close", "volume", 
                       "quote_asset_volume", "number_of_trades",
                       "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
        
        return df
    
    def fetch_trades_batch(self, symbol: str, start_time: datetime, 
                          end_time: datetime, from_id: Optional[int] = None) -> pd.DataFrame:
        """Fetch a batch of trade data"""
        params = {
            "symbol": symbol,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "limit": self.config.max_batch_size
        }
        
        if from_id:
            params["fromId"] = from_id
        
        data = self._make_request("historicalTrades", params, signed=True)
        
        # Convert to DataFrame
        df = pd.DataFrame(data, columns=[
            "id", "price", "quantity", "quote_quantity", "timestamp", 
            "is_buyer_maker", "is_best_match"
        ])
        
        # Convert data types
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        numeric_cols = ["id", "price", "quantity", "quote_quantity"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
        df["is_buyer_maker"] = df["is_buyer_maker"].astype(bool)
        df["is_best_match"] = df["is_best_match"].astype(bool)
        
        return df
    
    def process_candle_trades(self, symbol: str, candle_start: datetime, 
                             candle_end: datetime) -> pd.DataFrame:
        """Fetch and process trades for a single candle"""
        # Try to load from cache first
        cached_trades = self.cache_manager.load_trades(symbol, candle_start)
        if cached_trades is not None:
            self.progress_tracker.update_trade_progress(len(cached_trades))
            return cached_trades
        
        # Fetch trades from API
        all_trades = []
        from_id = None
        
        while True:
            trades_batch = self.fetch_trades_batch(symbol, candle_start, candle_end, from_id)
            
            if trades_batch.empty:
                break
                
            all_trades.append(trades_batch)
            self.progress_tracker.update_trade_progress(len(trades_batch))
            
            # Check if we need to fetch more
            if len(trades_batch) < self.config.max_batch_size:
                break
                
            # Set from_id for next batch
            from_id = trades_batch["id"].iloc[-1] + 1
        
        if not all_trades:
            return pd.DataFrame()
        
        # Combine all batches
        trades_df = pd.concat(all_trades, ignore_index=True)
        
        # Validate trades
        validation_result = self.validator.validate_trades(trades_df)
        if not validation_result["valid"]:
            print(f"Trade validation issues: {validation_result['issues']}")
        
        # Compute bid/ask volumes
        trades_df = compute_bid_ask_volumes(trades_df)
        
        # Save to cache
        self.cache_manager.save_trades(trades_df, symbol, candle_start)
        
        return trades_df
    
    def fetch_complete_dataset(self, symbol: str, interval: str, 
                              start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Fetch complete dataset with OHLC and trade data"""
        print(f"Fetching data for {symbol} from {start_time} to {end_time}")
        
        # Calculate number of candles
        interval_minutes = pd.Timedelta(interval).total_seconds() / 60
        total_minutes = (end_time - start_time).total_seconds() / 60
        total_candles = int(total_minutes / interval_minutes)
        
        # Estimate trades (rough approximation)
        avg_trades_per_candle = 100  # Adjust based on symbol and interval
        total_trades = total_candles * avg_trades_per_candle
        
        # Set progress totals
        self.progress_tracker.set_totals(total_candles, total_trades)
        
        # Try to load OHLC from cache
        ohlc_df = self.cache_manager.load_ohlc(symbol, interval, start_time, end_time)
        
        if ohlc_df is None:
            # Fetch OHLC data
            ohlc_df = self.fetch_ohlc_batch(symbol, interval, start_time, end_time)
            
            # Validate OHLC
            validation_result = self.validator.validate_ohlc(ohlc_df)
            if not validation_result["valid"]:
                print(f"OHLC validation issues: {validation_result['issues']}")
            
            # Save to cache
            self.cache_manager.save_ohlc(ohlc_df, symbol, interval, start_time, end_time)
        
        # Process each candle to get trades
        all_candle_data = []
        
        for _, candle in ohlc_df.iterrows():
            candle_start = candle["timestamp"]
            candle_end = candle_start + pd.Timedelta(interval)
            
            # Get trades for this candle
            candle_trades = self.process_candle_trades(symbol, candle_start, candle_end)
            
            # Add candle info to trades
            candle_data = candle_trades.copy()
            candle_data["candle_open"] = candle["open"]
            candle_data["candle_high"] = candle["high"]
            candle_data["candle_low"] = candle["low"]
            candle_data["candle_close"] = candle["close"]
            candle_data["candle_volume"] = candle["volume"]
            
            all_candle_data.append(candle_data)
            self.progress_tracker.update_candle_progress()
        
        # Combine all candle data
        if not all_candle_data:
            return pd.DataFrame()
        
        complete_df = pd.concat(all_candle_data, ignore_index=True)
        
        # Finalize progress
        self.progress_tracker.finish()
        
        return complete_df
