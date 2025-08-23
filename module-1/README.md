# Module 1 — Binance Data Fetching and Processing

Module 1 is responsible for fetching, validating, caching, and tracking progress of Binance OHLC (candlestick) and trade data. It serves as the foundation for Modules x-y-z

---

## Module 1 Structure
module1/
│
├─ config.py # Configuration settings (API keys, cache, rate limits)
├─ rate_limiter.py # Manages API request rate limits
├─ data_validator.py # Validates OHLC and trade data
├─ cache_manager.py # Caches fetched data locally
├─ progress_tracker.py # Tracks progress of fetching process
├─ binance_fetcher.py # Fetches OHLC and trades from Binance API
├─ utils.py # Utility functions (retry, signature, bid/ask volumes)
├─ init.py # Initializes Module 1 package
└─ README.md # This file

---

1### "config.py"
Central configuration file for all module parameters.

**Configuration Categories:**
- **API Settings**: `api_key`, `api_secret`
- **Cache Management**: `cache_dir`, `cache_version`
- **Rate Limiting**: `max_requests_per_minute`, `safety_margin`
- **Request Handling**: `max_batch_size`, `retry_attempts`, `retry_delay`
- **Data Validation**: `max_volatility_threshold`

Centralizes all configurable parameters for easy customization without code modification.

2### "rate_limiter.py"
Manages Binance API rate limits to prevent bans.

- Tracks request timestamps
- Enforces waiting periods when limits are reached
- Uses configurable parameters
- Required for all API requests

3### "data_validator.py"
Validates OHLC and trade data integrity.

**OHLC Validation:**
- Required columns: `timestamp`, `open`, `high`, `low`, `close`, `volume`
- Duplicate and missing timestamp detection
- Negative price/volume validation
- Logical consistency checks (e.g., high ≥ low)
- Extreme volatility detection

**Trade Validation:**
- Required columns: `id`, `timestamp`, `price`, `quantity`, `is_buyer_maker`
- Price and quantity positivity checks
- Trade ID gap detection

Returns validation results with issues and metrics for quality assessment.

4### "cache_manager.py"
Local caching system using Parquet files.

- Saves and loads OHLC and trade data batches
- Stores metadata (symbol, interval, time range, record count, version)
- Cache validation ensures data integrity
- Prevents redundant API calls
- Organized by symbol, interval, and candle for trades

5### "progress_tracker.py"
Progress monitoring for data fetching operations.

- Live percentage completion updates
- Elapsed time tracking
- Estimated time of arrival (ETA)
- Separate tracking for candles and trades
- Clean finalization upon completion

Useful for monitoring large dataset downloads.

6### "binance_fetcher.py"
Main fetching engine that coordinates all module components.

**Key Methods:**
```python
fetch_ohlc_batch()           # Fetch OHLC data batch
fetch_trades_batch()         # Fetch trades batch
process_candle_trades()      # Fetch trades for single candle
fetch_complete_dataset()     # Fetch OHLC + all trades for symbol/interval
```

**Features:**
- Rate-limited API requests via `RateLimiter`
- Data validation via `DataValidator`
- Caching via `CacheManager`
- Progress tracking via `ProgressTracker`
- Bid/ask volume computation per trade

7### "utils.py"
Utility functions supporting the module.

- `retry()` - Decorator for API request retries
- `generate_signature()` - HMAC signature generation for authenticated Binance requests
- `compute_bid_ask_volumes()` - Bid/ask volume calculation for trade analysis



## Main Features

- Load OHLC and trade data from local cache if available, otherwise fetch from Binance API.
- Validate data for integrity and completeness.
- Cache results using Parquet-based local storage.
- Track progress in real-time for large datasets (candles and trades separately).
- Handles retries, rate-limiting, and bid/ask volume computation automatically.

  flow:
            +-------------------+
          |   User / Script   |
          +-------------------+
                    |
                    v
          +-------------------+
          | BinanceDataFetcher|
          +-------------------+
                    |
          +-------------------+
          | Check OHLC Cache  |
          +-------------------+
            /            \
      Cache Hit       Cache Miss
        |                 |
        v                 v
  +-----------+     +----------------+
  | Load Data |     | Fetch OHLC API |
  +-----------+     +----------------+
                        |
                        v
                +-------------------+
                | Validate OHLC     |
                +-------------------+
                        |
                        v
                +-------------------+
                | Save OHLC to Cache|
                +-------------------+
                        |
                        v
          +----------------------------+
          | Process Each Candle Trades |
          +----------------------------+
                        |
          +----------------------------+
          | Check Trades Cache         |
          +----------------------------+
            /            \
      Cache Hit       Cache Miss
        |                 |
        v                 v
  +-----------+     +----------------+
  | Load Data |     | Fetch Trades API|
  +-----------+     +----------------+
                        |
                        v
                +-------------------+
                | Validate Trades   |
                +-------------------+
                        |
                        v
                +-------------------+
                | Compute Bid/Ask   |
                +-------------------+
                        |
                        v
                +-------------------+
                | Save Trades Cache |
                +-------------------+
                        |
                        v
          +--------------------------+
          | Merge OHLC + Trades      |
          +--------------------------+
                        |
                        v
                +-------------------+
                | Final Dataset     |
                +-------------------+



  
