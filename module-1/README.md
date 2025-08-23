# Module 1

## Description
Module 1 handles fetching, caching, validating, and tracking Binance OHLC (candlestick) and trade data. It forms the foundation for Modules 2 and 3.

## Folder Structure
- `config.py` - Configuration (API keys, cache, rate limits)
- `rate_limiter.py` - API request rate limiter
- `data_validator.py` - OHLC & trade data validation
- `cache_manager.py` - Local Parquet caching
- `progress_tracker.py` - Fetch progress tracking
- `binance_fetcher.py` - Core fetching engine
- `utils.py` - Utility functions
- `__init__.py` - Package initializer
- `README.md` - This file

## Files

### config.py
**Description:** Central configuration for the module

**Categories:**
- **API**
  - api_key
  - api_secret
- **Cache**
  - cache_dir
  - cache_version
- **Rate_limiting**
  - max_requests_per_minute
  - safety_margin
- **Requests**
  - max_batch_size
  - retry_attempts
  - retry_delay
- **Validation**
  - max_volatility_threshold

### rate_limiter.py
**Description:** Prevents exceeding Binance API limits

**Responsibilities:**
- Tracks request timestamps
- Waits when rate limit is reached
- Used by all API calls

### data_validator.py
**Description:** Ensures OHLC and trade data integrity

**OHLC checks:**
- required_columns: ["timestamp","open","high","low","close","volume"]
- duplicates
- negative_values
- logical_consistency: "high >= low"
- extreme_volatility

**Trade checks:**
- required_columns: ["id","timestamp","price","quantity","is_buyer_maker"]
- positive_values
- trade_id_gaps

**Output:**
- valid: boolean
- issues: list
- metrics: dict

### cache_manager.py
**Description:** Parquet-based caching system

**Features:**
- save/load OHLC and trade data
- metadata: symbol, interval, time range, version
- cache validation
- cache-first fetching before API

### progress_tracker.py
**Description:** Tracks progress of data fetching

**Features:**
- separate candle & trade counters
- live percentages, elapsed time, ETA
- clean completion handling

### binance_fetcher.py
**Description:** Main fetching engine

**Behavior:**
- cache-first data fetching
- rate-limited API calls
- data validation
- bid/ask volume computation

**Key methods:**
- fetch_ohlc_batch
- fetch_trades_batch
- process_candle_trades
- fetch_complete_dataset

### utils.py
**Description:** Helper functions

**Functions:**
- retry: "Retries API calls with exponential backoff"
- generate_signature: "HMAC signature for Binance requests"
- compute_bid_ask_volumes: "Bid/ask volume calculation"

### __init__.py
**Description:** Simplifies module imports

**Imports:**
- Config
- BinanceDataFetcher

## Features
- Cache-first OHLC & trade fetching
- Validation with integrity checks
- Parquet-based caching & metadata
- Real-time progress tracking
- Automatic retries & rate-limiting
- Bid/ask volume computation


```mermaid
graph TD
    A[User / Script] --> B[BinanceDataFetcher]
    B --> C[Check OHLC Cache]
    C -->|Cache Hit| D[Load Data]
    C -->|Cache Miss| E[Fetch OHLC API]
    E --> F[Validate OHLC]
    F --> G[Save OHLC to Cache]
    G --> H[Process Each Candle Trades]
    H --> I[Check Trades Cache]
    I -->|Cache Hit| J[Load Data]
    I -->|Cache Miss| K[Fetch Trades API]
    K --> L[Validate Trades]
    L --> M[Compute Bid/Ask]
    M --> N[Save Trades Cache]
    N --> O[Merge OHLC + Trades]
    O --> P[Final Dataset]
