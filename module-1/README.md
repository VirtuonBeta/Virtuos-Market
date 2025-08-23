Config sub module
Overview

The Config module serves as the central configuration for the Binance data fetcher system.
It stores all constants and user-configurable parameters related to API access, caching, rate limiting, and request handling.
| Parameter                 | Type            | Default     | Description                                                        |
| ------------------------- | --------------- | ----------- | ------------------------------------------------------------------ |
| `api_key`                 | `str` or `None` | `None`      | Binance API key for authenticated requests.                        |
| `api_secret`              | `str` or `None` | `None`      | Binance API secret for signed requests.                            |
| `cache_dir`               | `str`           | `"./cache"` | Directory where fetched OHLC and trade data will be cached.        |
| `max_requests_per_minute` | `int`           | `1200`      | Maximum allowed API requests per minute.                           |
| `safety_margin`           | `float`         | `0.9`       | Fraction of the limit to safely avoid hitting Binance rate limits. |
| `max_batch_size`          | `int`           | `1000`      | Maximum number of items to fetch per API batch request.            |
| `retry_attempts`          | `int`           | `3`         | Number of times to retry a failed API request.                     |
| `retry_delay`             | `float`         | `1.0`       | Seconds to wait between retries.                                   |


RateLimiter Sub Module
Overview

The RateLimiter module ensures that API requests to Binance do not exceed allowed rate limits, preventing temporary bans or throttling.
It tracks requests over time and sleeps when approaching the limit. This module uses a rolling 1-minute window and respects a configurable safety margin.

Parameters & Behavior
| Attribute            | Type          | Description                                                                               |
| -------------------- | ------------- | ----------------------------------------------------------------------------------------- |
| `request_timestamps` | `List[float]` | Keeps track of the timestamps of recent requests.                                         |
| `max_requests`       | `int`         | Maximum allowed requests in a rolling window (`max_requests_per_minute * safety_margin`). |
| `time_window`        | `int`         | Duration of the rolling window in seconds (currently 60s). 

Rolling window: Only requests within the last time_window seconds are counted.

Safety margin: Prevents hitting exact Binance limits.

Automatic sleep: If the number of requests reaches max_requests, the module waits until requests expire from the window.


DataValidator Sub Module
Overview

The DataValidator module ensures that fetched OHLC and trade data are accurate, complete, and logically consistent.
It checks for missing columns, invalid timestamps, negative or zero values, logical inconsistencies, extreme volatility, and gaps in time or trade IDs.
This module is fully interval-agnostic for OHLC data, allowing validation of multiple candle intervals

Key Features
| Feature                  | Description                                                                                                                                                                                 |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **OHLC Validation**      | Checks for required columns (`timestamp`, `open`, `high`, `low`, `close`, `volume`). Detects invalid timestamps, duplicates, negative prices/volumes, `high < low`, and extreme volatility. |
| **Trade Validation**     | Checks for required columns (`timestamp`, `price`, `quantity`, `is_buyer_maker`). Detects non-positive prices/quantities and optional trade ID gaps.                                        |
| **Interval-Agnostic**    | Supports multiple OHLC intervals (`1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`). Completeness checks adjust dynamically.                                       |
| **Metrics & Issues**     | Returns structured dictionary: `valid` (bool), `issues` (list of human-readable issues), `metrics` (counts of duplicates, gaps, negative values, extreme volatility, etc.).                 |
| **Completeness Checks**  | Detects missing or irregular candle intervals for OHLC data and gaps in trade IDs (if present).                                                                                             |
| **Modular & Extensible** | Can be integrated before caching or processing. Additional checks and thresholds can be added easily.                                                                                       |


CacheManager Sub Module
Overview

The CacheManager module is responsible for efficiently storing and retrieving OHLC and trade data from disk while maintaining metadata integrity and reducing redundant API calls.
It supports Parquet-based caching, in-memory caching with LRU eviction, and UTC-aware timestamp handling, making it robust and fast for high-frequency data fetching.

Key Features
| Feature                       | Description                                                                                                                 |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| **OHLC Caching**              | Save and load OHLC data per symbol, interval, and date range with metadata verification.                                    |
| **Trade Caching**             | Save and load trade data per candle with metadata verification.                                                             |
| **Metadata Management**       | Stores symbol, interval, start/end times, record count, cache version, cached\_at timestamp. Ensures cached data integrity. |
| **In-memory LRU Cache**       | Keeps most recently accessed 10 datasets in memory to reduce disk reads. Evicts least recently used items automatically.    |
| **UTC-aware timestamps**      | Ensures all datetime objects are timezone-aware to prevent cross-region inconsistencies.                                    |
| **Error Handling & Logging**  | Logs errors during load/save, warns on version mismatches, and raises exceptions on save failures.                          |
| **Deterministic Cache Paths** | `_get_cache_path()` constructs file paths systematically for OHLC and trades.                                               |
| **Extensible**                | Can adjust memory cache size, implement retention policies, or add parallel-safe access in future.                          |

ProgressTracker Sub Module
Overview 

ProgressTracker Module provides real-time tracking of the progress of fetching and processing candles (OHLC) and trades. It supports console output by default and optional Streamlit integration

Key Features
| Feature | Description |
|---------|-------------|
| Candle progress tracking | Tracks the number of candles processed. |
| Trade progress tracking | Tracks the number of trades fetched. |
| ETA calculation | Estimates remaining time for both candles and trades using a weighted average. |
| Output modes | Console (default) or Streamlit progress bars and status messages. |
| Automatic fallback | Falls back to console if Streamlit is unavailable. |
| Update interval | Configurable display refresh frequency (default 1 second). |
| Finish display | Shows total elapsed time upon completion. |

Methods
| Method                                    | Description                                                   |
| ----------------------------------------- | ------------------------------------------------------------- |
| `update_candle_progress(increment=1)`     | Increment the number of processed candles and update display. |
| `update_trade_progress(increment=1)`      | Increment the number of fetched trades and update display.    |
| `set_totals(candles_total, trades_total)` | Set the total number of candles and trades to process.        |
| `_update_display()`                       | Internal method to refresh console or Streamlit display.      |
| `finish()`                                | Finalize display and show total elapsed time.                 |


BinaceFetcher Sub module
Overview

Fetches OHLC and trade data from Binance, with caching, validation, and progress tracking.

Key Features
| Feature                       | Description                                                                                                           |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| **OHLC Fetching**             | Fetches candlestick data via Binance `klines` API and converts it to a pandas DataFrame with proper numeric types.    |
| **Trade Fetching**            | Fetches aggregate trades via Binance `aggTrades` API with pagination and converts to DataFrame.                       |
| **Batch Processing**          | Handles fetching in batches based on `max_batch_size` until all requested data is retrieved.                          |
| **Candle-Trade Processing**   | Processes trades per candle, merges with OHLC data, computes bid/ask volumes, and caches results.                     |
| **Caching**                   | Uses `CacheManager` to store and load OHLC and trade data to reduce API calls.                                        |
| **Validation**                | Validates OHLC and trade data using `DataValidator`. `strict_validation` option can raise errors if validation fails. |
| **Progress Tracking**         | Tracks progress of candles and trades using `ProgressTracker`, displaying ETA and completion status.                  |
| **Retry & Rate Limiting**     | Retries requests up to 3 times and uses `RateLimiter` to avoid API throttling.                                        |
| **Signed Requests**           | Supports API key/secret for signed endpoints if provided.                                                             |
| **Complete Dataset Fetching** | Provides `fetch_complete_dataset()` to fetch OHLC and trades for a symbol, interval, and date range.                  |
| **Interval Handling**         | Maps Binance interval strings to minutes for accurate candle processing.                                              |
| **Error Handling**            | Optionally raises exceptions on validation errors via `strict_validation`.                                            |


Utils Sub Module
Overview

utils.py contains utility functions that support the Binance data fetching pipeline. It provides robust, reusable helpers for retries, request signing, and trade volume computations.

key feutures
|Feature                       | Description                                                                                                                                                                                                                                     |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **retry decorator**               | Provides automatic retrying of function calls with configurable maximum attempts, delay, and exceptions. Supports exponential backoff and optional mockable sleep for testing. Logs retries and final errors.                                   |
| **generate\_signature**           | Generates HMAC-SHA256 signature for Binance API requests. Validates inputs, sorts parameters before signing, and returns a hexadecimal signature string.                                                                                        |
| **compute\_bid\_ask\_volumes**    | Computes bid and ask volumes from trade data in a DataFrame. Uses the `is_buyer_maker` flag to determine trade direction. Columns are initialized and added safely to avoid Pandas warnings. Supports custom column names and input validation. |
| **Input validation**              | Functions check for proper types and required columns, raising exceptions if invalid.                                                                                                                                                           |
| **Logging**                       | Uses `logging` to report retry attempts, wait times, and final errors.                                                                                                                                                                          |
| **Type annotations & docstrings** | All functions have clear type hints and detailed docstrings including arguments, return values, examples, and exceptions.                                                                                                                       |
| **Vectorized implementation**     | All DataFrame operations are vectorized for efficiency and scalability.                                                                                                                                                                         |
| **Test-friendly features**        | `retry` accepts an optional `sleep_func` to allow mocking delays during unit testing.                                                                                                                                                           |
