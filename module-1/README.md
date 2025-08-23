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
