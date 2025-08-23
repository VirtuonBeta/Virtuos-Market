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


