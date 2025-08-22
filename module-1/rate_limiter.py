# rate_limiter.py
import time
from typing import List
from config import Config

class RateLimiter:
    def __init__(self, config: Config):
        self.config = config
        self.request_timestamps: List[float] = []
        self.max_requests = int(config.max_requests_per_minute * config.safety_margin)
        self.time_window = 60  # 1 minute in seconds
    
    def wait_if_needed(self):
        """Check if we're approaching rate limit and wait if necessary"""
        now = time.time()
        # Remove timestamps older than our time window
        self.request_timestamps = [t for t in self.request_timestamps if now - t < self.time_window]
        
        if len(self.request_timestamps) >= self.max_requests:
            # Calculate how long to wait until the oldest request is outside the window
            sleep_time = self.time_window - (now - self.request_timestamps[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                # After waiting, clear old timestamps
                self.request_timestamps = []
        
        # Record this request
        self.request_timestamps.append(now)
