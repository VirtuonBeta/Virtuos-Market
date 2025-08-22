# progress_tracker.py
import time
from typing import Optional
from datetime import datetime, timedelta
from config import Config

class ProgressTracker:
    def __init__(self, config: Config):
        self.config = config
        self.candles_total = 0
        self.candles_processed = 0
        self.trades_total = 0
        self.trades_fetched = 0
        self.start_time = time.time()
        self.last_update_time = time.time()
        self.update_interval = 1.0  # Update display every second
    
    def update_candle_progress(self, increment: int = 1):
        """Update the number of candles processed"""
        self.candles_processed += increment
        self._update_display()
    
    def update_trade_progress(self, increment: int = 1):
        """Update the number of trades fetched"""
        self.trades_fetched += increment
        self._update_display()
    
    def set_totals(self, candles_total: int, trades_total: int):
        """Set the total number of candles and trades to process"""
        self.candles_total = candles_total
        self.trades_total = trades_total
        self._update_display()
    
    def _update_display(self):
        """Update progress display if enough time has passed"""
        current_time = time.time()
        if current_time - self.last_update_time < self.update_interval:
            return
        
        self.last_update_time = current_time
        elapsed = current_time - self.start_time
        
        # Calculate progress percentages
        candle_pct = (self.candles_processed / max(1, self.candles_total)) * 100
        trade_pct = (self.trades_fetched / max(1, self.trades_total)) * 100
        
        # Calculate ETAs
        if self.candles_processed > 0:
            candle_eta = (elapsed / self.candles_processed) * (self.candles_total - self.candles_processed)
        else:
            candle_eta = 0
        
        if self.trades_fetched > 0:
            trade_eta = (elapsed / self.trades_fetched) * (self.trades_total - self.trades_fetched)
        else:
            trade_eta = 0
        
        # Format the progress message
        msg = (
            f"Progress: Candles {self.candles_processed}/{self.candles_total} ({candle_pct:.1f}%) | "
            f"Trades {self.trades_fetched}/{self.trades_total} ({trade_pct:.1f}%) | "
            f"Elapsed: {timedelta(seconds=int(elapsed))} | "
            f"Candle ETA: {timedelta(seconds=int(candle_eta))} | "
            f"Trade ETA: {timedelta(seconds=int(trade_eta))}"
        )
        
        print(f"\r{msg}", end="", flush=True)
    
    def finish(self):
        """Finalize progress display"""
        print()  # Move to next line after progress display
        elapsed = time.time() - self.start_time
        print(f"Completed in {timedelta(seconds=int(elapsed))}")
