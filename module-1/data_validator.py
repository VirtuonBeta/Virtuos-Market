# data_validator.py
import pandas as pd
from typing import Dict, Any, Optional
from config import Config

class DataValidator:
    def __init__(self, config: Config):
        self.config = config
        # Set default volatility threshold (moved from Config as noted in previous review)
        self.max_volatility_threshold = 0.5  # 50% price change in one candle
    
    def validate_ohlc(self, df: pd.DataFrame, candle_interval: str = '1m') -> Dict[str, Any]:
        """Validate OHLC data and return metrics
        
        Args:
            df: DataFrame containing OHLC data
            candle_interval: Time interval for candles (e.g., '1m', '5m', '1h')
        
        Returns:
            Dictionary with validation results, issues, and metrics
        """
        issues = []
        metrics = {}
        
        # Check required columns
        required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
        
        # Check for invalid timestamps
        if 'timestamp' in df.columns:
            if df['timestamp'].isna().any():
                issues.append("Missing timestamps found")
            
            # Check for duplicates
            duplicates = df['timestamp'].duplicated()
            if duplicates.any():
                metrics['duplicate_timestamps'] = duplicates.sum()
                issues.append(f"Duplicate timestamps found: {duplicates.sum()} rows")
        
        # Check for negative prices or volumes (optimized to check all at once)
        price_cols = [col for col in ['open', 'high', 'low', 'close'] if col in df.columns]
        if price_cols:
            negative_prices = df[price_cols] < 0
            for col in price_cols:
                if negative_prices[col].any():
                    metrics[f'negative_{col}_count'] = negative_prices[col].sum()
                    issues.append(f"Negative values in {col} column")
        
        if 'volume' in df.columns:
            negative_volumes = df['volume'] < 0
            if negative_volumes.any():
                metrics['negative_volume_count'] = negative_volumes.sum()
                issues.append("Negative values in volume column")
        
        # Check for logical inconsistencies (high < low, etc.)
        if all(col in df.columns for col in ['high', 'low']):
            invalid_hl = df['high'] < df['low']
            if invalid_hl.any():
                metrics['invalid_high_low_count'] = invalid_hl.sum()
                issues.append(f"High < Low in {invalid_hl.sum()} rows")
        
        # Check for extreme volatility
        if all(col in df.columns for col in ['open', 'close']):
            with pd.option_context('mode.use_inf_as_na', True):
                volatility = (df['close'] - df['open']) / df['open'].replace(0, pd.NA)
                extreme_volatility = volatility.abs() > self.max_volatility_threshold
                if extreme_volatility.any():
                    metrics['extreme_volatility_count'] = extreme_volatility.sum()
                    issues.append(f"Extreme volatility detected in {extreme_volatility.sum()} candles")
        
        # Calculate completeness metrics
        if 'timestamp' in df.columns and len(df) > 1:
            df_sorted = df.sort_values('timestamp')
            time_diffs = df_sorted['timestamp'].diff().dropna()
            
            # Convert candle interval to seconds for comparison
            interval_map = {
                '1m': 60, '3m': 180, '5m': 300, '15m': 900,
                '30m': 1800, '1h': 3600, '2h': 7200, '4h': 14400,
                '6h': 21600, '8h': 28800, '12h': 43200, '1d': 86400
            }
            expected_diff = interval_map.get(candle_interval, 60)  # Default to 1 minute
            
            if not time_diffs.empty:
                # Convert Timedelta objects to seconds before comparing
                time_diffs_seconds = time_diffs.dt.total_seconds()
                gaps = time_diffs_seconds[time_diffs_seconds > expected_diff * 1.5]  # Allow some tolerance
                metrics['time_gaps'] = len(gaps)
                if len(gaps) > 0:
                    issues.append(f"Time gaps detected: {len(gaps)} intervals")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues,
            'metrics': metrics
        }
    
    def validate_trades(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate trade data and return metrics
        
        Args:
            df: DataFrame containing trade data
        
        Returns:
            Dictionary with validation results, issues, and metrics
        
        Note:
            If 'id' column is present, will check for trade ID gaps.
            This column is optional but recommended for completeness.
        """
        issues = []
        metrics = {}
        
        # Check required columns
        required_cols = ['timestamp', 'price', 'quantity', 'is_buyer_maker']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
        
        # Check for invalid values
        if 'price' in df.columns:
            non_positive_prices = df['price'] <= 0
            if non_positive_prices.any():
                metrics['non_positive_price_count'] = non_positive_prices.sum()
                issues.append("Non-positive prices found")
        
        if 'quantity' in df.columns:
            non_positive_quantities = df['quantity'] <= 0
            if non_positive_quantities.any():
                metrics['non_positive_quantity_count'] = non_positive_quantities.sum()
                issues.append("Non-positive quantities found")
        
        # Check for trade ID gaps if available (optional column)
        if 'id' in df.columns and len(df) > 1:
            df_sorted = df.sort_values('id')
            id_diffs = df_sorted['id'].diff().dropna()
            gaps = id_diffs[id_diffs > 1]
            metrics['id_gaps'] = len(gaps)
            if len(gaps) > 0:
                issues.append(f"Trade ID gaps detected: {len(gaps)} gaps")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues,
            'metrics': metrics
        }
