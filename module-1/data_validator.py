# data_validator.py
import pandas as pd
from typing import Dict, Any
from config import Config

class DataValidator:
    def __init__(self, config: Config):
        self.config = config
    
    def validate_ohlc(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate OHLC data and return metrics"""
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
            if df['timestamp'].duplicated().any():
                issues.append("Duplicate timestamps found")
        
        # Check for negative prices or volumes
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in df.columns and (df[col] < 0).any():
                issues.append(f"Negative values in {col} column")
        
        if 'volume' in df.columns and (df['volume'] < 0).any():
            issues.append("Negative values in volume column")
        
        # Check for logical inconsistencies (high < low, etc.)
        if all(col in df.columns for col in ['high', 'low']):
            invalid_hl = df['high'] < df['low']
            if invalid_hl.any():
                issues.append(f"High < Low in {invalid_hl.sum()} rows")
        
        # Check for extreme volatility
        if all(col in df.columns for col in ['open', 'close']):
            volatility = (df['close'] - df['open']) / df['open']
            extreme_volatility = volatility.abs() > self.config.max_volatility_threshold
            if extreme_volatility.any():
                metrics['extreme_volatility_count'] = extreme_volatility.sum()
                issues.append(f"Extreme volatility detected in {extreme_volatility.sum()} candles")
        
        # Calculate completeness metrics
        if 'timestamp' in df.columns:
            df_sorted = df.sort_values('timestamp')
            time_diffs = df_sorted['timestamp'].diff().dropna()
            expected_diff = pd.Timedelta(minutes=1).total_seconds()  # Assuming 1m candles
            
            if not time_diffs.empty:
                gaps = time_diffs[time_diffs > expected_diff * 1.5]  # Allow some tolerance
                metrics['time_gaps'] = len(gaps)
                if len(gaps) > 0:
                    issues.append(f"Time gaps detected: {len(gaps)} intervals")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues,
            'metrics': metrics
        }
    
    def validate_trades(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate trade data and return metrics"""
        issues = []
        metrics = {}
        
        # Check required columns
        required_cols = ['timestamp', 'price', 'quantity', 'is_buyer_maker']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
        
        # Check for invalid values
        if 'price' in df.columns:
            if (df['price'] <= 0).any():
                issues.append("Non-positive prices found")
        
        if 'quantity' in df.columns:
            if (df['quantity'] <= 0).any():
                issues.append("Non-positive quantities found")
        
        # Check for trade ID gaps if available
        if 'id' in df.columns:
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
