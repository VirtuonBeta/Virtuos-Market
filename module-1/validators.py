"""
Data integrity validation for Module 1.

This module handles:
- Data quality checks
- Schema validation
- Business rule validation
- Data correction
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass

from .config import Config
from .monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of data validation."""
    valid: bool
    issues: List[str]
    warnings: List[str]
    errors: List[str]
    metrics: Dict[str, Any]
    score: float  # 0.0 to 1.0


class DataValidator:
    """
    Validates data integrity and quality.
    
    Features:
    - Schema validation
    - Data quality checks
    - Business rule validation
    - Automatic data correction
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.metrics = MetricsCollector(config)
        
        # Validation thresholds
        self.max_volatility_threshold = 0.5  # 50% price change
        self.max_missing_percentage = 0.05  # 5% missing data
        self.max_duplicate_percentage = 0.01  # 1% duplicate data
        
        # Required columns
        self.ohlc_required_columns = [
            'timestamp', 'open', 'high', 'low', 'close', 'volume'
        ]
        self.trades_required_columns = [
            'timestamp', 'price', 'quantity', 'is_buyer_maker'
        ]
        
        logger.info("Initialized DataValidator")
    
    def validate_ohlc(self, df: pd.DataFrame, interval: str = '1m') -> ValidationResult:
        """
        Validate OHLC data.
        
        Args:
            df: DataFrame with OHLC data
            interval: Time interval for candles
            
        Returns:
            ValidationResult with validation results
        """
        start_time = datetime.now()
        result = ValidationResult(
            valid=True,
            issues=[],
            warnings=[],
            errors=[],
            metrics={},
            score=1.0
        )
        
        if df.empty:
            result.valid = False
            result.errors.append("DataFrame is empty")
            self._record_validation(result, start_time)
            return result
        
        # Check required columns
        missing_cols = [
            col for col in self.ohlc_required_columns 
            if col not in df.columns
        ]
        if missing_cols:
            result.valid = False
            result.errors.append(f"Missing required columns: {missing_cols}")
        
        # Check for missing values
        for col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                missing_pct = missing_count / len(df) * 100
                if missing_pct > self.max_missing_percentage * 100:
                    result.errors.append(
                        f"Too many missing values in {col}: {missing_pct:.2f}%"
                    )
                    result.valid = False
                else:
                    result.warnings.append(
                        f"Missing values in {col}: {missing_pct:.2f}%"
                    )
                result.metrics[f"missing_{col}"] = missing_count
        
        # Check for negative values
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    result.errors.append(f"Negative values in {col}: {negative_count}")
                    result.valid = False
                    result.metrics[f"negative_{col}"] = negative_count
        
        # Check for logical inconsistencies
        if all(col in df.columns for col in ['high', 'low']):
            invalid_hl = df['high'] < df['low']
            if invalid_hl.any():
                result.errors.append(f"High < Low in {invalid_hl.sum()} rows")
                result.valid = False
                result.metrics["invalid_high_low"] = invalid_hl.sum()
        
        # Check for extreme volatility
        if all(col in df.columns for col in ['open', 'close']):
            volatility = (df['close'] - df['open']) / df['open']
            extreme_volatility = volatility.abs() > self.max_volatility_threshold
            if extreme_volatility.any():
                result.warnings.append(
                    f"Extreme volatility in {extreme_volatility.sum()} candles"
                )
                result.metrics["extreme_volatility"] = extreme_volatility.sum()
        
        # Check for time gaps
        if 'timestamp' in df.columns and len(df) > 1:
            df_sorted = df.sort_values('timestamp')
            time_diffs = df_sorted['timestamp'].diff().dropna()
            
            # Convert to seconds
            interval_seconds = {
                '1m': 60, '5m': 300, '15m': 900, '1h': 3600, '1d': 86400
            }.get(interval, 60)
            
            gaps = time_diffs[time_diffs > pd.Timedelta(seconds=interval_seconds * 1.5)]
            if len(gaps) > 0:
                result.warnings.append(f"Time gaps detected: {len(gaps)} intervals")
                result.metrics["time_gaps"] = len(gaps)
        
        # Calculate score
        result.score = self._calculate_score(result)
        
        # Record validation
        self._record_validation(result, start_time)
        
        return result
    
    def validate_trades(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate trades data.
        
        Args:
            df: DataFrame with trades data
            
        Returns:
            ValidationResult with validation results
        """
        start_time = datetime.now()
        result = ValidationResult(
            valid=True,
            issues=[],
            warnings=[],
            errors=[],
            metrics={},
            score=1.0
        )
        
        if df.empty:
            result.valid = False
            result.errors.append("DataFrame is empty")
            self._record_validation(result, start_time)
            return result
        
        # Check required columns
        missing_cols = [
            col for col in self.trades_required_columns 
            if col not in df.columns
        ]
        if missing_cols:
            result.valid = False
            result.errors.append(f"Missing required columns: {missing_cols}")
        
        # Check for missing values
        for col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                missing_pct = missing_count / len(df) * 100
                if missing_pct > self.max_missing_percentage * 100:
                    result.errors.append(
                        f"Too many missing values in {col}: {missing_pct:.2f}%"
                    )
                    result.valid = False
                else:
                    result.warnings.append(
                        f"Missing values in {col}: {missing_pct:.2f}%"
                    )
                result.metrics[f"missing_{col}"] = missing_count
        
        # Check for non-positive values
        if 'price' in df.columns:
            non_positive = df['price'] <= 0
            if non_positive.any():
                result.errors.append(
                    f"Non-positive prices: {non_positive.sum()} rows"
                )
                result.valid = False
                result.metrics["non_positive_prices"] = non_positive.sum()
        
        if 'quantity' in df.columns:
            non_positive = df['quantity'] <= 0
            if non_positive.any():
                result.errors.append(
                    f"Non-positive quantities: {non_positive.sum()} rows"
                )
                result.valid = False
                result.metrics["non_positive_quantities"] = non_positive.sum()
        
        # Check for duplicates
        if 'timestamp' in df.columns and 'price' in df.columns:
            duplicates = df.duplicated(subset=['timestamp', 'price'])
            duplicate_count = duplicates.sum()
            if duplicate_count > 0:
                duplicate_pct = duplicate_count / len(df) * 100
                if duplicate_pct > self.max_duplicate_percentage * 100:
                    result.errors.append(
                        f"Too many duplicates: {duplicate_pct:.2f}%"
                    )
                    result.valid = False
                else:
                    result.warnings.append(
                        f"Duplicates found: {duplicate_pct:.2f}%"
                    )
                result.metrics["duplicates"] = duplicate_count
        
        # Calculate score
        result.score = self._calculate_score(result)
        
        # Record validation
        self._record_validation(result, start_time)
        
        return result
    
    def fix_common_issues(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """
        Fix common data quality issues.
        
        Args:
            df: DataFrame to fix
            data_type: Type of data ('ohlc' or 'trades')
            
        Returns:
            Fixed DataFrame
        """
        df = df.copy()
        
        if data_type == 'ohlc':
            # Fix high < low
            if all(col in df.columns for col in ['high', 'low']):
                invalid_hl = df['high'] < df['low']
                if invalid_hl.any():
                    logger.info(f"Fixing {invalid_hl.sum()} rows where high < low")
                    df.loc[invalid_hl, ['high', 'low']] = df.loc[invalid_hl, ['low', 'high']]
            
            # Fix negative values
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if col in df.columns:
                    negative = df[col] < 0
                    if negative.any():
                        logger.info(f"Fixing {negative.sum()} negative values in {col}")
                        df.loc[negative, col] = df.loc[negative, col].abs()
        
        elif data_type == 'trades':
            # Fix non-positive values
            for col in ['price', 'quantity']:
                if col in df.columns:
                    non_positive = df[col] <= 0
                    if non_positive.any():
                        logger.info(f"Fixing {non_positive.sum()} non-positive values in {col}")
                        df.loc[non_positive, col] = df.loc[non_positive, col].abs()
        
        return df
    
    def _calculate_score(self, result: ValidationResult) -> float:
        """Calculate data quality score (0.0 to 1.0)."""
        score = 1.0
        
        # Deduct for errors
        if result.errors:
            score -= min(0.5, len(result.errors) * 0.1)
        
        # Deduct for warnings
        if result.warnings:
            score -= min(0.3, len(result.warnings) * 0.05)
        
        return max(0.0, score)
    
    def _record_validation(self, result: ValidationResult, start_time: datetime):
        """Record validation metrics."""
        validation_time = (datetime.now() - start_time).total_seconds()
        
        self.metrics.record_validation(
            data_type="ohlc" if "open" in result.metrics else "trades",
            valid=result.valid,
            score=result.score,
            validation_time=validation_time,
            issues=len(result.issues),
            errors=len(result.errors),
            warnings=len(result.warnings)
        )
