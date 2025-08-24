"""
Performance metrics collection for Module 1.

This module handles:
- Metrics collection and aggregation
- Performance tracking
- Metrics export
"""

import time
import threading
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict, deque

from ..config import Config

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """A single metric data point."""
    timestamp: datetime
    value: float
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricSummary:
    """Summary statistics for a metric."""
    count: int = 0
    sum: float = 0.0
    min: float = float('inf')
    max: float = float('-inf')
    avg: float = 0.0
    
    def update(self, value: float):
        """Update summary with new value."""
        self.count += 1
        self.sum += value
        self.min = min(self.min, value)
        self.max = max(self.max, value)
        self.avg = self.sum / self.count


class MetricsCollector:
    """
    Collects and aggregates performance metrics.
    
    Features:
    - Multiple metric types
    - Time-based aggregation
    - Performance tracking
    - Metrics export
    """
    
    def __init__(self, config: Config):
        self.config = config
        
        # Metric storage
        self.counters = defaultdict(int)
        self.gauges = defaultdict(float)
        self.histograms = defaultdict(lambda: defaultdict(int))
        self.timings = defaultdict(list)
        self.recent_points = defaultdict(lambda: deque(maxlen=1000))
        
        # Aggregation
        self.summaries = defaultdict(MetricSummary)
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Export timer
        self.last_export = time.time()
        self.export_interval = config.monitoring.metrics_interval
        
        logger.info("Initialized MetricsCollector")
    
    def increment(self, name: str, value: int = 1, tags: Optional[Dict[str, str]] = None):
        """
        Increment a counter metric.
        
        Args:
            name: Metric name
            value: Value to increment by
            tags: Optional tags for the metric
        """
        with self.lock:
            self.counters[name] += value
            
            # Record point
            self._record_point(name, value, tags)
    
    def set_gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """
        Set a gauge metric.
        
        Args:
            name: Metric name
            value: Value to set
            tags: Optional tags for the metric
        """
        with self.lock:
            self.gauges[name] = value
            
            # Record point
            self._record_point(name, value, tags)
    
    def record_timing(self, name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """
        Record a timing metric.
        
        Args:
            name: Metric name
            value: Timing value in seconds
            tags: Optional tags for the metric
        """
        with self.lock:
            self.timings[name].append(value)
            
            # Update histogram
            bucket = self._get_histogram_bucket(value)
            self.histograms[name][bucket] += 1
            
            # Update summary
            self.summaries[name].update(value)
            
            # Record point
            self._record_point(name, value, tags)
    
    def record_api_call(self, endpoint: str, duration: float, response_size: int):
        """Record API call metrics."""
        self.increment("api_calls_total", tags={"endpoint": endpoint})
        self.record_timing("api_duration_seconds", duration, tags={"endpoint": endpoint})
        self.increment("api_response_size_bytes", response_size, tags={"endpoint": endpoint})
    
    def record_cache_hit(self, tags: Optional[Dict[str, str]] = None):
        """Record cache hit."""
        self.increment("cache_hits", tags=tags)
    
    def record_cache_miss(self, tags: Optional[Dict[str, str]] = None):
        """Record cache miss."""
        self.increment("cache_misses", tags=tags)
    
    def record_error(self, component: str, error_type: str, tags: Optional[Dict[str, str]] = None):
        """Record error metric."""
        self.increment(
            "errors_total", 
            tags={"component": component, "error_type": error_type, **(tags or {})}
        )
    
    def record_validation(
        self, 
        data_type: str, 
        valid: bool, 
        score: float, 
        validation_time: float,
        issues: int,
        errors: int,
        warnings: int
    ):
        """Record validation metrics."""
        tags = {"data_type": data_type, "valid": str(valid)}
        
        self.increment("validation_total", tags=tags)
        self.set_gauge("validation_score", score, tags=tags)
        self.record_timing("validation_duration_seconds", validation_time, tags=tags)
        self.increment("validation_issues", issues, tags=tags)
        self.increment("validation_errors", errors, tags=tags)
        self.increment("validation_warnings", warnings, tags=tags)
    
    def _record_point(self, name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Record a metric data point."""
        point = MetricPoint(
            timestamp=datetime.now(),
            value=value,
            tags=tags or {}
        )
        self.recent_points[name].append(point)
    
    def _get_histogram_bucket(self, value: float) -> str:
        """Get histogram bucket for a value."""
        if value < 0.01:
            return "0.01"
        elif value < 0.1:
            return "0.1"
        elif value < 1:
            return "1"
        elif value < 10:
            return "10"
        else:
            return "inf"
    
    def get_counter(self, name: str) -> int:
        """Get counter value."""
        with self.lock:
            return self.counters.get(name, 0)
    
    def get_gauge(self, name: str) -> float:
        """Get gauge value."""
        with self.lock:
            return self.gauges.get(name, 0.0)
    
    def get_timing_stats(self, name: str) -> Dict[str, float]:
        """Get timing statistics."""
        with self.lock:
            timings = self.timings.get(name, [])
            if not timings:
                return {}
            
            return {
                "count": len(timings),
                "min": min(timings),
                "max": max(timings),
                "avg": sum(timings) / len(timings),
                "p50": self._percentile(timings, 0.5),
                "p95": self._percentile(timings, 0.95),
                "p99": self._percentile(timings, 0.99)
            }
    
    def _percentile(self, values: List[float], p: float) -> float:
        """Calculate percentile of values."""
        if not values:
            return 0.0
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        k = (n - 1) * p
        f = int(k)
        c = k - f
        
        if f + 1 < n:
            return sorted_values[f] + c * (sorted_values[f + 1] - sorted_values[f])
        else:
            return sorted_values[f]
    
    def get_recent_metrics(self, name: str, since: Optional[datetime] = None) -> List[MetricPoint]:
        """Get recent metric points."""
        if since is None:
            since = datetime.now() - timedelta(hours=1)
        
        with self.lock:
            points = self.recent_points.get(name, [])
            return [p for p in points if p.timestamp >= since]
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all current metrics."""
        with self.lock:
            return {
                "counters": dict(self.counters),
                "gauges": dict(self.gauges),
                "timings": {
                    name: self.get_timing_stats(name)
                    for name in self.timings
                },
                "summaries": {
                    name: {
                        "count": summary.count,
                        "sum": summary.sum,
                        "min": summary.min,
                        "max": summary.max,
                        "avg": summary.avg
                    }
                    for name, summary in self.summaries.items()
                }
            }
    
    def reset(self):
        """Reset all metrics."""
        with self.lock:
            self.counters.clear()
            self.gauges.clear()
            self.histograms.clear()
            self.timings.clear()
            self.recent_points.clear()
            self.summaries.clear()
        
        logger.info("Reset all metrics")
    
    def start_export_timer(self):
        """Start the export timer."""
        def export_loop():
            while True:
                time.sleep(self.export_interval)
                self._export_metrics()
        
        thread = threading.Thread(target=export_loop, daemon=True)
        thread.start()
        logger.info("Started metrics export timer")
    
    def _export_metrics(self):
        """Export metrics to external systems."""
        # This would integrate with Prometheus, StatsD, etc.
        # For now, just log the metrics
        metrics = self.get_all_metrics()
        logger.debug(f"Exporting metrics: {metrics}")
