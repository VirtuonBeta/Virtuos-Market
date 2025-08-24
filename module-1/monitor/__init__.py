"""
Monitoring subpackage for Module 1.

This package handles:
- Performance metrics collection
- Alert management
- Dashboard visualization
"""

from .metrics import MetricsCollector
from .alerts import AlertManager
from .dashboard import Dashboard

__all__ = ["MetricsCollector", "AlertManager", "Dashboard"]
