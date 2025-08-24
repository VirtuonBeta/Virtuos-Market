"""
Alert management for Module 1.

This module handles:
- Alert rule definition
- Alert notification
- Alert aggregation
- Alert suppression
"""

import time
import threading
import logging
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

from ..config import Config

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """An alert instance."""
    name: str
    severity: AlertSeverity
    message: str
    timestamp: datetime
    tags: Dict[str, str] = field(default_factory=dict)
    resolved: bool = False
    resolved_at: Optional[datetime] = None


@dataclass
class AlertRule:
    """An alert rule definition."""
    name: str
    condition: Callable[[Dict[str, Any]], bool]
    severity: AlertSeverity
    message_template: str
    tags: Dict[str, str] = field(default_factory=dict)
    cooldown: int = 300  # Cooldown period in seconds
    last_triggered: Optional[datetime] = None


class AlertManager:
    """
    Manages alert rules and notifications.
    
    Features:
    - Flexible alert rules
    - Multiple notification channels
    - Alert aggregation
    - Alert suppression
    """
    
    def __init__(self, config: Config):
        self.config = config
        
        # Alert storage
        self.rules: Dict[str, AlertRule] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        
        # Notification channels
        self.channels = {
            "log": self._log_notification,
            "console": self._console_notification
        }
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Initialize default rules
        self._init_default_rules()
        
        logger.info("Initialized AlertManager")
    
    def _init_default_rules(self):
        """Initialize default alert rules."""
        # High error rate rule
        self.add_rule(
            AlertRule(
                name="high_error_rate",
                condition=lambda m: m.get("errors", 0) / max(1, m.get("requests", 1)) > 0.1,
                severity=AlertSeverity.ERROR,
                message_template="High error rate: {error_rate:.2%}",
                tags={"component": "api"}
            )
        )
        
        # Rate limiting rule
        self.add_rule(
            AlertRule(
                name="rate_limit_exceeded",
                condition=lambda m: m.get("rate_limit", {}).get("requests_limited", 0) > 10,
                severity=AlertSeverity.WARNING,
                message_template="Rate limit exceeded: {requests_limited} requests limited",
                tags={"component": "rate_limiter"}
            )
        )
        
        # Cache miss rate rule
        self.add_rule(
            AlertRule(
                name="high_cache_miss_rate",
                condition=lambda m: m.get("cache", {}).get("misses", 0) / max(1, m.get("cache", {}).get("hits", 1)) > 0.5,
                severity=AlertSeverity.WARNING,
                message_template="High cache miss rate: {miss_rate:.2%}",
                tags={"component": "cache"}
            )
        )
        
        # Validation failures rule
        self.add_rule(
            AlertRule(
                name="validation_failures",
                condition=lambda m: m.get("validation", {}).get("errors", 0) > 5,
                severity=AlertSeverity.ERROR,
                message_template="Validation failures: {errors} errors",
                tags={"component": "validator"}
            )
        )
    
    def add_rule(self, rule: AlertRule):
        """Add an alert rule."""
        with self.lock:
            self.rules[rule.name] = rule
        logger.info(f"Added alert rule: {rule.name}")
    
    def remove_rule(self, name: str):
        """Remove an alert rule."""
        with self.lock:
            if name in self.rules:
                del self.rules[name]
                logger.info(f"Removed alert rule: {name}")
    
    def evaluate_rules(self, metrics: Dict[str, Any]):
        """Evaluate all alert rules against metrics."""
        if not self.config.monitoring.enable_alerts:
            return
        
        with self.lock:
            for rule_name, rule in self.rules.items():
                try:
                    # Check cooldown
                    if (rule.last_triggered and 
                        (datetime.now() - rule.last_triggered).total_seconds() < rule.cooldown):
                        continue
                    
                    # Evaluate condition
                    if rule.condition(metrics):
                        # Format message
                        message = rule.message_template.format(**metrics)
                        
                        # Create alert
                        alert = Alert(
                            name=rule_name,
                            severity=rule.severity,
                            message=message,
                            timestamp=datetime.now(),
                            tags=rule.tags.copy()
                        )
                        
                        # Check if already active
                        if rule_name not in self.active_alerts:
                            self.active_alerts[rule_name] = alert
                            self.alert_history.append(alert)
                            rule.last_triggered = datetime.now()
                            
                            # Send notification
                            self._send_notification(alert)
                            
                            logger.warning(f"Alert triggered: {rule_name} - {message}")
                
                except Exception as e:
                    logger.error(f"Error evaluating alert rule {rule_name}: {str(e)}")
    
    def resolve_alert(self, name: str):
        """Resolve an active alert."""
        with self.lock:
            if name in self.active_alerts:
                alert = self.active_alerts[name]
                alert.resolved = True
                alert.resolved_at = datetime.now()
                del self.active_alerts[name]
                
                logger.info(f"Alert resolved: {name}")
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts."""
        with self.lock:
            return list(self.active_alerts.values())
    
    def get_alert_history(self, since: Optional[datetime] = None) -> List[Alert]:
        """Get alert history."""
        if since is None:
            since = datetime.now() - timedelta(days=7)
        
        with self.lock:
            return [a for a in self.alert_history if a.timestamp >= since]
    
    def add_notification_channel(self, name: str, callback: Callable[[Alert], None]):
        """Add a notification channel."""
        self.channels[name] = callback
        logger.info(f"Added notification channel: {name}")
    
    def _send_notification(self, alert: Alert):
        """Send alert notification through configured channels."""
        for channel_name in self.config.monitoring.alert_channels:
            if channel_name in self.channels:
                try:
                    self.channels[channel_name](alert)
                except Exception as e:
                    logger.error(f"Error sending notification via {channel_name}: {str(e)}")
    
    def _log_notification(self, alert: Alert):
        """Log notification handler."""
        log_method = {
            AlertSeverity.INFO: logger.info,
            AlertSeverity.WARNING: logger.warning,
            AlertSeverity.ERROR: logger.error,
            AlertSeverity.CRITICAL: logger.critical
        }[alert.severity]
        
        log_method(f"ALERT [{alert.severity.value.upper()}] {alert.name}: {alert.message}")
    
    def _console_notification(self, alert: Alert):
        """Console notification handler."""
        print(f"\n{'='*50}")
        print(f"ALERT [{alert.severity.value.upper()}] {alert.name}")
        print(f"Time: {alert.timestamp}")
        print(f"Message: {alert.message}")
        if alert.tags:
            print(f"Tags: {alert.tags}")
        print(f"{'='*50}\n")
    
    def start_evaluation_timer(self, metrics_callback: Callable[[], Dict[str, Any]]):
        """Start the alert evaluation timer."""
        def evaluation_loop():
            while True:
                time.sleep(60)  # Evaluate every minute
                try:
                    metrics = metrics_callback()
                    self.evaluate_rules(metrics)
                except Exception as e:
                    logger.error(f"Error in alert evaluation: {str(e)}")
        
        thread = threading.Thread(target=evaluation_loop, daemon=True)
        thread.start()
        logger.info("Started alert evaluation timer")
