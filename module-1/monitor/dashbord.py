"""
Real-time dashboard for Module 1 monitoring.

This module handles:
- Web-based dashboard
- Real-time metrics visualization
- Alert management
- System status display
"""

import time
import threading
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

try:
    from flask import Flask, render_template, jsonify, request
    from flask_socketio import SocketIO
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

from ..config import Config
from .metrics import MetricsCollector
from .alerts import AlertManager, Alert

logger = logging.getLogger(__name__)


@dataclass
class SystemStatus:
    """System status information."""
    healthy: bool = True
    uptime: float = 0.0
    last_activity: Optional[datetime] = None
    components: Dict[str, bool] = field(default_factory=dict)


class Dashboard:
    """
    Real-time monitoring dashboard.
    
    Features:
    - Web-based interface
    - Real-time metrics
    - Alert management
    - System status
    """
    
    def __init__(self, config: Config, metrics: MetricsCollector, alerts: AlertManager):
        self.config = config
        self.metrics = metrics
        self.alerts = alerts
        
        # System status
        self.status = SystemStatus()
        self.start_time = time.time()
        
        # Initialize Flask app if available
        self.app = None
        self.socketio = None
        self.thread = None
        
        if FLASK_AVAILABLE:
            self._init_flask()
        else:
            logger.warning("Flask not available, dashboard disabled")
    
    def _init_flask(self):
        """Initialize Flask application."""
        self.app = Flask(__name__)
        self.app.config['SECRET_KEY'] = 'module1-dashboard-secret'
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")
        
        # Register routes
        self._register_routes()
        
        # Register socket events
        self._register_socket_events()
        
        logger.info("Initialized Flask dashboard")
    
    def _register_routes(self):
        """Register Flask routes."""
        
        @self.app.route('/')
        def index():
            """Dashboard home page."""
            return self._render_dashboard()
        
        @self.app.route('/api/metrics')
        def get_metrics():
            """Get current metrics."""
            return jsonify(self.metrics.get_all_metrics())
        
        @self.app.route('/api/alerts')
        def get_alerts():
            """Get active alerts."""
            active_alerts = self.alerts.get_active_alerts()
            return jsonify([{
                'name': alert.name,
                'severity': alert.severity.value,
                'message': alert.message,
                'timestamp': alert.timestamp.isoformat(),
                'tags': alert.tags
            } for alert in active_alerts])
        
        @self.app.route('/api/alerts/<name>/resolve', methods=['POST'])
        def resolve_alert(name):
            """Resolve an alert."""
            self.alerts.resolve_alert(name)
            return jsonify({'status': 'resolved'})
        
        @self.app.route('/api/status')
        def get_status():
            """Get system status."""
            return jsonify({
                'healthy': self.status.healthy,
                'uptime': time.time() - self.start_time,
                'last_activity': self.status.last_activity.isoformat() if self.status.last_activity else None,
                'components': self.status.components
            })
        
        @self.app.route('/api/health')
        def health_check():
            """Health check endpoint."""
            return jsonify({'status': 'healthy'})
    
    def _register_socket_events(self):
        """Register Socket.IO events."""
        
        @self.socketio.on('connect')
        def handle_connect():
            """Handle client connection."""
            logger.debug("Dashboard client connected")
            self._send_initial_data()
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            """Handle client disconnection."""
            logger.debug("Dashboard client disconnected")
    
    def _render_dashboard(self):
        """Render dashboard HTML."""
        # Simple HTML template for the dashboard
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Module 1 Dashboard</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <script src="https://cdn.socket.io/4.0.0/socket.io.min.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            <style>
                body { font-family: Arial, sans-serif; margin: 0; padding: 20px; }
                .header { text-align: center; margin-bottom: 30px; }
                .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
                .metric-card { background: #f5f5f5; padding: 20px; border-radius: 8px; }
                .metric-value { font-size: 24px; font-weight: bold; }
                .alerts { margin-top: 30px; }
                .alert { padding: 10px; margin: 10px 0; border-radius: 4px; }
                .alert-error { background: #ffebee; border-left: 4px solid #f44336; }
                .alert-warning { background: #fff3e0; border-left: 4px solid #ff9800; }
                .status { position: fixed; top: 10px; right: 10px; }
                .status-healthy { color: green; }
                .status-unhealthy { color: red; }
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Module 1 Data Ingestion Dashboard</h1>
                <div class="status" id="status">Status: <span id="status-text">Checking...</span></div>
            </div>
            
            <div class="metrics" id="metrics">
                <!-- Metrics will be populated here -->
            </div>
            
            <div class="alerts">
                <h2>Active Alerts</h2>
                <div id="alerts">
                    <!-- Alerts will be populated here -->
                </div>
            </div>
            
            <script>
                const socket = io();
                
                // Handle initial data
                socket.on('initial_data', (data) => {
                    updateMetrics(data.metrics);
                    updateAlerts(data.alerts);
                    updateStatus(data.status);
                });
                
                // Handle metrics updates
                socket.on('metrics_update', (data) => {
                    updateMetrics(data);
                });
                
                // Handle alert updates
                socket.on('alerts_update', (data) => {
                    updateAlerts(data);
                });
                
                // Handle status updates
                socket.on('status_update', (data) => {
                    updateStatus(data);
                });
                
                function updateMetrics(metrics) {
                    const container = document.getElementById('metrics');
                    container.innerHTML = '';
                    
                    // Display counters
                    if (metrics.counters) {
                        for (const [name, value] of Object.entries(metrics.counters)) {
                            addMetricCard(name, value, 'counter');
                        }
                    }
                    
                    // Display gauges
                    if (metrics.gauges) {
                        for (const [name, value] of Object.entries(metrics.gauges)) {
                            addMetricCard(name, value, 'gauge');
                        }
                    }
                }
                
                function addMetricCard(name, value, type) {
                    const container = document.getElementById('metrics');
                    const card = document.createElement('div');
                    card.className = 'metric-card';
                    card.innerHTML = `
                        <h3>${name}</h3>
                        <div class="metric-value">${value}</div>
                        <div>Type: ${type}</div>
                    `;
                    container.appendChild(card);
                }
                
                function updateAlerts(alerts) {
                    const container = document.getElementById('alerts');
                    container.innerHTML = '';
                    
                    if (alerts.length === 0) {
                        container.innerHTML = '<p>No active alerts</p>';
                        return;
                    }
                    
                    alerts.forEach(alert => {
                        const alertDiv = document.createElement('div');
                        alertDiv.className = `alert alert-${alert.severity}`;
                        alertDiv.innerHTML = `
                            <strong>${alert.name}</strong> (${alert.severity})
                            <p>${alert.message}</p>
                            <small>${new Date(alert.timestamp).toLocaleString()}</small>
                        `;
                        container.appendChild(alertDiv);
                    });
                }
                
                function updateStatus(status) {
                    const statusText = document.getElementById('status-text');
                    const statusDiv = document.getElementById('status');
                    
                    if (status.healthy) {
                        statusText.textContent = 'Healthy';
                        statusDiv.className = 'status status-healthy';
                    } else {
                        statusText.textContent = 'Unhealthy';
                        statusDiv.className = 'status status-unhealthy';
                    }
                }
            </script>
        </body>
        </html>
        """
    
    def _send_initial_data(self):
        """Send initial data to connected client."""
        if self.socketio:
            self.socketio.emit('initial_data', {
                'metrics': self.metrics.get_all_metrics(),
                'alerts': [alert.__dict__ for alert in self.alerts.get_active_alerts()],
                'status': {
                    'healthy': self.status.healthy,
                    'uptime': time.time() - self.start_time,
                    'last_activity': self.status.last_activity.isoformat() if self.status.last_activity else None,
                    'components': self.status.components
                }
            })
    
    def update_metrics(self):
        """Broadcast metrics update."""
        if self.socketio:
            self.socketio.emit('metrics_update', self.metrics.get_all_metrics())
    
    def update_alerts(self):
        """Broadcast alerts update."""
        if self.socketio:
            self.socketio.emit('alerts_update', [
                alert.__dict__ for alert in self.alerts.get_active_alerts()
            ])
    
    def update_status(self):
        """Broadcast status update."""
        if self.socketio:
            self.socketio.emit('status_update', {
                'healthy': self.status.healthy,
                'uptime': time.time() - self.start_time,
                'last_activity': self.status.last_activity.isoformat() if self.status.last_activity else None,
                'components': self.status.components
            })
    
    def set_component_status(self, component: str, healthy: bool):
        """Set component health status."""
        self.status.components[component] = healthy
        self.status.healthy = all(self.status.components.values())
        self.status.last_activity = datetime.now()
        self.update_status()
    
    def start(self):
        """Start the dashboard server."""
        if not FLASK_AVAILABLE:
            logger.error("Cannot start dashboard: Flask not available")
            return
        
        def run_server():
            self.socketio.run(
                self.app,
                host=self.config.monitoring.dashboard_host,
                port=self.config.monitoring.dashboard_port,
                debug=False,
                use_reloader=False
            )
        
        self.thread = threading.Thread(target=run_server, daemon=True)
        self.thread.start()
        
        logger.info(
            f"Dashboard started at http://{self.config.monitoring.dashboard_host}:"
            f"{self.config.monitoring.dashboard_port}"
        )
    
    def start_update_timer(self):
        """Start the update timer."""
        def update_loop():
            while True:
                time.sleep(5)  # Update every 5 seconds
                try:
                    self.update_metrics()
                    self.update_alerts()
                    self.update_status()
                except Exception as e:
                    logger.error(f"Error updating dashboard: {str(e)}")
        
        thread = threading.Thread(target=update_loop, daemon=True)
        thread.start()
        logger.info("Started dashboard update timer")
