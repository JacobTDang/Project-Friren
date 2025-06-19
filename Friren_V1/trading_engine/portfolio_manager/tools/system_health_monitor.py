"""
decision_engine/tools/system_health_tool.py

System Health Monitoring Tool - Pure Tool Pattern

This tool monitors system resources with special focus on XGBoost CPU spikes.
Designed as a pure tool (no process inheritance) that sends priority messages
to the decision engine for throttling and resource management.

Key Design Decisions:
1. Pure tool pattern - no BaseProcess inheritance for clean separation
2. XGBoost-specific monitoring - tracks CPU spikes during ML conflict resolution
3. Conservative thresholds - prevent system overload on t3.micro
4. Priority message system - emergency/high/normal/low alert levels
5. Memory-efficient - fits within 15MB budget allocation

Architecture Integration:
- Sends ToolMessage to priority queue for decision engine processing
- Coordinates with shared state for system-wide resource awareness
- Provides throttle recommendations without making decisions directly
"""

import psutil
import time
import threading
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque
from enum import Enum
import logging
import json
import sys
import os

# Message types for tool communication
class ToolMessageType(Enum):
    # System Health Messages
    CPU_SPIKE_ALERT = "cpu_spike_alert"          # XGBoost overwhelming system
    MEMORY_PRESSURE = "memory_pressure"          # Approaching memory limits
    PROCESS_UNHEALTHY = "process_unhealthy"      # Process needs restart
    SYSTEM_RECOVERY = "system_recovery"          # System health restored

    # Performance Messages
    XGBOOST_THROTTLE_NEEDED = "xgboost_throttle" # Suggest XGBoost limitations
    QUEUE_OVERLOAD = "queue_overload"            # Message queue backing up
    DECISION_LATENCY_HIGH = "decision_latency"   # Decision engine too slow

class ToolMessagePriority(Enum):
    EMERGENCY = 1    # System crash imminent, immediate action needed
    HIGH = 2         # CPU/memory critical, throttling recommended
    NORMAL = 3       # Performance degradation detected
    LOW = 4          # Informational, optimization suggestions

@dataclass
class ToolMessage:
    """Message sent from tools to decision engine priority queue"""
    message_type: ToolMessageType
    priority: ToolMessagePriority
    sender_id: str
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for queue transmission"""
        return {
            'message_type': self.message_type.value,
            'priority': self.priority.value,
            'sender_id': self.sender_id,
            'payload': self.payload,
            'timestamp': self.timestamp.isoformat()
        }

@dataclass
class SystemHealthMetrics:
    """Current system health snapshot"""
    cpu_percent: float
    memory_percent: float
    memory_available_mb: float
    cpu_per_core: List[float]

    # XGBoost specific metrics
    xgboost_active: bool = False
    xgboost_cpu_usage: float = 0.0
    xgboost_duration_seconds: float = 0.0

    # Process health
    decision_engine_healthy: bool = True
    process_count: int = 0
    queue_size: int = 0

    # Derived metrics
    system_stress_level: float = 0.0  # 0-100 composite stress score
    throttle_recommended: bool = False
    emergency_action_needed: bool = False

@dataclass
class SystemThresholds:
    """Configurable system health thresholds for t3.micro optimization"""
    # CPU thresholds (%)
    cpu_warning_threshold: float = 70.0      # Start monitoring closely
    cpu_critical_threshold: float = 85.0     # Throttle non-essential processes
    cpu_emergency_threshold: float = 95.0    # Emergency throttling

    # XGBoost specific thresholds
    xgboost_cpu_limit: float = 60.0          # Max CPU for XGBoost
    xgboost_timeout_seconds: float = 30.0    # Max XGBoost execution time
    xgboost_memory_limit_mb: float = 200.0   # Max memory for XGBoost

    # Memory thresholds (MB) - t3.micro has ~920MB usable
    memory_warning_mb: float = 700.0         # Start cleanup
    memory_critical_mb: float = 800.0        # Aggressive cleanup
    memory_emergency_mb: float = 850.0       # Emergency actions

    # Process health thresholds
    max_queue_size: int = 100                # Max messages in queue
    max_decision_latency_ms: float = 5000.0  # Max decision time
    max_process_count: int = 6               # Max concurrent processes

class SystemHealthTool:
    """
    Pure System Health Monitoring Tool

    Monitors system resources with special attention to XGBoost performance
    and t3.micro resource constraints. Sends priority messages to guide
    decision engine throttling and resource management.

    Design Philosophy:
    - Conservative monitoring: Detect issues early before they become critical
    - XGBoost awareness: Special handling for ML conflict resolution resource usage
    - Pure tool pattern: No process inheritance, just monitoring and messaging
    - Memory efficient: Designed to operate within 15MB allocation
    - Actionable alerts: Each alert includes specific throttle recommendations
    """

    def __init__(self,
                 memory_budget_mb: float = 15.0,
                 thresholds: Optional[SystemThresholds] = None,
                 monitoring_interval: float = 5.0):
        """
        Initialize system health monitoring tool

        Args:
            memory_budget_mb: Memory budget for this tool (default 15MB)
            thresholds: System health thresholds (defaults to t3.micro optimized)
            monitoring_interval: How often to check system health in seconds
        """
        self.tool_id = "system_health_tool"
        self.logger = logging.getLogger(f"tools.{self.tool_id}")

        # Configuration
        self.memory_budget_mb = memory_budget_mb
        self.thresholds = thresholds or SystemThresholds()
        self.monitoring_interval = monitoring_interval

        # State tracking (memory-efficient with size limits)
        self.cpu_history = deque(maxlen=60)  # Last 5 minutes at 5-second intervals
        self.memory_history = deque(maxlen=60)
        self.xgboost_sessions = deque(maxlen=10)  # Last 10 XGBoost sessions
        self.alert_history = deque(maxlen=50)     # Recent alerts sent

        # XGBoost monitoring state
        self.xgboost_start_time: Optional[datetime] = None
        self.xgboost_baseline_cpu: float = 0.0
        self.xgboost_active: bool = False

        # Alert suppression (prevent spam)
        self.last_alerts: Dict[ToolMessageType, datetime] = {}
        self.alert_cooldown_seconds = 30.0  # Min time between same alert types

        # Monitoring thread
        self.monitoring_active = False
        self.monitoring_thread: Optional[threading.Thread] = None

        self.logger.info(f"SystemHealthTool initialized with {memory_budget_mb}MB budget")

    def start_monitoring(self) -> None:
        """Start continuous system health monitoring"""
        if self.monitoring_active:
            self.logger.warning("Monitoring already active")
            return

        self.monitoring_active = True
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            name="SystemHealthMonitor",
            daemon=True
        )
        self.monitoring_thread.start()
        self.logger.info("System health monitoring started")

    def stop_monitoring(self) -> None:
        """Stop system health monitoring"""
        self.monitoring_active = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5.0)
        self.logger.info("System health monitoring stopped")

    def _monitoring_loop(self) -> None:
        """Main monitoring loop - runs in background thread"""
        self.logger.info("System health monitoring loop started")

        while self.monitoring_active:
            try:
                # Get current system metrics
                metrics = self._collect_system_metrics()

                # Update history for trend analysis
                self._update_metric_history(metrics)

                # Analyze metrics and generate alerts
                alerts = self._analyze_system_health(metrics)

                # Send alerts to decision engine (would integrate with queue system)
                for alert in alerts:
                    self._send_alert(alert)

                # Sleep until next monitoring cycle
                time.sleep(self.monitoring_interval)

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.monitoring_interval)

    def _collect_system_metrics(self) -> SystemHealthMetrics:
        """
        Collect current system health metrics

        Returns:
            SystemHealthMetrics with current system state
        """
        # Basic system metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        cpu_per_core = psutil.cpu_percent(interval=0.1, percpu=True)

        # XGBoost detection (check for high CPU processes that might be XGBoost)
        xgboost_active, xgboost_cpu = self._detect_xgboost_activity()

        # Process counting
        process_count = len(psutil.pids())

        # Create metrics object
        metrics = SystemHealthMetrics(
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            memory_available_mb=memory.available / (1024 * 1024),
            cpu_per_core=cpu_per_core,
            xgboost_active=xgboost_active,
            xgboost_cpu_usage=xgboost_cpu,
            process_count=process_count,
            system_stress_level=self._calculate_stress_level(cpu_percent, memory.percent)
        )

        # Update XGBoost session tracking
        if xgboost_active and not self.xgboost_active:
            self._start_xgboost_session()
        elif not xgboost_active and self.xgboost_active:
            self._end_xgboost_session()

        self.xgboost_active = xgboost_active

        return metrics

    def _detect_xgboost_activity(self) -> Tuple[bool, float]:
        """
        Detect if XGBoost is currently running based on CPU patterns

        This is heuristic-based since we can't directly detect XGBoost.
        We look for sustained high CPU usage patterns typical of ML training.

        Returns:
            Tuple of (is_xgboost_active, estimated_cpu_usage)
        """
        try:
            # Get current processes and CPU usage
            current_cpu = psutil.cpu_percent(interval=0.1)

            # Simple heuristic: if CPU > 50% and sustained, might be XGBoost
            # In production, you'd have more sophisticated detection
            if current_cpu > 50.0:
                # Check if this is a sustained spike (not just momentary)
                if len(self.cpu_history) > 3:
                    recent_avg = sum(list(self.cpu_history)[-3:]) / 3
                    if recent_avg > 40.0:  # Sustained high usage
                        return True, current_cpu

            return False, 0.0

        except Exception as e:
            self.logger.error(f"Error detecting XGBoost activity: {e}")
            return False, 0.0

    def _calculate_stress_level(self, cpu_percent: float, memory_percent: float) -> float:
        """
        Calculate composite system stress level (0-100)

        Args:
            cpu_percent: Current CPU usage percentage
            memory_percent: Current memory usage percentage

        Returns:
            Stress level from 0-100 (100 = maximum stress)
        """
        # Weighted combination of CPU and memory stress
        cpu_stress = min(100.0, (cpu_percent / self.thresholds.cpu_critical_threshold) * 100)
        memory_stress = min(100.0, (memory_percent / 80.0) * 100)  # 80% memory as baseline

        # CPU gets higher weight since it's more immediately problematic
        composite_stress = (cpu_stress * 0.7) + (memory_stress * 0.3)

        return min(100.0, composite_stress)

    def _update_metric_history(self, metrics: SystemHealthMetrics) -> None:
        """Update historical metrics for trend analysis"""
        self.cpu_history.append(metrics.cpu_percent)
        self.memory_history.append(metrics.memory_percent)

    def _analyze_system_health(self, metrics: SystemHealthMetrics) -> List[ToolMessage]:
        """
        Analyze system metrics and generate appropriate alerts

        Args:
            metrics: Current system health metrics

        Returns:
            List of ToolMessage alerts to send to decision engine
        """
        alerts = []

        # CPU Monitoring
        if metrics.cpu_percent >= self.thresholds.cpu_emergency_threshold:
            alerts.append(self._create_cpu_alert(
                ToolMessagePriority.EMERGENCY,
                metrics,
                "CPU usage critical - emergency throttling needed"
            ))
        elif metrics.cpu_percent >= self.thresholds.cpu_critical_threshold:
            alerts.append(self._create_cpu_alert(
                ToolMessagePriority.HIGH,
                metrics,
                "CPU usage high - throttling recommended"
            ))
        elif metrics.cpu_percent >= self.thresholds.cpu_warning_threshold:
            alerts.append(self._create_cpu_alert(
                ToolMessagePriority.NORMAL,
                metrics,
                "CPU usage elevated - monitoring closely"
            ))

        # Memory Monitoring
        available_mb = metrics.memory_available_mb
        if available_mb <= (1024 - self.thresholds.memory_emergency_mb):
            alerts.append(self._create_memory_alert(
                ToolMessagePriority.EMERGENCY,
                metrics,
                "Memory critically low - emergency cleanup needed"
            ))
        elif available_mb <= (1024 - self.thresholds.memory_critical_mb):
            alerts.append(self._create_memory_alert(
                ToolMessagePriority.HIGH,
                metrics,
                "Memory low - cleanup recommended"
            ))

        # XGBoost Specific Monitoring
        if metrics.xgboost_active:
            if metrics.xgboost_cpu_usage > self.thresholds.xgboost_cpu_limit:
                alerts.append(self._create_xgboost_alert(
                    ToolMessagePriority.HIGH,
                    metrics,
                    "XGBoost consuming excessive CPU - consider throttling"
                ))

        # Filter out alerts that are too frequent (cooldown)
        filtered_alerts = []
        for alert in alerts:
            if self._should_send_alert(alert):
                filtered_alerts.append(alert)
                self.last_alerts[alert.message_type] = datetime.now()

        return filtered_alerts

    def _create_cpu_alert(self, priority: ToolMessagePriority, metrics: SystemHealthMetrics, message: str) -> ToolMessage:
        """Create CPU usage alert with throttle recommendations"""

        # Suggest specific throttle actions based on severity
        throttle_actions = []
        if priority == ToolMessagePriority.EMERGENCY:
            throttle_actions = [
                "disable_xgboost_temporarily",
                "reduce_queue_processing_rate",
                "skip_low_confidence_signals",
                "increase_decision_intervals"
            ]
        elif priority == ToolMessagePriority.HIGH:
            throttle_actions = [
                "limit_xgboost_execution_time",
                "reduce_parallel_tasks",
                "increase_signal_thresholds"
            ]
        else:
            throttle_actions = [
                "monitor_closely",
                "prepare_for_throttling"
            ]

        payload = {
            "cpu_percent": metrics.cpu_percent,
            "cpu_per_core": metrics.cpu_per_core,
            "system_stress_level": metrics.system_stress_level,
            "message": message,
            "throttle_actions": throttle_actions,
            "threshold_breached": self.thresholds.cpu_critical_threshold,
            "recommended_target": max(50.0, self.thresholds.cpu_warning_threshold)
        }

        return ToolMessage(
            message_type=ToolMessageType.CPU_SPIKE_ALERT,
            priority=priority,
            sender_id=self.tool_id,
            payload=payload
        )

    def _create_memory_alert(self, priority: ToolMessagePriority, metrics: SystemHealthMetrics, message: str) -> ToolMessage:
        """Create memory pressure alert with cleanup recommendations"""

        cleanup_actions = []
        if priority == ToolMessagePriority.EMERGENCY:
            cleanup_actions = [
                "clear_signal_buffers",
                "reduce_history_retention",
                "force_garbage_collection",
                "restart_memory_intensive_processes"
            ]
        else:
            cleanup_actions = [
                "cleanup_old_signals",
                "reduce_buffer_sizes",
                "optimize_memory_usage"
            ]

        payload = {
            "memory_available_mb": metrics.memory_available_mb,
            "memory_percent": metrics.memory_percent,
            "message": message,
            "cleanup_actions": cleanup_actions,
            "target_memory_mb": self.thresholds.memory_warning_mb
        }

        return ToolMessage(
            message_type=ToolMessageType.MEMORY_PRESSURE,
            priority=priority,
            sender_id=self.tool_id,
            payload=payload
        )

    def _create_xgboost_alert(self, priority: ToolMessagePriority, metrics: SystemHealthMetrics, message: str) -> ToolMessage:
        """Create XGBoost-specific performance alert"""

        payload = {
            "xgboost_cpu_usage": metrics.xgboost_cpu_usage,
            "xgboost_active": metrics.xgboost_active,
            "message": message,
            "throttle_actions": [
                "limit_xgboost_features",
                "reduce_xgboost_iterations",
                "increase_xgboost_timeout",
                "use_rule_based_fallback"
            ],
            "cpu_limit": self.thresholds.xgboost_cpu_limit
        }

        return ToolMessage(
            message_type=ToolMessageType.XGBOOST_THROTTLE_NEEDED,
            priority=priority,
            sender_id=self.tool_id,
            payload=payload
        )

    def _should_send_alert(self, alert: ToolMessage) -> bool:
        """
        Check if alert should be sent based on cooldown periods

        Args:
            alert: Alert to check

        Returns:
            True if alert should be sent, False if still in cooldown
        """
        last_sent = self.last_alerts.get(alert.message_type)
        if last_sent is None:
            return True

        time_since = (datetime.now() - last_sent).total_seconds()
        return time_since >= self.alert_cooldown_seconds

    def _send_alert(self, alert: ToolMessage) -> None:
        """
        Send alert to decision engine priority queue

        In production, this would integrate with your queue system.
        For now, we'll log the alert and store it for retrieval.

        Args:
            alert: Alert message to send
        """
        # Store alert in history
        self.alert_history.append(alert)

        # Log the alert
        priority_str = alert.priority.name
        message_type_str = alert.message_type.value

        self.logger.warning(
            f"[{priority_str}] {message_type_str}: {alert.payload.get('message', 'No message')}"
        )

        # In production, this would be:
        # self.queue_manager.send_priority_message(alert.to_dict())

    def _start_xgboost_session(self) -> None:
        """Mark start of XGBoost session for timing"""
        self.xgboost_start_time = datetime.now()
        self.xgboost_baseline_cpu = sum(list(self.cpu_history)[-5:]) / 5 if self.cpu_history else 0
        self.logger.info("XGBoost session started")

    def _end_xgboost_session(self) -> None:
        """Mark end of XGBoost session and record metrics"""
        if self.xgboost_start_time:
            duration = (datetime.now() - self.xgboost_start_time).total_seconds()

            session_data = {
                'start_time': self.xgboost_start_time,
                'duration_seconds': duration,
                'baseline_cpu': self.xgboost_baseline_cpu,
                'peak_cpu': max(list(self.cpu_history)[-10:]) if self.cpu_history else 0
            }

            self.xgboost_sessions.append(session_data)
            self.logger.info(f"XGBoost session ended - duration: {duration:.1f}s")

            self.xgboost_start_time = None

    def get_current_health_status(self) -> Dict[str, Any]:
        """
        Get current system health status for external monitoring

        Returns:
            Dictionary with current health metrics and recommendations
        """
        metrics = self._collect_system_metrics()

        return {
            'timestamp': datetime.now().isoformat(),
            'system_health': {
                'cpu_percent': metrics.cpu_percent,
                'memory_available_mb': metrics.memory_available_mb,
                'memory_percent': metrics.memory_percent,
                'system_stress_level': metrics.system_stress_level,
                'process_count': metrics.process_count
            },
            'xgboost_status': {
                'active': metrics.xgboost_active,
                'cpu_usage': metrics.xgboost_cpu_usage,
                'sessions_completed': len(self.xgboost_sessions)
            },
            'thresholds': {
                'cpu_warning': self.thresholds.cpu_warning_threshold,
                'cpu_critical': self.thresholds.cpu_critical_threshold,
                'memory_warning_mb': self.thresholds.memory_warning_mb,
                'xgboost_cpu_limit': self.thresholds.xgboost_cpu_limit
            },
            'recommendations': self._get_current_recommendations(metrics),
            'recent_alerts': len(self.alert_history),
            'tool_memory_usage_mb': self._estimate_memory_usage()
        }

    def _get_current_recommendations(self, metrics: SystemHealthMetrics) -> List[str]:
        """Get current performance recommendations"""
        recommendations = []

        if metrics.cpu_percent > self.thresholds.cpu_warning_threshold:
            recommendations.append("Consider throttling CPU-intensive operations")

        if metrics.memory_available_mb < 200:
            recommendations.append("Memory pressure detected - cleanup recommended")

        if metrics.xgboost_active and metrics.xgboost_cpu_usage > self.thresholds.xgboost_cpu_limit:
            recommendations.append("XGBoost using excessive resources - consider limits")

        if metrics.system_stress_level > 70:
            recommendations.append("High system stress - enable conservative mode")

        return recommendations

    def _estimate_memory_usage(self) -> float:
        """Estimate current memory usage of this tool"""
        # Simple estimation based on data structure sizes
        memory_estimate = 0.0

        # History buffers (rough estimate)
        memory_estimate += len(self.cpu_history) * 8  # 8 bytes per float
        memory_estimate += len(self.memory_history) * 8
        memory_estimate += len(self.alert_history) * 1024  # ~1KB per alert

        return memory_estimate / (1024 * 1024)  # Convert to MB


# Integration Example - How Decision Engine Would Use This Tool
def decision_engine_integration_example():
    """
    Example of how the decision engine would integrate with SystemHealthTool

    This shows the message flow and throttling decisions.
    """

    # Initialize tool
    health_tool = SystemHealthTool(memory_budget_mb=15.0)
    health_tool.start_monitoring()

    # In decision engine main loop:
    while True:  # Decision engine main loop
        try:
            # Check for health alerts
            current_health = health_tool.get_current_health_status()

            # Make throttling decisions based on health
            if current_health['system_health']['system_stress_level'] > 80:
                # Enable aggressive throttling
                decision_mode = "conservative"
                enable_xgboost = False
            elif current_health['system_health']['system_stress_level'] > 60:
                # Enable moderate throttling
                decision_mode = "balanced"
                enable_xgboost = True  # But with limits
            else:
                # Normal operation
                decision_mode = "aggressive"
                enable_xgboost = True

            # Process signals with appropriate throttling...

        except Exception as e:
            logging.error(f"Decision engine error: {e}")

        time.sleep(1.0)  # Decision engine cycle time

    health_tool.stop_monitoring()
