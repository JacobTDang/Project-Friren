"""
portfolio_manager/processes/position_health_monitor.py

Refactored Position Health Monitor - Uses generic multiprocess manager
and optimized position health analyzer for clean separation.
"""

import time
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

from multiprocess_infrastructure.base_process import BaseProcess, ProcessState
from multiprocess_infrastructure.queue_manager import QueueMessage, MessageType, MessagePriority

# Import clean components
from ..tools.multiprocess_manager import MultiprocessManager, TaskResult
from ..analytics.position_health_analyzer import (
    OptimizedPositionHealthAnalyzer, ActiveStrategy, StrategyStatus, PositionHealthResult
)
from ...data.yahoo import StockDataFetcher


def position_health_worker(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker function for position health analysis

    Args:
        task: Dictionary containing analysis task data

    Returns:
        Dictionary with analysis results
    """
    try:
        # Initialize analyzer in worker (clean separation)
        analyzer = OptimizedPositionHealthAnalyzer()

        # Extract task data
        symbol = task['symbol']
        strategy = task['strategy']
        market_data = task['market_data']
        analysis_type = task.get('analysis_type', 'comprehensive')

        # Run comprehensive analysis using optimized analyzer
        if analysis_type == 'comprehensive':
            analysis_results = analyzer.analyze_position_comprehensive(strategy, market_data)
        else:
            # Could add specific analysis types if needed
            analysis_results = analyzer.analyze_position_comprehensive(strategy, market_data)

        return {
            'symbol': symbol,
            'strategy_type': strategy.strategy_type,
            'analysis_results': analysis_results,
            'success': True
        }

    except Exception as e:
        return {
            'symbol': task.get('symbol', 'unknown'),
            'strategy_type': task.get('strategy', {}).get('strategy_type', 'unknown'),
            'analysis_results': {},
            'success': False,
            'error': str(e)
        }


class PositionHealthMonitor(BaseProcess):
    """
    Refactored Position Health Monitor Process

    Clean Architecture:
    - Layer 1: Process infrastructure (BaseProcess, queues, shared state)
    - Layer 2: Task parallelization (MultiprocessManager)
    - Analytics: Business logic (OptimizedPositionHealthAnalyzer)

    Responsibilities:
    - Process lifecycle management
    - Timing and scheduling (every 10 seconds)
    - Queue message sending for health alerts
    - Shared state updates
    - Orchestrates parallel health analysis
    """

    def __init__(self, process_id: str = "position_health_monitor",
                 check_interval: int = 10,
                 risk_threshold: float = 0.05):
        super().__init__(process_id)

        self.check_interval = check_interval
        self.risk_threshold = risk_threshold

        # Layer 2: Task parallelization (initialized in _initialize)
        self.multiprocess_manager = None

        # Analytics components (initialized in _initialize)
        self.health_analyzer = None
        self.data_fetcher = None

        # Process state tracking
        self.active_strategies = {}  # {symbol: ActiveStrategy}
        self.last_check_time = None
        self.alerts_sent_count = 0
        self.health_checks_count = 0

        self.logger.info(f"PositionHealthMonitor configured - interval: {check_interval}s")

    def _initialize(self):
        """Initialize process components"""
        self.logger.info("Initializing PositionHealthMonitor...")

        try:
            # Layer 2: Initialize generic multiprocess manager
            self.multiprocess_manager = MultiprocessManager(
                max_workers=2,  # Suitable for t3.micro
                max_tasks=50
            )
            self.logger.info("Generic MultiprocessManager initialized")

            # Analytics: Initialize optimized position health analyzer
            self.health_analyzer = OptimizedPositionHealthAnalyzer()
            self.logger.info("OptimizedPositionHealthAnalyzer initialized")

            # Data fetcher
            self.data_fetcher = StockDataFetcher()
            self.logger.info("StockDataFetcher initialized")

            # Load active strategies from shared state
            self._load_active_strategies()

            self.state = ProcessState.RUNNING
            self.logger.info("PositionHealthMonitor initialization complete")

        except Exception as e:
            self.logger.error(f"Failed to initialize PositionHealthMonitor: {e}")
            self.state = ProcessState.ERROR
            raise

    def _process_cycle(self):
        """Main processing cycle - orchestrates health monitoring"""
        try:
            # Check if it's time for health monitoring
            if not self._should_run_health_check():
                time.sleep(2)
                return

            if not self.active_strategies:
                time.sleep(self.check_interval)
                return

            self.logger.info(f"Starting position health check cycle #{self.health_checks_count + 1}")
            start_time = time.time()

            # Fetch market data for all active positions
            market_data_dict = self._fetch_market_data()

            if not market_data_dict:
                self.logger.warning("No market data available for health monitoring")
                time.sleep(30)
                return

            # Run parallel health analysis using generic manager
            health_results = self._run_parallel_health_analysis(market_data_dict)

            # Process results and send alerts
            alerts_sent = self._process_health_results(health_results)

            # Update process state
            self.last_check_time = datetime.now()
            self.health_checks_count += 1
            self.alerts_sent_count += alerts_sent

            # Update shared state and process status
            self._update_shared_state(health_results)
            self._update_process_status(health_results, alerts_sent)

            # Log cycle completion
            cycle_time = time.time() - start_time
            self.logger.info(f"Health check cycle complete - {cycle_time:.2f}s, {alerts_sent} alerts sent")

        except Exception as e:
            self.logger.error(f"Error in position health monitor cycle: {e}")
            self.error_count += 1
            time.sleep(30)

    def _should_run_health_check(self) -> bool:
        """Check if it's time to run health monitoring"""
        if self.last_check_time is None:
            return True

        time_since_last = (datetime.now() - self.last_check_time).total_seconds()
        return time_since_last >= self.check_interval

    def _load_active_strategies(self):
        """Load active strategies from shared state"""
        try:
            if self.shared_state:
                positions = self.shared_state.get_all_positions()

                # Convert position data to ActiveStrategy objects
                for symbol, position_data in positions.items():
                    try:
                        # Create ActiveStrategy from position data
                        strategy = self._create_active_strategy_from_position(symbol, position_data)
                        if strategy:
                            self.active_strategies[symbol] = strategy
                            self.logger.debug(f"Loaded active strategy for {symbol}")
                    except Exception as e:
                        self.logger.warning(f"Failed to create strategy for {symbol}: {e}")

                self.logger.info(f"Loaded {len(self.active_strategies)} active strategies")

        except Exception as e:
            self.logger.warning(f"Failed to load active strategies: {e}")

    def _create_active_strategy_from_position(self, symbol: str, position_data: Dict) -> Optional[ActiveStrategy]:
        """Create ActiveStrategy object from position data"""
        try:
            return ActiveStrategy(
                symbol=symbol,
                strategy_type=position_data.get('strategy_type', 'unknown'),
                entry_time=datetime.fromisoformat(position_data.get('entry_time', datetime.now().isoformat())),
                entry_price=position_data.get('entry_price', 0.0),
                position_size=position_data.get('shares', 0) * 0.01,  # Convert to percentage
                target_size=position_data.get('shares', 0) * 0.015,  # 1.5x current as target
                status=StrategyStatus.ACTIVE,
                parameters={'vol_scale_threshold': 0.35},
                exit_conditions={
                    'max_holding_days': 30,
                    'profit_target_pct': 15.0,
                    'stop_loss_pct': -8.0,
                    'trailing_stop_pct': -5.0
                }
            )
        except Exception as e:
            self.logger.error(f"Error creating ActiveStrategy for {symbol}: {e}")
            return None

    def _fetch_market_data(self) -> Dict[str, Any]:
        """Fetch market data for all active positions"""
        try:
            market_data_dict = {}

            for symbol in self.active_strategies.keys():
                try:
                    # Get recent data (last 50 days for health analysis)
                    df = self.data_fetcher.extract_data(symbol, period="50d", interval="1d")

                    if not df.empty:
                        market_data_dict[symbol] = df
                        self.logger.debug(f"Fetched {len(df)} days of data for {symbol}")
                    else:
                        self.logger.warning(f"No data received for {symbol}")

                except Exception as e:
                    self.logger.warning(f"Failed to fetch data for {symbol}: {e}")
                    continue

            self.logger.info(f"Market data fetched for {len(market_data_dict)} symbols")
            return market_data_dict

        except Exception as e:
            self.logger.error(f"Error fetching market data: {e}")
            return {}

    def _run_parallel_health_analysis(self, market_data_dict: Dict[str, Any]) -> List[TaskResult]:
        """Run health analysis using generic multiprocess manager"""
        try:
            # Create tasks for parallel execution
            tasks = []
            for symbol, strategy in self.active_strategies.items():
                if symbol in market_data_dict:
                    tasks.append({
                        'task_id': f"health-{symbol}",
                        'task_type': 'position_health',
                        'symbol': symbol,
                        'strategy': strategy,
                        'market_data': market_data_dict[symbol],
                        'analysis_type': 'comprehensive'
                    })

            if not tasks:
                self.logger.warning("No tasks created for health analysis")
                return []

            # Execute tasks in parallel using Layer 2 (generic manager)
            results = self.multiprocess_manager.execute_tasks_parallel(
                tasks, position_health_worker, timeout=30
            )

            self.logger.info(f"Parallel health analysis complete - {len(results)} results collected")
            return results

        except Exception as e:
            self.logger.error(f"Error in parallel health analysis: {e}")
            return []

    def _process_health_results(self, task_results: List[TaskResult]) -> int:
        """Process health results and send alerts"""
        alerts_sent = 0

        try:
            # Organize results for portfolio summary
            all_analysis_results = []

            for task_result in task_results:
                if task_result.success:
                    analysis_results = task_result.data.get('analysis_results', {})
                    all_analysis_results.append(analysis_results)
                else:
                    self.logger.warning(f"Failed health analysis for {task_result.task_id}: {task_result.error}")

            if not all_analysis_results:
                return 0

            # Generate portfolio summary using analytics component
            portfolio_summary = self.health_analyzer.generate_portfolio_summary({
                'exit': [r.get('exit') for r in all_analysis_results if r.get('exit')],
                'scaling': [r.get('scaling') for r in all_analysis_results if r.get('scaling')],
                'risk': [r.get('risk') for r in all_analysis_results if r.get('risk')],
                'metrics': [r.get('metrics') for r in all_analysis_results if r.get('metrics')]
            })

            # Send critical alerts via Layer 1 (priority queue)
            for alert in portfolio_summary['alerts']:
                if alert.severity == 'CRITICAL':
                    success = self._send_health_alert_to_queue(alert)
                    if success:
                        alerts_sent += 1

            # Send urgent action recommendations
            for action in portfolio_summary['actions']:
                if action['urgency'] == 'HIGH':
                    success = self._send_action_recommendation_to_queue(action)
                    if success:
                        alerts_sent += 1

            self.logger.info(f"Processed {len(portfolio_summary['alerts'])} alerts, {len(portfolio_summary['actions'])} actions")
            return alerts_sent

        except Exception as e:
            self.logger.error(f"Error processing health results: {e}")
            return 0

    def _send_health_alert_to_queue(self, alert) -> bool:
        """Send health alert to priority queue (Layer 1)"""
        try:
            if not self.priority_queue:
                return False

            message = QueueMessage(
                message_type=MessageType.HEALTH_ALERT,
                priority=MessagePriority.CRITICAL,
                sender_id=self.process_id,
                recipient_id="market_decision_engine",
                payload={
                    'alert_type': alert.alert_type,
                    'symbol': alert.symbol,
                    'severity': alert.severity,
                    'message': alert.message,
                    'action_required': alert.action_required,
                    'metrics': alert.metrics,
                    'timestamp': alert.timestamp.isoformat()
                }
            )

            self.priority_queue.put(message, block=False)
            self.logger.warning(f"Sent CRITICAL health alert for {alert.symbol}: {alert.message}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send health alert: {e}")
            return False

    def _send_action_recommendation_to_queue(self, action: Dict[str, Any]) -> bool:
        """Send action recommendation to priority queue (Layer 1)"""
        try:
            if not self.priority_queue:
                return False

            message = QueueMessage(
                message_type=MessageType.POSITION_UPDATE,
                priority=MessagePriority.HIGH,
                sender_id=self.process_id,
                recipient_id="market_decision_engine",
                payload={
                    'action_type': 'RECOMMENDATION',
                    'symbol': action['symbol'],
                    'recommended_action': action['action'],
                    'reason': action['reason'],
                    'urgency': action['urgency'],
                    'metrics': action.get('metrics', {})
                }
            )

            self.priority_queue.put(message, block=False)
            self.logger.warning(f"Sent HIGH urgency recommendation for {action['symbol']}: {action['action']}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send action recommendation: {e}")
            return False

    def _get_portfolio_state(self) -> Dict[str, Any]:
        """Get current portfolio state from shared state (Layer 1)"""
        try:
            if self.shared_state:
                return {
                    'total_value': self.shared_state.get_portfolio_value(),
                    'positions': self.shared_state.get_all_positions()
                }
            return {}
        except Exception as e:
            self.logger.warning(f"Failed to get portfolio state: {e}")
            return {}

    def _update_shared_state(self, task_results: List[TaskResult]):
        """Update shared state with health monitoring results (Layer 1)"""
        try:
            if not self.shared_state:
                return

            # Update position health metrics
            for task_result in task_results:
                if task_result.success:
                    symbol = task_result.data.get('symbol')
                    analysis_results = task_result.data.get('analysis_results', {})

                    # Update shared state with health metrics
                    if symbol and analysis_results:
                        metrics = analysis_results.get('metrics', {})
                        if metrics and metrics.current_metrics:
                            # Update position health data
                            pass  # Implementation depends on shared state structure

            self.logger.debug("Updated shared state with health results")

        except Exception as e:
            self.logger.warning(f"Failed to update shared state: {e}")

    def _update_process_status(self, task_results: List[TaskResult], alerts_sent: int):
        """Update process status in shared state (Layer 1)"""
        try:
            if not self.shared_state:
                return

            # Calculate summary statistics
            total_positions = len(self.active_strategies)
            successful_analyses = sum(1 for r in task_results if r.success)

            # Get multiprocess manager stats
            manager_stats = self.multiprocess_manager.get_stats()

            status_data = {
                'last_check_time': self.last_check_time.isoformat() if self.last_check_time else None,
                'health_checks_count': self.health_checks_count,
                'total_positions_monitored': total_positions,
                'successful_analyses': successful_analyses,
                'alerts_sent_this_cycle': alerts_sent,
                'total_alerts_sent': self.alerts_sent_count,
                'check_interval_seconds': self.check_interval,
                'manager_stats': manager_stats,
                'health_status': 'healthy' if self.error_count < 3 else 'degraded',
                'error_count': self.error_count,
                'process_state': self.state.value
            }

            self.shared_state.update_system_status(self.process_id, status_data)

        except Exception as e:
            self.logger.warning(f"Failed to update process status: {e}")

    def _cleanup(self):
        """Cleanup process resources"""
        self.logger.info("Cleaning up PositionHealthMonitor...")

        try:
            # Clear references (generic manager cleans itself up)
            self.multiprocess_manager = None
            self.health_analyzer = None
            self.data_fetcher = None
            self.active_strategies.clear()

            self.logger.info("PositionHealthMonitor cleanup complete")

        except Exception as e:
            self.logger.error(f"Error during PositionHealthMonitor cleanup: {e}")

    def get_process_info(self) -> Dict[str, Any]:
        """Return process-specific information for monitoring"""
        base_info = {
            'process_type': 'position_health_monitor',
            'check_interval_seconds': self.check_interval,
            'risk_threshold': self.risk_threshold,
            'active_strategies_count': len(self.active_strategies),
            'last_check_time': self.last_check_time.isoformat() if self.last_check_time else None,
            'health_checks_count': self.health_checks_count,
            'alerts_sent_count': self.alerts_sent_count,
            'error_count': self.error_count,
            'state': self.state.value,
            'strategies_monitored': list(self.active_strategies.keys())
        }

        # Add multiprocess manager stats if available
        if self.multiprocess_manager:
            base_info['manager_stats'] = self.multiprocess_manager.get_stats()

        return base_info

    # Public interface methods for external control

    def add_strategy_to_monitor(self, strategy: ActiveStrategy) -> bool:
        """Add a new strategy to health monitoring"""
        try:
            self.active_strategies[strategy.symbol] = strategy
            self.logger.info(f"Added strategy to monitoring: {strategy.symbol} ({strategy.strategy_type})")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add strategy {strategy.symbol}: {e}")
            return False

    def remove_strategy_from_monitor(self, symbol: str) -> bool:
        """Remove a strategy from health monitoring"""
        try:
            if symbol in self.active_strategies:
                strategy = self.active_strategies.pop(symbol)
                strategy.status = StrategyStatus.COMPLETED
                self.logger.info(f"Removed strategy from monitoring: {symbol}")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to remove strategy {symbol}: {e}")
            return False

    def force_health_check(self) -> Dict[str, Any]:
        """Force an immediate health check (for testing/debugging)"""
        self.logger.info("Forcing immediate health check")

        try:
            if not self.active_strategies:
                return {'success': False, 'error': 'No active strategies to monitor'}

            # Fetch market data
            market_data_dict = self._fetch_market_data()
            if not market_data_dict:
                return {'success': False, 'error': 'No market data available'}

            # Run analysis using generic manager
            task_results = self._run_parallel_health_analysis(market_data_dict)

            # Process results
            alerts_sent = self._process_health_results(task_results)

            # Generate summary
            successful_results = [r for r in task_results if r.success]

            return {
                'success': True,
                'alerts_sent': alerts_sent,
                'results_collected': len(task_results),
                'successful_analyses': len(successful_results),
                'manager_stats': self.multiprocess_manager.get_stats() if self.multiprocess_manager else {}
            }

        except Exception as e:
            self.logger.error(f"Error in forced health check: {e}")
            return {'success': False, 'error': str(e)}
