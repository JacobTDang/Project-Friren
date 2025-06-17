"""
portfolio_manager/processes/position_health_monitor.py

Position Health Monitor Process - Process infrastructure wrapper that uses
the pure analytics/position_health_analyzer.py for business logic.

Handles timing, queues, shared state, and multiprocess coordination.
"""

import time
import multiprocessing as mp
import queue
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

from multiprocess_infrastructure.base_process import BaseProcess, ProcessState
from multiprocess_infrastructure.queue_manager import QueueMessage, MessageType, MessagePriority

# Import the pure analytics component
from ..analytics.position_health_analyzer import (
    PositionHealthAnalyzer, ActiveStrategy, StrategyStatus, PositionHealthResult
)
from ...data.yahoo import StockDataFetcher


def position_health_worker(task_queue: mp.Queue, result_queue: mp.Queue):
    """Worker process for parallel position health monitoring"""

    # Initialize analyzer in worker
    analyzer = PositionHealthAnalyzer()

    while True:
        try:
            task = task_queue.get(timeout=2)
            if task is None:
                break

            try:
                symbol = task['symbol']
                active_strategy = task['active_strategy']
                market_data = task['market_data']
                task_type = task['task_type']

                # Route to appropriate analysis method
                if task_type == 'exit':
                    result = analyzer.analyze_exit_conditions(active_strategy, market_data)
                elif task_type == 'scaling':
                    portfolio_state = task.get('portfolio_state', {})
                    result = analyzer.analyze_scaling_conditions(active_strategy, market_data, portfolio_state)
                elif task_type == 'risk':
                    risk_limits = task.get('risk_limits', {})
                    result = analyzer.analyze_risk_conditions(active_strategy, market_data, risk_limits)
                elif task_type == 'metrics':
                    result = analyzer.update_strategy_metrics(active_strategy, market_data)
                else:
                    result = PositionHealthResult(
                        symbol=symbol,
                        strategy_type=active_strategy.strategy_type,
                        analysis_type=task_type,
                        success=False,
                        metadata={'error': f'Unknown task type: {task_type}'}
                    )

                result_queue.put(result)

            except Exception as e:
                logging.error(f"Worker error processing {symbol}: {e}")
                error_result = PositionHealthResult(
                    symbol=symbol,
                    strategy_type='unknown',
                    analysis_type=task.get('task_type', 'unknown'),
                    success=False,
                    metadata={'error': str(e)}
                )
                result_queue.put(error_result)

        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Worker error: {e}")
            continue


class PositionHealthMonitor(BaseProcess):
    """
    Position Health Monitor Process

    Process Infrastructure Responsibilities:
    - Process lifecycle management (start/stop/restart)
    - Timing and scheduling (every 5-10 seconds)
    - Queue message sending for health alerts
    - Shared state updates with position health
    - Multiprocess coordination for parallel monitoring

    Business Logic Responsibilities (delegated to analytics):
    - Position health analysis and alert generation
    - Exit condition checking
    - Position scaling recommendations
    - Risk monitoring and scoring
    """

    def __init__(self, process_id: str = "position_health_monitor",
                 check_interval: int = 10,  # 10 seconds
                 risk_threshold: float = 0.05):
        super().__init__(process_id)

        self.check_interval = check_interval
        self.risk_threshold = risk_threshold

        # Analytics component (initialized in _initialize)
        self.health_analyzer = None
        self.data_fetcher = None

        # Multiprocess components
        self.task_queue = None
        self.result_queue = None
        self.workers = []
        self.num_workers = 2  # Suitable for t3.micro

        # Process state tracking
        self.active_strategies = {}  # {symbol: ActiveStrategy}
        self.last_check_time = None
        self.alerts_sent_count = 0
        self.health_checks_count = 0

        self.logger.info(f"PositionHealthMonitor configured - interval: {check_interval}s")

    def _initialize(self):
        """Initialize process-specific components"""
        self.logger.info("Initializing PositionHealthMonitor...")

        try:
            # Initialize analytics component
            self.health_analyzer = PositionHealthAnalyzer()
            self.logger.info("PositionHealthAnalyzer initialized")

            # Initialize data fetcher
            self.data_fetcher = StockDataFetcher()
            self.logger.info("StockDataFetcher initialized")

            # Initialize multiprocess queues
            self.task_queue = mp.Queue()
            self.result_queue = mp.Queue()

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
                time.sleep(2)  # Short sleep if not time yet
                return

            if not self.active_strategies:
                # No active strategies to monitor
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

            # Start workers for parallel processing
            self._start_workers()

            # Run health analysis using multiprocess coordination
            health_results = self._run_parallel_health_analysis(market_data_dict)

            # Stop workers
            self._stop_workers()

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
            time.sleep(30)  # Wait before retry

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
                # Get active positions from shared state
                positions = self.shared_state.get_all_positions()

                # Convert to ActiveStrategy objects
                for symbol, position_data in positions.items():
                    # Create ActiveStrategy from position data
                    # This would need proper conversion logic based on your position data structure
                    self.logger.debug(f"Loaded active strategy for {symbol}")

                self.logger.info(f"Loaded {len(self.active_strategies)} active strategies")

        except Exception as e:
            self.logger.warning(f"Failed to load active strategies: {e}")

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

    def _start_workers(self):
        """Start worker processes for parallel health analysis"""
        try:
            for i in range(self.num_workers):
                worker = mp.Process(
                    target=position_health_worker,
                    args=(self.task_queue, self.result_queue),
                    name=f'HealthWorker-{i}'
                )
                worker.start()
                self.workers.append(worker)

            self.logger.debug(f"Started {len(self.workers)} health monitoring workers")

        except Exception as e:
            self.logger.error(f"Error starting workers: {e}")
            raise

    def _stop_workers(self):
        """Stop worker processes"""
        try:
            # Send stop signals
            for _ in self.workers:
                self.task_queue.put(None)

            # Wait for workers to finish
            for worker in self.workers:
                worker.join(timeout=5)
                if worker.is_alive():
                    worker.terminate()
                    worker.join()

            self.workers.clear()
            self.logger.debug("Health monitoring workers stopped")

        except Exception as e:
            self.logger.error(f"Error stopping workers: {e}")

    def _run_parallel_health_analysis(self, market_data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Run health analysis using parallel workers"""
        try:
            # Submit tasks for all analysis types
            task_count = 0
            for symbol, strategy in self.active_strategies.items():
                if symbol not in market_data_dict:
                    continue

                # Submit different analysis tasks
                for task_type in ['exit', 'scaling', 'risk', 'metrics']:
                    task = {
                        'symbol': symbol,
                        'active_strategy': strategy,
                        'market_data': market_data_dict[symbol],
                        'task_type': task_type,
                        'portfolio_state': self._get_portfolio_state(),
                        'risk_limits': self._get_risk_limits()
                    }
                    self.task_queue.put(task)
                    task_count += 1

            # Collect results
            results = []
            collected = 0
            timeout = 30
            start_time = time.time()

            while collected < task_count and time.time() - start_time < timeout:
                try:
                    result = self.result_queue.get(timeout=1)
                    results.append(result)
                    collected += 1
                except queue.Empty:
                    continue

            # Organize results by analysis type
            organized_results = {'exit': [], 'scaling': [], 'risk': [], 'metrics': []}
            for result in results:
                if result.analysis_type in organized_results:
                    organized_results[result.analysis_type].append(result)

            self.logger.info(f"Collected {collected}/{task_count} health analysis results")
            return organized_results

        except Exception as e:
            self.logger.error(f"Error in parallel health analysis: {e}")
            return {'exit': [], 'scaling': [], 'risk': [], 'metrics': []}

    def _process_health_results(self, health_results: Dict[str, List[PositionHealthResult]]) -> int:
        """Process health results and send critical alerts to queue"""
        alerts_sent = 0

        try:
            # Generate portfolio summary
            portfolio_summary = self.health_analyzer.generate_portfolio_summary(health_results)

            # Send critical alerts to queue
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

            self.logger.info(f"Processed health results: {len(portfolio_summary['alerts'])} alerts, {len(portfolio_summary['actions'])} actions")
            return alerts_sent

        except Exception as e:
            self.logger.error(f"Error processing health results: {e}")
            return 0

    def _send_health_alert_to_queue(self, alert) -> bool:
        """Send health alert to priority queue"""
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
                    'strategy_type': alert.strategy_type,
                    'severity': alert.severity,
                    'message': alert.message,
                    'action_required': alert.action_required,
                    'metrics': alert.metrics,
                    'timestamp': alert.timestamp
                }
            )

            self.priority_queue.put(message, block=False)
            self.logger.warning(f"Sent CRITICAL health alert for {alert.symbol}: {alert.message}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send health alert: {e}")
            return False

    def _send_action_recommendation_to_queue(self, action: Dict[str, Any]) -> bool:
        """Send action recommendation to priority queue"""
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
        """Get current portfolio state from shared state"""
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

    def _get_risk_limits(self) -> Dict[str, Any]:
        """Get risk limits configuration"""
        return {
            'max_volatility': 0.50,
            'max_drawdown': -10.0,
            'max_position_size': 0.15
        }

    def _update_shared_state(self, health_results: Dict[str, List[PositionHealthResult]]):
        """Update shared state with health monitoring results"""
        try:
            if not self.shared_state:
                return

            # Update position health metrics
            for results_list in health_results.values():
                for result in results_list:
                    if result.success and result.current_metrics:
                        # Update shared state with health metrics
                        # This would depend on your shared state structure
                        pass

            self.logger.debug("Updated shared state with health results")

        except Exception as e:
            self.logger.warning(f"Failed to update shared state: {e}")

    def _update_process_status(self, health_results: Dict[str, List[PositionHealthResult]], alerts_sent: int):
        """Update process status in shared state"""
        try:
            if not self.shared_state:
                return

            # Calculate summary statistics
            total_positions = len(self.active_strategies)
            healthy_positions = sum(1 for results in health_results.get('risk', [])
                                  if results.success and results.risk_level != 'HIGH')

            status_data = {
                'last_check_time': self.last_check_time.isoformat() if self.last_check_time else None,
                'health_checks_count': self.health_checks_count,
                'total_positions_monitored': total_positions,
                'healthy_positions': healthy_positions,
                'alerts_sent_this_cycle': alerts_sent,
                'total_alerts_sent': self.alerts_sent_count,
                'check_interval_seconds': self.check_interval,
                'worker_count': self.num_workers,
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
            # Stop any running workers
            self._stop_workers()

            # Close queues
            if self.task_queue:
                self.task_queue.close()
            if self.result_queue:
                self.result_queue.close()

            # Clear references
            self.health_analyzer = None
            self.data_fetcher = None
            self.active_strategies.clear()

            self.logger.info("PositionHealthMonitor cleanup complete")

        except Exception as e:
            self.logger.error(f"Error during PositionHealthMonitor cleanup: {e}")

    def get_process_info(self) -> Dict[str, Any]:
        """Return process-specific information for monitoring"""
        return {
            'process_type': 'position_health_monitor',
            'check_interval_seconds': self.check_interval,
            'risk_threshold': self.risk_threshold,
            'active_strategies_count': len(self.active_strategies),
            'last_check_time': self.last_check_time.isoformat() if self.last_check_time else None,
            'health_checks_count': self.health_checks_count,
            'alerts_sent_count': self.alerts_sent_count,
            'worker_count': self.num_workers,
            'error_count': self.error_count,
            'state': self.state.value,
            'strategies_monitored': list(self.active_strategies.keys())
        }

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

            # Start workers
            self._start_workers()

            # Run analysis
            health_results = self._run_parallel_health_analysis(market_data_dict)

            # Stop workers
            self._stop_workers()

            # Process results
            alerts_sent = self._process_health_results(health_results)

            # Generate summary
            portfolio_summary = self.health_analyzer.generate_portfolio_summary(health_results)

            return {
                'success': True,
                'alerts_sent': alerts_sent,
                'summary': portfolio_summary['summary'],
                'alerts': [
                    {
                        'symbol': alert.symbol,
                        'severity': alert.severity,
                        'message': alert.message,
                        'action_required': alert.action_required
                    }
                    for alert in portfolio_summary['alerts']
                ],
                'actions': portfolio_summary['actions']
            }

        except Exception as e:
            self.logger.error(f"Error in forced health check: {e}")
            return {'success': False, 'error': str(e)}

    def get_strategy_health_summary(self) -> Dict[str, Any]:
        """Get summary of all monitored strategies health"""
        try:
            if not self.active_strategies:
                return {'total_strategies': 0, 'strategies': []}

            strategy_summaries = []
            total_pnl = 0
            total_allocation = 0

            for symbol, strategy in self.active_strategies.items():
                # Calculate health score if analyzer is available
                health_score = 50.0  # Default
                if self.health_analyzer:
                    try:
                        # Would need market data for accurate health score
                        health_score = 50.0  # Placeholder
                    except:
                        pass

                strategy_info = {
                    'symbol': symbol,
                    'strategy_type': strategy.strategy_type,
                    'status': strategy.status.value,
                    'position_size': strategy.position_size,
                    'pnl': strategy.unrealized_pnl,
                    'pnl_pct': strategy.unrealized_pnl_pct,
                    'time_active': str(strategy.time_in_position()),
                    'risk_score': strategy.risk_score,
                    'health_score': health_score,
                    'entry_price': strategy.entry_price,
                    'current_price': strategy.current_price
                }
                strategy_summaries.append(strategy_info)
                total_pnl += strategy.unrealized_pnl
                total_allocation += strategy.position_size

            # Calculate status breakdown
            status_breakdown = {}
            for status in StrategyStatus:
                count = len([s for s in self.active_strategies.values() if s.status == status])
                if count > 0:
                    status_breakdown[status.value] = count

            return {
                'total_strategies': len(self.active_strategies),
                'total_allocation': total_allocation,
                'total_pnl': total_pnl,
                'average_health_score': sum(s['health_score'] for s in strategy_summaries) / len(strategy_summaries),
                'strategies': sorted(strategy_summaries, key=lambda x: x['health_score'], reverse=True),
                'status_breakdown': status_breakdown,
                'last_check': self.last_check_time.isoformat() if self.last_check_time else None
            }

        except Exception as e:
            self.logger.error(f"Error getting strategy health summary: {e}")
            return {'error': str(e)}

    def update_strategy_status(self, symbol: str, new_status: StrategyStatus) -> bool:
        """Update the status of a monitored strategy"""
        try:
            if symbol in self.active_strategies:
                old_status = self.active_strategies[symbol].status
                self.active_strategies[symbol].status = new_status
                self.logger.info(f"Updated {symbol} status: {old_status.value} -> {new_status.value}")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to update strategy status for {symbol}: {e}")
            return False
