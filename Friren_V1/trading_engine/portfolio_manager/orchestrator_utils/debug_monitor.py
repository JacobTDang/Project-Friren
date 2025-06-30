"""
orchestrator_utils/debug_monitor.py

Comprehensive Debug Monitor - See Every Action in Real-Time

Shows detailed information about:
- Process lifecycle (start, stop, health)
- Queue states (incoming, outgoing, pending)
- Shared memory/state contents
- Message routing between processes
- Decision-making steps
- Database operations
- API calls

NO UNICODE CHARACTERS - Plain text only for Windows compatibility
"""

import time
import threading
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
import json


@dataclass
class ProcessDebugInfo:
    """Debug information for a single process"""
    process_id: str
    status: str
    uptime_seconds: float
    message_count: int
    queue_size: int
    last_activity: datetime
    memory_usage_mb: float
    error_count: int
    last_error: Optional[str]


@dataclass
class QueueDebugInfo:
    """Debug information for message queues"""
    queue_name: str
    size: int
    pending_messages: List[str]
    messages_processed: int
    average_processing_time: float
    last_message_time: Optional[datetime]


@dataclass
class SharedStateDebugInfo:
    """Debug information for shared state"""
    total_keys: int
    keys_by_type: Dict[str, int]
    recent_updates: List[str]
    memory_size_mb: float
    last_cleanup: Optional[datetime]


class DetailedDebugMonitor:
    """
    Comprehensive debug monitor showing every system action

    Provides real-time visibility into:
    1. Process states and activities
    2. Message queue contents and flow
    3. Shared state/memory contents
    4. Decision-making pipeline
    5. Trade execution steps
    6. Error conditions and recovery
    """

    def __init__(self, config, status, logger):
        self.config = config
        self.status = status
        self.logger = logger

        # Monitoring control
        self.monitoring_thread = None
        self.shutdown_event = threading.Event()
        self.monitor_interval = 2.0  # 2 second detailed monitoring

        # Component references (Redis-based)
        self.process_manager = None
        self.redis_manager = None  # Replaces queue_manager and shared_state
        self.account_manager = None
        self.execution_orchestrator = None
        self.symbol_coordinator = None
        self.message_router = None

        # Debug data storage
        self.process_debug_history = {}
        self.queue_debug_history = {}
        self.shared_state_history = {}
        self.decision_log = []
        self.api_call_log = []
        self.error_log = []

        # Performance tracking
        self.loop_count = 0
        self.start_time = datetime.now()

        self.logger.info("DetailedDebugMonitor initialized - NO Unicode characters")

    def set_components(self, **components):
        """Set component references for monitoring"""
        self.process_manager = components.get('process_manager')
        self.redis_manager = components.get('redis_manager')  # Redis replaces queue_manager and shared_state
        self.account_manager = components.get('account_manager')
        self.execution_orchestrator = components.get('execution_orchestrator')
        self.symbol_coordinator = components.get('symbol_coordinator')
        self.message_router = components.get('message_router')

        component_count = sum(1 for c in components.values() if c is not None)
        self.logger.info(f"Debug monitor connected to {component_count} components")

    def start_monitoring(self):
        """Start comprehensive debug monitoring"""
        self.monitoring_thread = threading.Thread(target=self._debug_monitoring_loop, daemon=True)
        self.monitoring_thread.start()
        self.logger.info("=== DETAILED DEBUG MONITORING STARTED ===")
        self.logger.info("Monitoring interval: 2 seconds")
        self.logger.info("Showing: Processes, Queues, Shared State, Decisions, API Calls")

    def stop_monitoring(self):
        """Stop debug monitoring"""
        self.shutdown_event.set()
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=5)
        self.logger.info("=== DEBUG MONITORING STOPPED ===")

    def _debug_monitoring_loop(self):
        """Main debug monitoring loop with comprehensive logging"""
        while not self.shutdown_event.is_set():
            try:
                self.loop_count += 1
                loop_start = time.time()

                # Header for each monitoring cycle
                self.logger.info(f"")
                self.logger.info(f"=== DEBUG CYCLE {self.loop_count} @ {datetime.now().strftime('%H:%M:%S')} ===")

                # 1. Monitor Process States
                self._monitor_process_states()

                # 2. Monitor Queue States
                self._monitor_queue_states()

                # 3. Monitor Shared State/Memory
                self._monitor_shared_state()

                # 4. Monitor Decision Pipeline
                self._monitor_decision_pipeline()

                # 5. Monitor API Calls and Database
                self._monitor_api_and_database()

                # 6. Monitor Portfolio Status
                self._monitor_portfolio_status()

                # 7. Monitor System Resources
                self._monitor_system_resources()

                # 8. Check for Errors and Warnings
                self._monitor_errors_and_warnings()

                # Performance info
                loop_time = time.time() - loop_start
                uptime = (datetime.now() - self.start_time).total_seconds()
                self.logger.info(f"MONITOR PERFORMANCE: Loop {self.loop_count} took {loop_time:.3f}s | Uptime: {uptime:.0f}s")

            except Exception as e:
                self.logger.error(f"DEBUG MONITOR ERROR: {e}")
                self.error_log.append(f"{datetime.now()}: Monitor error: {e}")

            # Sleep for monitoring interval
            time.sleep(self.monitor_interval)

    def _monitor_process_states(self):
        """Monitor detailed process states and activities"""
        self.logger.info("--- PROCESS MONITORING ---")

        if not self.process_manager:
            self.logger.warning("PROCESS MANAGER: Component not available for monitoring")
            return

        try:
            # Get process status
            if hasattr(self.process_manager, 'get_process_status'):
                process_status = self.process_manager.get_process_status()

                self.logger.info(f"TOTAL PROCESSES: {len(process_status)}")

                for process_id, status in process_status.items():
                    is_running = status.get('is_running', False)
                    is_healthy = status.get('is_healthy', False)
                    uptime = status.get('uptime_seconds', 0)
                    message_count = status.get('messages_processed', 0)
                    last_activity = status.get('last_activity', 'Unknown')

                    status_str = "RUNNING" if is_running else "STOPPED"
                    health_str = "HEALTHY" if is_healthy else "UNHEALTHY"

                    self.logger.info(f"PROCESS {process_id}: {status_str} | {health_str} | "
                                   f"Uptime: {uptime:.0f}s | Messages: {message_count} | "
                                   f"Last Activity: {last_activity}")

                    # Check for process-specific details
                    if 'queue_size' in status:
                        self.logger.info(f"  -> Queue Size: {status['queue_size']}")
                    if 'memory_usage' in status:
                        self.logger.info(f"  -> Memory: {status['memory_usage']:.1f}MB")
                    if 'last_error' in status and status['last_error']:
                        self.logger.warning(f"  -> Last Error: {status['last_error']}")

            # Get process list if available
            if hasattr(self.process_manager, 'get_running_processes'):
                running_processes = self.process_manager.get_running_processes()
                self.logger.info(f"RUNNING PROCESS PIDs: {list(running_processes.keys())}")

        except Exception as e:
            self.logger.error(f"PROCESS MONITORING ERROR: {e}")

    def _monitor_queue_states(self):
        """Monitor Redis message queue states and contents"""
        self.logger.info("--- REDIS QUEUE MONITORING ---")

        if not self.redis_manager:
            self.logger.warning("REDIS MANAGER: Component not available for monitoring")
            return

        try:
            # Get Redis queue status
            if hasattr(self.redis_manager, 'get_queue_status'):
                queue_status = self.redis_manager.get_queue_status()

                for queue_name, status in queue_status.items():
                    # DEFENSIVE FIX: Handle case where status might be an int instead of dict
                    if isinstance(status, dict):
                        size = status.get('size', 0)
                        pending = status.get('pending_messages', 0)
                        processed = status.get('messages_processed', 0)
                        last_message = status.get('last_message_time', 'Never')
                    elif isinstance(status, (int, float)):
                        # Handle simple numeric status values
                        size = int(status)
                        pending = 0
                        processed = 0
                        last_message = 'N/A'
                    else:
                        # Handle other status types
                        size = 0
                        pending = 0
                        processed = 0
                        last_message = f'Unknown status type: {type(status)}'

                    self.logger.info(f"REDIS QUEUE {queue_name}: Size={size} | Pending={pending} | "
                                   f"Processed={processed} | Last Message={last_message}")

                    # Show pending message types if available  
                    if isinstance(status, dict) and 'message_types' in status:
                        types = status['message_types']
                        self.logger.info(f"  -> Message Types: {types}")

            # Show Redis communication status
            if hasattr(self.redis_manager, 'get_system_status'):
                system_status = self.redis_manager.get_system_status()
                active_processes = system_status.get('active_processes', 0)
                message_count = system_status.get('total_messages', 0)
                self.logger.info(f"REDIS SYSTEM: Active Processes={active_processes} | Total Messages={message_count}")

            # Show message routing if available
            if self.message_router and hasattr(self.message_router, 'get_routing_status'):
                routing_status = self.message_router.get_routing_status()
                for route, count in routing_status.items():
                    self.logger.info(f"MESSAGE ROUTE {route}: {count} messages")

        except Exception as e:
            self.logger.error(f"REDIS QUEUE MONITORING ERROR: {e}")

    def _monitor_shared_state(self):
        """Monitor Redis shared state/memory contents"""
        self.logger.info("--- REDIS SHARED STATE MONITORING ---")

        if not self.redis_manager:
            self.logger.warning("REDIS SHARED STATE: Component not available for monitoring")
            return

        try:
            # Get all keys and their types from Redis
            if hasattr(self.redis_manager, 'get_all_shared_keys'):
                all_keys = self.redis_manager.get_all_shared_keys()
                self.logger.info(f"REDIS SHARED STATE KEYS: {len(all_keys)} total")

                # Group by key type/category
                key_categories = {}
                for key in all_keys:
                    category = key.split('_')[0] if '_' in key else 'other'
                    key_categories.setdefault(category, []).append(key)

                for category, keys in key_categories.items():
                    self.logger.info(f"  -> {category.upper()}: {len(keys)} keys")
                    for key in keys[:3]:  # Show first 3 keys per category
                        try:
                            value = self.redis_manager.get_shared_state(key)
                            value_type = type(value).__name__
                            value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                            self.logger.info(f"     {key} ({value_type}): {value_preview}")
                        except Exception as e:
                            self.logger.info(f"     {key}: Error reading - {e}")

            # Check for recent updates
            if hasattr(self.redis_manager, 'get_recent_state_updates'):
                recent_updates = self.redis_manager.get_recent_state_updates()
                if recent_updates:
                    self.logger.info(f"RECENT STATE UPDATES: {len(recent_updates)}")
                    for update in recent_updates[-3:]:  # Show last 3 updates
                        self.logger.info(f"  -> {update}")

            # Show Redis connection status
            if hasattr(self.redis_manager, 'is_connected'):
                is_connected = self.redis_manager.is_connected()
                self.logger.info(f"REDIS CONNECTION: {'CONNECTED' if is_connected else 'DISCONNECTED'}")

        except Exception as e:
            self.logger.error(f"REDIS SHARED STATE MONITORING ERROR: {e}")

    def _monitor_decision_pipeline(self):
        """Monitor decision-making pipeline and trade execution"""
        self.logger.info("--- DECISION PIPELINE MONITORING ---")

        try:
            # Monitor symbol coordinator
            if self.symbol_coordinator and hasattr(self.symbol_coordinator, 'get_coordination_status'):
                # ENHANCED DEBUG: Track object reference and state
                self.logger.info(f"DEBUG: Symbol coordinator object ID in monitor: {id(self.symbol_coordinator)}")
                
                try:
                    coord_status = self.symbol_coordinator.get_coordination_status()
                    self.logger.info(f"DEBUG: Successfully retrieved coordination status")
                except Exception as e:
                    self.logger.error(f"DEBUG: Failed to get coordination status: {e}")
                    coord_status = {}

                # DEBUG: Log the actual status being read
                if coord_status:
                    symbols_dict = coord_status.get('symbols', {})
                    self.logger.info(f"DEBUG: Symbol coordinator has {len(symbols_dict)} symbols")
                    self.logger.info(f"DEBUG: Total symbols: {coord_status.get('total_symbols', 0)}, Intensive: {coord_status.get('intensive_symbols', 0)}")
                    
                    # DETAILED DEBUG: Log what symbols are actually in the status
                    if symbols_dict:
                        self.logger.info(f"DEBUG: Symbols in coord_status: {list(symbols_dict.keys())}")
                        for sym, data in symbols_dict.items():
                            self.logger.info(f"DEBUG: {sym} raw data: {data}")
                    else:
                        self.logger.warning("DEBUG: symbols dict is empty despite coord_status existing")
                else:
                    self.logger.warning("DEBUG: Symbol coordinator status is empty!")

                # Count active symbols (symbols with positions > 0) and monitoring symbols
                active_symbols = []  # Symbols with actual positions > 0
                monitoring_symbols = []  # Symbols being monitored regardless of position
                pending_decisions = 0

                if 'symbols' in coord_status:
                    for symbol, symbol_data in coord_status['symbols'].items():
                        position = symbol_data.get('position', 0.0)
                        intensity = symbol_data.get('intensity', 'unknown')
                        self.logger.info(f"DEBUG: Symbol {symbol}: position={position}, intensity={intensity}")
                        
                        # Add to monitoring if has active/intensive intensity (regardless of position)
                        if intensity in ['active', 'intensive']:
                            monitoring_symbols.append(symbol)
                        
                        # Add to active only if has actual position
                        if position > 0:
                            active_symbols.append(symbol)
                            self.logger.info(f"DEBUG: {symbol} counted as ACTIVE (position={position})")
                        else:
                            self.logger.info(f"DEBUG: {symbol} monitoring but no position (position={position})")

                # Count pending decisions from symbol states
                if 'symbols' in coord_status:
                    for symbol_data in coord_status['symbols'].values():
                        # Check if symbol has pending decisions (this would need to be implemented)
                        # For now, we'll count symbols that are in intensive monitoring as "pending"
                        intensity = symbol_data.get('intensity', 'passive')
                        if intensity == 'intensive':
                            pending_decisions += 1

                self.logger.info(f"DEBUG: Calculated - Active symbols: {active_symbols}, Monitoring symbols: {monitoring_symbols}, Pending decisions: {pending_decisions}")
                self.logger.info(f"SYMBOL COORDINATOR: Active={len(active_symbols)} | Monitoring={len(monitoring_symbols)} | Pending={pending_decisions}")
                if active_symbols:
                    self.logger.info(f"  -> Active Symbols (with positions): {', '.join(active_symbols)}")
                else:
                    self.logger.info(f"  -> No active symbols with positions")
                
                if monitoring_symbols:
                    self.logger.info(f"  -> Monitoring Symbols (ready for trading): {', '.join(monitoring_symbols)}")
                else:
                    self.logger.info(f"  -> No symbols being monitored for trading opportunities")

                # Show coordination metrics
                if 'metrics' in coord_status:
                    metrics = coord_status['metrics']
                    self.logger.info(f"  -> Total Symbols: {metrics.get('total_symbols_managed', 0)}")
                    self.logger.info(f"  -> Intensive: {metrics.get('intensive_symbols_count', 0)}")
                    self.logger.info(f"  -> Active: {metrics.get('active_symbols_count', 0)}")
                    self.logger.info(f"  -> Passive: {metrics.get('passive_symbols_count', 0)}")
            else:
                self.logger.warning("DEBUG: Symbol coordinator not available or missing get_coordination_status method")

            # Monitor execution orchestrator
            if self.execution_orchestrator and hasattr(self.execution_orchestrator, 'get_execution_status'):
                exec_status = self.execution_orchestrator.get_execution_status()
                active_executions = exec_status.get('active_executions', 0)
                completed_today = exec_status.get('completed_today', 0)
                success_rate = exec_status.get('success_rate', 0.0)

                self.logger.info(f"EXECUTION ORCHESTRATOR: Active={active_executions} | "
                               f"Completed={completed_today} | Success Rate={success_rate:.1f}%")

            # Show recent decisions from log
            if self.decision_log:
                recent_decisions = self.decision_log[-3:]  # Last 3 decisions
                self.logger.info(f"RECENT DECISIONS: {len(recent_decisions)}")
                for decision in recent_decisions:
                    self.logger.info(f"  -> {decision}")

        except Exception as e:
            self.logger.error(f"DECISION PIPELINE MONITORING ERROR: {e}")
            import traceback
            traceback.print_exc()

    def _monitor_api_and_database(self):
        """Monitor API calls and database operations"""
        self.logger.info("--- API & DATABASE MONITORING ---")

        try:
            # Monitor account manager API calls
            if self.account_manager and hasattr(self.account_manager, 'get_api_status'):
                api_status = self.account_manager.get_api_status()
                api_calls_today = api_status.get('calls_today', 0)
                last_call = api_status.get('last_call_time', 'Never')
                call_success_rate = api_status.get('success_rate', 0.0)

                self.logger.info(f"ALPACA API: Calls Today={api_calls_today} | Last Call={last_call} | "
                               f"Success Rate={call_success_rate:.1f}%")

            # Show recent API calls from log
            if self.api_call_log:
                recent_api_calls = self.api_call_log[-3:]  # Last 3 API calls
                self.logger.info(f"RECENT API CALLS: {len(recent_api_calls)}")
                for api_call in recent_api_calls:
                    self.logger.info(f"  -> {api_call}")

        except Exception as e:
            self.logger.error(f"API & DATABASE MONITORING ERROR: {e}")

    def _monitor_system_resources(self):
        """Monitor system resource usage"""
        self.logger.info("--- SYSTEM RESOURCES ---")

        try:
            # Get current resource usage
            memory_mb = self.status.memory_usage_mb if hasattr(self.status, 'memory_usage_mb') else 0
            cpu_percent = self.status.cpu_usage_percent if hasattr(self.status, 'cpu_usage_percent') else 0

            self.logger.info(f"MEMORY USAGE: {memory_mb:.1f} MB")
            self.logger.info(f"CPU USAGE: {cpu_percent:.1f}%")

            # Check for resource warnings
            if memory_mb > 500:  # More than 500MB
                self.logger.warning(f"HIGH MEMORY USAGE: {memory_mb:.1f}MB")
            if cpu_percent > 80:  # More than 80% CPU
                self.logger.warning(f"HIGH CPU USAGE: {cpu_percent:.1f}%")

        except Exception as e:
            self.logger.error(f"SYSTEM RESOURCES MONITORING ERROR: {e}")

    def _monitor_errors_and_warnings(self):
        """Monitor errors and warning conditions"""
        self.logger.info("--- ERROR & WARNING MONITORING ---")

        try:
            # Show recent errors from log
            if self.error_log:
                recent_errors = self.error_log[-3:]  # Last 3 errors
                self.logger.warning(f"RECENT ERRORS: {len(recent_errors)}")
                for error in recent_errors:
                    self.logger.warning(f"  -> {error}")
            else:
                self.logger.info("NO RECENT ERRORS")

        except Exception as e:
            self.logger.error(f"ERROR MONITORING ERROR: {e}")

    def log_decision(self, symbol: str, decision_type: str, details: str):
        """Log a trading decision for monitoring"""
        decision_entry = f"{datetime.now().strftime('%H:%M:%S')} | {symbol} | {decision_type} | {details}"
        self.decision_log.append(decision_entry)

        # Keep only last 20 decisions
        if len(self.decision_log) > 20:
            self.decision_log = self.decision_log[-20:]

    def log_api_call(self, api_name: str, endpoint: str, result: str):
        """Log an API call for monitoring"""
        api_entry = f"{datetime.now().strftime('%H:%M:%S')} | {api_name} | {endpoint} | {result}"
        self.api_call_log.append(api_entry)

        # Keep only last 20 API calls
        if len(self.api_call_log) > 20:
            self.api_call_log = self.api_call_log[-20:]

    def log_error(self, component: str, error_message: str):
        """Log an error for monitoring"""
        error_entry = f"{datetime.now().strftime('%H:%M:%S')} | {component} | {error_message}"
        self.error_log.append(error_entry)

        # Keep only last 20 errors
        if len(self.error_log) > 20:
            self.error_log = self.error_log[-20:]

    def _monitor_portfolio_status(self):
        """Monitor portfolio value and current holdings"""
        self.logger.info("--- PORTFOLIO STATUS ---")

        try:
            # Try to get alpaca interface from account manager
            alpaca_interface = None
            if hasattr(self, 'account_manager') and self.account_manager:
                if hasattr(self.account_manager, 'alpaca_interface'):
                    alpaca_interface = self.account_manager.alpaca_interface
                elif hasattr(self.account_manager, 'get_alpaca_interface'):
                    alpaca_interface = self.account_manager.get_alpaca_interface()

            # If no direct access, try to find alpaca interface in components
            if not alpaca_interface:
                for component_name in ['alpaca_interface', 'trading_interface', 'broker_interface']:
                    if hasattr(self, component_name):
                        alpaca_interface = getattr(self, component_name)
                        break

            if alpaca_interface:
                # Get account information
                account_info = alpaca_interface.get_account_info()
                if account_info:
                    self.logger.info(f"ACCOUNT STATUS:")
                    self.logger.info(f"  PORTFOLIO VALUE: ${account_info.portfolio_value:,.2f}")
                    self.logger.info(f"  BUYING POWER: ${account_info.buying_power:,.2f}")
                    self.logger.info(f"  CASH: ${account_info.cash:,.2f}")
                    self.logger.info(f"  EQUITY: ${account_info.equity:,.2f}")

                    # Calculate day change
                    day_change = account_info.equity - account_info.last_equity
                    day_change_pct = (day_change / account_info.last_equity * 100) if account_info.last_equity > 0 else 0
                    self.logger.info(f"  DAY CHANGE: ${day_change:+,.2f} ({day_change_pct:+.2f}%)")

                # Get current positions
                positions = alpaca_interface.get_all_positions()
                if positions:
                    self.logger.info(f"CURRENT HOLDINGS: {len(positions)} positions")
                    total_market_value = 0
                    total_unrealized_pl = 0

                    for position in positions:
                        market_value = position.market_value
                        unrealized_pl = position.unrealized_pl
                        unrealized_plpc = position.unrealized_plpc
                        total_market_value += market_value
                        total_unrealized_pl += unrealized_pl

                        self.logger.info(f"  {position.symbol}:")
                        self.logger.info(f"    QUANTITY: {position.quantity}")
                        self.logger.info(f"    CURRENT PRICE: ${position.current_price:.2f}")
                        self.logger.info(f"    MARKET VALUE: ${market_value:,.2f}")
                        self.logger.info(f"    AVG ENTRY: ${position.avg_entry_price:.2f}")
                        self.logger.info(f"    UNREALIZED P/L: ${unrealized_pl:+,.2f} ({unrealized_plpc:+.2f}%)")

                    self.logger.info(f"PORTFOLIO TOTALS:")
                    self.logger.info(f"  TOTAL MARKET VALUE: ${total_market_value:,.2f}")
                    self.logger.info(f"  TOTAL UNREALIZED P/L: ${total_unrealized_pl:+,.2f}")
                else:
                    self.logger.info("CURRENT HOLDINGS: No open positions")

                # Get recent orders
                recent_orders = alpaca_interface.get_recent_orders(limit=5)
                if recent_orders:
                    self.logger.info(f"RECENT ORDERS: {len(recent_orders)} orders")
                    for order in recent_orders:
                        self.logger.info(f"  {order.symbol} {order.side} {order.quantity} @ {order.order_type}")
                        self.logger.info(f"    STATUS: {order.status} | FILLED: {order.filled_qty}")
                        if order.filled_avg_price:
                            self.logger.info(f"    FILL PRICE: ${order.filled_avg_price:.2f}")
                else:
                    self.logger.info("RECENT ORDERS: No recent orders")

            else:
                self.logger.warning("PORTFOLIO STATUS: Alpaca interface not available")

                # Try to get portfolio info from Redis shared state
                if hasattr(self, 'redis_manager') and self.redis_manager:
                    try:
                        # Look for portfolio keys in Redis shared state
                        all_keys = self.redis_manager.get_all_shared_keys() if hasattr(self.redis_manager, 'get_all_shared_keys') else []
                        portfolio_keys = [k for k in all_keys if 'portfolio' in k.lower() or 'position' in k.lower()]

                        if portfolio_keys:
                            self.logger.info(f"PORTFOLIO DATA FROM REDIS STATE: {len(portfolio_keys)} keys")
                            for key in portfolio_keys[:5]:  # Show first 5 portfolio keys
                                try:
                                    value = self.redis_manager.get_shared_state(key)
                                    self.logger.info(f"  {key}: {value}")
                                except Exception as e:
                                    self.logger.info(f"  {key}: Error reading - {e}")
                        else:
                            self.logger.info("NO PORTFOLIO DATA: No portfolio information found in Redis state")
                    except Exception as e:
                        self.logger.error(f"Error reading portfolio from Redis state: {e}")

        except Exception as e:
            self.logger.error(f"PORTFOLIO MONITORING ERROR: {e}")
            import traceback
            self.logger.error(f"TRACEBACK: {traceback.format_exc()}")
