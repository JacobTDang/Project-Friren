#!/usr/bin/env python3
"""
decision_coordinator.py

Decision engine coordination and processing management
"""

import time
import threading
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import random


class DecisionCoordinator:
    """Handles decision engine coordination and processing"""

    def __init__(self, config, status, logger):
        self.config = config
        self.status = status
        self.logger = logger
        self.shutdown_event = threading.Event()
        self.decision_thread: Optional[threading.Thread] = None

        # References to other components (set by orchestrator)
        self.process_manager = None
        self.execution_orchestrator = None
        self.symbol_coordinator = None
        self.message_router = None

        # Decision tracking
        self._last_decision_check = None

    def set_components(self, process_manager=None, execution_orchestrator=None,
                      symbol_coordinator=None, message_router=None):
        """Set references to other components"""
        self.process_manager = process_manager
        self.execution_orchestrator = execution_orchestrator
        self.symbol_coordinator = symbol_coordinator
        self.message_router = message_router

    def start_decision_coordination(self):
        """Start decision engine coordination"""
        self.decision_thread = threading.Thread(target=self._decision_coordination_loop, daemon=True)
        self.decision_thread.start()
        self.logger.info("Decision coordination started")

    def stop_decision_coordination(self):
        """Stop decision coordination"""
        self.shutdown_event.set()
        if self.decision_thread and self.decision_thread.is_alive():
            self.decision_thread.join(timeout=10)

    def _decision_coordination_loop(self):
        """Decision engine coordination loop with symbol awareness"""
        while not self.shutdown_event.is_set():
            try:
                # Symbol-aware coordination if enabled
                if self.config.symbol_coordination_enabled and self.symbol_coordinator:
                    self._coordinate_symbol_processing()

                # Check for pending decisions from decision engine
                self._check_pending_decisions()

                # Coordinate strategy analysis
                self._coordinate_strategy_analysis()

                # Process any pending decisions from symbol processes
                self._process_pending_decisions()

                time.sleep(self.config.decision_cycle_interval)

            except Exception as e:
                self.logger.error(f"Error in decision coordination loop: {e}")
                time.sleep(60)

    def _coordinate_symbol_processing(self):
        """Coordinate symbol processing across all processes"""
        try:
            if not self.symbol_coordinator:
                return

            # Get next symbol for processing based on priority
            symbol_to_process = self.symbol_coordinator.get_next_symbol_to_process()

            if symbol_to_process:
                # Send symbol processing message to relevant processes
                self._send_symbol_processing_message(symbol_to_process)
                self.logger.debug(f"Coordinated processing for symbol {symbol_to_process}")

        except Exception as e:
            self.logger.error(f"Error in symbol processing coordination: {e}")

    def _send_symbol_processing_message(self, symbol: str):
        """Send symbol processing message to relevant processes"""
        try:
            if not self.message_router:
                self.logger.debug(f"No message router available, skipping message for {symbol}")
                return

            # Create symbol processing message
            try:
                from Friren_V1.trading_engine.portfolio_manager.symbol_coordination.message_router import QueueMessage, MessageType, MessagePriority

                message_data = {
                    'symbol': symbol,
                    'timestamp': datetime.now().isoformat(),
                    'source': 'decision_coordinator',
                    'action': 'process_symbol',
                    'message_id': f"proc_{symbol}_{int(time.time())}"
                }

                # Create proper QueueMessage object
                queue_message = QueueMessage(
                    message_type=MessageType.SYMBOL_COORDINATION,
                    priority=MessagePriority.NORMAL,
                    sender_id='decision_coordinator',
                    recipient_id='symbol_coordinator',
                    payload=message_data
                )

                self.logger.info(f"SENDING MESSAGE: Processing request for {symbol} - {message_data}")

                # Route message through symbol coordination
                success = self.message_router.route_message(symbol, queue_message)

                if success:
                    self.logger.info(f"MESSAGE SENT: Successfully routed processing message for {symbol}")
                else:
                    self.logger.warning(f"MESSAGE FAILED: Could not route processing message for {symbol}")

            except ImportError:
                self.logger.debug("Message routing not available for symbol coordination")

        except Exception as e:
            self.logger.error(f"Error sending symbol processing message for {symbol}: {e}")

    def _check_pending_decisions(self):
        """Check for pending decisions from decision engine"""
        try:
            # Simulate decision engine queue management
            if self._last_decision_check is not None:
                time_since_last = time.time() - self._last_decision_check
                if time_since_last < 300:  # Only check every 5 minutes
                    return

            self._last_decision_check = time.time()

            # Simulate adding work to decision engine queue
            if random.random() < 0.4:  # 40% chance of new analysis needed
                symbol = random.choice(self.config.symbols)
                decision_task = {
                    'symbol': symbol,
                    'task_type': 'market_analysis',
                    'timestamp': datetime.now().isoformat(),
                    'priority': random.choice(['HIGH', 'MEDIUM', 'LOW']),
                    'data_sources': ['price', 'volume', 'sentiment', 'news']
                }

                # Add to decision engine queue
                if hasattr(self.process_manager, 'decision_queue'):
                    self.process_manager.decision_queue.append(decision_task)
                    self.logger.info(f"DECISION TASK QUEUED: {symbol} analysis ({len(self.process_manager.decision_queue)} in queue)")

                    # Try to start decision engine if needed
                    if hasattr(self.process_manager, 'start_decision_engine_if_needed'):
                        if self.process_manager.start_decision_engine_if_needed():
                            self.logger.info("Decision engine started to process queued tasks")
                else:
                    # Fallback: simulate decision directly
                    decision_data = self._simulate_decision(symbol)
                    self._process_decision(symbol, decision_data)

            # Simulate decision engine processing (consume queue)
            if (hasattr(self.process_manager, 'decision_queue') and
                self.process_manager.decision_queue and
                random.random() < 0.6):  # 60% chance to process an item

                task = self.process_manager.decision_queue.pop(0)
                self.logger.info(f"DECISION ENGINE PROCESSING: {task['symbol']} ({len(self.process_manager.decision_queue)} remaining)")

                # Update last decision time
                self.status.last_decision_time = datetime.now()

                # If queue is empty, stop decision engine
                if (hasattr(self.process_manager, 'stop_decision_engine_if_idle') and
                    not self.process_manager.decision_queue):
                    if self.process_manager.stop_decision_engine_if_idle():
                        self.logger.info("Decision engine stopped (queue empty)")

        except Exception as e:
            self.logger.error(f"Error checking pending decisions: {e}")

    def _simulate_decision(self, symbol: str) -> Dict[str, Any]:
        """Simulate a trading decision"""
        return {
            'symbol': symbol,
            'action': random.choice(['BUY', 'SELL', 'HOLD']),
            'confidence': round(random.uniform(0.6, 0.95), 2),
            'timestamp': datetime.now().isoformat(),
            'reason': f"Simulated analysis for {symbol}",
            'quantity': random.randint(1, 10),
            'price_target': round(random.uniform(100, 300), 2)
        }

    def _process_decision(self, symbol: str, decision_data: Dict[str, Any]):
        """Process a trading decision"""
        self.logger.info(f"DECISION ENGINE RESPONSE: {symbol} -> {decision_data['action']} (confidence: {decision_data['confidence']})")
        self.logger.info(f"DECISION DETAILS: {decision_data}")

        # Update last decision time
        self.status.last_decision_time = datetime.now()

        # Execute high confidence trading decisions
        if decision_data['action'] in ['BUY', 'SELL'] and decision_data['confidence'] > 0.7:
            self.logger.info(f"HIGH CONFIDENCE DECISION: Executing {decision_data['action']} for {symbol}")

            # Execute the trade
            try:
                trade_executed = self.execute_trade_decision(symbol, decision_data)
                if trade_executed:
                    self.logger.info(f"TRADE EXECUTED SUCCESSFULLY: {decision_data['action']} {decision_data['quantity']} shares of {symbol}")
                    self.status.trades_today += 1
                    self.status.last_trade_time = datetime.now()
                else:
                    self.logger.warning(f"TRADE EXECUTION FAILED: {decision_data['action']} for {symbol}")
            except Exception as e:
                self.logger.error(f"Error executing trade for {symbol}: {e}")
        else:
            self.logger.info(f"LOW CONFIDENCE DECISION: Skipping execution for {symbol} (confidence: {decision_data['confidence']})")

    def execute_trade_decision(self, symbol: str, decision_data: Dict[str, Any]) -> bool:
        """Execute a trade decision through the execution orchestrator"""
        try:
            self.logger.info(f"Executing trade decision for {symbol}")

            if not self.execution_orchestrator:
                self.logger.error("Execution orchestrator not available")
                return False

            # Check system state
            if self.status.state.value != "running":
                self.logger.warning(f"System not in running state: {self.status.state}")
                return False

            # Execute through orchestrator
            execution_result = self.execution_orchestrator.execute_approved_decision(
                decision_data.get('risk_validation'),
                decision_data.get('strategy_name', 'unknown'),
                decision_data.get('confidence', 0.5)
            )

            if execution_result and execution_result.was_successful:
                self.logger.info(f"Trade executed successfully: {execution_result.execution_summary}")
                return True
            else:
                error_msg = execution_result.error_message if execution_result else "Unknown error"
                self.logger.error(f"Trade execution failed: {error_msg}")
                return False

        except Exception as e:
            self.logger.error(f"Error executing trade decision: {e}")
            return False

    def _coordinate_strategy_analysis(self):
        """Coordinate strategy analysis across processes"""
        # Enhanced coordination with symbol-specific message routing
        if self.symbol_coordinator and self.message_router:
            try:
                # Get next symbol for processing based on priority
                symbol_to_process = self.symbol_coordinator.get_next_symbol_to_process()

                if symbol_to_process:
                    # Create strategy analysis message
                    try:
                        from Friren_V1.multiprocess_infrastructure.queue_manager import QueueMessage, MessageType, MessagePriority

                        message = QueueMessage(
                            message_type=MessageType.STRATEGY_SIGNAL,
                            priority=MessagePriority.HIGH,
                            sender_id='decision_coordinator',
                            recipient_id='strategy_analyzer',
                            payload={
                                'symbol': symbol_to_process,
                                'timestamp': datetime.now().isoformat(),
                                'source': 'decision_coordinator'
                            }
                        )

                        # Route message through symbol coordination
                        self.message_router.route_message(symbol_to_process, message)

                        self.logger.debug(f"Coordinated strategy analysis for {symbol_to_process}")

                    except ImportError:
                        self.logger.debug("Queue infrastructure not available for strategy coordination")

            except Exception as e:
                self.logger.error(f"Error in strategy analysis coordination: {e}")
        else:
            # Fallback to basic coordination
            pass

    def _process_pending_decisions(self):
        """Process any pending decisions from symbol processes"""
        if not self.process_manager:
            return

        try:
            # Update account data before processing decisions (only when needed for trading)
            if hasattr(self, 'system_monitor') and self.system_monitor:
                self.system_monitor.update_account_data_on_demand()

            process_status = self.process_manager.get_process_status()

            for process_id, status in process_status.items():
                if status.get('has_pending_decision', False):
                    self.logger.info(f"Processing pending decision from {process_id}")

                    # Get decision data
                    decision_data = status.get('decision_data', {})

                    # Execute decision through orchestrator
                    if self.execution_orchestrator:
                        try:
                            result = self.execution_orchestrator.execute_decision(decision_data)
                            self.logger.info(f"Decision execution result: {result}")
                        except Exception as e:
                            self.logger.error(f"Error executing decision: {e}")

        except Exception as e:
            self.logger.error(f"Error processing pending decisions: {e}")
