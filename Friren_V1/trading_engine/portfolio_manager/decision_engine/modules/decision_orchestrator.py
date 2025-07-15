"""
Decision Orchestrator Module for Enhanced Decision Engine
========================================================

Main orchestration module that coordinates signal processing, risk management,
and execution for the enhanced decision engine. This module contains the main
decision engine class and handles the complete decision-making workflow.

Extracted from decision_engine.py for better maintainability.
"""

import sys
import os
import time
import threading
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import deque, defaultdict

# Import infrastructure
from Friren_V1.multiprocess_infrastructure.redis_base_process import RedisBaseProcess, ProcessState
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
)

# Import modular components
from .signal_processing_module import SignalProcessingModule, AggregatedSignal, DecisionType
from .risk_management_module import RiskManagementModule, RiskAssessment, PositionSizingResult

# Import enhanced components
from ..execution_orchestrator import ExecutionOrchestrator
from Friren_V1.trading_engine.output.output_coordinator import OutputCoordinator


class MonitoringStrategyStatus(Enum):
    """Status of active monitoring strategies per symbol"""
    ACTIVE = "active"
    TRANSITIONING = "transitioning"
    PENDING_CONFIRMATION = "pending_confirmation"
    FAILED = "failed"
    EMERGENCY_EXIT = "emergency_exit"


@dataclass
class DecisionMetrics:
    """Decision engine performance metrics"""
    decisions_made: int = 0
    signals_processed: int = 0
    executions_attempted: int = 0
    executions_successful: int = 0
    
    # Performance metrics
    average_decision_time_ms: float = 0.0
    total_processing_time_ms: float = 0.0
    
    # Quality metrics
    high_confidence_decisions: int = 0
    risk_rejections: int = 0
    
    # System metrics
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    # Error tracking
    error_count: int = 0
    last_error_time: Optional[datetime] = None
    
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class MarketRegimeState:
    """Market regime state tracking"""
    current: str = "UNKNOWN"
    confidence: float = 0.0
    updated: Optional[datetime] = None
    volatility: str = "NORMAL"
    trend: str = "SIDEWAYS"


class DecisionOrchestratorModule(RedisBaseProcess):
    """
    Decision Orchestrator Module
    
    Main decision engine that orchestrates the complete decision-making workflow:
    1. Message processing and signal aggregation
    2. Risk assessment and position sizing
    3. Trading decision execution
    4. Market regime monitoring and adaptation
    """
    
    def __init__(self, process_id: str = "market_decision_engine"):
        """
        Initialize decision orchestrator
        
        Args:
            process_id: Process identifier
        """
        super().__init__(process_id)
        
        # Core modules
        self.signal_processor: Optional[SignalProcessingModule] = None
        self.risk_manager: Optional[RiskManagementModule] = None
        self.execution_orchestrator: Optional[ExecutionOrchestrator] = None
        self.output_coordinator: Optional[OutputCoordinator] = None
        
        # State tracking
        self.regime_state = MarketRegimeState()
        self.last_regime_check = datetime.now()
        self.monitoring_strategies: Dict[str, MonitoringStrategyStatus] = {}
        
        # Decision tracking
        self.pending_decisions: Dict[str, AggregatedSignal] = {}
        self.decision_history: deque = deque(maxlen=100)
        self.last_decision_time: Optional[datetime] = None
        
        # Performance tracking
        self.metrics = DecisionMetrics()
        self.processing_history = deque(maxlen=50)
        
        # Configuration
        self.decision_timeout_minutes = 5
        self.max_decisions_per_minute = 10
        self.daily_execution_limit = 100
        self.daily_executions_count = 0
        self.last_daily_reset = datetime.now().date()
        
        # Threading control
        self._decision_lock = threading.Lock()
        
    def _initialize(self):
        """Initialize all decision engine components"""
        try:
            self.logger.info(f"=== DECISION ORCHESTRATOR INITIALIZATION START: {self.process_id} ===")
            
            # Initialize OutputCoordinator first
            self.logger.info("Step 1: Initializing output coordinator...")
            self._initialize_output_coordinator()
            
            # Initialize Signal Processing Module
            self.logger.info("Step 2: Initializing signal processing module...")
            self._initialize_signal_processor()
            
            # Initialize Risk Management Module
            self.logger.info("Step 3: Initializing risk management module...")
            self._initialize_risk_manager()
            
            # Initialize Execution Orchestrator
            self.logger.info("Step 4: Initializing execution orchestrator...")
            self._initialize_execution_orchestrator()
            
            # Validate initialization
            self._validate_initialization()
            
            self.state = ProcessState.RUNNING
            self.logger.info("=== DECISION ORCHESTRATOR INITIALIZATION COMPLETE ===")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Decision Orchestrator: {e}")
            self.state = ProcessState.ERROR
            raise
    
    def _initialize_output_coordinator(self):
        """Initialize output coordinator"""
        try:
            self.output_coordinator = OutputCoordinator(
                redis_client=self.redis_manager.redis_client if self.redis_manager else None,
                enable_terminal=True,
                enable_logging=True
            )
            self.logger.info("OutputCoordinator initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize OutputCoordinator: {e}")
            self.output_coordinator = None
    
    def _initialize_signal_processor(self):
        """Initialize signal processing module"""
        try:
            self.signal_processor = SignalProcessingModule(
                output_coordinator=self.output_coordinator,
                process_id=self.process_id
            )
            
            if self.signal_processor.initialize():
                self.logger.info("Signal processing module initialized successfully")
            else:
                raise Exception("Signal processing module initialization failed")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize signal processing module: {e}")
            self.signal_processor = None
    
    def _initialize_risk_manager(self):
        """Initialize risk management module"""
        try:
            self.risk_manager = RiskManagementModule(
                output_coordinator=self.output_coordinator,
                process_id=self.process_id
            )
            
            if self.risk_manager.initialize():
                self.logger.info("Risk management module initialized successfully")
            else:
                raise Exception("Risk management module initialization failed")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize risk management module: {e}")
            self.risk_manager = None
    
    def _initialize_execution_orchestrator(self):
        """Initialize execution orchestrator"""
        try:
            self.execution_orchestrator = ExecutionOrchestrator()
            self.logger.info("Execution orchestrator initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize execution orchestrator: {e}")
            self.execution_orchestrator = None
    
    def _validate_initialization(self):
        """Validate that critical components are initialized"""
        critical_components = [
            ('signal_processor', self.signal_processor),
            ('risk_manager', self.risk_manager)
        ]
        
        for name, component in critical_components:
            if component is None:
                raise Exception(f"Critical component {name} failed to initialize")
        
        self.logger.info("All critical components initialized successfully")
    
    def _execute(self):
        """Execute main decision engine logic"""
        self._process_cycle()
    
    def _process_cycle(self):
        """Main decision processing cycle - event-driven"""
        try:
            cycle_start = time.time()
            messages_processed = 0
            
            # Reset daily execution count if new day
            self._reset_daily_counters()
            
            # Periodically check for market regime updates
            if (datetime.now() - self.last_regime_check).total_seconds() > 300:  # 5 minutes
                if self._update_market_regime_from_redis():
                    self.logger.debug("Market regime updated from Redis")
                self.last_regime_check = datetime.now()
            
            # Process pending messages
            message = self._get_next_message(timeout=0.1)
            if message is None:
                # No messages - sleep to reduce CPU usage
                time.sleep(30.0)
                return
            
            # Process the message
            self._process_message(message)
            messages_processed += 1
            
            # Check for additional messages (limited processing)
            additional_cycles = 0
            while additional_cycles < 10:  # Max 10 additional messages per cycle
                message = self._get_next_message(timeout=0.01)
                if message is None:
                    break
                
                self._process_message(message)
                messages_processed += 1
                additional_cycles += 1
                
                # Safety check for processing time
                if time.time() - cycle_start > 2.0:
                    self.logger.warning(f"Processing cycle taking too long, processed {messages_processed} messages")
                    break
            
            # Make trading decisions if we processed messages
            if messages_processed > 0:
                self._output_business_logic(messages_processed)
                self._make_enhanced_trading_decisions()
                self.logger.info(f"Decision engine processed {messages_processed} signals")
            
            # Periodic maintenance
            self._maybe_adapt_parameters()
            self._periodic_maintenance()
            
            # Update metrics
            cycle_time = (time.time() - cycle_start) * 1000
            self.metrics.total_processing_time_ms += cycle_time
            self.metrics.signals_processed += messages_processed
            
            if messages_processed > 0:
                self.logger.debug(f"Event-driven cycle: {messages_processed} messages in {cycle_time:.1f}ms")
                
        except Exception as e:
            self.logger.error(f"Error in decision processing cycle: {e}")
            self.metrics.error_count += 1
            self.metrics.last_error_time = datetime.now()
            time.sleep(30)
    
    def _process_message(self, message: ProcessMessage):
        """Process individual message"""
        try:
            if not self.signal_processor:
                self.logger.error("Signal processor not available")
                return
            
            # Process message through signal processor
            aggregated_signal = self.signal_processor.process_message(message)
            
            if aggregated_signal:
                # Store for decision making
                self.pending_decisions[aggregated_signal.symbol] = aggregated_signal
                self.logger.debug(f"Processed signal for {aggregated_signal.symbol}: {aggregated_signal.decision_type.value}")
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
    
    def _make_enhanced_trading_decisions(self):
        """Make enhanced trading decisions based on aggregated signals"""
        try:
            if not self.signal_processor or not self.risk_manager:
                self.logger.error("Required modules not available for decision making")
                return
            
            with self._decision_lock:
                # Get symbols with pending signals
                pending_symbols = self.signal_processor.get_pending_symbols()
                
                for symbol in pending_symbols:
                    try:
                        self._make_symbol_decision(symbol)
                    except Exception as e:
                        self.logger.error(f"Error making decision for {symbol}: {e}")
                
                # Clear expired signals
                self.signal_processor.clear_expired_signals()
                
        except Exception as e:
            self.logger.error(f"Error in enhanced trading decisions: {e}")
    
    def _make_symbol_decision(self, symbol: str):
        """Make trading decision for a specific symbol"""
        try:
            # Aggregate signals for symbol
            aggregated_signal = self.signal_processor.aggregate_signals_for_symbol(symbol)
            if not aggregated_signal:
                return
            
            start_time = time.time()
            
            # Check rate limits
            if not self.risk_manager.check_rate_limits():
                wait_time = self.risk_manager.get_rate_limit_wait_time()
                self.logger.info(f"Rate limit reached, waiting {wait_time:.1f}s")
                time.sleep(min(wait_time, 5.0))  # Cap wait time
                return
            
            # Determine trading action and quantity
            action = self._convert_decision_to_action(aggregated_signal.decision_type)
            base_quantity = self._calculate_base_quantity(symbol, aggregated_signal)
            
            # Perform risk assessment
            risk_assessment = self.risk_manager.assess_trading_risk(
                symbol=symbol,
                action=action,
                quantity=base_quantity
            )
            
            # Calculate optimal position size
            sizing_result = self.risk_manager.calculate_position_size(
                symbol=symbol,
                action=action,
                signal_confidence=aggregated_signal.confidence,
                risk_assessment=risk_assessment
            )
            
            # Check if decision is acceptable
            if not self._is_decision_acceptable(aggregated_signal, risk_assessment, sizing_result):
                self.logger.info(f"Decision rejected for {symbol}: Risk too high or insufficient confidence")
                self.metrics.risk_rejections += 1
                return
            
            # Execute the decision
            execution_result = self._execute_trading_decision(
                symbol=symbol,
                action=action,
                quantity=sizing_result.recommended_size,
                aggregated_signal=aggregated_signal,
                risk_assessment=risk_assessment
            )
            
            # Record decision
            self._record_decision(symbol, aggregated_signal, risk_assessment, sizing_result, execution_result)
            
            # Update metrics
            decision_time = (time.time() - start_time) * 1000
            self.metrics.average_decision_time_ms = (
                (self.metrics.average_decision_time_ms * self.metrics.decisions_made + decision_time) /
                (self.metrics.decisions_made + 1)
            )
            self.metrics.decisions_made += 1
            
            if aggregated_signal.confidence > 80:
                self.metrics.high_confidence_decisions += 1
            
            self.logger.info(f"Decision completed for {symbol}: {action} {sizing_result.recommended_size} shares (confidence: {aggregated_signal.confidence:.1f}%)")
            
        except Exception as e:
            self.logger.error(f"Error making decision for {symbol}: {e}")
    
    def _convert_decision_to_action(self, decision_type: DecisionType) -> str:
        """Convert decision type to trading action"""
        if decision_type == DecisionType.BUY:
            return "BUY"
        elif decision_type == DecisionType.SELL:
            return "SELL"
        elif decision_type == DecisionType.CLOSE:
            return "SELL"  # Close position = sell
        elif decision_type == DecisionType.FORCE_CLOSE:
            return "SELL"
        else:
            return "HOLD"
    
    def _calculate_base_quantity(self, symbol: str, signal: AggregatedSignal) -> int:
        """Calculate base quantity for position sizing"""
        try:
            # Base quantity calculation based on signal confidence
            confidence_factor = signal.confidence / 100.0
            base_shares = max(1, int(100 * confidence_factor))  # Scale with confidence
            
            return base_shares
            
        except Exception:
            return 10  # Default quantity
    
    def _is_decision_acceptable(self, signal: AggregatedSignal, risk_assessment: RiskAssessment, 
                              sizing_result: PositionSizingResult) -> bool:
        """Check if trading decision is acceptable"""
        try:
            # Check minimum confidence threshold
            if signal.confidence < 60.0:
                return False
            
            # Check risk level
            if risk_assessment.is_high_risk():
                return False
            
            # Check position size
            if sizing_result.recommended_size <= 0:
                return False
            
            # Check daily execution limit
            if self.daily_executions_count >= self.daily_execution_limit:
                return False
            
            return True
            
        except Exception:
            return False
    
    def _execute_trading_decision(self, symbol: str, action: str, quantity: int,
                                aggregated_signal: AggregatedSignal, risk_assessment: RiskAssessment) -> Dict[str, Any]:
        """Execute trading decision"""
        try:
            if not self.execution_orchestrator:
                self.logger.warning("Execution orchestrator not available")
                return {'success': False, 'reason': 'no_executor'}
            
            # Record API call for rate limiting
            self.risk_manager.record_api_call()
            
            # Prepare execution data
            execution_data = {
                'symbol': symbol,
                'action': action,
                'quantity': quantity,
                'signal_confidence': aggregated_signal.confidence,
                'risk_score': risk_assessment.overall_risk_score,
                'decision_sources': aggregated_signal.sources,
                'timestamp': datetime.now().isoformat()
            }
            
            # Execute through orchestrator
            execution_result = self.execution_orchestrator.execute_trade(execution_data)
            
            # Update metrics
            self.metrics.executions_attempted += 1
            if execution_result.get('success', False):
                self.metrics.executions_successful += 1
                self.daily_executions_count += 1
                
                # Output execution result
                if self.output_coordinator:
                    self._output_execution_result(symbol, action, quantity, execution_result)
            
            return execution_result
            
        except Exception as e:
            self.logger.error(f"Error executing decision for {symbol}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _output_execution_result(self, symbol: str, action: str, quantity: int, execution_result: Dict):
        """Output execution result using OutputCoordinator"""
        try:
            if self.output_coordinator and execution_result.get('success'):
                self.output_coordinator.output_execution(
                    symbol=symbol,
                    action=action,
                    quantity=quantity,
                    price=execution_result.get('execution_price', 0.0),
                    order_id=execution_result.get('order_id', 'unknown'),
                    total_value=execution_result.get('total_value', 0.0)
                )
        except Exception as e:
            self.logger.warning(f"Error outputting execution result: {e}")
    
    def _record_decision(self, symbol: str, signal: AggregatedSignal, risk_assessment: RiskAssessment,
                        sizing_result: PositionSizingResult, execution_result: Dict):
        """Record decision in history"""
        try:
            decision_record = {
                'symbol': symbol,
                'timestamp': datetime.now(),
                'signal': signal,
                'risk_assessment': risk_assessment,
                'sizing_result': sizing_result,
                'execution_result': execution_result
            }
            
            self.decision_history.append(decision_record)
            self.last_decision_time = datetime.now()
            
        except Exception as e:
            self.logger.error(f"Error recording decision: {e}")
    
    def _get_next_message(self, timeout: float = 0.1) -> Optional[ProcessMessage]:
        """Get next message from Redis queue"""
        try:
            if not self.redis_manager:
                return None
            
            return self.redis_manager.receive_message(timeout=timeout)
            
        except Exception as e:
            self.logger.debug(f"Error getting message: {e}")
            return None
    
    def _update_market_regime_from_redis(self) -> bool:
        """Update market regime from Redis shared state"""
        try:
            if not self.redis_manager:
                return False
            
            regime_data = self.redis_manager.get_shared_state('market_regime', namespace='market')
            
            if regime_data and isinstance(regime_data, dict):
                current_regime = regime_data.get('regime')
                confidence = regime_data.get('confidence', 0.0)
                
                if current_regime and current_regime != 'UNKNOWN':
                    old_regime = self.regime_state.current
                    
                    self.regime_state = MarketRegimeState(
                        current=current_regime,
                        confidence=confidence,
                        updated=datetime.now(),
                        volatility=regime_data.get('volatility_regime', 'NORMAL'),
                        trend=regime_data.get('trend', 'SIDEWAYS')
                    )
                    
                    if old_regime != current_regime:
                        self.logger.info(f"Market regime updated: {old_regime} -> {current_regime} (confidence: {confidence:.1f}%)")
                    
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error updating market regime: {e}")
            return False
    
    def _output_business_logic(self, messages_processed: int):
        """Output business logic using terminal system"""
        try:
            # Add project root for color system import
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
            if project_root not in sys.path:
                sys.path.append(project_root)
            
            from terminal_color_system import print_decision_engine
            print_decision_engine(f"Decision Engine: Processed {messages_processed} trading signals")
        except ImportError:
            print(f"[DECISION ENGINE] Decision Engine: Processed {messages_processed} trading signals")
    
    def _reset_daily_counters(self):
        """Reset daily execution counters if new day"""
        current_date = datetime.now().date()
        if current_date != self.last_daily_reset:
            self.daily_executions_count = 0
            self.last_daily_reset = current_date
            self.logger.info("Daily execution counters reset")
    
    def _maybe_adapt_parameters(self):
        """Perform parameter adaptation if needed"""
        try:
            if self.risk_manager:
                self.risk_manager.adapt_parameters()
        except Exception as e:
            self.logger.debug(f"Parameter adaptation error: {e}")
    
    def _periodic_maintenance(self):
        """Perform periodic maintenance tasks"""
        try:
            # Clear old decision history
            if len(self.decision_history) > 100:
                self.decision_history = deque(list(self.decision_history)[-50:], maxlen=100)
            
            # Update system metrics
            try:
                import psutil
                process = psutil.Process()
                self.metrics.memory_usage_mb = process.memory_info().rss / 1024 / 1024
                self.metrics.cpu_usage_percent = process.cpu_percent()
            except Exception:
                pass
            
        except Exception as e:
            self.logger.debug(f"Maintenance error: {e}")
    
    def get_process_info(self) -> Dict[str, Any]:
        """Get comprehensive process information"""
        try:
            return {
                'process_type': 'decision_orchestrator',
                'state': self.state.value,
                'last_decision_time': self.last_decision_time.isoformat() if self.last_decision_time else None,
                'market_regime': {
                    'current': self.regime_state.current,
                    'confidence': self.regime_state.confidence,
                    'updated': self.regime_state.updated.isoformat() if self.regime_state.updated else None
                },
                'metrics': {
                    'decisions_made': self.metrics.decisions_made,
                    'signals_processed': self.metrics.signals_processed,
                    'executions_attempted': self.metrics.executions_attempted,
                    'executions_successful': self.metrics.executions_successful,
                    'average_decision_time_ms': self.metrics.average_decision_time_ms,
                    'high_confidence_decisions': self.metrics.high_confidence_decisions,
                    'risk_rejections': self.metrics.risk_rejections,
                    'error_count': self.metrics.error_count
                },
                'daily_stats': {
                    'executions_count': self.daily_executions_count,
                    'execution_limit': self.daily_execution_limit,
                    'last_reset': self.last_daily_reset.isoformat()
                }
            }
        except Exception as e:
            self.logger.error(f"Error getting process info: {e}")
            return {'error': str(e)}
    
    def force_decision_cycle(self) -> Dict[str, Any]:
        """Force an immediate decision cycle (for testing/debugging)"""
        try:
            self.logger.info("Forcing immediate decision cycle")
            
            start_time = time.time()
            self._make_enhanced_trading_decisions()
            processing_time = (time.time() - start_time) * 1000
            
            return {
                'success': True,
                'processing_time_ms': processing_time,
                'decisions_made': self.metrics.decisions_made,
                'pending_symbols': len(self.pending_decisions)
            }
            
        except Exception as e:
            self.logger.error(f"Error in forced decision cycle: {e}")
            return {'success': False, 'error': str(e)}
    
    def _cleanup(self):
        """Cleanup orchestrator resources"""
        try:
            self.logger.info("Cleaning up Decision Orchestrator...")
            
            # Cleanup modules
            if self.signal_processor:
                self.signal_processor.cleanup()
            
            if self.risk_manager:
                self.risk_manager.cleanup()
            
            if self.execution_orchestrator and hasattr(self.execution_orchestrator, 'cleanup'):
                self.execution_orchestrator.cleanup()
            
            # Clear tracking data
            self.pending_decisions.clear()
            self.decision_history.clear()
            self.processing_history.clear()
            self.monitoring_strategies.clear()
            
            self.logger.info("Decision Orchestrator cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during orchestrator cleanup: {e}")


# Alias for backward compatibility
MarketDecisionEngine = DecisionOrchestratorModule