"""
Execution Coordinator - Extracted from Decision Engine

This module handles trade execution coordination, portfolio management, and execution tracking.
Provides centralized execution logic with real market-driven calculations.

Features:
- Real trade execution coordination
- Execution result tracking and analysis
- Portfolio position management
- Force close execution handling
- Terminal output for business logic visibility
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

# Import market metrics for dynamic calculations
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics


@dataclass
class ExecutionResult:
    """Execution result with comprehensive tracking"""
    was_successful: bool
    symbol: str
    action: str
    executed_amount: float
    execution_price: float
    execution_summary: str
    error_message: Optional[str] = None
    execution_slippage: float = 0.0
    order_id: Optional[str] = None
    execution_time: Optional[datetime] = None


class ExecutionCoordinator:
    """Coordinates trade execution with market-driven analysis and real business logic output"""

    def __init__(self, process_id: str, execution_orchestrator=None):
        self.process_id = process_id
        self.execution_orchestrator = execution_orchestrator
        self.logger = logging.getLogger(f"{__name__}.ExecutionCoordinator")
        
        # Execution tracking
        self.daily_execution_count = 0
        self.force_close_symbols = set()
        self.execution_history = []

    def execute_validated_decision(self, risk_validation, strategy_name: str) -> ExecutionResult:
        """Execute validated trading decision with real market execution"""
        try:
            if self.execution_orchestrator and getattr(risk_validation, 'should_execute', False):
                # Show decision being made (business logic output)
                try:
                    from terminal_color_system import print_decision_engine, print_trade_execution
                    
                    symbol = getattr(risk_validation.original_decision, 'symbol', 'UNKNOWN')
                    action = getattr(risk_validation.original_decision, 'action', 'UNKNOWN')
                    confidence = getattr(risk_validation.original_decision, 'final_confidence', 0.0)
                    
                    print_decision_engine(f"[DECISION ENGINE] Decision: {action} {symbol} - strategy: {strategy_name}, confidence: {confidence:.2f}")
                    
                except ImportError:
                    print(f"BUSINESS LOGIC: Decision engine executing {strategy_name} decision")
                
                # Execute through real orchestrator
                result = self.execution_orchestrator.execute_approved_decision(
                    risk_validation=risk_validation,
                    strategy_name=strategy_name,
                    strategy_confidence=getattr(risk_validation.original_decision, 'final_confidence', 0.5)
                )

                self.daily_execution_count += 1

                # Convert result to standardized format
                execution_result = self._standardize_execution_result(result, risk_validation)
                
                # Business logic output for execution result
                self._output_execution_result(execution_result, risk_validation)
                
                # Track execution in history
                self.execution_history.append({
                    'timestamp': datetime.now(),
                    'result': execution_result,
                    'strategy': strategy_name,
                    'validation': risk_validation
                })

                return execution_result
            else:
                # No execution orchestrator available - cannot execute real trades
                self.logger.error("Cannot execute trade: No execution orchestrator available")
                return ExecutionResult(
                    was_successful=False,
                    symbol=getattr(risk_validation, 'symbol', 'UNKNOWN'),
                    action='FAILED',
                    executed_amount=0.0,
                    execution_price=0.0,
                    execution_summary=f"Execution failed: No real execution system available for {getattr(risk_validation, 'symbol', 'UNKNOWN')}",
                    error_message='No execution orchestrator configured',
                    execution_slippage=0.0
                )

        except Exception as e:
            self.logger.error(f"Error executing decision: {e}")
            return ExecutionResult(
                was_successful=False,
                symbol=getattr(risk_validation, 'symbol', 'unknown'),
                action='ERROR',
                executed_amount=0.0,
                execution_price=0.0,
                execution_summary=f"Failed execution: {str(e)}",
                error_message=str(e),
                execution_slippage=0.0
            )

    def execute_approved_recommendation(self, risk_validation, signal: Dict[str, Any]) -> ExecutionResult:
        """Execute approved trading recommendation with comprehensive tracking"""
        try:
            symbol = signal['symbol']
            action = signal['action']
            confidence = signal['confidence']

            self.logger.info(f"=== EXECUTING APPROVED RECOMMENDATION ===")
            self.logger.info(f"SYMBOL: {symbol}")
            self.logger.info(f"ACTION: {action}")
            self.logger.info(f"CONFIDENCE: {confidence:.3f}")

            # BUSINESS LOGIC OUTPUT: Decision execution
            try:
                from terminal_color_system import print_decision_engine, print_xgboost, print_trade_execution
                print_decision_engine(f"Decision: {action} {symbol} - XGBoost confidence: {confidence:.2f}")
                print_xgboost(f"XGBoost decision: {action} {symbol} (confidence: {confidence:.3f})")
            except ImportError:
                print(f"BUSINESS LOGIC: Executing {action} {symbol} with confidence {confidence:.3f}")

            # Use existing execution orchestrator
            if hasattr(self, 'execution_orchestrator') and self.execution_orchestrator:
                result = self.execution_orchestrator.execute_approved_decision(
                    risk_validation=risk_validation,
                    strategy_name="news_recommendation",
                    confidence=confidence
                )

                execution_result = self._standardize_execution_result(result, risk_validation)
                
                if execution_result.was_successful:
                    self.logger.info(f"EXECUTION SUCCESS: {execution_result.execution_summary}")
                else:
                    self.logger.error(f"EXECUTION FAILED: {execution_result.error_message}")
                    
                # Business logic output for execution
                self._output_execution_result(execution_result, risk_validation)
                
                return execution_result
            else:
                self.logger.warning("Execution orchestrator not available - cannot execute recommendation")
                return ExecutionResult(
                    was_successful=False,
                    symbol=symbol,
                    action=action,
                    executed_amount=0.0,
                    execution_price=0.0,
                    execution_summary="No execution orchestrator available",
                    error_message="Execution orchestrator not configured"
                )

        except Exception as e:
            self.logger.error(f"Error executing approved recommendation: {e}")
            return ExecutionResult(
                was_successful=False,
                symbol=signal.get('symbol', 'UNKNOWN'),
                action=signal.get('action', 'ERROR'),
                executed_amount=0.0,
                execution_price=0.0,
                execution_summary=f"Execution error: {str(e)}",
                error_message=str(e)
            )

    def execute_force_close(self, symbol: str, reason: str) -> ExecutionResult:
        """Execute force close with real market execution"""
        try:
            self.logger.warning(f"Force closing {symbol}: {reason}")

            # Get market metrics for informed force close
            market_metrics = get_all_metrics(symbol)
            
            if self.execution_orchestrator:
                # Create force close execution request with market-driven parameters
                force_close_params = {
                    'symbol': symbol,
                    'action': 'FORCE_CLOSE',
                    'reason': reason,
                    'urgency': self._calculate_close_urgency(market_metrics, reason),
                    'market_conditions': self._analyze_close_market_conditions(market_metrics)
                }
                
                # Execute force close through orchestrator
                result = self.execution_orchestrator.execute_force_close(force_close_params)
                execution_result = self._standardize_execution_result(result, None)
                
                # Business logic output
                try:
                    from terminal_color_system import print_trade_execution
                    if execution_result.was_successful:
                        print_trade_execution(f"Force Close: EXECUTED {symbol} - reason: {reason}")
                    else:
                        print_trade_execution(f"Force Close: FAILED {symbol} - {execution_result.error_message}")
                except ImportError:
                    print(f"BUSINESS LOGIC: Force close {symbol} - {reason}")
                
                self.logger.info(f"Force close executed for {symbol}: {execution_result.execution_summary}")
            else:
                # No orchestrator available
                execution_result = ExecutionResult(
                    was_successful=False,
                    symbol=symbol,
                    action='FORCE_CLOSE',
                    executed_amount=0.0,
                    execution_price=0.0,
                    execution_summary=f"Force close failed: No execution system available",
                    error_message="No execution orchestrator configured"
                )

            # Remove from active tracking regardless of success
            self.force_close_symbols.discard(symbol)
            
            return execution_result

        except Exception as e:
            self.logger.error(f"Error force closing {symbol}: {e}")
            self.force_close_symbols.discard(symbol)  # Remove on error too
            return ExecutionResult(
                was_successful=False,
                symbol=symbol,
                action='FORCE_CLOSE',
                executed_amount=0.0,
                execution_price=0.0,
                execution_summary=f"Force close error: {str(e)}",
                error_message=str(e)
            )

    def track_execution_performance(self, symbol: str, execution_result: ExecutionResult, decision_data: Dict[str, Any]):
        """Track execution performance for analysis and improvement"""
        try:
            # Get market metrics for performance context
            market_metrics = get_all_metrics(symbol)
            
            performance_data = {
                'symbol': symbol,
                'timestamp': datetime.now(),
                'execution_success': execution_result.was_successful,
                'execution_slippage': execution_result.execution_slippage,
                'decision_confidence': decision_data.get('final_confidence', 0.0),
                'market_volatility': market_metrics.volatility if market_metrics and market_metrics.volatility else 0.0,
                'market_volume': market_metrics.volume if market_metrics and market_metrics.volume else 0.0,
                'strategy_name': decision_data.get('strategy_name', 'unknown')
            }
            
            # Store in execution history for analysis
            self.execution_history.append(performance_data)
            
            # Keep only recent history (last 100 executions)
            if len(self.execution_history) > 100:
                self.execution_history = self.execution_history[-100:]
            
            self.logger.debug(f"Execution performance tracked for {symbol}: success={execution_result.was_successful}")
            
        except Exception as e:
            self.logger.error(f"Error tracking execution performance for {symbol}: {e}")

    def get_execution_statistics(self) -> Dict[str, Any]:
        """Get execution statistics with market-driven analysis"""
        try:
            if not self.execution_history:
                return {
                    'total_executions': 0,
                    'success_rate': 0.0,
                    'average_slippage': 0.0,
                    'daily_count': self.daily_execution_count
                }
            
            successful_executions = [e for e in self.execution_history if e.get('execution_success', False)]
            total_executions = len(self.execution_history)
            
            # Calculate success rate
            success_rate = len(successful_executions) / total_executions if total_executions > 0 else 0.0
            
            # Calculate average slippage
            slippages = [e.get('execution_slippage', 0.0) for e in self.execution_history if 'execution_slippage' in e]
            average_slippage = sum(slippages) / len(slippages) if slippages else 0.0
            
            # Analyze by market conditions
            high_vol_executions = [e for e in self.execution_history if e.get('market_volatility', 0.0) > 0.6]
            high_vol_success_rate = len([e for e in high_vol_executions if e.get('execution_success', False)]) / len(high_vol_executions) if high_vol_executions else 0.0
            
            return {
                'total_executions': total_executions,
                'successful_executions': len(successful_executions),
                'success_rate': success_rate,
                'average_slippage': average_slippage,
                'daily_count': self.daily_execution_count,
                'high_volatility_success_rate': high_vol_success_rate,
                'recent_performance': self._calculate_recent_performance()
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating execution statistics: {e}")
            return {'error': str(e)}

    def _standardize_execution_result(self, result, risk_validation) -> ExecutionResult:
        """Convert orchestrator result to standardized ExecutionResult"""
        try:
            if hasattr(result, '__dict__'):
                # Real result object
                return ExecutionResult(
                    was_successful=getattr(result, 'was_successful', False),
                    symbol=getattr(result, 'symbol', getattr(risk_validation, 'symbol', 'UNKNOWN')),
                    action=getattr(result, 'action', 'UNKNOWN'),
                    executed_amount=getattr(result, 'executed_amount', 0.0),
                    execution_price=getattr(result, 'execution_price', 0.0),
                    execution_summary=getattr(result, 'execution_summary', 'No summary'),
                    error_message=getattr(result, 'error_message', None),
                    execution_slippage=getattr(result, 'execution_slippage', 0.0),
                    order_id=getattr(result, 'order_id', None),
                    execution_time=getattr(result, 'execution_time', datetime.now())
                )
            else:
                # Fallback for simple result
                return ExecutionResult(
                    was_successful=False,
                    symbol='UNKNOWN',
                    action='UNKNOWN',
                    executed_amount=0.0,
                    execution_price=0.0,
                    execution_summary='Unknown result format',
                    error_message='Result format not recognized'
                )
        except Exception as e:
            self.logger.error(f"Error standardizing execution result: {e}")
            return ExecutionResult(
                was_successful=False,
                symbol='ERROR',
                action='ERROR',
                executed_amount=0.0,
                execution_price=0.0,
                execution_summary=f"Standardization error: {str(e)}",
                error_message=str(e)
            )

    def _output_execution_result(self, execution_result: ExecutionResult, risk_validation):
        """Output execution result for business logic visibility"""
        try:
            if execution_result.was_successful:
                # BUSINESS LOGIC OUTPUT: Trade execution success
                try:
                    from terminal_color_system import print_trade_execution
                    action = getattr(risk_validation.original_decision, 'action', execution_result.action)
                    print_trade_execution(f"Execution: {action} {execution_result.executed_amount} {execution_result.symbol} at ${execution_result.execution_price}")
                except ImportError:
                    print(f"[EXECUTION] {execution_result.action} {execution_result.executed_amount} {execution_result.symbol} at ${execution_result.execution_price}")
                    
            else:
                # BUSINESS LOGIC OUTPUT: Trade execution failure  
                try:
                    from terminal_color_system import print_trade_execution
                    print_trade_execution(f"Execution: FAILED {execution_result.symbol} - {execution_result.error_message}")
                except ImportError:
                    print(f"[EXECUTION] FAILED {execution_result.symbol} - {execution_result.error_message}")
                    
        except Exception as e:
            self.logger.error(f"Error outputting execution result: {e}")

    def _calculate_close_urgency(self, market_metrics, reason: str) -> str:
        """Calculate urgency level for force close based on market conditions"""
        try:
            # Base urgency on reason
            urgency_map = {
                'risk_limit_exceeded': 'HIGH',
                'stop_loss_triggered': 'HIGH',
                'margin_call': 'CRITICAL',
                'strategy_failure': 'MEDIUM',
                'user_request': 'LOW',
                'market_close': 'MEDIUM'
            }
            
            base_urgency = urgency_map.get(reason.lower(), 'MEDIUM')
            
            # Adjust based on market conditions
            if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility:
                if market_metrics.volatility > 0.8:  # Very high volatility
                    if base_urgency == 'LOW':
                        return 'MEDIUM'
                    elif base_urgency == 'MEDIUM':
                        return 'HIGH'
                # HIGH and CRITICAL stay the same in high volatility
            
            return base_urgency
            
        except Exception as e:
            self.logger.error(f"Error calculating close urgency: {e}")
            return 'MEDIUM'

    def _analyze_close_market_conditions(self, market_metrics) -> Dict[str, Any]:
        """Analyze market conditions for informed force close execution"""
        try:
            if not market_metrics:
                return {'analysis': 'no_data', 'liquidity': 'unknown', 'timing': 'standard'}
            
            conditions = {}
            
            # Liquidity analysis
            if hasattr(market_metrics, 'volume') and market_metrics.volume:
                if market_metrics.volume > 1000000:  # High volume
                    conditions['liquidity'] = 'high'
                elif market_metrics.volume > 100000:  # Medium volume
                    conditions['liquidity'] = 'medium'
                else:
                    conditions['liquidity'] = 'low'
            else:
                conditions['liquidity'] = 'unknown'
            
            # Volatility impact
            if hasattr(market_metrics, 'volatility') and market_metrics.volatility:
                if market_metrics.volatility > 0.7:
                    conditions['volatility_impact'] = 'high_slippage_expected'
                elif market_metrics.volatility > 0.4:
                    conditions['volatility_impact'] = 'moderate_slippage'
                else:
                    conditions['volatility_impact'] = 'low_slippage'
            
            # Timing recommendation
            if conditions.get('liquidity') == 'high' and conditions.get('volatility_impact') == 'low_slippage':
                conditions['timing'] = 'optimal'
            elif conditions.get('liquidity') == 'low' or conditions.get('volatility_impact') == 'high_slippage_expected':
                conditions['timing'] = 'challenging'
            else:
                conditions['timing'] = 'standard'
            
            return conditions
            
        except Exception as e:
            self.logger.error(f"Error analyzing close market conditions: {e}")
            return {'analysis': 'error', 'timing': 'standard'}

    def _calculate_recent_performance(self) -> Dict[str, float]:
        """Calculate recent execution performance metrics"""
        try:
            # Look at last 20 executions or last 24 hours, whichever is smaller
            recent_cutoff = datetime.now().timestamp() - 86400  # 24 hours ago
            recent_executions = [
                e for e in self.execution_history[-20:] 
                if e.get('timestamp', datetime.now()).timestamp() > recent_cutoff
            ]
            
            if not recent_executions:
                return {'success_rate': 0.0, 'avg_slippage': 0.0, 'count': 0}
            
            successful = len([e for e in recent_executions if e.get('execution_success', False)])
            total = len(recent_executions)
            
            slippages = [e.get('execution_slippage', 0.0) for e in recent_executions]
            avg_slippage = sum(slippages) / len(slippages) if slippages else 0.0
            
            return {
                'success_rate': successful / total if total > 0 else 0.0,
                'avg_slippage': avg_slippage,
                'count': total
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating recent performance: {e}")
            return {'success_rate': 0.0, 'avg_slippage': 0.0, 'count': 0}

    def reset_daily_counters(self):
        """Reset daily execution counters"""
        self.daily_execution_count = 0
        self.logger.debug("Daily execution counters reset")