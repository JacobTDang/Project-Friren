"""
portfolio_manager/processes/market_regime_detector.py

Market Regime Detection Process - Independent Market Analysis
"""

import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging
import pandas as pd
import sys
import os

# Add project root for color system import
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import color system for market regime detection (dark yellow)
try:
    from terminal_color_system import print_decision_engine, print_error, print_warning, print_success, create_colored_logger
    COLOR_SYSTEM_AVAILABLE = True
except ImportError:
    COLOR_SYSTEM_AVAILABLE = False

from Friren_V1.multiprocess_infrastructure.redis_base_process import RedisBaseProcess, ProcessState
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
)

# Import market analysis components
from ..analytics.market_analyzer import MarketAnalyzer, MarketRegimeResult
from ...data.data_utils import StockDataTools
from ...data.yahoo_price import YahooFinancePriceData


class MarketRegimeDetector(RedisBaseProcess):
    """
    Independent Market Regime Detection Process

    Runs market regime analysis on major indices and updates shared state
    with current market regime information for use by other processes.
    """

    def __init__(self, process_id: str = "market_regime_detector",
                 check_interval: int = 300,  # 5 minutes
                 symbols: List[str] = None):
        super().__init__(process_id)

        self.check_interval = check_interval
        self.symbols = symbols or ['SPY', 'QQQ', 'IWM', '^VIX']  # Major indices + VIX
        self.last_check_time = None
        self.regime_checks_count = 0
        self.market_analyzer = None
        self.data_tools = None
        self.current_regime = 'UNKNOWN'
        self.regime_confidence = 0.0
        self.last_regime_update = None

    def _initialize(self):
        self.logger.critical("EMERGENCY: ENTERED _initialize for market_regime_detector")
        print("EMERGENCY: ENTERED _initialize for market_regime_detector")
        """Initialize market regime detection components"""
        try:
            # Initialize data fetcher for getting market data
            self.data_fetcher = YahooFinancePriceData()
            self.logger.info("YahooFinancePriceData initialized")

            # Initialize market analyzer
            self.market_analyzer = MarketAnalyzer()
            self.logger.info("MarketAnalyzer initialized")

            # Initialize data tools for technical indicators
            self.data_tools = StockDataTools()
            self.logger.info("StockDataTools initialized")

            self.state = ProcessState.RUNNING
            self.logger.info("MarketRegimeDetector initialization complete")

        except Exception as e:
            self.logger.error(f"Failed to initialize MarketRegimeDetector: {e}")
            self.state = ProcessState.ERROR
            raise
        self.logger.critical("EMERGENCY: EXITING _initialize for market_regime_detector")
        print("EMERGENCY: EXITING _initialize for market_regime_detector")

    def _execute(self):
        """Execute main process logic (required by RedisBaseProcess)"""
        self._process_cycle()

    def _process_cycle(self):
        self.logger.critical("EMERGENCY: Market regime detector main loop running - attempting to analyze and update regime")
        print("EMERGENCY: Market regime detector main loop running - attempting to analyze and update regime")
        """Main processing cycle for market regime detection"""
        
        # BUSINESS LOGIC VERIFICATION: Test Redis communication and data access
        try:
            self.logger.info("BUSINESS LOGIC TEST: Verifying market regime detector functionality...")
            
            # Test 1: Redis communication
            if self.redis_manager:
                test_data = {"regime_test": "market_regime_detector_active", "timestamp": datetime.now().isoformat()}
                self.redis_manager.set_shared_state("regime_detector_status", test_data)
                self.logger.info("✓ BUSINESS LOGIC: Redis state update successful")
            
            # Test 2: Data fetcher availability
            if hasattr(self, 'data_fetcher') and self.data_fetcher:
                self.logger.info("✓ BUSINESS LOGIC: Data fetcher available")
            else:
                self.logger.error("✗ BUSINESS LOGIC: Data fetcher not available")
            
            # Test 3: Market analyzer availability
            if hasattr(self, 'market_analyzer') and self.market_analyzer:
                self.logger.info("✓ BUSINESS LOGIC: Market analyzer available")
            else:
                self.logger.error("✗ BUSINESS LOGIC: Market analyzer not available")
                
        except Exception as e:
            self.logger.error(f"✗ BUSINESS LOGIC TEST FAILED: {e}")
        
        try:
            # Check if it's time for regime analysis
            if not self._should_run_regime_check():
                time.sleep(10)
                return

            self.logger.info(f"Starting market regime analysis cycle #{self.regime_checks_count + 1}")
            start_time = time.time()

            # Fetch market data for analysis
            market_data = self._fetch_market_data()

            if not market_data:
                self.logger.warning("No market data available for regime analysis")
                time.sleep(60)
                return

            # Perform market regime analysis
            regime_result = self._analyze_market_regime(market_data)

            # Update shared state with regime information
            self._update_shared_state(regime_result)

            # Update process state
            self.last_check_time = datetime.now()
            self.regime_checks_count += 1
            self.current_regime = regime_result.primary_regime
            self.regime_confidence = regime_result.regime_confidence
            self.last_regime_update = datetime.now()

            # Log cycle completion
            cycle_time = time.time() - start_time
            self.logger.info(f"Market regime analysis complete - {cycle_time:.2f}s, "
                           f"Regime: {regime_result.primary_regime} "
                           f"(Confidence: {regime_result.regime_confidence:.1f}%)")

        except Exception as e:
            self.logger.error(f"Error in market regime detection cycle: {e}")
            self.error_count += 1
            time.sleep(30)

    def _should_run_regime_check(self) -> bool:
        """Check if it's time to run regime analysis"""
        if self.last_check_time is None:
            return True

        time_since_last = (datetime.now() - self.last_check_time).total_seconds()
        return time_since_last >= self.check_interval

    def _fetch_market_data(self) -> Optional[pd.DataFrame]:
        """Fetch market data for regime analysis"""
        try:
            # Use data fetcher to get SPY data as the primary market indicator
            df = self.data_fetcher.extract_data("SPY", period="100d", interval="1d")

            if df is None or df.empty:
                self.logger.warning("No SPY data available")
                return None

            # Add technical indicators using data tools
            df = self.data_tools.add_all_regime_indicators(df)

            self.logger.debug(f"Fetched {len(df)} days of SPY data for regime analysis")
            return df

        except Exception as e:
            self.logger.error(f"Error fetching market data: {e}")
            return None

    def _analyze_market_regime(self, market_data: pd.DataFrame) -> MarketRegimeResult:
        """Analyze market regime using MarketAnalyzer"""
        try:
            # Perform comprehensive market regime analysis
            regime_result = self.market_analyzer.analyze_regime(market_data)

            self.logger.info(f"Market regime analysis complete: {regime_result.primary_regime} "
                           f"(Confidence: {regime_result.regime_confidence:.1f}%)")

            return regime_result

        except Exception as e:
            self.logger.error(f"Error in market regime analysis: {e}")
            # Return default regime result
            return MarketRegimeResult(
                primary_regime='UNKNOWN',
                regime_confidence=0.0,
                trend='UNKNOWN',
                trend_strength=0.0,
                volatility_regime='UNKNOWN',
                current_volatility=0.15,
                rsi_condition='NEUTRAL',
                current_rsi=50.0,
                enhanced_regime='UNKNOWN',
                enhanced_confidence=0.0,
                entropy_regime='UNKNOWN',
                entropy_confidence=0.0,
                entropy_measures={},
                regime_persistence=0.0,
                regime_transition_probability=0.5,
                regime_scores={},
                market_stress_level=50.0,
                regime_consistency=0.0
            )

    def _update_shared_state(self, regime_result: MarketRegimeResult):
        """Update Redis shared state with market regime information"""
        try:
            if self.redis_manager:
                # Update market regime in Redis shared state
                regime_data = {
                    'regime': regime_result.primary_regime,
                    'confidence': regime_result.regime_confidence,
                    'trend': regime_result.trend,
                    'trend_strength': regime_result.trend_strength,
                    'volatility_regime': regime_result.volatility_regime,
                    'current_volatility': regime_result.current_volatility,
                    'rsi_condition': regime_result.rsi_condition,
                    'current_rsi': regime_result.current_rsi,
                    'enhanced_regime': regime_result.enhanced_regime,
                    'enhanced_confidence': regime_result.enhanced_confidence,
                    'entropy_regime': regime_result.entropy_regime,
                    'entropy_confidence': regime_result.entropy_confidence,
                    'market_stress_level': regime_result.market_stress_level,
                    'regime_consistency': regime_result.regime_consistency,
                    'last_update': datetime.now().isoformat()
                }

                self.redis_manager.set_shared_state('market_regime', regime_data)
                self.redis_manager.set_shared_state('last_regime_update', datetime.now().isoformat())

                self.logger.debug(f"Updated Redis shared state with market regime: {regime_result.primary_regime}")
                
                # BUSINESS LOGIC VERIFICATION: Confirm Redis update worked
                verification = self.redis_manager.get_shared_state('market_regime')
                if verification and verification.get('regime') == regime_result.primary_regime:
                    self.logger.info("✓ BUSINESS LOGIC: Market regime Redis update verified successfully")
                else:
                    self.logger.error("✗ BUSINESS LOGIC: Market regime Redis update verification failed")

        except Exception as e:
            self.logger.error(f"Error updating Redis shared state: {e}")

    def _cleanup(self):
        """Cleanup resources"""
        self.logger.info("Cleaning up MarketRegimeDetector...")
        self.logger.info("MarketRegimeDetector cleanup complete")

    def get_process_info(self) -> Dict[str, Any]:
        """Get process information"""
        return {
            'process_id': self.process_id,
            'state': self.state.value,
            'uptime_seconds': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
            'regime_checks_count': self.regime_checks_count,
            'current_regime': self.current_regime,
            'regime_confidence': self.regime_confidence,
            'last_regime_update': self.last_regime_update.isoformat() if self.last_regime_update else None,
            'error_count': self.error_count,
            'symbols_monitored': self.symbols
        }
