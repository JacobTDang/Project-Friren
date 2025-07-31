"""
Enhanced Business Intelligence Module for Friren Trading System
==============================================================

Implements sophisticated business logic intelligence including advanced market
calculations, intelligent risk metrics, dynamic strategy optimization, and
real-time portfolio analytics. This module replaces hardcoded values with
intelligent algorithms based on real market data.

Phase 10.3: Enhanced Business Logic Intelligence
"""

import numpy as np
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import statistics
from collections import deque


@dataclass
class MarketIntelligence:
    """Advanced market intelligence analysis result"""
    symbol: str
    
    # Price Intelligence
    trend_strength: float          # 0-100, strength of current trend
    trend_direction: str          # BULLISH, BEARISH, SIDEWAYS
    momentum_score: float         # -100 to +100, momentum indicator
    volatility_regime: str        # LOW, NORMAL, HIGH, EXTREME
    volatility_percentile: float  # 0-100, current vs historical volatility
    
    # Support/Resistance Intelligence
    support_levels: List[float]    # Dynamic support levels
    resistance_levels: List[float] # Dynamic resistance levels
    current_price_position: float # 0-100, where price sits in range
    
    # Volume Intelligence
    volume_trend: str             # INCREASING, DECREASING, STABLE
    volume_strength: float        # 0-100, relative volume strength
    accumulation_score: float     # -100 to +100, accumulation/distribution
    
    # Market Regime Intelligence
    regime_classification: str     # RISK_ON, RISK_OFF, TRANSITIONAL
    sector_momentum: float        # -100 to +100, sector relative performance
    correlation_breakdown: bool   # True if correlations are breaking down
    
    # Timing Intelligence
    optimal_entry_score: float    # 0-100, entry timing quality
    optimal_exit_score: float     # 0-100, exit timing quality
    hold_duration_estimate: int   # Days, estimated optimal holding period
    
    # Risk Intelligence
    tail_risk_indicator: float    # 0-100, probability of extreme moves
    liquidity_score: float        # 0-100, market liquidity assessment
    gap_risk: float              # 0-100, overnight/weekend gap risk
    
    confidence: float             # 0-100, confidence in analysis
    timestamp: datetime
    calculation_time_ms: float


@dataclass
class PortfolioIntelligence:
    """Advanced portfolio intelligence analysis"""
    
    # Performance Intelligence
    risk_adjusted_return: float   # Sharpe-like ratio with dynamic risk-free rate
    information_ratio: float      # Alpha generation efficiency
    sortino_ratio: float          # Downside risk-adjusted return
    calmar_ratio: float           # Return vs max drawdown
    
    # Risk Intelligence
    portfolio_var: float          # Dynamic VaR based on current conditions
    component_var: Dict[str, float] # Individual position VaR contributions
    expected_shortfall: float     # Conditional VaR (CVaR)
    risk_attribution: Dict[str, float] # Risk sources breakdown
    
    # Diversification Intelligence
    effective_holdings: float     # Number of effective independent positions
    concentration_score: float   # 0-100, portfolio concentration risk
    sector_balance_score: float  # 0-100, sector diversification quality
    correlation_stability: float # 0-100, how stable are correlations
    
    # Alpha Intelligence
    alpha_generation: float       # Excess return vs benchmark
    alpha_consistency: float     # Consistency of alpha generation
    alpha_sources: Dict[str, float] # Where alpha is coming from
    
    # Regime Adaptability
    regime_sensitivity: float    # How sensitive portfolio is to regime changes
    adaptability_score: float   # 0-100, how well portfolio adapts
    
    timestamp: datetime


class TrendAnalyzer:
    """Advanced trend analysis with multiple timeframes"""
    
    @staticmethod
    def analyze_trend_strength(prices: pd.Series, volumes: pd.Series = None) -> Tuple[float, str]:
        """
        Analyze trend strength using multiple indicators
        
        Returns:
            (trend_strength, trend_direction)
        """
        try:
            if len(prices) < 20:
                return 50.0, "SIDEWAYS"
            
            # Multiple moving averages for trend identification
            sma_10 = prices.rolling(10).mean()
            sma_20 = prices.rolling(20).mean()
            sma_50 = prices.rolling(50).mean() if len(prices) >= 50 else sma_20
            
            current_price = prices.iloc[-1]
            
            # Trend direction based on moving average alignment
            bullish_alignment = (current_price > sma_10.iloc[-1] > sma_20.iloc[-1] > sma_50.iloc[-1])
            bearish_alignment = (current_price < sma_10.iloc[-1] < sma_20.iloc[-1] < sma_50.iloc[-1])
            
            if bullish_alignment:
                trend_direction = "BULLISH"
            elif bearish_alignment:
                trend_direction = "BEARISH"
            else:
                trend_direction = "SIDEWAYS"
            
            # Calculate trend strength using multiple factors
            factors = []
            
            # Factor 1: Price momentum
            if len(prices) >= 10:
                momentum = (current_price - prices.iloc[-10]) / prices.iloc[-10]
                factors.append(min(100, abs(momentum) * 500))  # Scale to 0-100
            
            # Factor 2: Moving average slope consistency
            if len(sma_20) >= 5:
                ma_slopes = []
                for i in range(1, 6):
                    slope = (sma_20.iloc[-i] - sma_20.iloc[-i-1]) / sma_20.iloc[-i-1]
                    ma_slopes.append(slope)
                
                slope_consistency = 1.0 - np.std(ma_slopes) / (np.mean(np.abs(ma_slopes)) + 1e-6)
                factors.append(slope_consistency * 100)
            
            # Factor 3: Volume confirmation (if available)
            if volumes is not None and len(volumes) >= 10:
                recent_volume = volumes.iloc[-5:].mean()
                historical_volume = volumes.iloc[-20:-5].mean()
                volume_factor = min(100, (recent_volume / historical_volume) * 50)
                factors.append(volume_factor)
            
            # Factor 4: Price range expansion/contraction
            if len(prices) >= 20:
                recent_range = prices.iloc[-10:].max() - prices.iloc[-10:].min()
                historical_range = prices.iloc[-20:-10].max() - prices.iloc[-20:-10].min()
                range_factor = min(100, (recent_range / (historical_range + 1e-6)) * 50)
                factors.append(range_factor)
            
            trend_strength = np.mean(factors) if factors else 50.0
            
            return min(100.0, max(0.0, trend_strength)), trend_direction
            
        except Exception as e:
            logging.getLogger(__name__).error(f"Error in trend analysis: {e}")
            return 50.0, "SIDEWAYS"


class VolatilityAnalyzer:
    """Advanced volatility analysis with regime detection"""
    
    @staticmethod
    def analyze_volatility_regime(prices: pd.Series, returns: pd.Series = None) -> Tuple[str, float]:
        """
        Analyze volatility regime and percentile
        
        Returns:
            (volatility_regime, volatility_percentile)
        """
        try:
            if returns is None:
                returns = prices.pct_change().dropna()
            
            if len(returns) < 20:
                return "NORMAL", 50.0
            
            # Calculate current volatility (annualized)
            current_vol = returns.iloc[-10:].std() * np.sqrt(252)
            
            # Historical volatility distribution
            if len(returns) >= 252:  # Need at least 1 year of data
                rolling_vol = returns.rolling(20).std() * np.sqrt(252)
                vol_percentile = (rolling_vol <= current_vol).mean() * 100
            else:
                # Use available data for percentile calculation
                rolling_vol = returns.expanding(min_periods=10).std() * np.sqrt(252)
                vol_percentile = (rolling_vol <= current_vol).mean() * 100
            
            # Classify volatility regime
            if vol_percentile >= 90:
                regime = "EXTREME"
            elif vol_percentile >= 75:
                regime = "HIGH"
            elif vol_percentile <= 25:
                regime = "LOW"
            else:
                regime = "NORMAL"
            
            return regime, vol_percentile
            
        except Exception as e:
            logging.getLogger(__name__).error(f"Error in volatility analysis: {e}")
            return "NORMAL", 50.0


class SupportResistanceAnalyzer:
    """Advanced support and resistance level detection"""
    
    @staticmethod
    def find_dynamic_levels(prices: pd.Series, volumes: pd.Series = None) -> Tuple[List[float], List[float]]:
        """
        Find dynamic support and resistance levels
        
        Returns:
            (support_levels, resistance_levels)
        """
        try:
            if len(prices) < 50:
                current_price = prices.iloc[-1]
                return [current_price * 0.95], [current_price * 1.05]
            
            # Find local peaks and troughs
            highs = []
            lows = []
            
            for i in range(2, len(prices) - 2):
                # Local high
                if (prices.iloc[i] > prices.iloc[i-1] and prices.iloc[i] > prices.iloc[i+1] and
                    prices.iloc[i] > prices.iloc[i-2] and prices.iloc[i] > prices.iloc[i+2]):
                    weight = 1.0
                    if volumes is not None:
                        # Weight by volume
                        avg_volume = volumes.iloc[i-5:i+5].mean()
                        weight = volumes.iloc[i] / avg_volume if avg_volume > 0 else 1.0
                    highs.append((prices.iloc[i], weight))
                
                # Local low
                if (prices.iloc[i] < prices.iloc[i-1] and prices.iloc[i] < prices.iloc[i+1] and
                    prices.iloc[i] < prices.iloc[i-2] and prices.iloc[i] < prices.iloc[i+2]):
                    weight = 1.0
                    if volumes is not None:
                        avg_volume = volumes.iloc[i-5:i+5].mean()
                        weight = volumes.iloc[i] / avg_volume if avg_volume > 0 else 1.0
                    lows.append((prices.iloc[i], weight))
            
            # Cluster levels and weight by importance
            current_price = prices.iloc[-1]
            price_range = prices.max() - prices.min()
            cluster_threshold = price_range * 0.02  # 2% clustering
            
            # Find resistance levels (above current price)
            resistance_candidates = [h[0] for h in highs if h[0] > current_price]
            resistance_levels = SupportResistanceAnalyzer._cluster_levels(
                resistance_candidates, cluster_threshold, max_levels=3
            )
            
            # Find support levels (below current price)
            support_candidates = [l[0] for l in lows if l[0] < current_price]
            support_levels = SupportResistanceAnalyzer._cluster_levels(
                support_candidates, cluster_threshold, max_levels=3
            )
            
            # Add psychological levels
            support_levels.extend(SupportResistanceAnalyzer._get_psychological_levels(current_price, "support"))
            resistance_levels.extend(SupportResistanceAnalyzer._get_psychological_levels(current_price, "resistance"))
            
            # Sort and limit
            support_levels = sorted(set(support_levels), reverse=True)[:3]
            resistance_levels = sorted(set(resistance_levels))[:3]
            
            return support_levels, resistance_levels
            
        except Exception as e:
            logging.getLogger(__name__).error(f"Error in support/resistance analysis: {e}")
            current_price = prices.iloc[-1] if len(prices) > 0 else 100.0
            return [current_price * 0.95], [current_price * 1.05]
    
    @staticmethod
    def _cluster_levels(levels: List[float], threshold: float, max_levels: int = 3) -> List[float]:
        """Cluster nearby levels together"""
        if not levels:
            return []
        
        clustered = []
        sorted_levels = sorted(levels)
        
        current_cluster = [sorted_levels[0]]
        
        for level in sorted_levels[1:]:
            if level - current_cluster[-1] <= threshold:
                current_cluster.append(level)
            else:
                # Finalize current cluster
                clustered.append(np.mean(current_cluster))
                current_cluster = [level]
        
        # Add final cluster
        clustered.append(np.mean(current_cluster))
        
        return clustered[:max_levels]
    
    @staticmethod
    def _get_psychological_levels(price: float, level_type: str) -> List[float]:
        """Get psychological support/resistance levels"""
        psychological = []
        
        # Round number levels
        if price > 100:
            base = int(price / 10) * 10
            if level_type == "support":
                psychological.extend([base - 10, base])
            else:
                psychological.extend([base + 10, base + 20])
        elif price > 10:
            base = int(price)
            if level_type == "support":
                psychological.extend([base - 1, base])
            else:
                psychological.extend([base + 1, base + 2])
        
        return [p for p in psychological if (p < price if level_type == "support" else p > price)]


class VolumeAnalyzer:
    """Advanced volume analysis for market intelligence"""
    
    @staticmethod
    def analyze_volume_intelligence(volumes: pd.Series, prices: pd.Series = None) -> Tuple[str, float, float]:
        """
        Analyze volume patterns and strength
        
        Returns:
            (volume_trend, volume_strength, accumulation_score)
        """
        try:
            if len(volumes) < 10:
                return "STABLE", 50.0, 0.0
            
            # Volume trend analysis
            recent_avg = volumes.iloc[-5:].mean()
            historical_avg = volumes.iloc[-20:-5].mean() if len(volumes) >= 20 else volumes.iloc[:-5].mean()
            
            if recent_avg > historical_avg * 1.2:
                volume_trend = "INCREASING"
            elif recent_avg < historical_avg * 0.8:
                volume_trend = "DECREASING"
            else:
                volume_trend = "STABLE"
            
            # Volume strength (relative to historical)
            if len(volumes) >= 50:
                volume_percentile = (volumes.iloc[-50:] <= volumes.iloc[-1]).mean() * 100
            else:
                volume_percentile = (volumes <= volumes.iloc[-1]).mean() * 100
            
            volume_strength = volume_percentile
            
            # Accumulation/Distribution analysis
            accumulation_score = 0.0
            if prices is not None and len(prices) == len(volumes):
                # On Balance Volume (OBV) concept
                price_changes = prices.pct_change()
                volume_weighted_changes = []
                
                for i in range(1, min(20, len(prices))):
                    if price_changes.iloc[-i] > 0:
                        volume_weighted_changes.append(volumes.iloc[-i])
                    elif price_changes.iloc[-i] < 0:
                        volume_weighted_changes.append(-volumes.iloc[-i])
                
                if volume_weighted_changes:
                    accumulation_score = np.mean(volume_weighted_changes)
                    # Normalize to -100 to +100 scale
                    max_volume = volumes.max()
                    accumulation_score = (accumulation_score / max_volume) * 100
                    accumulation_score = max(-100, min(100, accumulation_score))
            
            return volume_trend, volume_strength, accumulation_score
            
        except Exception as e:
            logging.getLogger(__name__).error(f"Error in volume analysis: {e}")
            return "STABLE", 50.0, 0.0


class EnhancedBusinessIntelligence:
    """
    Enhanced Business Intelligence Engine
    
    Provides sophisticated market analysis and intelligence calculations
    using advanced algorithms and real market data. Replaces hardcoded
    values with intelligent, data-driven insights.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Initialize analyzers
        self.trend_analyzer = TrendAnalyzer()
        self.volatility_analyzer = VolatilityAnalyzer()
        self.support_resistance_analyzer = SupportResistanceAnalyzer()
        self.volume_analyzer = VolumeAnalyzer()
        
        # Intelligence cache
        self.intelligence_cache: Dict[str, MarketIntelligence] = {}
        self.cache_ttl_minutes = 5
        
        # Performance tracking
        self.calculation_times = deque(maxlen=100)
        
    def analyze_market_intelligence(self, symbol: str, price_data: pd.DataFrame,
                                  market_data: Optional[Dict] = None) -> MarketIntelligence:
        """
        Perform comprehensive market intelligence analysis
        
        Args:
            symbol: Stock symbol
            price_data: DataFrame with OHLCV data
            market_data: Additional market context data
            
        Returns:
            Comprehensive market intelligence analysis
        """
        start_time = time.time()
        
        try:
            # Check cache first
            if symbol in self.intelligence_cache:
                cached = self.intelligence_cache[symbol]
                cache_age = (datetime.now() - cached.timestamp).total_seconds() / 60
                if cache_age < self.cache_ttl_minutes:
                    return cached
            
            # Extract price and volume series
            prices = price_data['Close'] if 'Close' in price_data.columns else price_data.iloc[:, 0]
            volumes = price_data['Volume'] if 'Volume' in price_data.columns else None
            
            # Trend Intelligence
            trend_strength, trend_direction = self.trend_analyzer.analyze_trend_strength(prices, volumes)
            momentum_score = self._calculate_momentum_score(prices)
            
            # Volatility Intelligence
            volatility_regime, volatility_percentile = self.volatility_analyzer.analyze_volatility_regime(prices)
            
            # Support/Resistance Intelligence
            support_levels, resistance_levels = self.support_resistance_analyzer.find_dynamic_levels(prices, volumes)
            current_price_position = self._calculate_price_position(prices.iloc[-1], support_levels, resistance_levels)
            
            # Volume Intelligence
            if volumes is not None:
                volume_trend, volume_strength, accumulation_score = self.volume_analyzer.analyze_volume_intelligence(volumes, prices)
            else:
                volume_trend, volume_strength, accumulation_score = "STABLE", 50.0, 0.0
            
            # Market Regime Intelligence
            regime_classification = self._classify_market_regime(prices, volatility_regime, market_data)
            sector_momentum = self._calculate_sector_momentum(symbol, market_data)
            correlation_breakdown = self._detect_correlation_breakdown(symbol, market_data)
            
            # Timing Intelligence
            optimal_entry_score = self._calculate_entry_timing_score(prices, volumes, trend_strength, volatility_percentile)
            optimal_exit_score = self._calculate_exit_timing_score(prices, trend_strength, support_levels, resistance_levels)
            hold_duration_estimate = self._estimate_optimal_holding_period(trend_strength, volatility_regime)
            
            # Risk Intelligence
            tail_risk_indicator = self._calculate_tail_risk(prices)
            liquidity_score = self._assess_liquidity(volumes, market_data)
            gap_risk = self._calculate_gap_risk(prices, volatility_regime)
            
            # Overall confidence
            confidence = self._calculate_analysis_confidence(len(prices), volumes is not None, market_data is not None)
            
            # Create intelligence result
            calculation_time = (time.time() - start_time) * 1000
            
            intelligence = MarketIntelligence(
                symbol=symbol,
                trend_strength=trend_strength,
                trend_direction=trend_direction,
                momentum_score=momentum_score,
                volatility_regime=volatility_regime,
                volatility_percentile=volatility_percentile,
                support_levels=support_levels,
                resistance_levels=resistance_levels,
                current_price_position=current_price_position,
                volume_trend=volume_trend,
                volume_strength=volume_strength,
                accumulation_score=accumulation_score,
                regime_classification=regime_classification,
                sector_momentum=sector_momentum,
                correlation_breakdown=correlation_breakdown,
                optimal_entry_score=optimal_entry_score,
                optimal_exit_score=optimal_exit_score,
                hold_duration_estimate=hold_duration_estimate,
                tail_risk_indicator=tail_risk_indicator,
                liquidity_score=liquidity_score,
                gap_risk=gap_risk,
                confidence=confidence,
                timestamp=datetime.now(),
                calculation_time_ms=calculation_time
            )
            
            # Cache result
            self.intelligence_cache[symbol] = intelligence
            self.calculation_times.append(calculation_time)
            
            self.logger.debug(f"Market intelligence calculated for {symbol} in {calculation_time:.1f}ms")
            
            return intelligence
            
        except Exception as e:
            self.logger.error(f"Error calculating market intelligence for {symbol}: {e}")
            
            # Return conservative default intelligence
            return MarketIntelligence(
                symbol=symbol,
                trend_strength=50.0,
                trend_direction="SIDEWAYS",
                momentum_score=0.0,
                volatility_regime="NORMAL",
                volatility_percentile=50.0,
                support_levels=[],
                resistance_levels=[],
                current_price_position=50.0,
                volume_trend="STABLE",
                volume_strength=50.0,
                accumulation_score=0.0,
                regime_classification="TRANSITIONAL",
                sector_momentum=0.0,
                correlation_breakdown=False,
                optimal_entry_score=50.0,
                optimal_exit_score=50.0,
                hold_duration_estimate=5,
                tail_risk_indicator=50.0,
                liquidity_score=50.0,
                gap_risk=30.0,
                confidence=0.0,
                timestamp=datetime.now(),
                calculation_time_ms=(time.time() - start_time) * 1000
            )
    
    def _calculate_momentum_score(self, prices: pd.Series) -> float:
        """Calculate momentum score using multiple timeframes"""
        try:
            if len(prices) < 20:
                return 0.0
            
            momentum_scores = []
            
            # Short-term momentum (5 days)
            if len(prices) >= 5:
                short_momentum = (prices.iloc[-1] / prices.iloc[-5] - 1) * 100
                momentum_scores.append(short_momentum)
            
            # Medium-term momentum (10 days)
            if len(prices) >= 10:
                medium_momentum = (prices.iloc[-1] / prices.iloc[-10] - 1) * 100
                momentum_scores.append(medium_momentum * 0.7)  # Weight less
            
            # Long-term momentum (20 days)
            if len(prices) >= 20:
                long_momentum = (prices.iloc[-1] / prices.iloc[-20] - 1) * 100
                momentum_scores.append(long_momentum * 0.5)  # Weight least
            
            overall_momentum = np.mean(momentum_scores) if momentum_scores else 0.0
            
            # Normalize to -100 to +100 scale
            return max(-100, min(100, overall_momentum))
            
        except Exception:
            return 0.0
    
    def _calculate_price_position(self, current_price: float, support_levels: List[float], 
                                resistance_levels: List[float]) -> float:
        """Calculate where current price sits in the support/resistance range"""
        try:
            if not support_levels or not resistance_levels:
                return 50.0
            
            nearest_support = max(support_levels) if support_levels else current_price * 0.9
            nearest_resistance = min(resistance_levels) if resistance_levels else current_price * 1.1
            
            if nearest_resistance <= nearest_support:
                return 50.0
            
            position = ((current_price - nearest_support) / (nearest_resistance - nearest_support)) * 100
            return max(0, min(100, position))
            
        except Exception:
            return 50.0
    
    def _classify_market_regime(self, prices: pd.Series, volatility_regime: str, 
                              market_data: Optional[Dict]) -> str:
        """Classify current market regime"""
        try:
            # Factor 1: Volatility regime
            vol_score = {"LOW": -1, "NORMAL": 0, "HIGH": 1, "EXTREME": 2}[volatility_regime]
            
            # Factor 2: Price trend
            if len(prices) >= 20:
                trend = (prices.iloc[-1] / prices.iloc[-20] - 1)
                trend_score = 1 if trend > 0.05 else -1 if trend < -0.05 else 0
            else:
                trend_score = 0
            
            # Factor 3: Market data context
            market_score = 0
            if market_data:
                vix = market_data.get('vix', 20)
                market_score = 1 if vix < 15 else -1 if vix > 25 else 0
            
            total_score = vol_score + trend_score + market_score
            
            if total_score >= 2:
                return "RISK_OFF"
            elif total_score <= -2:
                return "RISK_ON"
            else:
                return "TRANSITIONAL"
                
        except Exception:
            return "TRANSITIONAL"
    
    def _calculate_sector_momentum(self, symbol: str, market_data: Optional[Dict]) -> float:
        """Calculate sector relative momentum"""
        try:
            if not market_data or 'sector_performance' not in market_data:
                return 0.0
            
            sector_perf = market_data['sector_performance']
            return max(-100, min(100, sector_perf * 100))
            
        except Exception:
            return 0.0
    
    def _detect_correlation_breakdown(self, symbol: str, market_data: Optional[Dict]) -> bool:
        """Detect if correlations are breaking down"""
        try:
            if not market_data or 'correlation_stability' not in market_data:
                return False
            
            return market_data['correlation_stability'] < 0.5
            
        except Exception:
            return False
    
    def _calculate_entry_timing_score(self, prices: pd.Series, volumes: pd.Series, 
                                    trend_strength: float, volatility_percentile: float) -> float:
        """Calculate optimal entry timing score"""
        try:
            factors = []
            
            # Factor 1: Trend alignment
            factors.append(trend_strength)
            
            # Factor 2: Volatility opportunity
            vol_factor = 100 - volatility_percentile if volatility_percentile > 50 else volatility_percentile + 20
            factors.append(vol_factor)
            
            # Factor 3: Recent pullback opportunity
            if len(prices) >= 10:
                recent_low = prices.iloc[-5:].min()
                current_price = prices.iloc[-1]
                pullback_factor = ((current_price - recent_low) / recent_low) * 1000
                factors.append(min(100, pullback_factor))
            
            # Factor 4: Volume confirmation
            if volumes is not None and len(volumes) >= 5:
                recent_vol = volumes.iloc[-5:].mean()
                historical_vol = volumes.iloc[-20:-5].mean() if len(volumes) >= 20 else volumes.iloc[:-5].mean()
                vol_confirmation = min(100, (recent_vol / historical_vol) * 50) if historical_vol > 0 else 50
                factors.append(vol_confirmation)
            
            return np.mean(factors) if factors else 50.0
            
        except Exception:
            return 50.0
    
    def _calculate_exit_timing_score(self, prices: pd.Series, trend_strength: float,
                                   support_levels: List[float], resistance_levels: List[float]) -> float:
        """Calculate optimal exit timing score"""
        try:
            factors = []
            
            # Factor 1: Trend exhaustion
            trend_exhaustion = 100 - trend_strength if trend_strength > 80 else 50
            factors.append(trend_exhaustion)
            
            # Factor 2: Proximity to resistance
            if resistance_levels:
                current_price = prices.iloc[-1]
                nearest_resistance = min(resistance_levels)
                distance_factor = ((nearest_resistance - current_price) / current_price) * 1000
                factors.append(max(0, 100 - distance_factor))
            
            # Factor 3: Momentum divergence
            if len(prices) >= 10:
                price_momentum = (prices.iloc[-1] / prices.iloc[-5] - 1)
                recent_momentum = (prices.iloc[-2] / prices.iloc[-6] - 1)
                divergence = abs(price_momentum - recent_momentum) * 1000
                factors.append(min(100, divergence))
            
            return np.mean(factors) if factors else 50.0
            
        except Exception:
            return 50.0
    
    def _estimate_optimal_holding_period(self, trend_strength: float, volatility_regime: str) -> int:
        """Estimate optimal holding period in days"""
        try:
            base_days = 5
            
            # Adjust for trend strength
            if trend_strength > 80:
                base_days += 10  # Strong trends can last longer
            elif trend_strength < 40:
                base_days = 2    # Weak trends should be held briefly
            
            # Adjust for volatility
            vol_multiplier = {"LOW": 1.5, "NORMAL": 1.0, "HIGH": 0.7, "EXTREME": 0.5}
            base_days = int(base_days * vol_multiplier[volatility_regime])
            
            return max(1, min(30, base_days))
            
        except Exception:
            return 5
    
    def _calculate_tail_risk(self, prices: pd.Series) -> float:
        """Calculate tail risk indicator"""
        try:
            if len(prices) < 20:
                return 50.0
            
            returns = prices.pct_change().dropna()
            
            # Calculate rolling 5th percentile (left tail)
            if len(returns) >= 50:
                tail_threshold = returns.quantile(0.05)
                recent_returns = returns.iloc[-10:]
                tail_proximity = (recent_returns <= tail_threshold).sum() / len(recent_returns)
                return min(100, tail_proximity * 200)
            else:
                return 30.0  # Conservative default
                
        except Exception:
            return 50.0
    
    def _assess_liquidity(self, volumes: pd.Series, market_data: Optional[Dict]) -> float:
        """Assess market liquidity score"""
        try:
            if volumes is None:
                return 50.0
            
            # Recent volume consistency
            if len(volumes) >= 10:
                recent_vol_std = volumes.iloc[-10:].std()
                recent_vol_mean = volumes.iloc[-10:].mean()
                consistency = 1 - (recent_vol_std / (recent_vol_mean + 1e-6))
                consistency_score = max(0, min(100, consistency * 100))
            else:
                consistency_score = 50.0
            
            # Absolute volume level
            if market_data and 'avg_daily_volume' in market_data:
                current_vol = volumes.iloc[-1]
                avg_vol = market_data['avg_daily_volume']
                volume_score = min(100, (current_vol / avg_vol) * 100) if avg_vol > 0 else 50
            else:
                volume_score = 50.0
            
            return (consistency_score + volume_score) / 2
            
        except Exception:
            return 50.0
    
    def _calculate_gap_risk(self, prices: pd.Series, volatility_regime: str) -> float:
        """Calculate overnight/weekend gap risk"""
        try:
            base_risk = {"LOW": 10, "NORMAL": 25, "HIGH": 50, "EXTREME": 80}[volatility_regime]
            
            # Adjust for recent gap history
            if len(prices) >= 10:
                gaps = []
                for i in range(1, min(10, len(prices))):
                    gap = abs(prices.iloc[-i] - prices.iloc[-i-1]) / prices.iloc[-i-1]
                    gaps.append(gap)
                
                avg_gap = np.mean(gaps) if gaps else 0.01
                gap_factor = min(50, avg_gap * 1000)
                
                return min(100, base_risk + gap_factor)
            
            return base_risk
            
        except Exception:
            return 30.0
    
    def _calculate_analysis_confidence(self, data_points: int, has_volume: bool, has_market_data: bool) -> float:
        """Calculate confidence in the analysis"""
        confidence = 0.0
        
        # Data quantity factor (0-40 points)
        if data_points >= 252:  # 1 year of data
            confidence += 40
        elif data_points >= 50:
            confidence += 30
        elif data_points >= 20:
            confidence += 20
        else:
            confidence += 10
        
        # Volume data factor (0-30 points)
        if has_volume:
            confidence += 30
        
        # Market context factor (0-30 points)
        if has_market_data:
            confidence += 30
        
        return min(100.0, confidence)
    
    def get_average_calculation_time(self) -> float:
        """Get average calculation time in milliseconds"""
        return np.mean(self.calculation_times) if self.calculation_times else 0.0
    
    def clear_cache(self):
        """Clear the intelligence cache"""
        self.intelligence_cache.clear()
        self.logger.info("Enhanced business intelligence cache cleared")