import logging
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import sys
import os

# Add project root for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

from Friren_V1.trading_engine.data.yahoo_price import YahooFinancePriceData


@dataclass
class MarketMetricsResult:
    """Result container for market metrics calculation"""
    symbol: str
    volatility: Optional[float] = None
    trend_strength: Optional[float] = None
    risk_score: Optional[float] = None
    liquidity_score: Optional[float] = None
    calculation_timestamp: datetime = None
    data_quality: str = "unknown"  # 'excellent', 'good', 'poor', 'insufficient'
    error_reason: Optional[str] = None

    def __post_init__(self):
        if self.calculation_timestamp is None:
            self.calculation_timestamp = datetime.now()


class RealMarketMetrics:
    """
    Real Market Metrics Calculator - NO HARDCODED VALUES

    Calculates real market metrics using Yahoo Finance data and technical analysis.
    Returns None if real data is unavailable rather than falling back to hardcoded values.
    """

    def __init__(self):
        self.logger = logging.getLogger("real_market_metrics")
        self.price_fetcher = YahooFinancePriceData()

        # Cache for recent calculations (5 minute cache)
        self._cache = {}
        self._cache_duration = timedelta(minutes=5)

        self.logger.info("RealMarketMetrics initialized - NO hardcoded fallbacks")

    def get_comprehensive_metrics(self, symbol: str, force_refresh: bool = False) -> MarketMetricsResult:
        """
        Get all market metrics for a symbol

        Args:
            symbol: Stock symbol
            force_refresh: Skip cache and recalculate

        Returns:
            MarketMetricsResult with all calculated metrics or None values if data unavailable
        """
        try:
            # Check cache first
            if not force_refresh and symbol in self._cache:
                cached_result, timestamp = self._cache[symbol]
                if datetime.now() - timestamp < self._cache_duration:
                    self.logger.debug(f"Using cached metrics for {symbol}")
                    return cached_result

            self.logger.info(f"Calculating real market metrics for {symbol}")

            # Get market data
            data = self._get_market_data(symbol)
            if data is None or data.empty:
                return MarketMetricsResult(
                    symbol=symbol,
                    error_reason="No market data available from Yahoo Finance",
                    data_quality="insufficient"
                )

            # Assess data quality
            data_quality = self._assess_data_quality(data)

            # Calculate all metrics
            volatility = self._calculate_volatility(data)
            trend_strength = self._calculate_trend_strength(data)
            risk_score = self._calculate_risk_score(data, volatility, trend_strength)
            liquidity_score = self._calculate_liquidity_score(data)

            result = MarketMetricsResult(
                symbol=symbol,
                volatility=volatility,
                trend_strength=trend_strength,
                risk_score=risk_score,
                liquidity_score=liquidity_score,
                data_quality=data_quality
            )

            # Cache result
            self._cache[symbol] = (result, datetime.now())

            self.logger.info(f"Real metrics calculated for {symbol}: vol={volatility:.3f}, trend={trend_strength:.3f}, risk={risk_score:.1f}")
            return result

        except Exception as e:
            self.logger.error(f"Error calculating metrics for {symbol}: {e}")
            return MarketMetricsResult(
                symbol=symbol,
                error_reason=f"Calculation error: {str(e)}",
                data_quality="insufficient"
            )

    def calculate_real_volatility(self, symbol: str, period_days: int = 30) -> Optional[float]:
        """
        Calculate REAL volatility from market data - NO HARDCODED VALUES

        Args:
            symbol: Stock symbol
            period_days: Number of days for volatility calculation

        Returns:
            Annualized volatility (0.0-1.0+) or None if data unavailable
        """
        try:
            # Get price data
            data = self.price_fetcher.extract_data(symbol, period=f"{period_days}d")

            if data is None or len(data) < 10:  # Need minimum 10 data points
                self.logger.warning(f"Insufficient data for volatility calculation: {symbol}")
                return None

            # Calculate daily returns
            returns = data['Close'].pct_change().dropna()

            if len(returns) < 5:  # Need minimum 5 returns
                self.logger.warning(f"Insufficient return data for volatility: {symbol}")
                return None

            # Annualized volatility (252 trading days)
            volatility = returns.std() * np.sqrt(252)

            # Validate result
            if np.isnan(volatility) or volatility < 0:
                self.logger.warning(f"Invalid volatility calculated for {symbol}: {volatility}")
                return None

            return float(volatility)

        except Exception as e:
            self.logger.error(f"Error calculating volatility for {symbol}: {e}")
            return None

    def calculate_real_trend_strength(self, symbol: str, period_days: int = 60) -> Optional[float]:
        """
        Calculate REAL trend strength from technical analysis - NO HARDCODED VALUES

        Args:
            symbol: Stock symbol
            period_days: Number of days for trend analysis

        Returns:
            Trend strength (0.0-1.0) or None if data unavailable
        """
        try:
            # Get price data
            data = self.price_fetcher.extract_data(symbol, period=f"{period_days}d")

            if data is None or len(data) < 20:  # Need minimum 20 data points
                self.logger.warning(f"Insufficient data for trend calculation: {symbol}")
                return None

            close_prices = data['Close']

            # Calculate moving averages
            ma_10 = close_prices.rolling(10).mean()
            ma_20 = close_prices.rolling(20).mean()
            ma_50 = close_prices.rolling(50).mean() if len(data) >= 50 else ma_20

            # Current price and MA values
            current_price = close_prices.iloc[-1]
            current_ma_10 = ma_10.iloc[-1]
            current_ma_20 = ma_20.iloc[-1]
            current_ma_50 = ma_50.iloc[-1]

            # Check for NaN values
            if any(np.isnan([current_price, current_ma_10, current_ma_20, current_ma_50])):
                self.logger.warning(f"NaN values in trend calculation for {symbol}")
                return None

            # Calculate trend strength components
            # 1. Price vs short-term MA
            price_ma10_strength = abs(current_price - current_ma_10) / current_ma_10

            # 2. MA alignment (trending when MAs are ordered)
            ma_alignment = 0.0
            if current_ma_10 > current_ma_20 > current_ma_50:  # Bullish alignment
                ma_alignment = 1.0
            elif current_ma_10 < current_ma_20 < current_ma_50:  # Bearish alignment
                ma_alignment = 1.0
            else:  # Mixed signals
                ma_alignment = 0.3

            # 3. Price momentum (slope of price over time)
            if len(close_prices) >= 10:
                recent_prices = close_prices.tail(10)
                price_slope = (recent_prices.iloc[-1] - recent_prices.iloc[0]) / recent_prices.iloc[0]
                momentum_strength = min(1.0, abs(price_slope) * 10)  # Scale momentum
            else:
                momentum_strength = 0.0

            # Combine components (weighted average)
            trend_strength = (
                price_ma10_strength * 0.4 +
                ma_alignment * 0.4 +
                momentum_strength * 0.2
            )

            # Cap at 1.0
            trend_strength = min(1.0, trend_strength)

            return float(trend_strength)

        except Exception as e:
            self.logger.error(f"Error calculating trend strength for {symbol}: {e}")
            return None

    def calculate_real_risk_score(self, symbol: str) -> Optional[float]:
        """
        Calculate REAL risk score from market metrics - NO HARDCODED VALUES

        Args:
            symbol: Stock symbol

        Returns:
            Risk score (0-100) or None if data unavailable
        """
        try:
            # Get volatility and trend strength
            volatility = self.calculate_real_volatility(symbol)
            trend_strength = self.calculate_real_trend_strength(symbol)

            if volatility is None or trend_strength is None:
                self.logger.warning(f"Cannot calculate risk score - missing volatility or trend data for {symbol}")
                return None

            # Risk components
            # 1. Volatility risk (higher volatility = higher risk)
            volatility_risk = min(100, volatility * 100)  # Scale to 0-100

            # 2. Trend inconsistency risk (lower trend strength = higher risk)
            trend_risk = (1 - trend_strength) * 50  # Scale to 0-50

            # 3. Market data availability risk
            data = self._get_market_data(symbol, period_days=30)
            if data is None or len(data) < 20:
                data_risk = 30  # High risk if limited data
            elif len(data) < 25:
                data_risk = 15  # Medium risk
            else:
                data_risk = 0   # Low risk with good data

            # Combined risk score
            risk_score = volatility_risk * 0.6 + trend_risk * 0.3 + data_risk * 0.1

            # Ensure within bounds
            risk_score = max(0.0, min(100.0, risk_score))

            return float(risk_score)

        except Exception as e:
            self.logger.error(f"Error calculating risk score for {symbol}: {e}")
            return None

    def calculate_real_liquidity_score(self, symbol: str, period_days: int = 30) -> Optional[float]:
        """
        Calculate REAL liquidity score from volume data - NO HARDCODED VALUES

        Args:
            symbol: Stock symbol
            period_days: Period for volume analysis

        Returns:
            Liquidity score (0-100) or None if data unavailable
        """
        try:
            # Get market data with volume
            data = self.price_fetcher.extract_data(symbol, period=f"{period_days}d")

            if data is None or 'Volume' not in data.columns or len(data) < 10:
                self.logger.warning(f"Insufficient volume data for liquidity score: {symbol}")
                return None

            # Calculate average volume
            avg_volume = data['Volume'].mean()

            if np.isnan(avg_volume) or avg_volume <= 0:
                self.logger.warning(f"Invalid volume data for {symbol}: {avg_volume}")
                return None

            # Volume-based liquidity scoring (logarithmic scale)
            if avg_volume >= 50_000_000:     # Very high volume
                liquidity_score = 95.0
            elif avg_volume >= 10_000_000:   # High volume
                liquidity_score = 85.0
            elif avg_volume >= 1_000_000:    # Moderate volume
                liquidity_score = 70.0
            elif avg_volume >= 100_000:      # Low volume
                liquidity_score = 50.0
            elif avg_volume >= 10_000:       # Very low volume
                liquidity_score = 30.0
            else:                            # Extremely low volume
                liquidity_score = 15.0

            # Adjust for volume consistency
            volume_std = data['Volume'].std()
            volume_cv = volume_std / avg_volume if avg_volume > 0 else float('inf')

            # Lower score for highly inconsistent volume
            if volume_cv > 2.0:  # Very inconsistent
                liquidity_score *= 0.8
            elif volume_cv > 1.0:  # Moderately inconsistent
                liquidity_score *= 0.9

            return float(liquidity_score)

        except Exception as e:
            self.logger.error(f"Error calculating liquidity score for {symbol}: {e}")
            return None

    def _get_market_data(self, symbol: str, period_days: int = 60) -> Optional[pd.DataFrame]:
        """Get market data for calculations"""
        try:
            data = self.price_fetcher.extract_data(symbol, period=f"{period_days}d")
            return data
        except Exception as e:
            self.logger.error(f"Error fetching market data for {symbol}: {e}")
            return None

    def _assess_data_quality(self, data: pd.DataFrame) -> str:
        """Assess quality of market data"""
        if data is None or data.empty:
            return "insufficient"
        elif len(data) >= 50:
            return "excellent"
        elif len(data) >= 30:
            return "good"
        elif len(data) >= 15:
            return "fair"
        else:
            return "poor"

    def _calculate_volatility(self, data: pd.DataFrame) -> Optional[float]:
        """Internal volatility calculation"""
        if data is None or len(data) < 10:
            return None

        returns = data['Close'].pct_change().dropna()
        if len(returns) < 5:
            return None

        volatility = returns.std() * np.sqrt(252)
        return float(volatility) if not np.isnan(volatility) else None

    def _calculate_trend_strength(self, data: pd.DataFrame) -> Optional[float]:
        """Internal trend strength calculation"""
        if data is None or len(data) < 20:
            return None

        close_prices = data['Close']

        # Moving averages
        ma_10 = close_prices.rolling(10).mean().iloc[-1]
        ma_20 = close_prices.rolling(20).mean().iloc[-1]
        current_price = close_prices.iloc[-1]

        if any(np.isnan([ma_10, ma_20, current_price])):
            return None

        # Simple trend strength based on price vs MAs
        price_trend = abs(current_price - ma_20) / ma_20
        ma_trend = abs(ma_10 - ma_20) / ma_20

        trend_strength = min(1.0, (price_trend + ma_trend) / 2)
        return float(trend_strength)

    def _calculate_risk_score(self, data: pd.DataFrame, volatility: Optional[float],
                             trend_strength: Optional[float]) -> Optional[float]:
        """Internal risk score calculation"""
        if volatility is None or trend_strength is None:
            return None

        # Risk based on volatility and trend inconsistency
        volatility_risk = min(100, volatility * 100)
        trend_risk = (1 - trend_strength) * 50

        risk_score = volatility_risk * 0.7 + trend_risk * 0.3
        return max(0.0, min(100.0, float(risk_score)))

    def _calculate_liquidity_score(self, data: pd.DataFrame) -> Optional[float]:
        """Internal liquidity score calculation"""
        if data is None or 'Volume' not in data.columns or len(data) < 5:
            return None

        avg_volume = data['Volume'].mean()
        if np.isnan(avg_volume) or avg_volume <= 0:
            return None

        # Logarithmic liquidity scoring
        if avg_volume >= 10_000_000:
            return 90.0
        elif avg_volume >= 1_000_000:
            return 75.0
        elif avg_volume >= 100_000:
            return 60.0
        elif avg_volume >= 10_000:
            return 40.0
        else:
            return 25.0


# Global instance for easy access
_market_metrics_instance = None

def get_market_metrics() -> RealMarketMetrics:
    """Get global market metrics instance"""
    global _market_metrics_instance
    if _market_metrics_instance is None:
        _market_metrics_instance = RealMarketMetrics()
    return _market_metrics_instance


# Convenience functions for direct use
def get_real_volatility(symbol: str) -> Optional[float]:
    """Get real volatility - NO hardcoded fallback"""
    return get_market_metrics().calculate_real_volatility(symbol)

def get_real_trend_strength(symbol: str) -> Optional[float]:
    """Get real trend strength - NO hardcoded fallback"""
    return get_market_metrics().calculate_real_trend_strength(symbol)

def get_real_risk_score(symbol: str) -> Optional[float]:
    """Get real risk score - NO hardcoded fallback"""
    return get_market_metrics().calculate_real_risk_score(symbol)

def get_real_liquidity_score(symbol: str) -> Optional[float]:
    """Get real liquidity score - NO hardcoded fallback"""
    return get_market_metrics().calculate_real_liquidity_score(symbol)

def get_all_metrics(symbol: str) -> MarketMetricsResult:
    """Get all metrics in one call"""
    return get_market_metrics().get_comprehensive_metrics(symbol)
