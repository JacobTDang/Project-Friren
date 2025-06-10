import sys
import os

# Path setup
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
from trading_engine.data.yahoo import StockDataTools

class MarketRegimeDetector:
    """
    Detect market regimes using multiple indicators

    Regimes:
    - BULL_MARKET: Strong uptrend
    - BEAR_MARKET: Strong downtrend
    - SIDEWAYS: Range-bound
    - HIGH_VOLATILITY: Volatile conditions
    - LOW_VOLATILITY: Stable conditions
    """

    def __init__(self):
        self.SDT = StockDataTools()

    def detect_regime(self, df):
        """
        Detect current market regime
        Returns: regime classification and confidence
        """
        # Add all regime indicators
        df_indicators = self.SDT.add_all_regime_indicators(df)

        # Get latest values (last row)
        latest = df_indicators.iloc[-1]

        # Initialize regime scores
        regime_scores = {
            'BULL_MARKET': 0,
            'BEAR_MARKET': 0,
            'SIDEWAYS': 0,
            'HIGH_VOLATILITY': 0,
            'LOW_VOLATILITY': 0
        }

        # 1. TREND ANALYSIS
        price_vs_sma20 = latest['Price_vs_SMA20']
        price_vs_sma50 = latest['Price_vs_SMA50']
        sma20_vs_sma50 = latest['SMA20_vs_SMA50']

        # Price vs moving averages
        if price_vs_sma20 > 0.02:  # 2%+ above 20-day MA
            regime_scores['BULL_MARKET'] += 1
        elif price_vs_sma20 < -0.02:  # 2%+ below 20-day MA
            regime_scores['BEAR_MARKET'] += 1
        else:
            regime_scores['SIDEWAYS'] += 1

        # Moving average alignment
        if sma20_vs_sma50 > 0.01:  # 20-day above 50-day
            regime_scores['BULL_MARKET'] += 1
        elif sma20_vs_sma50 < -0.01:
            regime_scores['BEAR_MARKET'] += 1
        else:
            regime_scores['SIDEWAYS'] += 1

        # 2. VOLATILITY ANALYSIS
        volatility = latest['Volatility_20']
        if volatility > 0.25:  # High volatility (25%+)
            regime_scores['HIGH_VOLATILITY'] += 2
        elif volatility < 0.15:  # Low volatility (15%-)
            regime_scores['LOW_VOLATILITY'] += 2

        # 3. MOMENTUM ANALYSIS
        momentum_score = latest['Momentum_Score']
        if momentum_score > 0.3:  # Strong positive momentum
            regime_scores['BULL_MARKET'] += 1
        elif momentum_score < -0.3:  # Strong negative momentum
            regime_scores['BEAR_MARKET'] += 1

        # 4. RSI ANALYSIS
        rsi = latest['RSI']
        if rsi > 65:  # Overbought territory
            regime_scores['BULL_MARKET'] += 0.5
        elif rsi < 35:  # Oversold territory
            regime_scores['BEAR_MARKET'] += 0.5

        # 5. DETERMINE PRIMARY REGIME
        # Volatility regimes take precedence
        if regime_scores['HIGH_VOLATILITY'] >= 2:
            primary_regime = 'HIGH_VOLATILITY'
        elif regime_scores['LOW_VOLATILITY'] >= 2:
            primary_regime = 'LOW_VOLATILITY'
        else:
            # Choose highest scoring trend regime
            trend_scores = {
                'BULL_MARKET': regime_scores['BULL_MARKET'],
                'BEAR_MARKET': regime_scores['BEAR_MARKET'],
                'SIDEWAYS': regime_scores['SIDEWAYS']
            }
            primary_regime = max(trend_scores, key=trend_scores.get)

        # Calculate confidence (0-1)
        total_possible = 5.5  # Maximum possible points
        confidence = regime_scores[primary_regime] / total_possible

        return {
            'regime': primary_regime,
            'confidence': confidence,
            'scores': regime_scores,
            'indicators': {
                'price_vs_sma20': price_vs_sma20,
                'price_vs_sma50': price_vs_sma50,
                'volatility': volatility,
                'momentum_score': momentum_score,
                'rsi': rsi
            }
        }
