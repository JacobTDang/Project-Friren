"""
Position Sizer Component for Portfolio Manager

Clean, focused position sizing component that integrates with existing
risk analyzer and strategy framework. Designed to be used by orchestrator.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import warnings
warnings.filterwarnings('ignore')

class SizingMethod(Enum):
    """Position sizing methodologies"""
    FIXED_FRACTIONAL = "fixed_fractional"
    VOLATILITY_ADJUSTED = "volatility_adjusted"
    KELLY_CRITERION = "kelly_criterion"
    RISK_PARITY = "risk_parity"
    CONFIDENCE_WEIGHTED = "confidence_weighted"
    HYBRID = "hybrid"

@dataclass
class PositionSizeResult:
    """Result from position sizing calculation"""
    symbol: str
    recommended_size: float  # Position size as % of portfolio
    base_size: float  # Base size before adjustments
    sizing_method: str
    confidence_score: float  # 0-100

    # Adjustments applied
    volatility_adjustment: float
    confidence_adjustment: float
    regime_adjustment: float
    risk_adjustment: float

    # Risk metrics
    estimated_volatility: float
    estimated_var_1d: float
    risk_contribution: float

    # Rationale
    sizing_rationale: str
    risk_warnings: List[str]

    timestamp: datetime

class PositionSizer:
    """
    Professional Position Sizing Component

    Provides multiple sizing methodologies with risk adjustments.
    Designed to be lightweight and integrate with existing components.
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()
        print(f"Position Sizer initialized - Base size: {self.config['base_position_size']:.1%}")

    def _default_config(self) -> Dict:
        """Default position sizing configuration"""
        return {
            # Base sizing parameters
            'base_position_size': 0.10,  # 10% base allocation
            'max_position_size': 0.20,   # 20% maximum per position
            'min_position_size': 0.02,   # 2% minimum per position

            # Risk parameters
            'target_volatility': 0.15,   # 15% target position volatility
            'volatility_lookback': 63,   # ~3 months for volatility calculation
            'confidence_scaling': True,  # Scale size by confidence

            # Kelly parameters
            'kelly_max_size': 0.15,      # 15% max Kelly size
            'kelly_lookback': 252,       # 1 year for Kelly calculation

            # Regime adjustments
            'regime_multipliers': {
                'BULL': 1.2,      # 20% larger in bull markets
                'BEAR': 0.7,      # 30% smaller in bear markets
                'SIDEWAYS': 0.9,  # 10% smaller in sideways
                'VOLATILE': 0.6,  # 40% smaller in volatile
                'UNKNOWN': 1.0    # No adjustment
            },

            # Strategy type adjustments
            'strategy_multipliers': {
                'MOMENTUM': 1.1,
                'MEAN_REVERSION': 0.9,
                'VOLATILITY': 0.8,
                'PAIRS': 1.0,
                'UNKNOWN': 1.0
            }
        }

    def calculate_position_size(self,
                              symbol: str,
                              strategy_signal: Any,
                              market_data: pd.DataFrame,
                              market_regime: str = 'UNKNOWN',
                              current_portfolio: Optional[Dict] = None,
                              method: SizingMethod = SizingMethod.HYBRID) -> PositionSizeResult:
        """
        Calculate position size for a single symbol

        Args:
            symbol: Stock symbol
            strategy_signal: Strategy signal with confidence and metadata
            market_data: OHLCV DataFrame with technical indicators
            market_regime: Current market regime (BULL/BEAR/SIDEWAYS/VOLATILE)
            current_portfolio: Current portfolio state (optional)
            method: Sizing methodology to use

        Returns:
            PositionSizeResult with comprehensive sizing analysis
        """

        # Extract signal properties
        confidence = getattr(strategy_signal, 'confidence', 75.0)
        strategy_type = getattr(strategy_signal, 'strategy_type', 'UNKNOWN')
        signal_strength = getattr(strategy_signal, 'signal_strength', 1.0)

        # Calculate base position size using selected method
        if method == SizingMethod.FIXED_FRACTIONAL:
            base_size = self._fixed_fractional_size()
        elif method == SizingMethod.VOLATILITY_ADJUSTED:
            base_size = self._volatility_adjusted_size(market_data)
        elif method == SizingMethod.KELLY_CRITERION:
            base_size = self._kelly_criterion_size(market_data, confidence)
        elif method == SizingMethod.RISK_PARITY:
            base_size = self._risk_parity_size(market_data, current_portfolio)
        elif method == SizingMethod.CONFIDENCE_WEIGHTED:
            base_size = self._confidence_weighted_size(confidence)
        else:  # HYBRID
            base_size = self._hybrid_size(market_data, confidence, current_portfolio)

        # Calculate adjustments
        adjustments = self._calculate_adjustments(
            confidence, market_regime, strategy_type, market_data, signal_strength
        )

        # Apply adjustments to get final size
        final_size = base_size
        for adj_value in adjustments.values():
            final_size *= adj_value

        # Apply size limits
        final_size = self._apply_size_limits(final_size)

        # Calculate risk metrics
        risk_metrics = self._calculate_risk_metrics(market_data, final_size)

        # Generate rationale and warnings
        rationale = self._generate_rationale(
            symbol, base_size, final_size, adjustments, method, confidence
        )
        warnings = self._generate_warnings(final_size, risk_metrics, confidence)

        return PositionSizeResult(
            symbol=symbol,
            recommended_size=final_size,
            base_size=base_size,
            sizing_method=method.value,
            confidence_score=confidence,

            volatility_adjustment=adjustments['volatility'],
            confidence_adjustment=adjustments['confidence'],
            regime_adjustment=adjustments['regime'],
            risk_adjustment=adjustments['risk'],

            estimated_volatility=risk_metrics['volatility'],
            estimated_var_1d=risk_metrics['var_1d'],
            risk_contribution=risk_metrics['risk_contribution'],

            sizing_rationale=rationale,
            risk_warnings=warnings,

            timestamp=datetime.now()
        )

    def _fixed_fractional_size(self) -> float:
        """Simple fixed fractional sizing"""
        return self.config['base_position_size']

    def _volatility_adjusted_size(self, df: pd.DataFrame) -> float:
        """Volatility-adjusted position sizing"""
        returns = df['Close'].pct_change().dropna()
        if len(returns) < 20:
            return self.config['base_position_size']

        # Calculate annualized volatility
        volatility = returns.tail(self.config['volatility_lookback']).std() * np.sqrt(252)

        # Inverse volatility scaling
        target_vol = self.config['target_volatility']
        vol_multiplier = target_vol / max(volatility, 0.05)  # Min 5% vol

        # Cap the adjustment
        vol_multiplier = max(0.5, min(2.0, vol_multiplier))

        return self.config['base_position_size'] * vol_multiplier

    def _kelly_criterion_size(self, df: pd.DataFrame, confidence: float) -> float:
        """Kelly criterion position sizing"""
        returns = df['Close'].pct_change().dropna()
        if len(returns) < 50:
            return self.config['base_position_size']

        # Use recent returns for Kelly calculation
        recent_returns = returns.tail(self.config['kelly_lookback'])

        # Estimate win rate and payoffs
        positive_returns = recent_returns[recent_returns > 0]
        negative_returns = recent_returns[recent_returns < 0]

        if len(positive_returns) == 0 or len(negative_returns) == 0:
            return self.config['base_position_size']

        win_rate = len(positive_returns) / len(recent_returns)
        avg_win = positive_returns.mean()
        avg_loss = abs(negative_returns.mean())

        if avg_loss == 0:
            return self.config['base_position_size']

        # Kelly fraction: f = (bp - q) / b
        b = avg_win / avg_loss
        p = win_rate
        q = 1 - win_rate

        kelly_fraction = (b * p - q) / b

        # Apply confidence scaling and limits
        confidence_factor = confidence / 100.0
        kelly_size = max(0, kelly_fraction * confidence_factor)

        return min(self.config['kelly_max_size'], kelly_size)

    def _risk_parity_size(self, df: pd.DataFrame, current_portfolio: Optional[Dict]) -> float:
        """Risk parity position sizing"""
        returns = df['Close'].pct_change().dropna()
        if len(returns) < 20:
            return self.config['base_position_size']

        asset_vol = returns.std() * np.sqrt(252)

        # If no portfolio data, use target volatility
        if not current_portfolio:
            target_vol = self.config['target_volatility']
            return (target_vol / max(asset_vol, 0.05)) * self.config['base_position_size']

        # Use portfolio average volatility
        portfolio_vol = current_portfolio.get('average_volatility', self.config['target_volatility'])
        risk_parity_size = (portfolio_vol / max(asset_vol, 0.05)) * self.config['base_position_size']

        return min(self.config['max_position_size'], risk_parity_size)

    def _confidence_weighted_size(self, confidence: float) -> float:
        """Confidence-weighted position sizing"""
        confidence_factor = confidence / 100.0

        # Scale base size by confidence
        # Low confidence (50%) -> 0.5x base, High confidence (95%) -> 1.5x base
        size_multiplier = 0.5 + confidence_factor

        return self.config['base_position_size'] * size_multiplier

    def _hybrid_size(self, df: pd.DataFrame, confidence: float,
                    current_portfolio: Optional[Dict]) -> float:
        """Hybrid sizing combining multiple methods"""

        # Calculate individual method sizes
        fixed_size = self._fixed_fractional_size()
        vol_size = self._volatility_adjusted_size(df)
        kelly_size = self._kelly_criterion_size(df, confidence)
        confidence_size = self._confidence_weighted_size(confidence)

        # Weight the methods
        weights = [0.2, 0.3, 0.3, 0.2]  # fixed, vol, kelly, confidence
        sizes = [fixed_size, vol_size, kelly_size, confidence_size]

        # Calculate weighted average
        hybrid_size = sum(w * s for w, s in zip(weights, sizes))

        return hybrid_size

    def _calculate_adjustments(self, confidence: float, market_regime: str,
                             strategy_type: str, df: pd.DataFrame,
                             signal_strength: float) -> Dict[str, float]:
        """Calculate all sizing adjustments"""

        adjustments = {}

        # Confidence adjustment (0.7 to 1.3 range)
        if self.config['confidence_scaling']:
            conf_factor = confidence / 100.0
            adjustments['confidence'] = 0.7 + (0.6 * conf_factor)
        else:
            adjustments['confidence'] = 1.0

        # Market regime adjustment
        adjustments['regime'] = self.config['regime_multipliers'].get(market_regime, 1.0)

        # Strategy type adjustment
        adjustments['strategy'] = self.config['strategy_multipliers'].get(strategy_type, 1.0)

        # Risk adjustment based on recent volatility
        returns = df['Close'].pct_change().dropna()
        if len(returns) >= 20:
            recent_vol = returns.tail(20).std() * np.sqrt(252)
            target_vol = self.config['target_volatility']
            risk_adj = max(0.6, min(1.4, target_vol / max(recent_vol, 0.05)))
            adjustments['risk'] = risk_adj
        else:
            adjustments['risk'] = 1.0

        # Signal strength adjustment
        adjustments['signal'] = max(0.8, min(1.2, signal_strength))

        return adjustments

    def _apply_size_limits(self, size: float) -> float:
        """Apply minimum and maximum size limits"""
        return max(self.config['min_position_size'],
                  min(self.config['max_position_size'], size))

    def _calculate_risk_metrics(self, df: pd.DataFrame, position_size: float) -> Dict[str, float]:
        """Calculate position risk metrics"""
        returns = df['Close'].pct_change().dropna()

        if len(returns) < 10:
            return {
                'volatility': 0.20,
                'var_1d': -0.02,
                'risk_contribution': position_size * 0.20
            }

        # Annualized volatility
        volatility = returns.std() * np.sqrt(252)

        # 1-day 95% VaR
        var_1d = np.percentile(returns, 5)

        # Risk contribution (simplified)
        risk_contribution = position_size * volatility

        return {
            'volatility': volatility,
            'var_1d': var_1d,
            'risk_contribution': risk_contribution
        }

    def _generate_rationale(self, symbol: str, base_size: float, final_size: float,
                          adjustments: Dict, method: SizingMethod, confidence: float) -> str:
        """Generate human-readable sizing rationale"""

        size_change_pct = ((final_size - base_size) / base_size) * 100

        rationale = f"{symbol} position size: {final_size:.1%} "
        rationale += f"(base: {base_size:.1%}, {size_change_pct:+.0f}%)\n"
        rationale += f"Method: {method.value.replace('_', ' ').title()}\n"
        rationale += f"Confidence: {confidence:.0f}%\n"

        # Key adjustments
        for adj_name, adj_value in adjustments.items():
            if abs(adj_value - 1.0) > 0.05:
                direction = "↑" if adj_value > 1.0 else "↓"
                rationale += f"• {adj_name.title()}: {direction} {abs(adj_value-1.0):.1%}\n"

        return rationale.strip()

    def _generate_warnings(self, position_size: float, risk_metrics: Dict,
                         confidence: float) -> List[str]:
        """Generate risk warnings"""
        warnings = []

        if position_size > 0.15:
            warnings.append(f"⚠️ Large position ({position_size:.1%}) - concentration risk")

        if risk_metrics['volatility'] > 0.35:
            warnings.append(f"⚠️ High volatility ({risk_metrics['volatility']:.1%})")

        if abs(risk_metrics['var_1d']) > 0.05:
            warnings.append(f"⚠️ High daily VaR ({risk_metrics['var_1d']:.1%})")

        if confidence < 65:
            warnings.append(f"⚠️ Low confidence signal ({confidence:.0f}%)")

        return warnings

    def calculate_portfolio_sizes(self, strategy_signals: List[Any],
                                market_data_dict: Dict[str, pd.DataFrame],
                                market_regime: str = 'UNKNOWN',
                                current_portfolio: Optional[Dict] = None,
                                method: SizingMethod = SizingMethod.HYBRID) -> List[PositionSizeResult]:
        """
        Calculate position sizes for multiple symbols

        Args:
            strategy_signals: List of strategy signals
            market_data_dict: Dict mapping symbols to DataFrames
            market_regime: Current market regime
            current_portfolio: Current portfolio state
            method: Sizing methodology

        Returns:
            List of PositionSizeResult objects
        """

        results = []

        for signal in strategy_signals:
            symbol = getattr(signal, 'symbol', 'UNKNOWN')

            if symbol not in market_data_dict:
                continue

            df = market_data_dict[symbol]

            result = self.calculate_position_size(
                symbol=symbol,
                strategy_signal=signal,
                market_data=df,
                market_regime=market_regime,
                current_portfolio=current_portfolio,
                method=method
            )

            results.append(result)

        return results

    def get_sizing_summary(self, results: List[PositionSizeResult]) -> Dict[str, Any]:
        """Get summary statistics for position sizing results"""

        if not results:
            return {}

        sizes = [r.recommended_size for r in results]
        confidences = [r.confidence_score for r in results]
        volatilities = [r.estimated_volatility for r in results]

        return {
            'total_positions': len(results),
            'total_allocation': sum(sizes),
            'average_size': np.mean(sizes),
            'largest_position': max(sizes),
            'smallest_position': min(sizes),
            'size_std': np.std(sizes),
            'average_confidence': np.mean(confidences),
            'average_volatility': np.mean(volatilities),
            'high_risk_positions': len([r for r in results if r.estimated_volatility > 0.30]),
            'warnings_count': sum(len(r.risk_warnings) for r in results),
            'timestamp': datetime.now()
        }
