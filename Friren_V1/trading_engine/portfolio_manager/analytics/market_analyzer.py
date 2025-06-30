"""
Market Analyzer - Unified Market Regime Detection

This component integrates both regime detection systems:
- EnhancedRegimeDetector (traditional technical analysis)
- EntropyRegimeDetector (information theory based)

Provides comprehensive market regime analysis for strategy selection.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import warnings
warnings.filterwarnings('ignore')

# Import your existing regime detectors
from ...regime_detection.regime_market import EnhancedRegimeDetector, RegimeConfig
from ...regime_detection.entropy_regime_detector import EntropyRegimeDetector, EntropyConfig
from ...data.data_utils import StockDataTools

@dataclass
class MarketRegimeResult:
    """Unified market regime analysis result"""
    # Primary regime classification
    primary_regime: str  # Main regime (BULLISH_TRENDING, BEARISH_TRENDING, etc.)
    regime_confidence: float  # 0-100 confidence in regime classification

    # Technical indicators
    trend: str  # UPTREND, DOWNTREND, SIDEWAYS
    trend_strength: float  # 0-1 strength of trend
    volatility_regime: str  # HIGH_VOLATILITY, NORMAL_VOLATILITY, LOW_VOLATILITY
    current_volatility: float  # Current volatility level
    rsi_condition: str  # OVERBOUGHT, OVERSOLD, NEUTRAL
    current_rsi: float  # Current RSI value

    # Enhanced regime signals
    enhanced_regime: str  # From EnhancedRegimeDetector
    enhanced_confidence: float  # Confidence from enhanced detector
    entropy_regime: str  # From EntropyRegimeDetector
    entropy_confidence: float  # Confidence from entropy detector

    # Market microstructure
    entropy_measures: Dict  # Detailed entropy analysis
    regime_persistence: float  # How stable the regime is
    regime_transition_probability: float  # Likelihood of regime change

    # Summary metrics
    regime_scores: Dict  # Detailed breakdown of regime scores
    market_stress_level: float  # 0-100 overall market stress
    regime_consistency: float  # How much both detectors agree

class MarketAnalyzer:
    """
    Unified Market Regime Analysis Component

    Integrates multiple regime detection methods to provide comprehensive
    market analysis for strategy selection. Combines traditional technical
    analysis with information theory approaches.
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()
        self.data_tools = StockDataTools()

        # Initialize both regime detectors
        self.enhanced_detector = EnhancedRegimeDetector(
            RegimeConfig(
                trend_percentile_threshold=self.config['enhanced_trend_threshold'],
                volatility_percentile_threshold=self.config['enhanced_vol_threshold'],
                percentile_lookback=self.config['enhanced_lookback']
            )
        )

        self.entropy_detector = EntropyRegimeDetector(
            EntropyConfig(
                price_entropy_window=self.config['entropy_price_window'],
                volume_entropy_window=self.config['entropy_volume_window'],
                high_entropy_threshold=self.config['entropy_high_threshold'],
                low_entropy_threshold=self.config['entropy_low_threshold']
            )
        )

        print("Market Analyzer initialized with dual regime detection")

    def _default_config(self) -> Dict:
        """Default configuration for market analysis"""
        return {
            # Enhanced detector config
            'enhanced_trend_threshold': 0.2,
            'enhanced_vol_threshold': 0.25,
            'enhanced_lookback': 252,

            # Entropy detector config
            'entropy_price_window': 20,
            'entropy_volume_window': 15,
            'entropy_high_threshold': 0.75,
            'entropy_low_threshold': 0.25,

            # Analysis parameters
            'min_data_points': 60,
            'regime_agreement_weight': 0.3,  # How much detector agreement matters
            'volatility_stress_threshold': 0.30,  # 30% vol = high stress
            'trend_strength_threshold': 0.05,  # 5% for strong trend
        }

    def analyze_regime(self, df: pd.DataFrame) -> MarketRegimeResult:
        """
        MAIN ANALYSIS METHOD

        Performs comprehensive market regime analysis using both detectors
        and synthesizes results into unified regime classification.
        """
        if len(df) < self.config['min_data_points']:
            return self._insufficient_data_result()

        try:
            # Ensure we have required indicators
            df_enhanced = self._ensure_indicators(df)

            # Run both regime detectors
            enhanced_analysis = self._run_enhanced_analysis(df_enhanced)
            entropy_analysis = self._run_entropy_analysis(df_enhanced)

            # Perform traditional technical analysis
            technical_analysis = self._analyze_technical_indicators(df_enhanced)

            # Synthesize all analyses into unified result
            unified_result = self._synthesize_regime_analysis(
                df_enhanced, enhanced_analysis, entropy_analysis, technical_analysis
            )

            return unified_result

        except Exception as e:
            print(f"Error in market regime analysis: {e}")
            return self._error_fallback_result(df)

    def _run_enhanced_analysis(self, df: pd.DataFrame) -> Dict:
        """Run EnhancedRegimeDetector analysis"""
        try:
            # Run regime detection
            df_with_regimes = self.enhanced_detector.detect_regime_history(df)

            if len(self.enhanced_detector.regime_history) == 0:
                return {'regime': 'UNKNOWN', 'confidence': 0.0}

            # Get latest regime
            latest_regime = self.enhanced_detector.regime_history[-1]

            # Get regime statistics
            regime_stats = self.enhanced_detector.get_regime_statistics()

            return {
                'regime': latest_regime['regime'],
                'confidence': latest_regime['confidence'],
                'regime_stats': regime_stats,
                'indicators': latest_regime,
                'transitions': self.enhanced_detector.analyze_regime_transitions()
            }

        except Exception as e:
            print(f"Enhanced regime detection failed: {e}")
            return {'regime': 'UNKNOWN', 'confidence': 0.0}

    def _run_entropy_analysis(self, df: pd.DataFrame) -> Dict:
        """Run EntropyRegimeDetector analysis"""
        try:
            # Run entropy-based regime detection
            df_with_entropy = self.entropy_detector.detect_regime_history(df)

            if len(self.entropy_detector.regime_history) == 0:
                return {'regime': 'UNKNOWN', 'confidence': 0.0, 'entropy_measures': {}}

            # Get latest entropy regime
            latest_entropy = self.entropy_detector.regime_history[-1]

            # Get entropy summary
            entropy_summary = self.entropy_detector.get_entropy_regime_summary(df)

            # Get transition signals
            transition_signals = self.entropy_detector.get_regime_transition_signals(df)

            return {
                'regime': latest_entropy['regime'],
                'confidence': latest_entropy['confidence'],
                'entropy_measures': {
                    'direction_entropy': latest_entropy.get('direction_entropy', 0),
                    'magnitude_entropy': latest_entropy.get('magnitude_entropy', 0),
                    'volume_entropy': latest_entropy.get('volume_entropy', 0),
                    'composite_entropy': latest_entropy.get('composite_entropy', 0)
                },
                'entropy_summary': entropy_summary,
                'transition_signals': transition_signals.iloc[-1].to_dict() if len(transition_signals) > 0 else {}
            }

        except Exception as e:
            print(f"Entropy regime detection failed: {e}")
            return {'regime': 'UNKNOWN', 'confidence': 0.0, 'entropy_measures': {}}

    def _analyze_technical_indicators(self, df: pd.DataFrame) -> Dict:
        """Traditional technical analysis for regime detection"""
        current = df.iloc[-1]

        # Trend Analysis
        if all(col in df.columns for col in ['SMA_20', 'SMA_50']):
            current_price = current['Close']
            sma_20 = current['SMA_20']
            sma_50 = current['SMA_50']

            if pd.notna(sma_20) and pd.notna(sma_50):
                if current_price > sma_20 > sma_50:
                    trend = 'UPTREND'
                    trend_strength = (current_price - sma_50) / sma_50
                elif current_price < sma_20 < sma_50:
                    trend = 'DOWNTREND'
                    trend_strength = (sma_50 - current_price) / sma_50
                else:
                    trend = 'SIDEWAYS'
                    trend_strength = abs(sma_20 - sma_50) / sma_50
            else:
                trend = 'UNKNOWN'
                trend_strength = 0.0
        else:
            trend = 'UNKNOWN'
            trend_strength = 0.0

        # Volatility Analysis
        returns = df['Close'].pct_change()
        current_vol = returns.rolling(20).std().iloc[-1] * np.sqrt(252)

        if pd.notna(current_vol):
            historical_vol = returns.rolling(60).std().mean() * np.sqrt(252)

            if pd.notna(historical_vol) and historical_vol > 0:
                if current_vol > historical_vol * 1.3:
                    vol_regime = 'HIGH_VOLATILITY'
                elif current_vol < historical_vol * 0.7:
                    vol_regime = 'LOW_VOLATILITY'
                else:
                    vol_regime = 'NORMAL_VOLATILITY'
            else:
                vol_regime = 'NORMAL_VOLATILITY'
        else:
            vol_regime = 'UNKNOWN'
            current_vol = 0.15  # Default assumption

        # RSI Analysis
        if 'RSI' in df.columns and pd.notna(current['RSI']):
            current_rsi = current['RSI']
            if current_rsi > 70:
                rsi_condition = 'OVERBOUGHT'
            elif current_rsi < 30:
                rsi_condition = 'OVERSOLD'
            else:
                rsi_condition = 'NEUTRAL'
        else:
            rsi_condition = 'NEUTRAL'
            current_rsi = 50

        return {
            'trend': trend,
            'trend_strength': trend_strength,
            'volatility_regime': vol_regime,
            'current_volatility': current_vol,
            'rsi_condition': rsi_condition,
            'current_rsi': current_rsi
        }

    def _synthesize_regime_analysis(self, df: pd.DataFrame, enhanced_analysis: Dict,
                                  entropy_analysis: Dict, technical_analysis: Dict) -> MarketRegimeResult:
        """
        Synthesize all analyses into unified market regime classification
        """
        # Extract regime classifications
        enhanced_regime = enhanced_analysis.get('regime', 'UNKNOWN')
        enhanced_confidence = enhanced_analysis.get('confidence', 0.0)
        entropy_regime = entropy_analysis.get('regime', 'UNKNOWN')
        entropy_confidence = entropy_analysis.get('confidence', 0.0)

        # Calculate regime consistency (how much detectors agree)
        regime_consistency = self._calculate_regime_consistency(
            enhanced_regime, entropy_regime, enhanced_confidence, entropy_confidence
        )

        # Determine primary regime using weighted combination
        primary_regime, regime_confidence = self._determine_primary_regime(
            enhanced_regime, enhanced_confidence,
            entropy_regime, entropy_confidence,
            technical_analysis,
            regime_consistency
        )

        # Calculate market stress level
        market_stress = self._calculate_market_stress(
            technical_analysis, entropy_analysis, enhanced_analysis
        )

        # Calculate regime persistence
        regime_persistence = self._calculate_regime_persistence(enhanced_analysis, entropy_analysis)

        # Calculate transition probability
        transition_probability = self._calculate_transition_probability(
            enhanced_analysis, entropy_analysis, technical_analysis
        )

        # Build comprehensive regime scores
        regime_scores = self._build_regime_scores(
            enhanced_analysis, entropy_analysis, technical_analysis
        )

        return MarketRegimeResult(
            # Primary classification
            primary_regime=primary_regime,
            regime_confidence=regime_confidence,

            # Technical indicators
            trend=technical_analysis['trend'],
            trend_strength=technical_analysis['trend_strength'],
            volatility_regime=technical_analysis['volatility_regime'],
            current_volatility=technical_analysis['current_volatility'],
            rsi_condition=technical_analysis['rsi_condition'],
            current_rsi=technical_analysis['current_rsi'],

            # Enhanced regime signals
            enhanced_regime=enhanced_regime,
            enhanced_confidence=enhanced_confidence,
            entropy_regime=entropy_regime,
            entropy_confidence=entropy_confidence,

            # Market microstructure
            entropy_measures=entropy_analysis.get('entropy_measures', {}),
            regime_persistence=regime_persistence,
            regime_transition_probability=transition_probability,

            # Summary metrics
            regime_scores=regime_scores,
            market_stress_level=market_stress,
            regime_consistency=regime_consistency
        )

    def _calculate_regime_consistency(self, enhanced_regime: str, entropy_regime: str,
                                    enhanced_conf: float, entropy_conf: float) -> float:
        """Calculate how much the two regime detectors agree"""

        # Direct regime agreement bonus
        if enhanced_regime == entropy_regime:
            agreement_bonus = 0.4
        else:
            # Check for compatible regimes
            compatible_pairs = [
                ('BULL_MARKET', 'TRENDING'),
                ('BEAR_MARKET', 'TRENDING'),
                ('SIDEWAYS', 'MEAN_REVERTING'),
                ('HIGH_VOLATILITY', 'VOLATILE'),
                ('LOW_VOLATILITY', 'STABLE')
            ]

            if any((enhanced_regime in pair and entropy_regime in pair) or
                   (entropy_regime in pair and enhanced_regime in pair)
                   for pair in compatible_pairs):
                agreement_bonus = 0.2
            else:
                agreement_bonus = 0.0

        # Confidence-weighted consistency
        avg_confidence = (enhanced_conf + entropy_conf) / 2
        confidence_weight = avg_confidence / 100.0

        base_consistency = 0.5 + agreement_bonus
        weighted_consistency = base_consistency * confidence_weight + (1 - confidence_weight) * 0.3

        return max(0, min(1, weighted_consistency))

    def _determine_primary_regime(self, enhanced_regime: str, enhanced_conf: float,
                                entropy_regime: str, entropy_conf: float,
                                technical_analysis: Dict, consistency: float) -> Tuple[str, float]:
        """Determine the primary market regime with confidence"""

        # Map regimes to standardized classifications
        regime_mapping = {
            'BULL_MARKET': 'BULLISH_TRENDING',
            'BEAR_MARKET': 'BEARISH_TRENDING',
            'SIDEWAYS': 'RANGE_BOUND_STABLE',
            'HIGH_VOLATILITY': 'HIGH_VOLATILITY_UNSTABLE',
            'LOW_VOLATILITY': 'RANGE_BOUND_STABLE',
            'TRENDING': 'TRENDING_MARKET',
            'MEAN_REVERTING': 'RANGE_BOUND_STABLE',
            'VOLATILE': 'HIGH_VOLATILITY_UNSTABLE',
            'STABLE': 'RANGE_BOUND_STABLE'
        }

        enhanced_mapped = regime_mapping.get(enhanced_regime, 'MIXED_SIGNALS')
        entropy_mapped = regime_mapping.get(entropy_regime, 'MIXED_SIGNALS')

        # Weight the regimes by confidence and consistency
        if enhanced_conf > entropy_conf:
            primary_regime = enhanced_mapped
            base_confidence = enhanced_conf
        else:
            primary_regime = entropy_mapped
            base_confidence = entropy_conf

        # Adjust confidence based on technical analysis agreement
        tech_trend = technical_analysis['trend']
        tech_vol = technical_analysis['volatility_regime']

        # Technical confirmation bonus
        tech_bonus = 0
        if 'BULLISH' in primary_regime and tech_trend == 'UPTREND':
            tech_bonus = 10
        elif 'BEARISH' in primary_regime and tech_trend == 'DOWNTREND':
            tech_bonus = 10
        elif 'RANGE_BOUND' in primary_regime and tech_trend == 'SIDEWAYS':
            tech_bonus = 10
        elif 'HIGH_VOLATILITY' in primary_regime and tech_vol == 'HIGH_VOLATILITY':
            tech_bonus = 15

        # Consistency bonus
        consistency_bonus = consistency * 20

        final_confidence = min(100, base_confidence + tech_bonus + consistency_bonus)

        return primary_regime, final_confidence

    def _calculate_market_stress(self, technical: Dict, entropy: Dict, enhanced: Dict) -> float:
        """Calculate overall market stress level (0-100)"""
        stress_factors = []

        # Volatility stress
        vol = technical.get('current_volatility', 0.15)
        if vol > self.config['volatility_stress_threshold']:
            vol_stress = min(40, (vol - 0.15) * 200)  # Up to 40 points
        else:
            vol_stress = 0
        stress_factors.append(vol_stress)

        # Entropy stress (high entropy = market confusion)
        entropy_measures = entropy.get('entropy_measures', {})
        composite_entropy = entropy_measures.get('composite_entropy', 0.5)
        if composite_entropy > 0.7:
            entropy_stress = (composite_entropy - 0.7) * 100  # Up to 30 points
        else:
            entropy_stress = 0
        stress_factors.append(entropy_stress)

        # Regime uncertainty stress
        enhanced_conf = enhanced.get('confidence', 50)
        entropy_conf = entropy.get('confidence', 50)
        uncertainty_stress = 50 - min(enhanced_conf, entropy_conf)  # Up to 50 points
        stress_factors.append(uncertainty_stress)

        # Trend weakness stress
        trend_strength = technical.get('trend_strength', 0)
        if trend_strength < 0.02:  # Very weak trend
            trend_stress = 20
        else:
            trend_stress = max(0, 20 - trend_strength * 400)
        stress_factors.append(trend_stress)

        total_stress = sum(stress_factors) / len(stress_factors) if stress_factors else 0
        return max(0, min(100, total_stress))

    def _calculate_regime_persistence(self, enhanced: Dict, entropy: Dict) -> float:
        """Calculate how stable/persistent the current regime is"""
        base_persistence = 0.5

        # Enhanced detector regime stats
        enhanced_stats = enhanced.get('regime_stats', {})
        if 'regime_persistence' in enhanced_stats:
            enhanced_persistence = enhanced_stats['regime_persistence']
            if enhanced.get('regime', 'UNKNOWN') in enhanced_persistence:
                regime_data = enhanced_persistence[enhanced['regime']]
                avg_duration = regime_data.get('avg_duration', 1)
                base_persistence += min(0.3, avg_duration / 20)  # Up to 0.3 bonus

        # Entropy consistency
        entropy_conf = entropy.get('confidence', 50)
        if entropy_conf > 70:
            base_persistence += 0.2

        return max(0, min(1, base_persistence))

    def _calculate_transition_probability(self, enhanced: Dict, entropy: Dict, technical: Dict) -> float:
        """Calculate probability of regime transition"""
        base_transition = 0.1  # 10% base probability

        # Check entropy transition signals
        transition_signals = entropy.get('transition_signals', {})
        if transition_signals.get('Transition_Warning', False):
            transition_type = transition_signals.get('Transition_Type', 'NONE')
            if transition_type != 'NONE':
                base_transition += 0.3  # 30% increase if entropy detects transition

        # Check regime confidence drops
        enhanced_conf = enhanced.get('confidence', 50)
        entropy_conf = entropy.get('confidence', 50)
        if min(enhanced_conf, entropy_conf) < 40:
            base_transition += 0.2  # 20% increase for low confidence

        # Check volatility spikes
        vol_regime = technical.get('volatility_regime', 'NORMAL')
        if vol_regime == 'HIGH_VOLATILITY':
            base_transition += 0.15  # 15% increase for high volatility

        return max(0, min(1, base_transition))

    def _build_regime_scores(self, enhanced: Dict, entropy: Dict, technical: Dict) -> Dict:
        """Build detailed regime scoring breakdown"""
        return {
            'enhanced_analysis': {
                'regime': enhanced.get('regime', 'UNKNOWN'),
                'confidence': enhanced.get('confidence', 0),
                'indicators': enhanced.get('indicators', {})
            },
            'entropy_analysis': {
                'regime': entropy.get('regime', 'UNKNOWN'),
                'confidence': entropy.get('confidence', 0),
                'measures': entropy.get('entropy_measures', {})
            },
            'technical_analysis': technical,
            'synthesis': {
                'consistency_score': self._calculate_regime_consistency(
                    enhanced.get('regime', ''), entropy.get('regime', ''),
                    enhanced.get('confidence', 0), entropy.get('confidence', 0)
                )
            }
        }

    def _ensure_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure DataFrame has all required technical indicators"""
        return self.data_tools.add_all_regime_indicators(df)

    def _insufficient_data_result(self) -> MarketRegimeResult:
        """Return result for insufficient data"""
        return MarketRegimeResult(
            primary_regime='INSUFFICIENT_DATA',
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

    def _error_fallback_result(self, df: pd.DataFrame) -> MarketRegimeResult:
        """Fallback result when analysis fails"""
        # Try basic technical analysis as fallback
        try:
            technical = self._analyze_technical_indicators(self._ensure_indicators(df))

            return MarketRegimeResult(
                primary_regime='MIXED_SIGNALS',
                regime_confidence=30.0,
                trend=technical['trend'],
                trend_strength=technical['trend_strength'],
                volatility_regime=technical['volatility_regime'],
                current_volatility=technical['current_volatility'],
                rsi_condition=technical['rsi_condition'],
                current_rsi=technical['current_rsi'],
                enhanced_regime='ERROR',
                enhanced_confidence=0.0,
                entropy_regime='ERROR',
                entropy_confidence=0.0,
                entropy_measures={},
                regime_persistence=0.3,
                regime_transition_probability=0.4,
                regime_scores={'error': 'Analysis failed, using technical fallback'},
                market_stress_level=60.0,
                regime_consistency=0.2
            )
        except:
            return self._insufficient_data_result()

    def get_regime_summary(self, result: MarketRegimeResult) -> Dict:
        """Get human-readable regime summary"""
        return {
            'regime': result.primary_regime,
            'confidence': f"{result.regime_confidence:.1f}%",
            'trend': f"{result.trend} (strength: {result.trend_strength:.2%})",
            'volatility': f"{result.volatility_regime} ({result.current_volatility:.1%})",
            'rsi_condition': f"{result.rsi_condition} (RSI: {result.current_rsi:.1f})",
            'market_stress': f"{result.market_stress_level:.0f}/100",
            'regime_stability': f"{result.regime_persistence:.1%}",
            'detector_agreement': f"{result.regime_consistency:.1%}",
            'transition_risk': f"{result.regime_transition_probability:.1%}"
        }
