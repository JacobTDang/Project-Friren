"""
Impact Calculator - Market impact scoring and risk assessment

Extracted from enhanced_news_pipeline_process.py to provide modular
market impact calculation capabilities with dynamic confidence adjustment.

Key Features:
- Market-based confidence adjustment
- Dynamic risk level calculation
- Volatility and trend analysis integration
- Market regime consideration
- Real-time market metrics integration
"""

import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

# Import market metrics for dynamic calculations
from Friren_V1.trading_engine.analytics.market_metrics import MarketMetricsResult


class RiskLevel(Enum):
    """Risk level classifications"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    EXTREME = "extreme"


@dataclass
class MarketImpactResult:
    """Market impact calculation result"""
    symbol: str
    base_confidence: float
    market_adjustment_factor: float
    final_confidence: float
    risk_level: RiskLevel
    risk_score: float
    
    # Market condition factors
    volatility_factor: float
    trend_strength_factor: float
    data_quality_factor: float
    
    # Supporting metrics
    market_volatility: Optional[float]
    trend_strength: Optional[float]
    market_risk_score: Optional[float]
    data_quality: Optional[str]
    
    # Processing metadata
    calculation_time_ms: float
    timestamp: datetime


class ImpactCalculator:
    """
    Calculates market impact and adjusts confidence based on market conditions
    
    Provides sophisticated market impact assessment with dynamic confidence
    adjustment based on volatility, trend strength, risk scores, and data quality.
    """
    
    def __init__(self):
        """Initialize impact calculator"""
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Configuration for market adjustment factors
        self.volatility_thresholds = {
            'very_high': 0.5,
            'high': 0.3,
            'low': 0.15
        }
        
        self.trend_strength_thresholds = {
            'strong': 0.7,
            'weak': 0.3
        }
        
        self.risk_score_thresholds = {
            'high': 70,
            'medium': 40,
            'low': 30
        }
        
        # Adjustment factors
        self.adjustment_factors = {
            'volatility': {
                'very_high': 0.7,
                'high': 0.85,
                'low': 1.1
            },
            'trend_strength': {
                'strong': 1.15,
                'weak': 0.85
            },
            'risk_score': {
                'high': 0.8,
                'low': 1.1
            },
            'data_quality': {
                'excellent': 1.05,
                'good': 1.0,
                'fair': 0.95,
                'poor': 0.9,
                'insufficient': 0.8
            }
        }
    
    def calculate_market_impact(self, symbol: str, base_confidence: float, 
                              market_metrics: MarketMetricsResult) -> MarketImpactResult:
        """
        Calculate market impact and adjust confidence based on market conditions
        
        Args:
            symbol: Symbol for calculation
            base_confidence: Base confidence from sentiment analysis
            market_metrics: Market metrics for the symbol
            
        Returns:
            MarketImpactResult with adjusted confidence and risk assessment
        """
        start_time = datetime.now()
        
        try:
            # Calculate market confidence adjustment factor
            adjustment_factor = self._calculate_market_confidence_adjustment(market_metrics)
            
            # Apply adjustment to base confidence
            final_confidence = min(0.95, base_confidence * adjustment_factor)
            
            # Calculate risk level
            risk_level = self._calculate_risk_level(market_metrics)
            
            # Calculate individual factors
            volatility_factor = self._calculate_volatility_factor(market_metrics.volatility)
            trend_factor = self._calculate_trend_strength_factor(market_metrics.trend_strength)
            quality_factor = self._calculate_data_quality_factor(market_metrics.data_quality)
            
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            
            return MarketImpactResult(
                symbol=symbol,
                base_confidence=base_confidence,
                market_adjustment_factor=adjustment_factor,
                final_confidence=final_confidence,
                risk_level=risk_level,
                risk_score=market_metrics.risk_score or 50.0,
                volatility_factor=volatility_factor,
                trend_strength_factor=trend_factor,
                data_quality_factor=quality_factor,
                market_volatility=market_metrics.volatility,
                trend_strength=market_metrics.trend_strength,
                market_risk_score=market_metrics.risk_score,
                data_quality=market_metrics.data_quality,
                calculation_time_ms=processing_time,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Error calculating market impact for {symbol}: {e}")
            return self._create_fallback_result(symbol, base_confidence, start_time)
    
    def _calculate_market_confidence_adjustment(self, market_metrics: MarketMetricsResult) -> float:
        """
        Calculate market-based confidence adjustment factor
        
        Args:
            market_metrics: Market metrics for analysis
            
        Returns:
            Adjustment factor between 0.5 and 1.3
        """
        try:
            adjustment_factor = 1.0
            
            # Adjust based on volatility - high volatility reduces confidence
            volatility_adjustment = self._calculate_volatility_factor(market_metrics.volatility)
            adjustment_factor *= volatility_adjustment
            
            # Adjust based on trend strength - strong trends increase confidence
            trend_adjustment = self._calculate_trend_strength_factor(market_metrics.trend_strength)
            adjustment_factor *= trend_adjustment
            
            # Adjust based on risk score - high risk reduces confidence
            risk_adjustment = self._calculate_risk_score_factor(market_metrics.risk_score)
            adjustment_factor *= risk_adjustment
            
            # Adjust based on data quality
            quality_adjustment = self._calculate_data_quality_factor(market_metrics.data_quality)
            adjustment_factor *= quality_adjustment
            
            # Cap between 0.5 and 1.3
            return min(1.3, max(0.5, adjustment_factor))
            
        except Exception as e:
            self.logger.error(f"Error calculating market confidence adjustment: {e}")
            return 0.9  # Conservative adjustment
    
    def _calculate_volatility_factor(self, volatility: Optional[float]) -> float:
        """Calculate volatility adjustment factor"""
        if volatility is None:
            return 1.0
        
        if volatility > self.volatility_thresholds['very_high']:
            return self.adjustment_factors['volatility']['very_high']
        elif volatility > self.volatility_thresholds['high']:
            return self.adjustment_factors['volatility']['high']
        elif volatility < self.volatility_thresholds['low']:
            return self.adjustment_factors['volatility']['low']
        else:
            return 1.0
    
    def _calculate_trend_strength_factor(self, trend_strength: Optional[float]) -> float:
        """Calculate trend strength adjustment factor"""
        if trend_strength is None:
            return 1.0
        
        if trend_strength > self.trend_strength_thresholds['strong']:
            return self.adjustment_factors['trend_strength']['strong']
        elif trend_strength < self.trend_strength_thresholds['weak']:
            return self.adjustment_factors['trend_strength']['weak']
        else:
            return 1.0
    
    def _calculate_risk_score_factor(self, risk_score: Optional[float]) -> float:
        """Calculate risk score adjustment factor"""
        if risk_score is None:
            return 1.0
        
        if risk_score > self.risk_score_thresholds['high']:
            return self.adjustment_factors['risk_score']['high']
        elif risk_score < self.risk_score_thresholds['low']:
            return self.adjustment_factors['risk_score']['low']
        else:
            return 1.0
    
    def _calculate_data_quality_factor(self, data_quality: Optional[str]) -> float:
        """Calculate data quality adjustment factor"""
        if data_quality is None:
            return 1.0
        
        return self.adjustment_factors['data_quality'].get(data_quality, 1.0)
    
    def _calculate_risk_level(self, market_metrics: MarketMetricsResult) -> RiskLevel:
        """
        Calculate risk level based on market metrics
        
        Args:
            market_metrics: Market metrics for analysis
            
        Returns:
            RiskLevel enum value
        """
        try:
            # Primary risk assessment based on risk score
            if market_metrics.risk_score is not None:
                if market_metrics.risk_score >= 80:
                    return RiskLevel.EXTREME
                elif market_metrics.risk_score >= 70:
                    return RiskLevel.HIGH
                elif market_metrics.risk_score >= 40:
                    return RiskLevel.MEDIUM
                else:
                    return RiskLevel.LOW
            
            # Fallback based on volatility if risk score unavailable
            if market_metrics.volatility is not None:
                if market_metrics.volatility > 0.6:
                    return RiskLevel.EXTREME
                elif market_metrics.volatility > 0.4:
                    return RiskLevel.HIGH
                elif market_metrics.volatility > 0.2:
                    return RiskLevel.MEDIUM
                else:
                    return RiskLevel.LOW
            
            # Conservative fallback
            return RiskLevel.MEDIUM
            
        except Exception as e:
            self.logger.error(f"Error calculating risk level: {e}")
            return RiskLevel.MEDIUM  # Conservative fallback
    
    def calculate_portfolio_impact(self, symbol_impacts: List[MarketImpactResult]) -> Dict[str, Any]:
        """
        Calculate portfolio-level impact from individual symbol impacts
        
        Args:
            symbol_impacts: List of MarketImpactResult objects
            
        Returns:
            Dictionary with portfolio impact metrics
        """
        if not symbol_impacts:
            return {
                'portfolio_confidence': 0.0,
                'portfolio_risk_level': RiskLevel.MEDIUM,
                'symbols_analyzed': 0,
                'risk_distribution': {},
                'average_adjustment_factor': 1.0
            }
        
        # Calculate average portfolio confidence
        portfolio_confidence = sum(impact.final_confidence for impact in symbol_impacts) / len(symbol_impacts)
        
        # Calculate risk distribution
        risk_distribution = {}
        for impact in symbol_impacts:
            risk_level = impact.risk_level.value
            risk_distribution[risk_level] = risk_distribution.get(risk_level, 0) + 1
        
        # Determine portfolio risk level (highest risk wins)
        portfolio_risk = RiskLevel.LOW
        for impact in symbol_impacts:
            if impact.risk_level.value == 'extreme':
                portfolio_risk = RiskLevel.EXTREME
                break
            elif impact.risk_level.value == 'high':
                portfolio_risk = RiskLevel.HIGH
            elif impact.risk_level.value == 'medium' and portfolio_risk == RiskLevel.LOW:
                portfolio_risk = RiskLevel.MEDIUM
        
        # Calculate average adjustment factor
        avg_adjustment = sum(impact.market_adjustment_factor for impact in symbol_impacts) / len(symbol_impacts)
        
        return {
            'portfolio_confidence': portfolio_confidence,
            'portfolio_risk_level': portfolio_risk,
            'symbols_analyzed': len(symbol_impacts),
            'risk_distribution': risk_distribution,
            'average_adjustment_factor': avg_adjustment,
            'individual_impacts': {impact.symbol: impact for impact in symbol_impacts}
        }
    
    def _create_fallback_result(self, symbol: str, base_confidence: float, start_time: datetime) -> MarketImpactResult:
        """Create fallback result when calculation fails"""
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return MarketImpactResult(
            symbol=symbol,
            base_confidence=base_confidence,
            market_adjustment_factor=0.9,  # Conservative adjustment
            final_confidence=base_confidence * 0.9,
            risk_level=RiskLevel.MEDIUM,
            risk_score=50.0,
            volatility_factor=1.0,
            trend_strength_factor=1.0,
            data_quality_factor=0.9,
            market_volatility=None,
            trend_strength=None,
            market_risk_score=None,
            data_quality=None,
            calculation_time_ms=processing_time,
            timestamp=datetime.now()
        )
    
    def get_calculator_config(self) -> Dict[str, Any]:
        """
        Get current calculator configuration
        
        Returns:
            Dictionary with calculator configuration
        """
        return {
            'volatility_thresholds': self.volatility_thresholds,
            'trend_strength_thresholds': self.trend_strength_thresholds,
            'risk_score_thresholds': self.risk_score_thresholds,
            'adjustment_factors': self.adjustment_factors
        }


def create_impact_calculator() -> ImpactCalculator:
    """
    Factory function to create impact calculator
    
    Returns:
        Configured ImpactCalculator instance
    """
    return ImpactCalculator()