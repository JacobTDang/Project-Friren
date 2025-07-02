"""
decision_engine/conflict_resolver.py

Conflict Resolver - XGBoost + Rule-based Fallback System

Resolves conflicts when aggregated signals disagree using:
1. XGBoost model for complex pattern recognition
2. SHAP explainability for decision transparency
3. Rule-based fallback when ML confidence is low
4. Confidence scoring and conflict detection

- ML first, rules as backup (hybrid intelligence)
- Explainable decisions (SHAP values stored in database)
- Fail-safe design (always returns a decision)
- Memory-efficient for t3.micro constraints
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging
import json
import pickle
import os
from pathlib import Path

# ML imports with fallbacks
try:
    import xgboost as xgb
    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False
    logging.warning("XGBoost not available, using rule-based fallback only")

# LAZY LOADING: Only import SHAP when actually needed to avoid memory issues
def _lazy_import_shap():
    """Lazy import SHAP to avoid memory issues during startup"""
    try:
        import shap
        return shap, True
    except ImportError:
        logging.warning("SHAP not available, explainability will be limited")
        return None, False
    except MemoryError:
        logging.warning("SHAP import failed due to memory constraints, explainability will be limited")
        return None, False

# Global variables for lazy loading
_shap_module = None
HAS_SHAP = None

def get_shap():
    """Get SHAP module with lazy loading"""
    global _shap_module, HAS_SHAP
    if HAS_SHAP is None:
        _shap_module, HAS_SHAP = _lazy_import_shap()
    return _shap_module if HAS_SHAP else None

# Import from your signal aggregator
from .signal_aggregator import AggregatedSignal, SignalType

# NEW: Import strategy management components from decision_engine
try:
    from .decision_engine import TransitionSignal, TransitionSignalType, MonitoringStrategyStatus
    STRATEGY_COMPONENTS_AVAILABLE = True
except ImportError:
    STRATEGY_COMPONENTS_AVAILABLE = False


class ConflictType(Enum):
    """Types of signal conflicts"""
    NO_CONFLICT = "no_conflict"          # Signals agree (< 0.3 spread)
    WEAK_CONFLICT = "weak_conflict"      # Minor disagreement (0.3-0.6 spread)
    STRONG_CONFLICT = "strong_conflict"  # Major disagreement (0.6-1.0 spread)
    EXTREME_CONFLICT = "extreme_conflict" # Opposite signals (> 1.0 spread)

    # NEW: Strategy-specific conflicts
    STRATEGY_TRANSITION_CONFLICT = "strategy_transition_conflict"  # Multiple strategy change signals
    MULTI_SIGNAL_PENDING = "multi_signal_pending"  # Awaiting additional signals


class ResolutionMethod(Enum):
    """How the conflict was resolved"""
    ML_PRIMARY = "ml_primary"           # XGBoost made the decision
    ML_ASSISTED = "ml_assisted"         # XGBoost + rules combined
    RULE_BASED = "rule_based"           # Pure rule-based fallback
    CONSENSUS = "consensus"             # Signals agreed
    FORCED_HOLD = "forced_hold"         # No clear resolution

    # NEW: Strategy-specific resolution methods
    MULTI_SIGNAL_CONFIRMATION = "multi_signal_confirmation"  # Multiple signals confirmed transition
    STRATEGY_CONTINUITY = "strategy_continuity"  # Keep current strategy
    EMERGENCY_STRATEGY_CHANGE = "emergency_strategy_change"  # Immediate strategy change needed


@dataclass
class ConflictAnalysis:
    """Analysis of signal conflicts"""
    conflict_type: ConflictType
    conflict_magnitude: float  # 0.0 to 2.0 (higher = more conflict)
    component_disagreement: Dict[str, float]  # How much each component disagrees
    confidence_spread: float  # Variance in confidence levels
    signal_count: int

    # Conflict details
    bullish_components: List[str]
    bearish_components: List[str]
    neutral_components: List[str]

    # Risk factors
    risk_flags: List[str] = field(default_factory=list)
    uncertainty_score: float = 0.0


@dataclass
class MLDecision:
    """ML model decision output"""
    predicted_direction: float  # -1.0 to 1.0
    ml_confidence: float       # 0.0 to 1.0 (model confidence)
    feature_importance: Dict[str, float]  # Feature contributions
    shap_values: Optional[Dict[str, float]] = None  # SHAP explanations
    model_version: str = "v1.0"

    # Model diagnostics
    prediction_probability: Optional[Dict[str, float]] = None
    model_uncertainty: float = 0.0


@dataclass
class RuleDecision:
    """Rule-based decision output"""
    rule_direction: float      # -1.0 to 1.0
    rule_confidence: float     # 0.0 to 1.0
    applied_rules: List[str]   # Which rules were triggered
    rule_reasoning: str        # Human-readable explanation

    # Rule details
    dominant_signal: str       # Which signal type dominated
    override_reasons: List[str] = field(default_factory=list)


@dataclass
class StrategyTransitionAnalysis:
    """Analysis for strategy transition conflicts"""
    transition_signals: List[Dict] = field(default_factory=list)  # Raw transition signals
    signal_confidence_scores: List[float] = field(default_factory=list)  # Individual confidences
    combined_confidence: float = 0.0  # Weighted combination
    transition_urgency: str = "LOW"  # LOW, MEDIUM, HIGH, CRITICAL
    conflicting_strategies: List[str] = field(default_factory=list)  # Strategies in conflict

    # Multi-signal confirmation status
    signals_needed: int = 2  # How many signals needed for confirmation
    signals_received: int = 0  # How many received so far
    signal_window_remaining: float = 0.0  # Minutes left in signal window

    # Strategy continuity factors
    current_strategy_performance: float = 0.0  # How well current strategy is doing
    transition_risk_score: float = 0.0  # Risk of changing strategy now


@dataclass
class StrategyTransitionDecision:
    """Strategy transition specific decision"""
    should_transition: bool
    recommended_strategy: Optional[str] = None
    transition_confidence: float = 0.0
    wait_for_more_signals: bool = False
    emergency_change: bool = False

    # Decision reasoning
    transition_reasoning: str = ""
    risk_factors: List[str] = field(default_factory=list)
    supporting_signals: List[str] = field(default_factory=list)


@dataclass
class ResolvedDecision:
    """Final resolved decision after conflict resolution"""
    symbol: str
    final_direction: float     # -1.0 to 1.0 (final decision)
    final_confidence: float    # 0.0 to 1.0 (overall confidence)
    resolution_method: ResolutionMethod

    # Component decisions
    ml_decision: Optional[MLDecision] = None
    rule_decision: Optional[RuleDecision] = None
    conflict_analysis: Optional[ConflictAnalysis] = None

    # NEW: Strategy management decisions
    strategy_transition_analysis: Optional[StrategyTransitionAnalysis] = None
    strategy_decision: Optional[StrategyTransitionDecision] = None

    # Metadata
    processing_time_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)

    # Explainability data (for database storage)
    explanation: str = ""
    shap_values_json: Optional[str] = None
    decision_reasoning: str = ""

    def get_recommendation(self) -> str:
        """Get human-readable recommendation"""
        if self.final_direction > 0.6:
            return "STRONG_BUY"
        elif self.final_direction > 0.3:
            return "BUY"
        elif self.final_direction > -0.3:
            return "HOLD"
        elif self.final_direction > -0.6:
            return "SELL"
        else:
            return "STRONG_SELL"

    def get_confidence_level(self) -> str:
        """Get confidence level description"""
        if self.final_confidence >= 0.8:
            return "HIGH"
        elif self.final_confidence >= 0.6:
            return "MEDIUM"
        elif self.final_confidence >= 0.4:
            return "LOW"
        else:
            return "VERY_LOW"


class ConflictResolver:
    """
    Hybrid Conflict Resolution System

    Architecture:
    1. Conflict Detection - Analyze signal disagreements
    2. ML Resolution - Use XGBoost for complex patterns
    3. Rule Fallback - Simple rules when ML fails
    4. Explanation Generation - SHAP + human-readable reasoning
    5. Confidence Scoring - Overall decision confidence
    """

    def __init__(self, model_path: Optional[str] = None,
                 config: Optional[Dict] = None):
        self.logger = logging.getLogger("conflict_resolver")

        # Configuration
        self.config = self._load_config(config)

        # ML Model loading
        self.xgb_model = None
        self.shap_explainer = None
        self.feature_names = None

        if HAS_XGBOOST and model_path and os.path.exists(model_path):
            self._load_ml_model(model_path)
        else:
            self.logger.warning("XGBoost model not available, using rule-based fallback only")

        # Rule-based system
        self.rule_engine = RuleBasedResolver(self.config)

        # Performance tracking
        self.resolution_stats = {
            'total_resolutions': 0,
            'ml_resolutions': 0,
            'rule_resolutions': 0,
            'consensus_resolutions': 0,
            'conflict_distributions': {ct.value: 0 for ct in ConflictType},
            'avg_processing_time_ms': 0.0,
            'ml_accuracy': 0.5,  # Will be updated based on feedback
            'rule_accuracy': 0.5
        }

        self.logger.info(f"ConflictResolver initialized (ML: {self.xgb_model is not None}, "
                        f"SHAP: {self.shap_explainer is not None})")

    def resolve_conflict(self, aggregated_signal: AggregatedSignal) -> ResolvedDecision:
        """
        Main conflict resolution entry point

        Args:
            aggregated_signal: AggregatedSignal from signal aggregator

        Returns:
            ResolvedDecision with final decision and explanation
        """
        start_time = datetime.now()

        try:
            # 1. Analyze conflicts in the aggregated signal
            conflict_analysis = self._analyze_conflicts(aggregated_signal)

            # 2. Determine resolution strategy based on conflict type
            resolution_method = self._determine_resolution_strategy(conflict_analysis)

            # 3. Resolve using appropriate method
            if resolution_method == ResolutionMethod.ML_PRIMARY and self.xgb_model:
                resolved = self._resolve_with_ml_primary(aggregated_signal, conflict_analysis)
            elif resolution_method == ResolutionMethod.ML_ASSISTED and self.xgb_model:
                resolved = self._resolve_with_ml_assisted(aggregated_signal, conflict_analysis)
            elif resolution_method == ResolutionMethod.CONSENSUS:
                resolved = self._resolve_consensus(aggregated_signal, conflict_analysis)
            else:
                resolved = self._resolve_with_rules(aggregated_signal, conflict_analysis)

            # 4. Add timing and metadata
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            resolved.processing_time_ms = processing_time
            resolved.conflict_analysis = conflict_analysis

            # 5. Update statistics
            self._update_resolution_stats(resolved)

            self.logger.info(f"Resolved conflict for {aggregated_signal.symbol}: "
                           f"{resolved.get_recommendation()} ({resolved.get_confidence_level()}) "
                           f"via {resolved.resolution_method.value}")

            # BUSINESS LOGIC OUTPUT: Recommendation generated
            try:
                from terminal_color_system import print_recommendation
                # Try to get article context from signal
                article_context = ""
                if hasattr(aggregated_signal, 'news_articles') and aggregated_signal.news_articles:
                    first_article = aggregated_signal.news_articles[0]
                    article_title = first_article.title[:50] + "..." if len(first_article.title) > 50 else first_article.title
                    article_context = f" | Article: '{article_title}'"
                
                print_recommendation(f"{aggregated_signal.symbol}: {resolved.get_recommendation()} "
                                   f"(confidence: {resolved.get_confidence_level()})"
                                   f"{article_context}")
            except ImportError:
                article_context = ""
                if hasattr(aggregated_signal, 'news_articles') and aggregated_signal.news_articles:
                    first_article = aggregated_signal.news_articles[0]
                    article_title = first_article.title[:50] + "..." if len(first_article.title) > 50 else first_article.title
                    article_context = f" | Article: '{article_title}'"
                
                print(f"[RECOMMENDATION] {aggregated_signal.symbol}: {resolved.get_recommendation()} "
                      f"(confidence: {resolved.get_confidence_level()})"
                      f"{article_context}")

            return resolved

        except Exception as e:
            self.logger.error(f"Error resolving conflict for {aggregated_signal.symbol}: {e}")
            # Return safe fallback decision
            return self._create_fallback_decision(aggregated_signal, str(e))

    def resolve_strategy_transition_signals(self, symbol: str, transition_signals: List[Dict],
                                          current_strategy: str, current_performance: float = 0.0) -> StrategyTransitionDecision:
        """
        NEW: Resolve strategy transition conflicts using multi-signal confirmation

        This is the core method for Phase 3 implementation per implementation_rules.xml:
        - Requires 2-3 confirming signals with >0.75 combined confidence
        - 15-minute signal window for confirmation
        - Emergency transitions for critical situations

        Args:
            symbol: Trading symbol
            transition_signals: List of transition signals to evaluate
            current_strategy: Currently active strategy
            current_performance: How well current strategy is performing (0.0-1.0)

        Returns:
            StrategyTransitionDecision with final transition recommendation
        """
        try:
            # Analyze transition signals
            transition_analysis = self._analyze_strategy_transition_signals(
                symbol, transition_signals, current_strategy, current_performance
            )

            # Apply multi-signal confirmation logic
            strategy_decision = self._apply_multi_signal_confirmation(transition_analysis)

            # Log decision for audit trail
            self.logger.info(
                f" Strategy transition analysis for {symbol}: "
                f"{'TRANSITION' if strategy_decision.should_transition else 'CONTINUE'} "
                f"(confidence: {strategy_decision.transition_confidence:.2f}, "
                f"signals: {transition_analysis.signals_received}/{transition_analysis.signals_needed})"
            )

            return strategy_decision

        except Exception as e:
            self.logger.error(f"Error resolving strategy transition for {symbol}: {e}")
            # Safe fallback: keep current strategy
            return StrategyTransitionDecision(
                should_transition=False,
                transition_reasoning=f"Error in analysis: {e}",
                risk_factors=["analysis_error"]
            )

    def _analyze_conflicts(self, signal: AggregatedSignal) -> ConflictAnalysis:
        """Analyze the types and magnitude of conflicts in aggregated signals"""

        # Get component values
        components = {
            'technical': signal.technical_component,
            'sentiment': signal.sentiment_component,
            'market': signal.market_component,
            'risk': signal.risk_component
        }

        # Calculate disagreement metrics
        component_values = list(components.values())
        non_zero_components = [v for v in component_values if abs(v) > 0.1]

        if len(non_zero_components) < 2:
            # Not enough signals for conflict
            return ConflictAnalysis(
                conflict_type=ConflictType.NO_CONFLICT,
                conflict_magnitude=0.0,
                component_disagreement=components,
                confidence_spread=0.0,
                signal_count=len(signal.raw_signals),
                bullish_components=[],
                bearish_components=[],
                neutral_components=list(components.keys())
            )

        # Calculate conflict magnitude (standard deviation of components)
        conflict_magnitude = np.std(component_values) if component_values else 0.0

        # Categorize components by direction
        bullish = [k for k, v in components.items() if v > 0.3]
        bearish = [k for k, v in components.items() if v < -0.3]
        neutral = [k for k, v in components.items() if -0.3 <= v <= 0.3]

        # Determine conflict type
        if conflict_magnitude < 0.3:
            conflict_type = ConflictType.NO_CONFLICT
        elif conflict_magnitude < 0.6:
            conflict_type = ConflictType.WEAK_CONFLICT
        elif conflict_magnitude < 1.0:
            conflict_type = ConflictType.STRONG_CONFLICT
        else:
            conflict_type = ConflictType.EXTREME_CONFLICT

        # Risk flags
        risk_flags = []
        if signal.confidence_spread > 0.4:
            risk_flags.append("high_confidence_variance")
        if len(bullish) > 0 and len(bearish) > 0:
            risk_flags.append("mixed_signals")
        if abs(signal.risk_component) > 0.5:
            risk_flags.append("high_risk_component")

        return ConflictAnalysis(
            conflict_type=conflict_type,
            conflict_magnitude=conflict_magnitude,
            component_disagreement=components,
            confidence_spread=signal.confidence_spread,
            signal_count=len(signal.raw_signals),
            bullish_components=bullish,
            bearish_components=bearish,
            neutral_components=neutral,
            risk_flags=risk_flags,
            uncertainty_score=min(1.0, conflict_magnitude + signal.confidence_spread)
        )

    def _determine_resolution_strategy(self, conflict_analysis: ConflictAnalysis) -> ResolutionMethod:
        """Determine which resolution method to use based on conflict analysis"""

        # No conflict - use consensus
        if conflict_analysis.conflict_type == ConflictType.NO_CONFLICT:
            return ResolutionMethod.CONSENSUS

        # Strong/extreme conflicts with ML available - use ML
        if (conflict_analysis.conflict_type in [ConflictType.STRONG_CONFLICT, ConflictType.EXTREME_CONFLICT]
            and self.xgb_model is not None):
            return ResolutionMethod.ML_PRIMARY

        # Weak conflicts with ML available - use ML assisted
        if (conflict_analysis.conflict_type == ConflictType.WEAK_CONFLICT
            and self.xgb_model is not None):
            return ResolutionMethod.ML_ASSISTED

        # Fallback to rules
        return ResolutionMethod.RULE_BASED

    def _resolve_with_ml_primary(self, signal: AggregatedSignal,
                               conflict_analysis: ConflictAnalysis) -> ResolvedDecision:
        """Resolve using ML model as primary decision maker"""

        # Prepare features for ML model
        features = self._prepare_ml_features(signal, conflict_analysis)

        # Get ML prediction
        ml_decision = self._predict_with_xgboost(features, signal.symbol)

        # Generate explanation
        explanation = self._generate_ml_explanation(ml_decision, conflict_analysis)

        return ResolvedDecision(
            symbol=signal.symbol,
            final_direction=ml_decision.predicted_direction,
            final_confidence=ml_decision.ml_confidence * 0.9,  # Slightly reduce for conflict
            resolution_method=ResolutionMethod.ML_PRIMARY,
            ml_decision=ml_decision,
            explanation=explanation,
            shap_values_json=json.dumps(ml_decision.shap_values) if ml_decision.shap_values else None,
            decision_reasoning=f"ML model resolved {conflict_analysis.conflict_type.value} with {ml_decision.ml_confidence:.2f} confidence"
        )

    def _resolve_with_ml_assisted(self, signal: AggregatedSignal,
                                conflict_analysis: ConflictAnalysis) -> ResolvedDecision:
        """Resolve using ML + rules combination"""

        # Get both ML and rule decisions
        features = self._prepare_ml_features(signal, conflict_analysis)
        ml_decision = self._predict_with_xgboost(features, signal.symbol)
        rule_decision = self.rule_engine.resolve_with_rules(signal, conflict_analysis)

        # Combine decisions (weighted by confidence)
        ml_weight = ml_decision.ml_confidence
        rule_weight = rule_decision.rule_confidence
        total_weight = ml_weight + rule_weight

        if total_weight > 0:
            final_direction = (
                (ml_decision.predicted_direction * ml_weight +
                 rule_decision.rule_direction * rule_weight) / total_weight
            )
            final_confidence = (ml_weight + rule_weight) / 2
        else:
            final_direction = 0.0
            final_confidence = 0.0

        explanation = f"Combined ML ({ml_decision.ml_confidence:.2f}) and rules ({rule_decision.rule_confidence:.2f})"

        return ResolvedDecision(
            symbol=signal.symbol,
            final_direction=final_direction,
            final_confidence=final_confidence,
            resolution_method=ResolutionMethod.ML_ASSISTED,
            ml_decision=ml_decision,
            rule_decision=rule_decision,
            explanation=explanation,
            shap_values_json=json.dumps(ml_decision.shap_values) if ml_decision.shap_values else None,
            decision_reasoning=f"ML+Rules hybrid resolution: {explanation}"
        )

    def _resolve_consensus(self, signal: AggregatedSignal,
                         conflict_analysis: ConflictAnalysis) -> ResolvedDecision:
        """Resolve when signals are in consensus (no conflict)"""

        # Use the aggregated signal directly since there's no conflict
        final_direction = signal.final_direction
        final_confidence = signal.final_confidence

        # Determine dominant component
        components = conflict_analysis.component_disagreement
        dominant_component = max(components.keys(), key=lambda k: abs(components[k]))

        explanation = f"Signal consensus: {dominant_component} component dominates"

        return ResolvedDecision(
            symbol=signal.symbol,
            final_direction=final_direction,
            final_confidence=final_confidence,
            resolution_method=ResolutionMethod.CONSENSUS,
            explanation=explanation,
            decision_reasoning=f"No conflict detected, using aggregated signal directly"
        )

    def _resolve_with_rules(self, signal: AggregatedSignal,
                          conflict_analysis: ConflictAnalysis) -> ResolvedDecision:
        """Resolve using pure rule-based system"""

        rule_decision = self.rule_engine.resolve_with_rules(signal, conflict_analysis)

        return ResolvedDecision(
            symbol=signal.symbol,
            final_direction=rule_decision.rule_direction,
            final_confidence=rule_decision.rule_confidence,
            resolution_method=ResolutionMethod.RULE_BASED,
            rule_decision=rule_decision,
            explanation=rule_decision.rule_reasoning,
            decision_reasoning=f"Rule-based resolution: {rule_decision.rule_reasoning}"
        )

    def _prepare_ml_features(self, signal: AggregatedSignal,
                           conflict_analysis: ConflictAnalysis) -> Dict[str, float]:
        """Prepare features for ML model"""

        features = {
            # Signal components
            'technical_component': signal.technical_component,
            'sentiment_component': signal.sentiment_component,
            'market_component': signal.market_component,
            'risk_component': signal.risk_component,

            # Aggregated signal metrics
            'final_direction': signal.final_direction,
            'final_confidence': signal.final_confidence,
            'signal_agreement': signal.signal_agreement,
            'confidence_spread': signal.confidence_spread,
            'signal_count': signal.signal_count,

            # Conflict metrics
            'conflict_magnitude': conflict_analysis.conflict_magnitude,
            'uncertainty_score': conflict_analysis.uncertainty_score,
            'bullish_count': len(conflict_analysis.bullish_components),
            'bearish_count': len(conflict_analysis.bearish_components),
            'neutral_count': len(conflict_analysis.neutral_components),

            # Risk flags (encoded)
            'has_high_confidence_variance': 1.0 if 'high_confidence_variance' in conflict_analysis.risk_flags else 0.0,
            'has_mixed_signals': 1.0 if 'mixed_signals' in conflict_analysis.risk_flags else 0.0,
            'has_high_risk': 1.0 if 'high_risk_component' in conflict_analysis.risk_flags else 0.0,

            # Applied weights (current signal weights)
            'technical_weight': signal.applied_weights.get('technical', 0.3),
            'sentiment_weight': signal.applied_weights.get('sentiment', 0.3),
            'market_weight': signal.applied_weights.get('market', 0.3),
            'risk_weight': signal.applied_weights.get('risk', 0.1)
        }

        return features

    def _predict_with_xgboost(self, features: Dict[str, float], symbol: str) -> MLDecision:
        """Make prediction using XGBoost model with validation"""

        # VALIDATION: Check model availability and health
        if not self.xgb_model:
            self.logger.warning(f"NO XGBOOST MODEL: Cannot make ML prediction for {symbol} - using rule-based fallback")
            return MLDecision(
                predicted_direction=0.0,
                ml_confidence=0.0,
                feature_importance={},
                model_version="none"
            )
        
        # VALIDATION: Check XGBoost library availability
        if not HAS_XGBOOST:
            self.logger.error(f"XGBOOST LIBRARY MISSING: Cannot make prediction for {symbol}")
            return MLDecision(
                predicted_direction=0.0,
                ml_confidence=0.0,
                feature_importance={},
                model_version="xgboost_library_missing"
            )
            
        # VALIDATION: Validate input features
        if not features or len(features) == 0:
            self.logger.warning(f"NO FEATURES: Cannot make XGBoost prediction for {symbol} without features")
            return MLDecision(
                predicted_direction=0.0,
                ml_confidence=0.0,
                feature_importance={},
                model_version="xgboost_no_features"
            )

        try:
            # Convert features to array (ensure correct order)
            if self.feature_names:
                feature_array = np.array([features.get(name, 0.0) for name in self.feature_names])
            else:
                feature_array = np.array(list(features.values()))

            feature_array = feature_array.reshape(1, -1)

            # Make prediction using sklearn interface
            if hasattr(self.xgb_model, 'predict_proba'):
                # Classification model - use probability predictions
                probabilities = self.xgb_model.predict_proba(feature_array)[0]
                # Assume classes are [sell, hold, buy] (0, 1, 2)
                predicted_direction = (probabilities[2] - probabilities[0])  # buy - sell
                ml_confidence = np.max(probabilities)
                prediction_probability = dict(zip(['sell', 'hold', 'buy'], probabilities))
            else:
                # Regression model or fallback
                prediction = self.xgb_model.predict(feature_array)[0]
                predicted_direction = np.clip(prediction, -1.0, 1.0)
                ml_confidence = 1.0 - abs(prediction - np.round(prediction))  # Confidence based on certainty
                prediction_probability = None

            # Feature importance
            if hasattr(self.xgb_model, 'feature_importances_'):
                importance_dict = {}
                for i, importance in enumerate(self.xgb_model.feature_importances_):
                    feature_name = self.feature_names[i] if self.feature_names else f"feature_{i}"
                    importance_dict[feature_name] = float(importance)
            else:
                importance_dict = {}

            # SHAP values if available
            shap_values = None
            if self.shap_explainer and HAS_SHAP:
                try:
                    shap_vals = self.shap_explainer.shap_values(feature_array)
                    if isinstance(shap_vals, list):
                        shap_vals = shap_vals[0]  # Take first class for classification

                    shap_values = {}
                    for i, shap_val in enumerate(shap_vals[0]):
                        feature_name = self.feature_names[i] if self.feature_names else f"feature_{i}"
                        shap_values[feature_name] = float(shap_val)
                except Exception as e:
                    self.logger.warning(f"SHAP calculation failed: {e}")

            # Convert direction to action for logging
            if predicted_direction > 0.1:
                action = "BUY"
            elif predicted_direction < -0.1:
                action = "SELL"
            else:
                action = "HOLD"
            
            # Log XGBoost decision (required business logic output)
            self.logger.info(f"XGBoost decision: {action} {symbol} (confidence: {ml_confidence:.2f})")
            
            # BUSINESS LOGIC OUTPUT: XGBoost colored terminal output
            try:
                from terminal_color_system import print_xgboost_decision
                print_xgboost_decision(f"{action} {symbol} (confidence: {ml_confidence:.2f})")
            except:
                print(f"[XGBOOST] {action} {symbol} (confidence: {ml_confidence:.2f})")
            
            return MLDecision(
                predicted_direction=predicted_direction,
                ml_confidence=ml_confidence,
                feature_importance=importance_dict,
                shap_values=shap_values,
                model_version="xgboost_v1.0",
                prediction_probability=prediction_probability,
                model_uncertainty=1.0 - ml_confidence
            )

        except Exception as e:
            self.logger.error(f"XGBoost prediction failed for {symbol}: {e}")
            return MLDecision(
                predicted_direction=0.0,
                ml_confidence=0.0,
                feature_importance={},
                model_version="xgboost_v1.0_failed"
            )

    def _generate_ml_explanation(self, ml_decision: MLDecision,
                               conflict_analysis: ConflictAnalysis) -> str:
        """Generate human-readable explanation for ML decision"""

        explanation_parts = []

        # Main decision
        direction_str = "bullish" if ml_decision.predicted_direction > 0 else "bearish"
        explanation_parts.append(f"ML model is {direction_str} with {ml_decision.ml_confidence:.1%} confidence")

        # Top contributing features
        if ml_decision.shap_values:
            top_features = sorted(ml_decision.shap_values.items(),
                                key=lambda x: abs(x[1]), reverse=True)[:3]
            if top_features:
                feature_explanations = []
                for feature, value in top_features:
                    impact = "positive" if value > 0 else "negative"
                    feature_explanations.append(f"{feature} ({impact})")
                explanation_parts.append(f"Key factors: {', '.join(feature_explanations)}")

        # Conflict context
        explanation_parts.append(f"Resolved {conflict_analysis.conflict_type.value}")

        return ". ".join(explanation_parts)

    def _create_fallback_decision(self, signal: AggregatedSignal, error: str) -> ResolvedDecision:
        """Create a safe fallback decision when resolution fails"""

        return ResolvedDecision(
            symbol=signal.symbol,
            final_direction=0.0,  # Safe hold decision
            final_confidence=0.0,
            resolution_method=ResolutionMethod.FORCED_HOLD,
            explanation=f"Fallback decision due to error: {error}",
            decision_reasoning="Error in conflict resolution, defaulting to HOLD for safety"
        )

    def _load_ml_model(self, model_path: str):
        """Load XGBoost model with comprehensive validation and health checks"""
        try:
            # VALIDATION: Check XGBoost availability before attempting load
            if not HAS_XGBOOST:
                self.logger.error("XGBOOST NOT AVAILABLE: Cannot load trading model - install xgboost library")
                self.xgb_model = None
                return
            
            # VALIDATION: Check model file exists and is readable
            if not os.path.exists(model_path):
                self.logger.error(f"MODEL FILE NOT FOUND: {model_path} - XGBoost decisions unavailable")
                self.xgb_model = None
                return
            
            # Check file size and modification time for basic validation
            file_stat = os.stat(model_path)
            if file_stat.st_size == 0:
                self.logger.error(f"MODEL FILE EMPTY: {model_path} - corrupted model file")
                self.xgb_model = None
                return
            
            # Try loading as sklearn XGBoost model first (for demo model)
            try:
                import joblib
                self.xgb_model = joblib.load(model_path.replace('.json', '.pkl'))
                self.logger.info("Loaded sklearn XGBoost model")
                model_type = "sklearn"
            except Exception as sklearn_error:
                try:
                    # Fallback to native XGBoost format
                    self.xgb_model = xgb.Booster()
                    self.xgb_model.load_model(model_path)
                    self.logger.info("Loaded native XGBoost model")
                    model_type = "native"
                except Exception as native_error:
                    self.logger.error(f"MODEL LOAD FAILED: sklearn error: {sklearn_error}, native error: {native_error}")
                    self.xgb_model = None
                    return

            # VALIDATION: Perform model health checks
            model_validation_result = self._validate_model_health()
            if not model_validation_result:
                self.logger.error("MODEL VALIDATION FAILED: XGBoost model failed health checks")
                self.xgb_model = None
                return

            # Load feature names if available
            feature_names_path = Path(model_path).parent / "feature_names.pkl"
            if feature_names_path.exists():
                try:
                    with open(feature_names_path, 'rb') as f:
                        self.feature_names = pickle.load(f)
                    self.logger.info(f"Loaded {len(self.feature_names)} feature names")
                except Exception as e:
                    self.logger.warning(f"Failed to load feature names: {e}")
                    self.feature_names = None
            else:
                self.logger.warning("Feature names file not found - using default names")

            # Setup SHAP explainer with lazy loading and validation
            shap_module = get_shap()
            if shap_module:
                try:
                    self.shap_explainer = shap_module.TreeExplainer(self.xgb_model)
                    self.logger.info("SHAP explainer initialized for model transparency")
                except Exception as e:
                    self.logger.warning(f"SHAP explainer setup failed: {e}")
                    self.shap_explainer = None
            else:
                self.shap_explainer = None
                self.logger.warning("SHAP not available - explainability will be limited")

            # Log successful initialization
            self.logger.info(f"XGBoost model successfully loaded and validated from {model_path}")
            self.logger.info(f"Model type: {model_type}, Features: {len(self.feature_names) if self.feature_names else 'unknown'}")
            self.logger.info(f"SHAP available: {self.shap_explainer is not None}")

        except Exception as e:
            self.logger.error(f"CRITICAL: XGBoost model loading failed: {e}")
            self.xgb_model = None
            # Don't raise exception - fall back to rule-based decisions

    def _validate_model_health(self) -> bool:
        """Perform comprehensive model health checks"""
        if not self.xgb_model:
            return False
        
        try:
            # Test prediction with dummy data
            test_features = np.array([[0.5, 0.3, -0.2, 0.1, 0.0]]).reshape(1, -1)
            
            # Try prediction based on model type
            if hasattr(self.xgb_model, 'predict_proba'):
                # Sklearn interface
                prediction = self.xgb_model.predict_proba(test_features)
                if prediction is None or len(prediction) == 0:
                    self.logger.error("Model health check failed: predict_proba returned invalid result")
                    return False
            elif hasattr(self.xgb_model, 'predict'):
                # Sklearn or native interface
                prediction = self.xgb_model.predict(test_features)
                if prediction is None or len(prediction) == 0:
                    self.logger.error("Model health check failed: predict returned invalid result")
                    return False
            else:
                self.logger.error("Model health check failed: no prediction method available")
                return False
            
            # Check feature importance availability
            if hasattr(self.xgb_model, 'feature_importances_'):
                importance = self.xgb_model.feature_importances_
                if importance is None or len(importance) == 0:
                    self.logger.warning("Model health check warning: no feature importances available")
            
            self.logger.info("XGBoost model passed health checks")
            return True
            
        except Exception as e:
            self.logger.error(f"Model health check failed with exception: {e}")
            return False

    def _load_config(self, config: Optional[Dict]) -> Dict:
        """Load configuration with defaults"""
        default_config = {
            'ml_confidence_threshold': 0.6,
            'rule_confidence_threshold': 0.5,
            'max_processing_time_ms': 500,  # t3.micro constraint
            'conflict_magnitude_threshold': 0.3,
            'enable_shap_explanations': True,
            'feature_importance_top_k': 5
        }

        if config:
            default_config.update(config)

        return default_config

    def _update_resolution_stats(self, resolved: ResolvedDecision):
        """Update resolution statistics"""
        self.resolution_stats['total_resolutions'] += 1

        if resolved.resolution_method == ResolutionMethod.ML_PRIMARY:
            self.resolution_stats['ml_resolutions'] += 1
        elif resolved.resolution_method == ResolutionMethod.RULE_BASED:
            self.resolution_stats['rule_resolutions'] += 1
        elif resolved.resolution_method == ResolutionMethod.CONSENSUS:
            self.resolution_stats['consensus_resolutions'] += 1

        if resolved.conflict_analysis:
            conflict_type = resolved.conflict_analysis.conflict_type.value
            self.resolution_stats['conflict_distributions'][conflict_type] += 1

        # Update average processing time
        total = self.resolution_stats['total_resolutions']
        old_avg = self.resolution_stats['avg_processing_time_ms']
        new_time = resolved.processing_time_ms
        self.resolution_stats['avg_processing_time_ms'] = ((old_avg * (total-1)) + new_time) / total

    def get_resolution_stats(self) -> Dict[str, Any]:
        """Get current resolution statistics"""
        return self.resolution_stats.copy()

    def update_model_performance(self, symbol: str, was_successful: bool,
                               resolution_method: ResolutionMethod):
        """Update model performance tracking based on feedback"""
        if resolution_method in [ResolutionMethod.ML_PRIMARY, ResolutionMethod.ML_ASSISTED]:
            # Update ML accuracy
            current_acc = self.resolution_stats['ml_accuracy']
            success_rate = 1.0 if was_successful else 0.0
            # Simple exponential moving average
            self.resolution_stats['ml_accuracy'] = 0.95 * current_acc + 0.05 * success_rate

        elif resolution_method == ResolutionMethod.RULE_BASED:
            # Update rule accuracy
            current_acc = self.resolution_stats['rule_accuracy']
            success_rate = 1.0 if was_successful else 0.0
            self.resolution_stats['rule_accuracy'] = 0.95 * current_acc + 0.05 * success_rate

    def _analyze_strategy_transition_signals(self, symbol: str, transition_signals: List[Dict],
                                           current_strategy: str, current_performance: float) -> StrategyTransitionAnalysis:
        """
        Analyze strategy transition signals for multi-signal confirmation

        Implements the Phase 3 analysis logic per implementation_rules.xml
        """
        if not transition_signals:
            return StrategyTransitionAnalysis(
                signals_received=0,
                signals_needed=2,
                transition_urgency="LOW",
                current_strategy_performance=current_performance
            )

        # Extract confidence scores and signal types
        confidence_scores = []
        signal_types = []
        conflicting_strategies = set()

        for signal in transition_signals:
            confidence_scores.append(signal.get('confidence', 0.0))
            signal_types.append(signal.get('type', 'unknown'))

            # Track conflicting strategy recommendations
            if 'recommended_strategy' in signal and signal['recommended_strategy'] != current_strategy:
                conflicting_strategies.add(signal['recommended_strategy'])

        # Calculate combined confidence using weighted average
        # More recent signals get higher weight
        if confidence_scores:
            weights = [1.0 + (i * 0.1) for i in range(len(confidence_scores))]  # Recent signals weighted higher
            total_weight = sum(weights)
            combined_confidence = sum(c * w for c, w in zip(confidence_scores, weights)) / total_weight
        else:
            combined_confidence = 0.0

        # Determine transition urgency
        max_confidence = max(confidence_scores) if confidence_scores else 0.0
        urgent_signal_types = ['HEALTH_ALERT', 'VOLATILITY_SPIKE', 'EMERGENCY_EXIT']

        if any(st in urgent_signal_types for st in signal_types) or max_confidence > 0.9:
            urgency = "CRITICAL"
        elif max_confidence > 0.8 or len(transition_signals) >= 3:
            urgency = "HIGH"
        elif max_confidence > 0.65 or len(transition_signals) >= 2:
            urgency = "MEDIUM"
        else:
            urgency = "LOW"

        # Calculate signal window remaining (15 minutes from first signal)
        if transition_signals:
            oldest_signal_time = min(signal.get('timestamp', datetime.now()) for signal in transition_signals)
            signal_window_remaining = max(0, 15.0 - (datetime.now() - oldest_signal_time).total_seconds() / 60)
        else:
            signal_window_remaining = 15.0

        # Calculate transition risk score
        risk_factors = []
        risk_score = 0.0

        if current_performance < 0.3:
            risk_score += 0.3  # Poor performance increases transition risk
            risk_factors.append("poor_current_performance")

        if len(conflicting_strategies) > 1:
            risk_score += 0.2  # Multiple strategy recommendations add risk
            risk_factors.append("multiple_strategy_conflicts")

        if signal_window_remaining < 5.0:
            risk_score += 0.1  # Time pressure adds risk
            risk_factors.append("time_pressure")

        return StrategyTransitionAnalysis(
            transition_signals=transition_signals,
            signal_confidence_scores=confidence_scores,
            combined_confidence=combined_confidence,
            transition_urgency=urgency,
            conflicting_strategies=list(conflicting_strategies),
            signals_needed=3 if urgency == "CRITICAL" else 2,  # Critical situations need more confirmation
            signals_received=len(transition_signals),
            signal_window_remaining=signal_window_remaining,
            current_strategy_performance=current_performance,
            transition_risk_score=min(1.0, risk_score)
        )

    def _apply_multi_signal_confirmation(self, analysis: StrategyTransitionAnalysis) -> StrategyTransitionDecision:
        """
        Apply multi-signal confirmation logic per implementation_rules.xml

        Rules:
        - Requires 2-3 confirming signals with >0.75 combined confidence
        - Emergency situations can override with single high-confidence signal
        - Must consider strategy continuity vs. change benefits
        """

        # Emergency override check
        if analysis.transition_urgency == "CRITICAL" and analysis.combined_confidence > 0.9:
            return StrategyTransitionDecision(
                should_transition=True,
                transition_confidence=analysis.combined_confidence,
                emergency_change=True,
                transition_reasoning="Emergency transition due to critical signal",
                supporting_signals=[s.get('type', 'unknown') for s in analysis.transition_signals]
            )

        # Multi-signal confirmation check
        has_enough_signals = analysis.signals_received >= analysis.signals_needed
        has_enough_confidence = analysis.combined_confidence > 0.75

        if has_enough_signals and has_enough_confidence:
            # Additional validation: ensure signals are diverse (not all from same source)
            signal_sources = set(s.get('source', 'unknown') for s in analysis.transition_signals)
            has_diverse_sources = len(signal_sources) >= 2

            if has_diverse_sources:
                # Find most recommended strategy
                strategy_votes = {}
                for signal in analysis.transition_signals:
                    strategy = signal.get('recommended_strategy')
                    if strategy:
                        strategy_votes[strategy] = strategy_votes.get(strategy, 0) + signal.get('confidence', 0.0)

                recommended_strategy = max(strategy_votes.keys(), key=strategy_votes.get) if strategy_votes else None

                return StrategyTransitionDecision(
                    should_transition=True,
                    recommended_strategy=recommended_strategy,
                    transition_confidence=analysis.combined_confidence,
                    transition_reasoning=f"Multi-signal confirmation: {analysis.signals_received} signals with {analysis.combined_confidence:.2f} confidence",
                    supporting_signals=[s.get('type', 'unknown') for s in analysis.transition_signals]
                )
            else:
                return StrategyTransitionDecision(
                    should_transition=False,
                    wait_for_more_signals=True,
                    transition_reasoning="Signals from single source, awaiting diverse confirmation",
                    risk_factors=["single_source_signals"]
                )

        # Not enough signals or confidence
        elif analysis.signal_window_remaining > 0:
            return StrategyTransitionDecision(
                should_transition=False,
                wait_for_more_signals=True,
                transition_reasoning=f"Awaiting additional signals: {analysis.signals_received}/{analysis.signals_needed} received, {analysis.combined_confidence:.2f} confidence",
                supporting_signals=[s.get('type', 'unknown') for s in analysis.transition_signals]
            )

        # Signal window expired
        else:
            return StrategyTransitionDecision(
                should_transition=False,
                transition_reasoning="Signal confirmation window expired without sufficient confirmation",
                risk_factors=["window_expired", "insufficient_signals"]
            )


class RuleBasedResolver:
    """
    Rule-based fallback system for conflict resolution

    Implements simple but effective rules when ML is unavailable or unreliable
    """

    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger("rule_resolver")

        # Rule weights (how much to trust each rule)
        self.rule_weights = {
            'consensus_rule': 0.8,      # When majority agrees
            'high_confidence_rule': 0.7, # When one signal has very high confidence
            'risk_override_rule': 0.9,   # Risk signals override others
            'sentiment_trend_rule': 0.6, # Sentiment momentum
            'technical_strong_rule': 0.7, # Strong technical signals
            'regime_shift_rule': 0.8     # Market regime changes
        }

    def resolve_with_rules(self, signal: AggregatedSignal,
                         conflict_analysis: ConflictAnalysis) -> RuleDecision:
        """Resolve conflicts using rule-based logic"""

        applied_rules = []
        rule_scores = []
        reasoning_parts = []

        # Rule 1: Risk Override - Risk signals have veto power
        if abs(signal.risk_component) > 0.5:
            rule_scores.append((-1.0 if signal.risk_component > 0 else 0.0, self.rule_weights['risk_override_rule']))
            applied_rules.append('risk_override_rule')
            reasoning_parts.append(f"High risk signal ({signal.risk_component:.2f}) overrides other signals")

        # Rule 2: Consensus - When majority of components agree
        components = [signal.technical_component, signal.sentiment_component, signal.market_component]
        positive_count = sum(1 for c in components if c > 0.3)
        negative_count = sum(1 for c in components if c < -0.3)

        if positive_count >= 2:
            avg_positive = np.mean([c for c in components if c > 0.3])
            rule_scores.append((avg_positive, self.rule_weights['consensus_rule']))
            applied_rules.append('consensus_rule')
            reasoning_parts.append(f"Consensus bullish: {positive_count}/3 components positive")
        elif negative_count >= 2:
            avg_negative = np.mean([c for c in components if c < -0.3])
            rule_scores.append((avg_negative, self.rule_weights['consensus_rule']))
            applied_rules.append('consensus_rule')
            reasoning_parts.append(f"Consensus bearish: {negative_count}/3 components negative")

        # Rule 3: High Confidence Override - Very high confidence in one component
        for comp_name, comp_value in [('technical', signal.technical_component),
                                     ('sentiment', signal.sentiment_component),
                                     ('market', signal.market_component)]:
            # Find signals of this type with high confidence
            high_conf_signals = [s for s in signal.raw_signals
                               if s.signal_type.value == comp_name and s.confidence > 0.85]
            if high_conf_signals and abs(comp_value) > 0.6:
                rule_scores.append((comp_value, self.rule_weights['high_confidence_rule']))
                applied_rules.append('high_confidence_rule')
                reasoning_parts.append(f"High confidence {comp_name} signal ({comp_value:.2f})")

        # Rule 4: Sentiment Trend - Recent sentiment momentum
        sentiment_signals = [s for s in signal.raw_signals if s.signal_type == SignalType.SENTIMENT]
        if len(sentiment_signals) >= 2:
            # Sort by timestamp
            sorted_sentiment = sorted(sentiment_signals, key=lambda x: x.timestamp)
            recent_trend = sorted_sentiment[-1].direction - sorted_sentiment[0].direction
            if abs(recent_trend) > 0.4:
                rule_scores.append((recent_trend, self.rule_weights['sentiment_trend_rule']))
                applied_rules.append('sentiment_trend_rule')
                reasoning_parts.append(f"Sentiment trend: {recent_trend:.2f}")

        # Rule 5: Technical Strength - Strong technical signals with multiple confirmations
        tech_signals = [s for s in signal.raw_signals if s.signal_type == SignalType.TECHNICAL]
        if len(tech_signals) >= 2:
            avg_tech_confidence = np.mean([s.confidence for s in tech_signals])
            if avg_tech_confidence > 0.75 and abs(signal.technical_component) > 0.5:
                rule_scores.append((signal.technical_component, self.rule_weights['technical_strong_rule']))
                applied_rules.append('technical_strong_rule')
                reasoning_parts.append(f"Strong technical signals: {len(tech_signals)} signals, avg confidence {avg_tech_confidence:.2f}")

        # Rule 6: Market Regime Shift - Strong regime signals
        regime_signals = [s for s in signal.raw_signals if s.signal_type == SignalType.MARKET_REGIME]
        if regime_signals:
            latest_regime = max(regime_signals, key=lambda x: x.timestamp)
            if latest_regime.confidence > 0.8 and abs(latest_regime.direction) > 0.6:
                rule_scores.append((latest_regime.direction, self.rule_weights['regime_shift_rule']))
                applied_rules.append('regime_shift_rule')
                reasoning_parts.append(f"Strong regime signal: {latest_regime.direction:.2f} confidence {latest_regime.confidence:.2f}")

        # Calculate final rule decision
        if rule_scores:
            # Weighted average of all applicable rules
            weighted_sum = sum(score * weight for score, weight in rule_scores)
            total_weight = sum(weight for _, weight in rule_scores)
            final_direction = weighted_sum / total_weight if total_weight > 0 else 0.0

            # Confidence based on agreement and weight
            rule_agreement = 1.0 - np.std([score for score, _ in rule_scores]) if len(rule_scores) > 1 else 1.0
            confidence = min(0.95, (total_weight / len(self.rule_weights)) * rule_agreement)
        else:
            # No rules applied - default to weak hold
            final_direction = 0.0
            confidence = 0.1
            applied_rules.append('default_hold')
            reasoning_parts.append("No clear rules applicable, defaulting to hold")

        # Determine dominant signal type
        component_abs_values = {
            'technical': abs(signal.technical_component),
            'sentiment': abs(signal.sentiment_component),
            'market': abs(signal.market_component),
            'risk': abs(signal.risk_component)
        }
        dominant_signal = max(component_abs_values.keys(), key=lambda k: component_abs_values[k])

        # Override reasons
        override_reasons = []
        if 'risk_override_rule' in applied_rules:
            override_reasons.append("Risk management override")
        if len(conflict_analysis.risk_flags) > 0:
            override_reasons.extend(conflict_analysis.risk_flags)

        return RuleDecision(
            rule_direction=final_direction,
            rule_confidence=confidence,
            applied_rules=applied_rules,
            rule_reasoning="; ".join(reasoning_parts) if reasoning_parts else "No applicable rules",
            dominant_signal=dominant_signal,
            override_reasons=override_reasons
        )
