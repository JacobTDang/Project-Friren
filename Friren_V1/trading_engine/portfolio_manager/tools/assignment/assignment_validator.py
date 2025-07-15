"""
Strategy Assignment Validator

Validates that all strategy assignments contain real data and no hardcoded fallbacks.
Integrates with the market_metrics system to ensure dynamic calculations throughout.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

# Import shared models 
from .models import StrategyAssignment, AssignmentValidationResult, AssignmentReason

# Avoid circular import by importing locally when needed
from ....analytics.market_metrics import get_all_metrics, MarketMetricsResult


# ValidationResult renamed to AssignmentValidationResult in models.py


class AssignmentValidator:
    """
    Validates strategy assignments to ensure no hardcoded fallbacks remain.
    
    Checks that all assignments use real market data and dynamic calculations.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Validation thresholds based on data quality
        self.quality_thresholds = {
            'excellent': {'min_confidence': 70.0, 'max_risk': 80.0},
            'good': {'min_confidence': 50.0, 'max_risk': 90.0},
            'poor': {'min_confidence': 30.0, 'max_risk': 95.0},
            'insufficient': {'min_confidence': 10.0, 'max_risk': 99.0}
        }
        
        self.logger.info("AssignmentValidator initialized - dynamic validation active")
    
    def validate_assignment(self, assignment) -> AssignmentValidationResult:
        """Validate that assignment contains real data and no hardcoded fallbacks"""
        
        warnings = []
        errors = []
        
        try:
            # Get market metrics to cross-validate assignment data
            market_metrics = get_all_metrics(assignment.symbol)
            
            # Validate confidence score
            confidence_validation = self._validate_confidence(assignment, market_metrics)
            if confidence_validation[1]:  # Has errors
                errors.extend(confidence_validation[1])
            if confidence_validation[2]:  # Has warnings
                warnings.extend(confidence_validation[2])
            
            # Validate risk score
            risk_validation = self._validate_risk_score(assignment, market_metrics)
            if risk_validation[1]:
                errors.extend(risk_validation[1])
            if risk_validation[2]:
                warnings.extend(risk_validation[2])
            
            # Validate expected return
            return_validation = self._validate_expected_return(assignment, market_metrics)
            if return_validation[1]:
                errors.extend(return_validation[1])
            if return_validation[2]:
                warnings.extend(return_validation[2])
            
            # Validate market regime consistency
            regime_validation = self._validate_market_regime(assignment, market_metrics)
            if regime_validation[1]:
                errors.extend(regime_validation[1])
            if regime_validation[2]:
                warnings.extend(regime_validation[2])
            
            # Determine overall data quality
            data_quality = self._assess_data_quality(market_metrics, len(warnings), len(errors))
            
            # Determine validation result
            is_valid = len(errors) == 0
            
            validation_result = AssignmentValidationResult(
                is_valid=is_valid,
                assignment=assignment,
                confidence_source=f"market_metrics_quality_{data_quality}",
                risk_source=f"market_volatility_quality_{data_quality}",
                return_source=f"market_analysis_quality_{data_quality}",
                validation_warnings=warnings,
                market_data_quality=data_quality,
                validation_timestamp=datetime.now()
            )
            
            # Add any errors as hardcoded value detections
            for error in errors:
                validation_result.hardcoded_values_detected.append(error)
            
            self.logger.info(f"Assignment validation for {assignment.symbol}: "
                           f"{'PASSED' if is_valid else 'FAILED'} "
                           f"(quality: {data_quality}, warnings: {len(warnings)}, errors: {len(errors)})")
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Assignment validation failed for {assignment.symbol}: {e}")
            dummy_assignment = StrategyAssignment(
                symbol=getattr(assignment, 'symbol', 'UNKNOWN'),
                recommended_strategy='ERROR',
                assignment_reason=AssignmentReason.MANUAL_OVERRIDE,
                confidence_score=0.0,
                risk_score=100.0,
                expected_return=0.0
            )
            result = AssignmentValidationResult(
                is_valid=False,
                assignment=dummy_assignment,
                confidence_source="validation_error",
                risk_source="validation_error", 
                return_source="validation_error",
                market_data_quality='insufficient',
                validation_timestamp=datetime.now()
            )
            result.add_warning(f"Validation system error: {str(e)}")
            return result
    
    def _validate_confidence(self, assignment: StrategyAssignment, 
                           market_metrics: Optional[MarketMetricsResult]) -> Tuple[bool, List[str], List[str]]:
        """Validate confidence score against market data"""
        
        errors = []
        warnings = []
        
        # Check for hardcoded confidence values
        if assignment.confidence_score in [50.0, 60.0, 70.0, 80.0, 90.0, 100.0]:
            warnings.append(f"Confidence {assignment.confidence_score}% appears to be hardcoded round number")
        
        # Cross-validate with market metrics if available
        if market_metrics and market_metrics.risk_score is not None:
            expected_confidence_range = (100 - market_metrics.risk_score, 100 - market_metrics.risk_score + 20)
            if not (expected_confidence_range[0] <= assignment.confidence_score <= expected_confidence_range[1]):
                warnings.append(f"Confidence {assignment.confidence_score}% inconsistent with market risk {market_metrics.risk_score}")
        
        # Check for unrealistic confidence values
        if assignment.confidence_score > 95.0:
            warnings.append("Extremely high confidence may indicate insufficient uncertainty modeling")
        elif assignment.confidence_score < 5.0:
            errors.append("Confidence below 5% indicates insufficient data for reliable assignment")
        
        return True, errors, warnings
    
    def _validate_risk_score(self, assignment: StrategyAssignment,
                           market_metrics: Optional[MarketMetricsResult]) -> Tuple[bool, List[str], List[str]]:
        """Validate risk score against market data"""
        
        errors = []
        warnings = []
        
        # Check for exact hardcoded risk values
        if assignment.risk_score in [50.0, 25.0, 75.0]:
            warnings.append(f"Risk score {assignment.risk_score} appears to be hardcoded fallback value")
        
        # Cross-validate with market metrics
        if market_metrics and market_metrics.risk_score is not None:
            risk_diff = abs(assignment.risk_score - market_metrics.risk_score)
            if risk_diff > 20.0:
                warnings.append(f"Assignment risk {assignment.risk_score} differs significantly from market risk {market_metrics.risk_score}")
        
        return True, errors, warnings
    
    def _validate_expected_return(self, assignment: StrategyAssignment,
                                market_metrics: Optional[MarketMetricsResult]) -> Tuple[bool, List[str], List[str]]:
        """Validate expected return calculations"""
        
        errors = []
        warnings = []
        
        # Check for hardcoded return values
        if assignment.expected_return in [0.0, 0.05, 0.1]:
            warnings.append(f"Expected return {assignment.expected_return} appears to be hardcoded")
        
        # Validate return reasonableness
        if assignment.expected_return > 0.5:  # 50% return
            errors.append(f"Expected return {assignment.expected_return:.2%} is unrealistically high")
        elif assignment.expected_return < -0.2:  # -20% return
            errors.append(f"Expected return {assignment.expected_return:.2%} is unrealistically negative")
        
        return True, errors, warnings
    
    def _validate_market_regime(self, assignment: StrategyAssignment,
                              market_metrics: Optional[MarketMetricsResult]) -> Tuple[bool, List[str], List[str]]:
        """Validate market regime consistency"""
        
        errors = []
        warnings = []
        
        # Check for placeholder regime values
        if assignment.market_regime in ['UNKNOWN', 'DEFAULT', 'PLACEHOLDER']:
            warnings.append(f"Market regime '{assignment.market_regime}' appears to be placeholder value")
        
        return True, errors, warnings
    
    def _assess_data_quality(self, market_metrics: Optional[MarketMetricsResult], 
                           warning_count: int, error_count: int) -> str:
        """Assess overall data quality for the assignment"""
        
        if error_count > 0:
            return 'insufficient'
        elif warning_count > 3:
            return 'poor'
        elif market_metrics is None or market_metrics.data_quality == 'poor':
            return 'poor'
        elif warning_count > 1:
            return 'good'
        else:
            return 'excellent'


def validate_assignment_batch(assignments: List[StrategyAssignment]) -> Dict[str, AssignmentValidationResult]:
    """Validate multiple assignments and return results"""
    validator = AssignmentValidator()
    results = {}
    
    for assignment in assignments:
        results[assignment.symbol] = validator.validate_assignment(assignment)
    
    return results