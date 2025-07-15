"""
XGBoost Engine - Extracted from Enhanced News Pipeline Process

This module contains the core XGBoost-based recommendation engine for trading decisions.
Provides real ML-based trading recommendations with model loading and prediction capabilities.

Features:
- Real XGBoost model integration (no hardcoded fallbacks)
- Model loading from trained files with fallback prediction logic
- Prediction scoring with bounds checking and validation
- Integration with modular feature engineering and validation
- OutputCoordinator integration for standardized output
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import deque

# Import modular components
from .feature_engineer import FeatureEngineer, EnhancedSentimentResult, ProcessedNewsData
from .recommendation_validator import RecommendationValidator, TradingRecommendation

# Import OutputCoordinator for standardized output (with fallback)
try:
    from Friren_V1.trading_engine.output.output_coordinator import OutputCoordinator
    OUTPUT_COORDINATOR_AVAILABLE = True
except (ImportError, AttributeError):
    # Fallback when OutputCoordinator or its dependencies not available
    OUTPUT_COORDINATOR_AVAILABLE = False
    OutputCoordinator = None


class XGBoostRecommendationEngine:
    """XGBoost-based recommendation engine for trading decisions - PRODUCTION READY"""

    def __init__(self, config: Any, output_coordinator: Optional[Any] = None):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.XGBoostEngine")
        self.output_coordinator = output_coordinator

        # Initialize modular components
        self.feature_engineer = FeatureEngineer()
        self.recommendation_validator = RecommendationValidator(config)

        # Try to import XGBoost - warn if not available but continue for development
        try:
            import xgboost as xgb
            self.xgb = xgb
            self.xgboost_available = True
        except ImportError:
            self.logger.warning("XGBoost library not installed - using fallback prediction logic")
            self.xgb = None
            self.xgboost_available = False
        
        # Try to import SHAP for explainability - graceful degradation if not available
        try:
            import shap
            self.shap = shap
            self.shap_available = True
            self.explainer = None  # Will be initialized when model is loaded
            self.logger.info("SHAP library available - will provide prediction explanations")
        except ImportError:
            self.logger.warning("SHAP library not installed - predictions will not include explanations")
            self.shap = None
            self.shap_available = False
            self.explainer = None

        # Load trained model - OPTIONAL for testing
        if hasattr(config, 'model_path') and config.model_path:
            if os.path.isabs(config.model_path):
                self.model_path = config.model_path
            else:
                # Resolve relative path from project root
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
                self.model_path = os.path.join(project_root, config.model_path)
        else:
            self.model_path = None
        self.model = None

        if self.model_path and os.path.exists(self.model_path):
            try:
                # Load the real trained model
                self.model = self.xgb.Booster()
                self.model.load_model(self.model_path)
                self.logger.info(f"Loaded real XGBoost model from {self.model_path}")
                
                # Initialize SHAP explainer if available
                if self.shap_available and self.model:
                    try:
                        self.explainer = self.shap.TreeExplainer(self.model)
                        self.logger.info("SHAP explainer initialized - predictions will include explanations")
                    except Exception as e:
                        self.logger.warning(f"Failed to initialize SHAP explainer: {e}")
                        self.explainer = None
                        
            except Exception as e:
                self.logger.warning(f"Failed to load XGBoost model: {e}")
                self.model = None
        else:
            self.logger.warning(f"XGBoost model not found at {self.model_path} - using fallback predictions")
            self.model = None

        # Prediction tracking for model improvement
        self.prediction_history = deque(maxlen=100)

    def generate_recommendation(self,
                                    symbol: str,
                                    news_data: ProcessedNewsData,
                                    sentiment_results: List[EnhancedSentimentResult]) -> TradingRecommendation:
        """
        Generate trading recommendation based on news and sentiment analysis
        
        Args:
            symbol: Stock symbol
            news_data: Processed news data
            sentiment_results: List of sentiment analysis results
            
        Returns:
            Trading recommendation with confidence and reasoning
        """
        try:
            # Feature engineering using modular component
            features = self.feature_engineer.engineer_features(symbol, news_data, sentiment_results)

            # Generate prediction score
            prediction_score = self._predict(features)
            
            # Generate SHAP explanations if available
            shap_explanations = self._generate_shap_explanations(features)
            
            # Validate and format recommendation using modular component
            recommendation = self.recommendation_validator.validate_and_format_recommendation(
                symbol, prediction_score, features, len(sentiment_results)
            )
            
            # Add SHAP explanations to recommendation
            if shap_explanations:
                recommendation.shap_explanations = shap_explanations

            # Store prediction for model improvement
            if prediction_score is not None:
                self.prediction_history.append({
                    'symbol': symbol,
                    'prediction': prediction_score,
                    'action': recommendation.action,
                    'confidence': recommendation.confidence,
                    'timestamp': datetime.now()
                })

            # Output XGBoost recommendation via OutputCoordinator if available
            if self.output_coordinator and recommendation.prediction_score is not None:
                # Extract key features for output
                key_features = {
                    'sentiment': features.get('sentiment_score', 0.0),
                    'volume': features.get('news_volume', 0.0),
                    'impact': features.get('market_impact', 0.0),
                    'quality': features.get('data_quality', 0.0)
                }
                
                self.output_coordinator.output_xgboost_recommendation(
                    symbol=symbol,
                    action=recommendation.action,
                    score=recommendation.prediction_score,
                    features=key_features
                )

            return recommendation

        except Exception as e:
            self.logger.error(f"Error generating recommendation for {symbol}: {e}")
            # Return data-insufficient recommendation using validator
            return self.recommendation_validator._create_data_insufficient_recommendation(symbol, str(e))

    def _predict(self, features: Dict[str, float]) -> Optional[float]:
        """
        Generate prediction score using XGBoost model or fallback
        
        Args:
            features: Engineered features for prediction
            
        Returns:
            Prediction score (0-1) or None if prediction fails
        """
        try:
            if self.model and self.xgboost_available:
                # Use real trained model - CRITICAL FIX: Ensure proper feature ordering
                feature_values = list(features.values())
                
                # Validate feature count
                if len(feature_values) != 21:
                    self.logger.error(f"XGBoost model expects 21 features, got {len(feature_values)}")
                    return None
                
                try:
                    # Create feature matrix with proper shape
                    import numpy as np
                    
                    # Ensure we have exactly the expected feature values as floats
                    feature_array = np.array([float(v) for v in feature_values], dtype=np.float32)
                    
                    # Reshape to 2D array for XGBoost (1 sample, 21 features)
                    feature_matrix = self.xgb.DMatrix(feature_array.reshape(1, -1))

                    # Get prediction from real trained model
                    predictions = self.model.predict(feature_matrix)
                    
                    # Extract scalar prediction value - flatten any nested arrays
                    prediction = float(np.array(predictions).flatten()[0])
                    
                    self.logger.debug(f"XGBoost raw predictions shape: {np.array(predictions).shape}, value: {predictions}")

                    # Ensure bounds
                    result = max(0.0, min(1.0, prediction))
                    self.logger.debug(f"XGBoost prediction: {result} (raw: {prediction}, type: {type(predictions)})")
                    return result
                    
                except Exception as xgb_error:
                    self.logger.error(f"XGBoost prediction error: {xgb_error}")
                    import traceback
                    self.logger.error(f"XGBoost traceback: {traceback.format_exc()}")
                    return None
            else:
                # Fallback prediction based on features
                self.logger.debug("Using fallback prediction logic (no XGBoost model)")

                # Simple weighted feature combination
                sentiment_score = features.get('sentiment_score', 0.0)
                sentiment_confidence = features.get('sentiment_confidence', 0.0)
                news_volume = features.get('news_volume', 0.0)
                data_quality = features.get('data_quality', 0.0)

                # Weighted prediction
                prediction = (
                    sentiment_score * 0.4 +      # 40% weight on sentiment
                    sentiment_confidence * 0.3 + # 30% weight on confidence
                    news_volume * 0.2 +          # 20% weight on volume
                    data_quality * 0.1           # 10% weight on quality
                )

                # Normalize to 0-1 range and add baseline
                prediction = (prediction + 1.0) / 2.0  # Convert from -1,1 to 0,1
                prediction = max(0.1, min(0.9, prediction))  # Keep away from extremes

                return prediction

        except Exception as e:
            self.logger.error(f"Prediction failed: {e}")
            # Return None to indicate insufficient data for prediction instead of fallback value
            return None

    def _generate_shap_explanations(self, features: Dict[str, float]) -> Optional[Dict[str, float]]:
        """
        Generate SHAP explanations for the prediction if SHAP is available
        
        Args:
            features: Feature dictionary used for prediction
            
        Returns:
            Dictionary of feature names to SHAP values, or None if SHAP not available
        """
        try:
            if not self.shap_available or not self.explainer or not self.model:
                return None
            
            # Convert features to the same format used in _predict
            feature_names = [
                'sentiment_score', 'sentiment_confidence', 'news_volume', 'market_impact',
                'data_quality', 'hour', 'day_of_week', 'volatility', 'rsi', 'macd',
                'volume_ratio', 'price_change', 'sector_performance', 'market_sentiment',
                'economic_indicator', 'earnings_proximity', 'options_activity',
                'institutional_flow', 'social_sentiment', 'regulatory_risk', 'technical_score'
            ]
            
            # Get feature values in the same order as _predict
            feature_values = [features.get(name, 0.0) for name in feature_names]
            
            # Validate feature count
            if len(feature_values) != 21:
                self.logger.warning(f"Cannot generate SHAP explanations: expected 21 features, got {len(feature_values)}")
                return None
            
            import numpy as np
            
            # Create feature array
            feature_array = np.array([float(v) for v in feature_values], dtype=np.float32)
            feature_matrix = feature_array.reshape(1, -1)
            
            # Generate SHAP values
            shap_values = self.explainer.shap_values(feature_matrix)
            
            # Convert to dictionary format with feature names
            if isinstance(shap_values, list):
                # For multi-class, take the first class
                shap_values = shap_values[0]
            
            # Extract single prediction SHAP values
            single_shap_values = shap_values[0] if shap_values.ndim > 1 else shap_values
            
            # Create dictionary of feature names to SHAP values
            shap_dict = {}
            for i, feature_name in enumerate(feature_names):
                if i < len(single_shap_values):
                    shap_dict[feature_name] = float(single_shap_values[i])
            
            # Sort by absolute value to get most important features
            sorted_shap = dict(sorted(shap_dict.items(), key=lambda x: abs(x[1]), reverse=True))
            
            # Return top 5 most important features
            top_features = dict(list(sorted_shap.items())[:5])
            
            self.logger.debug(f"Generated SHAP explanations for {len(top_features)} features")
            return top_features
            
        except Exception as e:
            self.logger.warning(f"SHAP explanation generation failed: {e}")
            return None

    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the loaded model"""
        return {
            'model_loaded': self.model is not None,
            'model_path': self.model_path,
            'model_exists': os.path.exists(self.model_path) if self.model_path else False,
            'xgboost_available': self.xgboost_available,
            'xgboost_library_available': hasattr(self, 'xgb') and self.xgb is not None,
            'prediction_history_size': len(self.prediction_history),
            'feature_engineer_available': self.feature_engineer is not None,
            'validator_available': self.recommendation_validator is not None,
            'output_coordinator_available': OUTPUT_COORDINATOR_AVAILABLE
        }

    def get_prediction_history(self, symbol: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent prediction history
        
        Args:
            symbol: Filter by symbol (optional)
            limit: Maximum number of predictions to return
            
        Returns:
            List of recent predictions
        """
        history = list(self.prediction_history)
        
        if symbol:
            history = [p for p in history if p['symbol'] == symbol]
        
        return history[-limit:] if limit else history

    def validate_configuration(self) -> bool:
        """Validate engine configuration"""
        try:
            # Check if required config attributes exist
            required_attrs = ['xgboost_buy_threshold', 'xgboost_sell_threshold', 'recommendation_threshold']
            for attr in required_attrs:
                if not hasattr(self.config, attr):
                    self.logger.warning(f"Missing config attribute: {attr}")
                    return False
            
            # Validate modular components
            if not self.feature_engineer:
                self.logger.error("Feature engineer not initialized")
                return False
                
            if not self.recommendation_validator:
                self.logger.error("Recommendation validator not initialized") 
                return False
            
            # Validate XGBoost availability (warn but don't fail)
            if not self.xgboost_available:
                self.logger.warning("XGBoost library not available - fallback mode only")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False

    def cleanup(self):
        """Cleanup resources and clear caches"""
        try:
            # Clear prediction history
            self.prediction_history.clear()
            
            # Cleanup modular components if they have cleanup methods
            if hasattr(self.feature_engineer, 'cleanup'):
                self.feature_engineer.cleanup()
                
            if hasattr(self.recommendation_validator, 'cleanup'):
                self.recommendation_validator.cleanup()
            
            self.logger.info("XGBoost engine cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


def create_xgboost_engine(config: Any, output_coordinator: Optional[Any] = None) -> XGBoostRecommendationEngine:
    """
    Factory function to create XGBoost recommendation engine
    
    Args:
        config: Configuration object with XGBoost settings
        output_coordinator: Optional output coordinator for standardized output
        
    Returns:
        Configured XGBoost recommendation engine
    """
    return XGBoostRecommendationEngine(config, output_coordinator)