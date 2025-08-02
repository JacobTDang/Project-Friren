"""
Risk Configuration Manager - Dynamic Parameter Management

This module provides centralized configuration management for the enhanced risk manager,
allowing for dynamic adjustment of risk parameters based on market conditions and 
system performance.

Features:
- Dynamic risk profile switching (conservative/moderate/aggressive)
- Market condition-based parameter adjustment
- Performance-based threshold optimization
- Configuration persistence and validation
- Real-time parameter updates without system restart
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import logging
from pathlib import Path

class RiskProfile(Enum):
    """Risk profile types"""
    CONSERVATIVE = "conservative"
    MODERATE = "moderate"
    AGGRESSIVE = "aggressive"
    CUSTOM = "custom"

class MarketRegime(Enum):
    """Market regime classifications"""
    BULL_MARKET = "bull_market"        # Low VIX, trending up
    BEAR_MARKET = "bear_market"        # High VIX, trending down
    VOLATILE_MARKET = "volatile_market" # High VIX, high volatility
    STABLE_MARKET = "stable_market"     # Low VIX, low volatility
    UNCERTAIN = "uncertain"             # Mixed signals

@dataclass
class RiskProfileConfig:
    """Configuration for a specific risk profile"""
    # Core risk limits
    max_position_size_pct: float = 0.15       # 15% max per symbol
    min_position_size_pct: float = 0.005      # 0.5% minimum
    max_portfolio_allocation_pct: float = 0.80 # 80% max total allocation
    
    # Confidence and approval thresholds
    approval_threshold: float = 0.65           # Minimum confidence for approval
    high_confidence_threshold: float = 0.80    # High confidence trades
    critical_confidence_threshold: float = 0.90 # Critical confidence trades
    
    # Enhanced feature risk weights
    sentiment_quality_weight: float = 1.0      # Weight for sentiment quality risk
    market_context_weight: float = 1.0         # Weight for market context risk
    data_quality_weight: float = 1.0           # Weight for data quality risk
    
    # Performance thresholds
    max_daily_trades: int = 50                 # Maximum trades per day
    performance_alert_threshold_ms: float = 200.0 # Performance alert threshold
    
    # Market condition responses
    vix_halt_threshold: float = 45.0           # VIX level for trading halt
    volatility_warning_threshold: float = 0.40 # Volatility warning level
    
    # Position sizing parameters
    base_position_multiplier: float = 1.0      # Base position size multiplier
    confidence_scaling_factor: float = 0.5     # How much confidence affects size
    
    # Risk scoring parameters
    high_risk_threshold: float = 70.0          # High risk score threshold
    critical_risk_threshold: float = 85.0      # Critical risk score threshold
    
    # Enhanced feature parameters
    enable_enhanced_features: bool = True      # Enable enhanced feature analysis
    enhanced_feature_timeout_ms: float = 500.0 # Timeout for enhanced features
    
    def __post_init__(self):
        """Validate configuration parameters"""
        self._validate_parameters()
    
    def _validate_parameters(self):
        """Validate that all parameters are within acceptable ranges"""
        # Position size validations
        if not (0.001 <= self.min_position_size_pct <= 0.05):
            raise ValueError(f"min_position_size_pct must be between 0.1% and 5%, got {self.min_position_size_pct:.3f}")
        
        if not (0.05 <= self.max_position_size_pct <= 0.30):
            raise ValueError(f"max_position_size_pct must be between 5% and 30%, got {self.max_position_size_pct:.3f}")
        
        if self.min_position_size_pct >= self.max_position_size_pct:
            raise ValueError("min_position_size_pct must be less than max_position_size_pct")
        
        # Confidence threshold validations
        if not (0.5 <= self.approval_threshold <= 0.95):
            raise ValueError(f"approval_threshold must be between 50% and 95%, got {self.approval_threshold:.2f}")
        
        # Risk threshold validations
        if not (50.0 <= self.high_risk_threshold <= 90.0):
            raise ValueError(f"high_risk_threshold must be between 50 and 90, got {self.high_risk_threshold}")
        
        if self.high_risk_threshold >= self.critical_risk_threshold:
            raise ValueError("high_risk_threshold must be less than critical_risk_threshold")

class RiskConfigManager:
    """
    Centralized configuration management for enhanced risk manager
    
    Provides dynamic parameter adjustment based on:
    - Market conditions (VIX, volatility, regime)
    - System performance metrics
    - Time-based adaptations
    - Manual overrides
    """
    
    def __init__(self, config_file_path: Optional[str] = None):
        self.logger = logging.getLogger(f"{__name__}.RiskConfigManager")
        
        # Configuration file path
        self.config_file_path = config_file_path or self._get_default_config_path()
        
        # Current active configuration
        self.current_profile = RiskProfile.MODERATE
        self.current_config = self._get_default_configs()[RiskProfile.MODERATE]
        
        # Configuration profiles
        self.profile_configs = self._get_default_configs()
        
        # Market condition tracking
        self.current_market_regime = MarketRegime.UNCERTAIN
        self.last_market_update = datetime.now()
        self.market_update_interval = timedelta(minutes=15)
        
        # Performance tracking for adaptive configuration
        self.performance_history = []
        self.config_change_history = []
        
        # Override tracking
        self.manual_overrides = {}
        self.temporary_overrides = {}
        
        # Load configuration from file if exists
        self._load_configuration()
        
        self.logger.info(f"Risk configuration manager initialized with {self.current_profile.value} profile")
    
    def get_current_config(self) -> RiskProfileConfig:
        """Get current active risk configuration with any overrides applied"""
        config = self.current_config
        
        # Apply manual overrides
        if self.manual_overrides:
            config = self._apply_overrides(config, self.manual_overrides)
        
        # Apply temporary overrides
        if self.temporary_overrides:
            # Check if temporary overrides have expired
            self._cleanup_expired_overrides()
            if self.temporary_overrides:
                config = self._apply_overrides(config, self.temporary_overrides)
        
        return config
    
    def update_market_conditions(self, vix: Optional[float] = None, 
                                volatility: Optional[float] = None,
                                trend_strength: Optional[float] = None) -> bool:
        """
        Update market conditions and adjust risk profile if needed
        
        Args:
            vix: Current VIX level
            volatility: Market volatility (0-1)
            trend_strength: Trend strength (-1 to 1, negative = bearish)
            
        Returns:
            True if risk profile was changed
        """
        # Check if enough time has passed for update
        if datetime.now() - self.last_market_update < self.market_update_interval:
            return False
        
        old_regime = self.current_market_regime
        old_profile = self.current_profile
        
        # Determine new market regime
        new_regime = self._classify_market_regime(vix, volatility, trend_strength)
        
        if new_regime != self.current_market_regime:
            self.current_market_regime = new_regime
            self.logger.info(f"Market regime changed: {old_regime.value} → {new_regime.value}")
        
        # Adjust risk profile based on market regime
        new_profile = self._get_profile_for_regime(new_regime, vix)
        
        if new_profile != self.current_profile:
            self._switch_risk_profile(new_profile, f"Market regime: {new_regime.value}")
            self.last_market_update = datetime.now()
            return True
        
        self.last_market_update = datetime.now()
        return False
    
    def update_performance_metrics(self, success_rate: float, avg_processing_time: float,
                                  risk_score_accuracy: Optional[float] = None) -> bool:
        """
        Update performance metrics and adjust configuration if needed
        
        Args:
            success_rate: Success rate of risk assessments (0-1)
            avg_processing_time: Average processing time in ms
            risk_score_accuracy: Accuracy of risk scoring (0-1)
            
        Returns:
            True if configuration was adjusted
        """
        performance_entry = {
            'timestamp': datetime.now(),
            'success_rate': success_rate,
            'avg_processing_time': avg_processing_time,
            'risk_score_accuracy': risk_score_accuracy
        }
        
        self.performance_history.append(performance_entry)
        
        # Keep only recent history (last 100 entries)
        if len(self.performance_history) > 100:
            self.performance_history = self.performance_history[-100:]
        
        # Adjust configuration based on performance
        return self._adjust_for_performance()
    
    def set_manual_override(self, parameter: str, value: Any, reason: str = "Manual override"):
        """Set manual override for a specific parameter"""
        self.manual_overrides[parameter] = {
            'value': value,
            'reason': reason,
            'timestamp': datetime.now()
        }
        
        self.logger.info(f"Manual override set: {parameter} = {value} ({reason})")
    
    def clear_manual_override(self, parameter: str):
        """Clear manual override for a specific parameter"""
        if parameter in self.manual_overrides:
            del self.manual_overrides[parameter]
            self.logger.info(f"Manual override cleared: {parameter}")
    
    def set_temporary_override(self, parameter: str, value: Any, duration_minutes: int, 
                             reason: str = "Temporary override"):
        """Set temporary override that expires after specified duration"""
        expiry_time = datetime.now() + timedelta(minutes=duration_minutes)
        
        self.temporary_overrides[parameter] = {
            'value': value,
            'reason': reason,
            'timestamp': datetime.now(),
            'expires_at': expiry_time
        }
        
        self.logger.info(f"Temporary override set: {parameter} = {value} for {duration_minutes} minutes ({reason})")
    
    def get_configuration_summary(self) -> Dict[str, Any]:
        """Get comprehensive configuration summary"""
        config = self.get_current_config()
        
        return {
            'current_profile': self.current_profile.value,
            'market_regime': self.current_market_regime.value,
            'last_market_update': self.last_market_update.isoformat(),
            'active_config': {
                'approval_threshold': config.approval_threshold,
                'max_position_size_pct': config.max_position_size_pct,
                'max_portfolio_allocation_pct': config.max_portfolio_allocation_pct,
                'max_daily_trades': config.max_daily_trades,
                'enable_enhanced_features': config.enable_enhanced_features
            },
            'overrides': {
                'manual_overrides': len(self.manual_overrides),
                'temporary_overrides': len(self.temporary_overrides)
            },
            'recent_changes': len([c for c in self.config_change_history if 
                                 (datetime.now() - c['timestamp']).total_seconds() < 3600])
        }
    
    def save_configuration(self) -> bool:
        """Save current configuration to file"""
        try:
            config_data = {
                'current_profile': self.current_profile.value,
                'profile_configs': {
                    profile.value: asdict(config) 
                    for profile, config in self.profile_configs.items()
                },
                'manual_overrides': self.manual_overrides,
                'last_saved': datetime.now().isoformat()
            }
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.config_file_path), exist_ok=True)
            
            with open(self.config_file_path, 'w') as f:
                json.dump(config_data, f, indent=2, default=str)
            
            self.logger.info(f"Configuration saved to {self.config_file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save configuration: {e}")
            return False
    
    def load_configuration(self) -> bool:
        """Load configuration from file"""
        return self._load_configuration()
    
    # Private helper methods
    
    def _get_default_config_path(self) -> str:
        """Get default configuration file path"""
        project_root = Path(__file__).parent.parent.parent.parent.parent
        config_dir = project_root / "config"
        return str(config_dir / "risk_config.json")
    
    def _get_default_configs(self) -> Dict[RiskProfile, RiskProfileConfig]:
        """Get default configuration profiles"""
        return {
            RiskProfile.CONSERVATIVE: RiskProfileConfig(
                max_position_size_pct=0.08,           # 8% max position
                approval_threshold=0.75,              # 75% confidence required
                max_portfolio_allocation_pct=0.60,    # 60% max allocation
                max_daily_trades=10,                  # Conservative trade limit
                vix_halt_threshold=25.0,              # Lower VIX threshold
                base_position_multiplier=0.7,         # Smaller positions
                high_risk_threshold=60.0,             # Lower risk tolerance
                critical_risk_threshold=75.0
            ),
            RiskProfile.MODERATE: RiskProfileConfig(
                max_position_size_pct=0.12,           # 12% max position
                approval_threshold=0.65,              # 65% confidence required
                max_portfolio_allocation_pct=0.75,    # 75% max allocation
                max_daily_trades=25,                  # Moderate trade limit
                vix_halt_threshold=35.0,              # Standard VIX threshold
                base_position_multiplier=1.0,         # Standard positions
                high_risk_threshold=70.0,             # Standard risk tolerance
                critical_risk_threshold=85.0
            ),
            RiskProfile.AGGRESSIVE: RiskProfileConfig(
                max_position_size_pct=0.18,           # 18% max position
                approval_threshold=0.55,              # 55% confidence required
                max_portfolio_allocation_pct=0.85,    # 85% max allocation
                max_daily_trades=50,                  # High trade limit
                vix_halt_threshold=45.0,              # Higher VIX tolerance
                base_position_multiplier=1.3,         # Larger positions
                high_risk_threshold=80.0,             # Higher risk tolerance
                critical_risk_threshold=95.0
            )
        }
    
    def _load_configuration(self) -> bool:
        """Load configuration from file if it exists"""
        try:
            if not os.path.exists(self.config_file_path):
                self.logger.info("No configuration file found, using defaults")
                return True
            
            with open(self.config_file_path, 'r') as f:
                config_data = json.load(f)
            
            # Load current profile
            if 'current_profile' in config_data:
                self.current_profile = RiskProfile(config_data['current_profile'])
                self.current_config = self.profile_configs[self.current_profile]
            
            # Load manual overrides
            if 'manual_overrides' in config_data:
                self.manual_overrides = config_data['manual_overrides']
            
            self.logger.info(f"Configuration loaded from {self.config_file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            return False
    
    def _classify_market_regime(self, vix: Optional[float], volatility: Optional[float], 
                               trend_strength: Optional[float]) -> MarketRegime:
        """Classify current market regime based on indicators"""
        if not any([vix, volatility, trend_strength]):
            return MarketRegime.UNCERTAIN
        
        # Use VIX as primary indicator
        if vix is not None:
            if vix > 35:
                if volatility and volatility > 0.6:
                    return MarketRegime.VOLATILE_MARKET
                elif trend_strength and trend_strength < -0.3:
                    return MarketRegime.BEAR_MARKET
                else:
                    return MarketRegime.VOLATILE_MARKET
            elif vix < 20:
                if trend_strength and trend_strength > 0.3:
                    return MarketRegime.BULL_MARKET
                else:
                    return MarketRegime.STABLE_MARKET
            else:
                return MarketRegime.STABLE_MARKET
        
        # Fallback to volatility and trend
        if volatility and volatility > 0.5:
            return MarketRegime.VOLATILE_MARKET
        elif trend_strength:
            if trend_strength > 0.4:
                return MarketRegime.BULL_MARKET
            elif trend_strength < -0.4:
                return MarketRegime.BEAR_MARKET
        
        return MarketRegime.UNCERTAIN
    
    def _get_profile_for_regime(self, regime: MarketRegime, vix: Optional[float]) -> RiskProfile:
        """Get appropriate risk profile for market regime"""
        if regime == MarketRegime.BULL_MARKET:
            return RiskProfile.AGGRESSIVE
        elif regime in [MarketRegime.BEAR_MARKET, MarketRegime.VOLATILE_MARKET]:
            return RiskProfile.CONSERVATIVE
        elif regime == MarketRegime.STABLE_MARKET:
            return RiskProfile.MODERATE
        else:
            # For uncertain conditions, use VIX if available
            if vix is not None:
                if vix > 30:
                    return RiskProfile.CONSERVATIVE
                elif vix < 15:
                    return RiskProfile.AGGRESSIVE
            return RiskProfile.MODERATE
    
    def _switch_risk_profile(self, new_profile: RiskProfile, reason: str):
        """Switch to new risk profile and log the change"""
        old_profile = self.current_profile
        
        self.current_profile = new_profile
        self.current_config = self.profile_configs[new_profile]
        
        # Record the change
        change_record = {
            'timestamp': datetime.now(),
            'old_profile': old_profile.value,
            'new_profile': new_profile.value,
            'reason': reason
        }
        
        self.config_change_history.append(change_record)
        
        # Keep only recent history
        if len(self.config_change_history) > 50:
            self.config_change_history = self.config_change_history[-50:]
        
        self.logger.info(f"Risk profile changed: {old_profile.value} → {new_profile.value} ({reason})")
    
    def _adjust_for_performance(self) -> bool:
        """Adjust configuration based on recent performance metrics"""
        if len(self.performance_history) < 10:
            return False  # Not enough data
        
        recent_performance = self.performance_history[-10:]
        avg_success_rate = sum(p['success_rate'] for p in recent_performance) / len(recent_performance)
        avg_processing_time = sum(p['avg_processing_time'] for p in recent_performance) / len(recent_performance)
        
        adjusted = False
        
        # Adjust for poor performance
        if avg_success_rate < 0.7:  # Less than 70% success rate
            if 'approval_threshold' not in self.temporary_overrides:
                self.set_temporary_override(
                    'approval_threshold', 
                    min(0.85, self.current_config.approval_threshold + 0.1),
                    duration_minutes=60,
                    reason="Poor success rate - increasing approval threshold"
                )
                adjusted = True
        
        # Adjust for slow processing
        if avg_processing_time > 300:  # More than 300ms average
            if 'enhanced_feature_timeout_ms' not in self.temporary_overrides:
                self.set_temporary_override(
                    'enhanced_feature_timeout_ms',
                    200.0,  # Reduce timeout
                    duration_minutes=30,
                    reason="Slow processing - reducing feature timeout"
                )
                adjusted = True
        
        return adjusted
    
    def _apply_overrides(self, config: RiskProfileConfig, overrides: Dict[str, Dict]) -> RiskProfileConfig:
        """Apply overrides to configuration"""
        # Create a copy of the config
        config_dict = asdict(config)
        
        for param, override_data in overrides.items():
            if param in config_dict:
                config_dict[param] = override_data['value']
        
        return RiskProfileConfig(**config_dict)
    
    def _cleanup_expired_overrides(self):
        """Remove expired temporary overrides"""
        now = datetime.now()
        expired_keys = []
        
        for key, override in self.temporary_overrides.items():
            if now > override['expires_at']:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.temporary_overrides[key]
            self.logger.info(f"Temporary override expired: {key}")


# Utility functions for integration

def create_risk_config_manager(config_file: Optional[str] = None) -> RiskConfigManager:
    """Factory function to create risk configuration manager"""
    return RiskConfigManager(config_file)

def get_default_risk_config() -> RiskProfileConfig:
    """Get default moderate risk configuration"""
    return RiskProfileConfig()