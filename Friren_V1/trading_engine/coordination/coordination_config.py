"""
Coordination Configuration Manager
================================

Dynamic configuration management for the coordination system.
Eliminates hardcoding by loading configuration from environment variables,
database, and configuration files.

Features:
- Environment variable configuration
- Database-driven configuration
- Real-time configuration updates
- No hardcoded values
- Production-ready configuration management
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field, asdict
from datetime import timedelta
from enum import Enum

# Import database manager for dynamic configuration
try:
    from ..portfolio_manager.tools.db_manager import DatabaseManager
except ImportError:
    DatabaseManager = None


@dataclass
class MemoryConfig:
    """Memory management configuration"""
    target_memory_mb: int
    redis_memory_ratio: float
    emergency_threshold_ratio: float
    pressure_thresholds: Dict[str, float]
    cleanup_strategies: Dict[str, str]


@dataclass
class ProcessConfig:
    """Process management configuration"""
    heartbeat_timeout_seconds: int
    health_check_interval_seconds: int
    max_restarts: int
    restart_delay_seconds: int
    priority_weights: Dict[str, int]


@dataclass
class QueueConfig:
    """Queue management configuration"""
    default_queue_name: str
    max_queue_size: int
    health_check_interval_seconds: int
    routing_cache_ttl_seconds: int
    deduplication_window_minutes: int


@dataclass
class MessageConfig:
    """Message coordination configuration"""
    max_concurrent_limits: Dict[str, int]
    cooldown_periods: Dict[str, float]
    priority_scores: Dict[str, float]
    required_fields: Dict[str, List[str]]
    valid_message_types: List[str]


class CoordinationConfigManager:
    """
    Manages all coordination system configuration
    Loads from environment variables and database
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db_manager = None
        self._config_cache = {}
        self._last_db_load = None
        
        # Initialize database connection if available
        if DatabaseManager:
            try:
                self.db_manager = DatabaseManager()
                self.logger.info("Database connection established for dynamic configuration")
                self.logger.info("Will attempt to load from coordination_config table with environment fallback")
            except Exception as e:
                self.db_manager = None
                if "connection" in str(e).lower() or "timeout" in str(e).lower():
                    self.logger.info("Database unavailable - using environment variables for all configuration")
                    self.logger.debug(f"Database connection error: {e}")
                else:
                    self.logger.warning(f"Database connection failed - using environment-only config: {e}")
        else:
            self.logger.info("DatabaseManager not available - using environment variables for all configuration")
        
        # Load initial configuration
        self._load_configuration()
    
    def _load_configuration(self):
        """Load configuration from all sources"""
        try:
            # Load memory configuration
            self.memory_config = self._load_memory_config()
            
            # Load process configuration
            self.process_config = self._load_process_config()
            
            # Load queue configuration
            self.queue_config = self._load_queue_config()
            
            # Load message configuration
            self.message_config = self._load_message_config()
            
            self.logger.info("Configuration loaded successfully from environment and database")
            
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            self._load_default_configuration()
    
    def _load_memory_config(self) -> MemoryConfig:
        """Load memory management configuration"""
        try:
            # Get from environment variables with sensible defaults
            target_memory_mb = int(os.getenv('FRIREN_TARGET_MEMORY_MB', '1024'))
            redis_memory_ratio = float(os.getenv('FRIREN_REDIS_MEMORY_RATIO', '0.625'))
            emergency_threshold_ratio = float(os.getenv('FRIREN_EMERGENCY_THRESHOLD_RATIO', '0.95'))
            
            # Pressure thresholds
            pressure_thresholds = {
                'low': float(os.getenv('FRIREN_MEMORY_PRESSURE_LOW', '0.70')),
                'moderate': float(os.getenv('FRIREN_MEMORY_PRESSURE_MODERATE', '0.80')),
                'high': float(os.getenv('FRIREN_MEMORY_PRESSURE_HIGH', '0.90')),
                'critical': float(os.getenv('FRIREN_MEMORY_PRESSURE_CRITICAL', '0.95')),
                'emergency': float(os.getenv('FRIREN_MEMORY_PRESSURE_EMERGENCY', '0.98'))
            }
            
            # Cleanup strategies
            cleanup_strategies = {
                'low': os.getenv('FRIREN_CLEANUP_LOW', 'expired_messages'),
                'moderate': os.getenv('FRIREN_CLEANUP_MODERATE', 'low_priority_messages'),
                'high': os.getenv('FRIREN_CLEANUP_HIGH', 'old_messages'),
                'critical': os.getenv('FRIREN_CLEANUP_CRITICAL', 'emergency_cleanup'),
                'emergency': os.getenv('FRIREN_CLEANUP_EMERGENCY', 'panic_cleanup')
            }
            
            return MemoryConfig(
                target_memory_mb=target_memory_mb,
                redis_memory_ratio=redis_memory_ratio,
                emergency_threshold_ratio=emergency_threshold_ratio,
                pressure_thresholds=pressure_thresholds,
                cleanup_strategies=cleanup_strategies
            )
            
        except Exception as e:
            self.logger.error(f"Error loading memory config: {e}")
            raise
    
    def _load_process_config(self) -> ProcessConfig:
        """Load process management configuration"""
        try:
            # Get from environment variables
            heartbeat_timeout = int(os.getenv('FRIREN_HEARTBEAT_TIMEOUT', '30'))
            health_check_interval = int(os.getenv('FRIREN_HEALTH_CHECK_INTERVAL', '5'))
            max_restarts = int(os.getenv('FRIREN_MAX_RESTARTS', '5'))
            restart_delay = int(os.getenv('FRIREN_RESTART_DELAY', '10'))
            
            # Load process priorities from database or environment
            priority_weights = self._load_process_priorities()
            
            return ProcessConfig(
                heartbeat_timeout_seconds=heartbeat_timeout,
                health_check_interval_seconds=health_check_interval,
                max_restarts=max_restarts,
                restart_delay_seconds=restart_delay,
                priority_weights=priority_weights
            )
            
        except Exception as e:
            self.logger.error(f"Error loading process config: {e}")
            raise
    
    def _load_queue_config(self) -> QueueConfig:
        """Load queue management configuration"""
        try:
            # Get from environment variables
            default_queue = os.getenv('FRIREN_DEFAULT_QUEUE', 'priority')
            max_queue_size = int(os.getenv('FRIREN_MAX_QUEUE_SIZE', '10000'))
            health_check_interval = int(os.getenv('FRIREN_QUEUE_HEALTH_INTERVAL', '10'))
            routing_cache_ttl = int(os.getenv('FRIREN_ROUTING_CACHE_TTL', '300'))
            dedup_window = int(os.getenv('FRIREN_DEDUP_WINDOW_MINUTES', '5'))
            
            return QueueConfig(
                default_queue_name=default_queue,
                max_queue_size=max_queue_size,
                health_check_interval_seconds=health_check_interval,
                routing_cache_ttl_seconds=routing_cache_ttl,
                deduplication_window_minutes=dedup_window
            )
            
        except Exception as e:
            self.logger.error(f"Error loading queue config: {e}")
            raise
    
    def _load_message_config(self) -> MessageConfig:
        """Load message coordination configuration"""
        try:
            # Load from database if available, otherwise use environment
            max_concurrent_limits = self._load_max_concurrent_limits()
            cooldown_periods = self._load_cooldown_periods()
            priority_scores = self._load_priority_scores()
            required_fields = self._load_required_fields()
            valid_message_types = self._load_valid_message_types()
            
            return MessageConfig(
                max_concurrent_limits=max_concurrent_limits,
                cooldown_periods=cooldown_periods,
                priority_scores=priority_scores,
                required_fields=required_fields,
                valid_message_types=valid_message_types
            )
            
        except Exception as e:
            self.logger.error(f"Error loading message config: {e}")
            raise
    
    def _load_process_priorities(self) -> Dict[str, int]:
        """Load process priorities from database or environment"""
        try:
            if self.db_manager:
                # Try to load from database
                priorities = self._load_from_database('process_priorities')
                if priorities:
                    return priorities
            
            # Fallback to environment variables
            return {
                'execution_engine': int(os.getenv('FRIREN_PRIORITY_EXECUTION', '4')),
                'risk_manager': int(os.getenv('FRIREN_PRIORITY_RISK', '3')),
                'decision_engine': int(os.getenv('FRIREN_PRIORITY_DECISION', '3')),
                'news_pipeline': int(os.getenv('FRIREN_PRIORITY_NEWS', '2')),
                'strategy_analyzer': int(os.getenv('FRIREN_PRIORITY_STRATEGY', '2')),
                'position_monitor': int(os.getenv('FRIREN_PRIORITY_POSITION', '1'))
            }
            
        except Exception as e:
            self.logger.error(f"Error loading process priorities: {e}")
            return {}
    
    def _load_max_concurrent_limits(self) -> Dict[str, int]:
        """Load maximum concurrent message limits"""
        try:
            if self.db_manager:
                limits = self._load_from_database('message_concurrent_limits')
                if limits:
                    return limits
            
            # Environment variable format: FRIREN_CONCURRENT_<MESSAGE_TYPE>=<LIMIT>
            limits = {}
            for key, value in os.environ.items():
                if key.startswith('FRIREN_CONCURRENT_'):
                    message_type = key.replace('FRIREN_CONCURRENT_', '').lower()
                    limits[message_type] = int(value)
            
            # Default limits if not specified
            default_limits = {
                'news_collection': int(os.getenv('FRIREN_CONCURRENT_NEWS', '2')),
                'sentiment_analysis': int(os.getenv('FRIREN_CONCURRENT_SENTIMENT', '3')),
                'trading_signal': int(os.getenv('FRIREN_CONCURRENT_SIGNAL', '5')),
                'risk_assessment': int(os.getenv('FRIREN_CONCURRENT_RISK', '3')),
                'execution_order': int(os.getenv('FRIREN_CONCURRENT_EXECUTION', '1')),
                'position_update': int(os.getenv('FRIREN_CONCURRENT_POSITION', '3')),
                'strategy_assignment': int(os.getenv('FRIREN_CONCURRENT_STRATEGY', '2')),
                'market_data': int(os.getenv('FRIREN_CONCURRENT_MARKET', '10')),
                'system_health': int(os.getenv('FRIREN_CONCURRENT_HEALTH', '5'))
            }
            
            return {**default_limits, **limits}
            
        except Exception as e:
            self.logger.error(f"Error loading concurrent limits: {e}")
            return {}
    
    def _load_cooldown_periods(self) -> Dict[str, float]:
        """Load message cooldown periods"""
        try:
            if self.db_manager:
                cooldowns = self._load_from_database('message_cooldown_periods')
                if cooldowns:
                    return cooldowns
            
            # Environment variable format: FRIREN_COOLDOWN_<MESSAGE_TYPE>=<SECONDS>
            cooldowns = {}
            for key, value in os.environ.items():
                if key.startswith('FRIREN_COOLDOWN_'):
                    message_type = key.replace('FRIREN_COOLDOWN_', '').lower()
                    cooldowns[message_type] = float(value)
            
            # Default cooldowns
            default_cooldowns = {
                'news_collection': float(os.getenv('FRIREN_COOLDOWN_NEWS', '5.0')),
                'sentiment_analysis': float(os.getenv('FRIREN_COOLDOWN_SENTIMENT', '1.0')),
                'trading_signal': float(os.getenv('FRIREN_COOLDOWN_SIGNAL', '0.0')),
                'risk_assessment': float(os.getenv('FRIREN_COOLDOWN_RISK', '0.0')),
                'execution_order': float(os.getenv('FRIREN_COOLDOWN_EXECUTION', '2.0')),
                'position_update': float(os.getenv('FRIREN_COOLDOWN_POSITION', '1.0')),
                'strategy_assignment': float(os.getenv('FRIREN_COOLDOWN_STRATEGY', '5.0')),
                'market_data': float(os.getenv('FRIREN_COOLDOWN_MARKET', '0.1')),
                'system_health': float(os.getenv('FRIREN_COOLDOWN_HEALTH', '1.0'))
            }
            
            return {**default_cooldowns, **cooldowns}
            
        except Exception as e:
            self.logger.error(f"Error loading cooldown periods: {e}")
            return {}
    
    def _load_priority_scores(self) -> Dict[str, float]:
        """Load message priority scores"""
        try:
            if self.db_manager:
                scores = self._load_from_database('message_priority_scores')
                if scores:
                    return scores
            
            # Environment variable format: FRIREN_PRIORITY_SCORE_<MESSAGE_TYPE>=<SCORE>
            scores = {}
            for key, value in os.environ.items():
                if key.startswith('FRIREN_PRIORITY_SCORE_'):
                    message_type = key.replace('FRIREN_PRIORITY_SCORE_', '').lower()
                    scores[message_type] = float(value)
            
            # Default priority scores
            default_scores = {
                'execution_order': float(os.getenv('FRIREN_SCORE_EXECUTION', '100.0')),
                'risk_assessment': float(os.getenv('FRIREN_SCORE_RISK', '90.0')),
                'trading_signal': float(os.getenv('FRIREN_SCORE_SIGNAL', '80.0')),
                'position_update': float(os.getenv('FRIREN_SCORE_POSITION', '70.0')),
                'sentiment_analysis': float(os.getenv('FRIREN_SCORE_SENTIMENT', '60.0')),
                'news_collection': float(os.getenv('FRIREN_SCORE_NEWS', '50.0')),
                'strategy_assignment': float(os.getenv('FRIREN_SCORE_STRATEGY', '40.0')),
                'market_data': float(os.getenv('FRIREN_SCORE_MARKET', '30.0')),
                'system_health': float(os.getenv('FRIREN_SCORE_HEALTH', '20.0'))
            }
            
            return {**default_scores, **scores}
            
        except Exception as e:
            self.logger.error(f"Error loading priority scores: {e}")
            return {}
    
    def _load_required_fields(self) -> Dict[str, List[str]]:
        """Load required fields for message types"""
        try:
            if self.db_manager:
                fields = self._load_from_database('message_required_fields')
                if fields:
                    return fields
            
            # Load from environment JSON or use defaults
            required_fields_json = os.getenv('FRIREN_REQUIRED_FIELDS')
            if required_fields_json:
                return json.loads(required_fields_json)
            
            # Default required fields
            return {
                'coordination': ['message_id', 'sender', 'recipient', 'message_type', 'timestamp'],
                'heartbeat': ['process_id', 'timestamp', 'status'],
                'state_update': ['process_id', 'state_data', 'timestamp'],
                'recovery': ['process_id', 'action_type', 'reason', 'timestamp']
            }
            
        except Exception as e:
            self.logger.error(f"Error loading required fields: {e}")
            return {}
    
    def _load_valid_message_types(self) -> List[str]:
        """Load valid message types"""
        try:
            if self.db_manager:
                types = self._load_from_database('valid_message_types')
                if types and isinstance(types, list):
                    return types
            
            # Load from environment or use defaults
            types_env = os.getenv('FRIREN_VALID_MESSAGE_TYPES')
            if types_env:
                return types_env.split(',')
            
            # Default message types
            return [
                'coordination', 'heartbeat', 'state_update', 'recovery',
                'news_collection', 'sentiment_analysis', 'trading_signal',
                'risk_assessment', 'execution_order', 'position_update',
                'strategy_assignment', 'market_data', 'system_health'
            ]
            
        except Exception as e:
            self.logger.error(f"Error loading valid message types: {e}")
            return []
    
    def _load_from_database(self, config_type: str) -> Optional[Any]:
        """Load configuration from database"""
        try:
            if not self.db_manager:
                return None
            
            # Try to get configuration from database
            # This would need to be implemented based on your database schema
            query = """
                SELECT config_value 
                FROM coordination_config 
                WHERE config_type = %s AND active = true
                ORDER BY created_at DESC 
                LIMIT 1
            """
            
            result = self.db_manager.execute_query(query, (config_type,))
            if result and len(result) > 0:
                config_value = result[0]['config_value']
                if isinstance(config_value, str):
                    return json.loads(config_value)
                return config_value
            
            return None
            
        except Exception as e:
            # Enhanced error handling for better debugging
            if "does not exist" in str(e).lower():
                self.logger.info(f"Database table coordination_config does not exist for {config_type} - using environment fallback")
                self.logger.info("SOLUTION: Apply migration 0006_create_coordination_config.py to enable dynamic configuration")
            elif "connection" in str(e).lower() or "timeout" in str(e).lower():
                self.logger.info(f"Database connection unavailable for {config_type} - using environment fallback")
                self.logger.debug(f"Database error details: {e}")
            else:
                self.logger.warning(f"Unexpected database error loading {config_type}: {e}")
            return None
    
    def _load_default_configuration(self):
        """Load minimal default configuration for emergency fallback"""
        self.logger.warning("Loading emergency default configuration")
        
        self.memory_config = MemoryConfig(
            target_memory_mb=1024,
            redis_memory_ratio=0.625,
            emergency_threshold_ratio=0.95,
            pressure_thresholds={'low': 0.70, 'high': 0.90, 'critical': 0.95},
            cleanup_strategies={'low': 'expired', 'high': 'old', 'critical': 'emergency'}
        )
        
        self.process_config = ProcessConfig(
            heartbeat_timeout_seconds=30,
            health_check_interval_seconds=5,
            max_restarts=5,
            restart_delay_seconds=10,
            priority_weights={'position_monitor': 4, 'decision_engine': 3}
        )
        
        self.queue_config = QueueConfig(
            default_queue_name='priority',
            max_queue_size=10000,
            health_check_interval_seconds=10,
            routing_cache_ttl_seconds=300,
            deduplication_window_minutes=5
        )
        
        self.message_config = MessageConfig(
            max_concurrent_limits={'execution_order': 1, 'trading_signal': 5},
            cooldown_periods={'execution_order': 2.0, 'news_collection': 5.0},
            priority_scores={'execution_order': 100.0, 'trading_signal': 80.0},
            required_fields={'coordination': ['message_id', 'sender', 'recipient']},
            valid_message_types=['coordination', 'heartbeat', 'trading_signal']
        )
    
    def reload_configuration(self):
        """Reload configuration from all sources"""
        try:
            self.logger.info("Reloading coordination configuration...")
            self._load_configuration()
            self.logger.info("Configuration reloaded successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error reloading configuration: {e}")
            return False
    
    def get_configuration_status(self) -> Dict[str, Any]:
        """Get current configuration status with detailed diagnostics"""
        
        # Test database connectivity
        db_status = "unavailable"
        db_error = None
        coordination_table_exists = False
        
        if self.db_manager is not None:
            try:
                # Test basic connection
                test_result = self.db_manager.execute_query("SELECT 1 as test")
                if test_result:
                    db_status = "connected"
                    
                    # Test coordination_config table specifically
                    table_result = self.db_manager.execute_query("""
                        SELECT COUNT(*) as count 
                        FROM information_schema.tables 
                        WHERE table_name = 'coordination_config'
                    """)
                    if table_result and table_result[0].get('count', 0) > 0:
                        coordination_table_exists = True
                        
                        # Test if table has data
                        data_result = self.db_manager.execute_query("""
                            SELECT COUNT(*) as count FROM coordination_config WHERE active = true
                        """)
                        if data_result:
                            table_data_count = data_result[0].get('count', 0)
                        else:
                            table_data_count = 0
                    else:
                        table_data_count = 0
                else:
                    db_status = "connection_failed"
                    
            except Exception as e:
                db_status = "error"
                db_error = str(e)
                if "does not exist" in str(e).lower():
                    db_status = "table_missing"
                elif "connection" in str(e).lower() or "timeout" in str(e).lower():
                    db_status = "connection_timeout"
        
        return {
            'memory_config': asdict(self.memory_config) if hasattr(self, 'memory_config') else None,
            'process_config': asdict(self.process_config) if hasattr(self, 'process_config') else None,
            'queue_config': asdict(self.queue_config) if hasattr(self, 'queue_config') else None,
            'message_config': asdict(self.message_config) if hasattr(self, 'message_config') else None,
            'database_status': {
                'manager_available': self.db_manager is not None,
                'connection_status': db_status,
                'coordination_table_exists': coordination_table_exists,
                'configuration_entries': table_data_count if coordination_table_exists else 0,
                'error': db_error
            },
            'configuration_source': 'database' if db_status == 'connected' and coordination_table_exists else 'environment_variables',
            'last_reload': self._last_db_load.isoformat() if self._last_db_load else None,
            'recommendations': self._get_configuration_recommendations(db_status, coordination_table_exists)
        }
    
    def _get_configuration_recommendations(self, db_status: str, table_exists: bool) -> List[str]:
        """Get recommendations for configuration improvements"""
        recommendations = []
        
        if db_status == "connection_timeout":
            recommendations.append("Database connection timeout - check network connectivity and RDS availability")
        elif db_status == "table_missing" or not table_exists:
            recommendations.append("Apply migration: python manage.py migrate to create coordination_config table")
            recommendations.append("This will enable dynamic configuration instead of environment variable fallback")
        elif db_status == "connected" and table_exists:
            recommendations.append("Database configuration fully operational - using dynamic configuration")
        elif db_status == "unavailable":
            recommendations.append("Database manager not available - system using environment variable configuration")
        
        return recommendations


# Global configuration manager instance
_config_manager: Optional[CoordinationConfigManager] = None


def get_coordination_config() -> CoordinationConfigManager:
    """Get or create global coordination configuration manager"""
    global _config_manager
    
    if _config_manager is None:
        _config_manager = CoordinationConfigManager()
    
    return _config_manager


def reload_coordination_config() -> bool:
    """Reload coordination configuration"""
    config_manager = get_coordination_config()
    return config_manager.reload_configuration()