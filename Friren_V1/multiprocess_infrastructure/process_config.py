"""
Pickleable Process Configuration Classes
=====================================

This module provides configuration classes that can be safely pickled for 
subprocess communication, avoiding issues with threading objects, loggers,
and other unpickleable components.

CRITICAL: These classes must NEVER contain:
- threading.Event, threading.Lock, or any threading objects
- logging.Logger instances
- Complex object graphs with circular references
- weakref objects
- File handles or network connections
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from datetime import datetime
import json


@dataclass
class ProcessConfig:
    """
    Pickleable process configuration for subprocess creation
    
    This class can be safely pickled and sent to subprocesses without
    causing serialization errors. All complex objects must be recreated
    on the subprocess side.
    """
    process_id: str
    class_name: str
    module_path: str
    process_args: Dict[str, Any] = field(default_factory=dict)
    
    # Resource limits
    startup_timeout: int = 45
    memory_limit_mb: int = None  # Will be set from environment variables
    max_memory_mb: int = None    # Will be set from environment variables
    
    def __post_init__(self):
        """Initialize memory limits from configuration manager - NO HARDCODED VALUES"""
        if self.memory_limit_mb is None or self.max_memory_mb is None:
            try:
                # PRODUCTION: Import configuration manager - NO FALLBACKS
                from Friren_V1.infrastructure.configuration_manager import get_memory_config
                memory_config = get_memory_config()
                
                if self.memory_limit_mb is None:
                    self.memory_limit_mb = memory_config['threshold_mb']
                if self.max_memory_mb is None:
                    self.max_memory_mb = memory_config['emergency_mb']
                    
                # Validate configuration
                if not self.memory_limit_mb or self.memory_limit_mb <= 0:
                    raise ValueError("PRODUCTION: MEMORY_THRESHOLD_MB must be configured and positive")
                if not self.max_memory_mb or self.max_memory_mb <= 0:
                    raise ValueError("PRODUCTION: MEMORY_EMERGENCY_MB must be configured and positive")
                    
            except ImportError:
                raise ImportError("CRITICAL: Configuration manager required for process memory limits. No hardcoded fallbacks allowed.")
    
    # Process behavior
    heartbeat_interval: int = 30
    cycle_time: float = 30.0
    queue_mode: bool = False
    
    # Error handling
    max_error_count: int = 10
    restart_on_error: bool = True
    
    # Logging configuration (as simple values)
    log_level: str = "INFO"
    enable_file_logging: bool = True
    enable_console_logging: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'process_id': self.process_id,
            'class_name': self.class_name,
            'module_path': self.module_path,
            'process_args': self.process_args,
            'startup_timeout': self.startup_timeout,
            'memory_limit_mb': self.memory_limit_mb,
            'max_memory_mb': self.max_memory_mb,
            'heartbeat_interval': self.heartbeat_interval,
            'cycle_time': self.cycle_time,
            'queue_mode': self.queue_mode,
            'max_error_count': self.max_error_count,
            'restart_on_error': self.restart_on_error,
            'log_level': self.log_level,
            'enable_file_logging': self.enable_file_logging,
            'enable_console_logging': self.enable_console_logging
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), default=str)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProcessConfig':
        """Create from dictionary"""
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'ProcessConfig':
        """Create from JSON string"""
        data = json.loads(json_str)
        return cls.from_dict(data)


@dataclass
class MemoryConfig:
    """Pickleable memory configuration - PRODUCTION CONFIG MANAGED"""
    memory_threshold_mb: float = None
    memory_resume_mb: float = None
    emergency_threshold_mb: float = None
    cleanup_interval: int = None
    enable_monitoring: bool = True
    
    def __post_init__(self):
        """Initialize from configuration manager - NO HARDCODED VALUES"""
        try:
            from Friren_V1.infrastructure.configuration_manager import get_memory_config, get_timing_config
            memory_config = get_memory_config()
            timing_config = get_timing_config()
            
            if self.memory_threshold_mb is None:
                self.memory_threshold_mb = memory_config['threshold_mb']
            if self.memory_resume_mb is None:
                self.memory_resume_mb = memory_config['resume_mb']
            if self.emergency_threshold_mb is None:
                self.emergency_threshold_mb = memory_config['emergency_mb']
            if self.cleanup_interval is None:
                self.cleanup_interval = timing_config['process_monitor_interval']
                
        except ImportError:
            raise ImportError("CRITICAL: Configuration manager required for memory config. No hardcoded values allowed.")


@dataclass
class RedisConfig:
    """Pickleable Redis configuration - PRODUCTION CONFIG MANAGED"""
    host: str = None
    port: int = None
    db: int = None
    password: Optional[str] = None
    connection_timeout: int = None
    socket_timeout: int = None
    max_retries: int = 3
    
    def __post_init__(self):
        """Initialize from configuration manager - NO HARDCODED VALUES"""
        try:
            from Friren_V1.infrastructure.configuration_manager import get_redis_config
            redis_config = get_redis_config()
            
            if self.host is None:
                self.host = redis_config['host']
            if self.port is None:
                self.port = redis_config['port']
            if self.db is None:
                self.db = redis_config['db']
            if self.connection_timeout is None:
                self.connection_timeout = redis_config['socket_connect_timeout']
            if self.socket_timeout is None:
                self.socket_timeout = redis_config['socket_timeout']
                
            # Validate critical configuration
            if not self.host:
                raise ValueError("PRODUCTION: REDIS_HOST must be configured")
            if not self.port or self.port <= 0:
                raise ValueError("PRODUCTION: REDIS_PORT must be configured and positive")
                
        except ImportError:
            raise ImportError("CRITICAL: Configuration manager required for Redis config. No hardcoded values allowed.")


@dataclass
class ProcessSpawnConfig:
    """
    Configuration for process spawning that avoids pickle issues
    
    This separates the configuration from runtime objects that can't be pickled.
    """
    configs: List[ProcessConfig] = field(default_factory=list)
    memory_config: MemoryConfig = field(default_factory=MemoryConfig)
    redis_config: RedisConfig = field(default_factory=RedisConfig)
    
    # Global settings
    enable_monitoring: bool = True
    shutdown_timeout: int = 30
    restart_delay: int = 5
    
    def add_process(self, config: ProcessConfig) -> None:
        """Add a process configuration"""
        self.configs.append(config)
    
    def get_process_config(self, process_id: str) -> Optional[ProcessConfig]:
        """Get configuration for a specific process"""
        for config in self.configs:
            if config.process_id == process_id:
                return config
        return None
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        # Check for duplicate process IDs
        process_ids = [config.process_id for config in self.configs]
        duplicates = set([pid for pid in process_ids if process_ids.count(pid) > 1])
        if duplicates:
            errors.append(f"Duplicate process IDs found: {duplicates}")
        
        # Validate individual process configs
        for config in self.configs:
            if not config.process_id:
                errors.append("Process ID cannot be empty")
            if not config.class_name:
                errors.append(f"Class name cannot be empty for {config.process_id}")
            if config.startup_timeout <= 0:
                errors.append(f"Invalid startup timeout for {config.process_id}")
            if config.memory_limit_mb <= 0:
                errors.append(f"Invalid memory limit for {config.process_id}")
        
        return errors


# Factory functions for creating common configurations
def create_enhanced_news_pipeline_config(symbols: List[str] = None) -> ProcessConfig:
    """Create configuration for enhanced news pipeline process"""
    import os
    
    # Use process-specific memory limit for news pipeline
    memory_limit = int(os.getenv('FRIREN_NEWS_PIPELINE_MEMORY_MB', '250'))
    max_memory = int(os.getenv('FRIREN_MAX_MEMORY_MB', '600'))
    
    return ProcessConfig(
        process_id="enhanced_news_pipeline",
        class_name="EnhancedNewsPipelineProcess",
        module_path="Friren_V1.trading_engine.portfolio_manager.processes.enhanced_news_pipeline_process",
        memory_limit_mb=memory_limit,
        max_memory_mb=max_memory,
        process_args={
            'watchlist_symbols': symbols or ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'NVDA'],
            'config': {
                'cycle_interval_minutes': 1,
                'max_articles_per_symbol': 15,
                'enable_xgboost': True,
                'recommendation_threshold': 0.4
            }
        },
        startup_timeout=90,  # Longer timeout for complex initialization
        cycle_time=30.0
    )


def create_decision_engine_config() -> ProcessConfig:
    """Create configuration for decision engine process"""
    import os
    
    # Use process-specific memory limit for decision engine
    memory_limit = int(os.getenv('FRIREN_DECISION_ENGINE_MEMORY_MB', '200'))
    max_memory = int(os.getenv('FRIREN_MAX_MEMORY_MB', '600'))
    
    return ProcessConfig(
        process_id="decision_engine",
        class_name="MarketDecisionEngineProcess",
        module_path="Friren_V1.trading_engine.portfolio_manager.decision_engine.decision_engine",
        memory_limit_mb=memory_limit,
        max_memory_mb=max_memory,
        process_args={
            'analysis_interval': 300,
            'decision_timeout': 60
        },
        startup_timeout=90,  # Longer timeout for complex initialization
        cycle_time=60.0
    )


def create_position_health_monitor_config() -> ProcessConfig:
    """Create configuration for position health monitor process"""
    import os
    
    # Use process-specific memory limit for position health monitor
    memory_limit = int(os.getenv('FRIREN_POSITION_MONITOR_MEMORY_MB', '150'))
    max_memory = int(os.getenv('FRIREN_MAX_MEMORY_MB', '600'))
    
    return ProcessConfig(
        process_id="position_health_monitor",
        class_name="PositionHealthMonitorProcess",
        module_path="Friren_V1.trading_engine.portfolio_manager.processes.position_health_monitor",
        memory_limit_mb=memory_limit,
        max_memory_mb=max_memory,
        process_args={
            'check_interval': 5,
            'alert_threshold': 0.05
        },
        startup_timeout=45,
        cycle_time=5.0
    )


def create_market_regime_detector_config() -> ProcessConfig:
    """Create configuration for market regime detector process"""
    import os
    
    # Use specific memory limit for market regime detector (lightweight process)
    memory_limit = int(os.getenv('FRIREN_REGIME_DETECTOR_MEMORY_MB', '80'))
    max_memory = int(os.getenv('FRIREN_MAX_MEMORY_MB', '600'))
    
    return ProcessConfig(
        process_id="market_regime_detector",
        class_name="MarketRegimeDetectorProcess", 
        module_path="Friren_V1.trading_engine.portfolio_manager.processes.market_regime_detector",
        memory_limit_mb=memory_limit,
        max_memory_mb=max_memory,
        process_args={
            'analysis_interval': 300,
            'regime_threshold': 0.7
        },
        startup_timeout=45,
        cycle_time=300.0
    )