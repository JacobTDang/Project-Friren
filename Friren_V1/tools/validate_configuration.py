"""
Configuration Validation Tool

Validates the configuration system setup and ensures all required
configuration values are properly set for the Friren trading system.

Features:
- Validates environment variable configuration
- Checks configuration schema compliance
- Tests configuration manager functionality
- Generates readiness reports
- Validates production deployment readiness
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import json

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import configuration manager
try:
    from Friren_V1.infrastructure.configuration_manager import (
        ConfigurationManager, ConfigurationEnvironment, ConfigurationError,
        get_config_manager, get_config
    )
    HAS_CONFIG_MANAGER = True
except ImportError as e:
    HAS_CONFIG_MANAGER = False
    print(f"❌ Configuration manager import failed: {e}")


class ConfigurationValidator:
    """
    Comprehensive configuration validation for the Friren trading system
    """
    
    def __init__(self):
        self.logger = logging.getLogger("config_validator")
        self.validation_results = {}
        self.errors = []
        self.warnings = []
        self.success = True
    
    def validate_all(self) -> Dict[str, Any]:
        """Run comprehensive configuration validation"""
        print("🔧 Friren Trading System - Configuration Validation")
        print("=" * 60)
        
        # Test 1: Configuration Manager Import
        self._test_config_manager_import()
        
        # Test 2: Environment Detection
        self._test_environment_detection()
        
        # Test 3: Configuration Schema Validation
        self._test_configuration_schemas()
        
        # Test 4: Required Configuration Values
        self._test_required_configurations()
        
        # Test 5: Configuration Value Types
        self._test_configuration_types()
        
        # Test 6: Production Readiness
        self._test_production_readiness()
        
        # Test 7: Integration Testing
        self._test_integration_functionality()
        
        # Generate final report
        return self._generate_validation_report()
    
    def _test_config_manager_import(self):
        """Test configuration manager import and initialization"""
        print("\n🧪 Test 1: Configuration Manager Import")
        
        if not HAS_CONFIG_MANAGER:
            self._add_error("Configuration manager import failed")
            return
        
        try:
            config_manager = get_config_manager()
            self._add_success("Configuration manager imported and initialized successfully")
            self.validation_results['config_manager_available'] = True
        except Exception as e:
            self._add_error(f"Configuration manager initialization failed: {e}")
            self.validation_results['config_manager_available'] = False
    
    def _test_environment_detection(self):
        """Test environment detection"""
        print("\n🧪 Test 2: Environment Detection")
        
        if not HAS_CONFIG_MANAGER:
            self._add_error("Skipping - configuration manager not available")
            return
        
        try:
            config_manager = get_config_manager()
            environment = config_manager.environment
            
            print(f"  Detected environment: {environment.value}")
            
            if environment in [ConfigurationEnvironment.DEVELOPMENT, ConfigurationEnvironment.STAGING, ConfigurationEnvironment.PRODUCTION]:
                self._add_success(f"Valid environment detected: {environment.value}")
            else:
                self._add_warning(f"Unknown environment: {environment.value}")
            
            self.validation_results['environment'] = environment.value
            
        except Exception as e:
            self._add_error(f"Environment detection failed: {e}")
    
    def _test_configuration_schemas(self):
        """Test configuration schema registration"""
        print("\n🧪 Test 3: Configuration Schema Validation")
        
        if not HAS_CONFIG_MANAGER:
            self._add_error("Skipping - configuration manager not available")
            return
        
        try:
            config_manager = get_config_manager()
            schemas = config_manager.get_all_schemas()
            
            required_schemas = [
                'REDIS_HOST', 'REDIS_PORT', 'REDIS_DB',
                'XGBOOST_MODEL_PATH', 'XGBOOST_BUY_THRESHOLD', 'XGBOOST_SELL_THRESHOLD',
                'MEMORY_THRESHOLD_MB', 'MEMORY_RESUME_MB', 'MEMORY_EMERGENCY_MB',
                'TRADING_SYMBOLS', 'FINBERT_MODEL_NAME'
            ]
            
            missing_schemas = []
            for schema_name in required_schemas:
                if schema_name not in schemas:
                    missing_schemas.append(schema_name)
            
            if missing_schemas:
                self._add_error(f"Missing required schemas: {missing_schemas}")
            else:
                self._add_success(f"All {len(required_schemas)} required schemas registered")
            
            print(f"  Total schemas: {len(schemas)}")
            print(f"  Required schemas: {len(required_schemas)}")
            
            self.validation_results['schema_count'] = len(schemas)
            self.validation_results['missing_schemas'] = missing_schemas
            
        except Exception as e:
            self._add_error(f"Schema validation failed: {e}")
    
    def _test_required_configurations(self):
        """Test that all required configurations are available"""
        print("\n🧪 Test 4: Required Configuration Values")
        
        if not HAS_CONFIG_MANAGER:
            self._add_error("Skipping - configuration manager not available")
            return
        
        critical_configs = {
            'REDIS_HOST': 'Redis connection host',
            'REDIS_PORT': 'Redis connection port',
            'XGBOOST_MODEL_PATH': 'XGBoost model file path',
            'MEMORY_THRESHOLD_MB': 'Memory management threshold',
            'TRADING_SYMBOLS': 'Trading symbol list',
            'FINBERT_MODEL_NAME': 'FinBERT model name'
        }
        
        missing_configs = []
        invalid_configs = []
        
        for config_name, description in critical_configs.items():
            try:
                value = get_config(config_name)
                if value is None:
                    missing_configs.append(config_name)
                elif config_name == 'TRADING_SYMBOLS' and not value:
                    invalid_configs.append(f"{config_name}: empty list")
                elif config_name.endswith('_MB') and (not isinstance(value, (int, float)) or value <= 0):
                    invalid_configs.append(f"{config_name}: invalid value {value}")
                elif config_name.endswith('_PATH') and not str(value):
                    invalid_configs.append(f"{config_name}: empty path")
                else:
                    print(f"  ✅ {config_name}: {self._format_config_value(value)}")
                    
            except Exception as e:
                missing_configs.append(f"{config_name} (error: {e})")
        
        if missing_configs:
            self._add_error(f"Missing critical configurations: {missing_configs}")
        if invalid_configs:
            self._add_error(f"Invalid configurations: {invalid_configs}")
        
        if not missing_configs and not invalid_configs:
            self._add_success(f"All {len(critical_configs)} critical configurations valid")
        
        self.validation_results['missing_configs'] = missing_configs
        self.validation_results['invalid_configs'] = invalid_configs
    
    def _test_configuration_types(self):
        """Test configuration value types"""
        print("\n🧪 Test 5: Configuration Value Types")
        
        if not HAS_CONFIG_MANAGER:
            self._add_error("Skipping - configuration manager not available")
            return
        
        type_tests = {
            'REDIS_PORT': (int, lambda x: 1 <= x <= 65535),
            'REDIS_DB': (int, lambda x: 0 <= x <= 15),
            'MEMORY_THRESHOLD_MB': ((int, float), lambda x: x > 0),
            'XGBOOST_BUY_THRESHOLD': ((int, float), lambda x: 0 <= x <= 1),
            'XGBOOST_SELL_THRESHOLD': ((int, float), lambda x: 0 <= x <= 1),
            'TRADING_SYMBOLS': (list, lambda x: len(x) > 0),
            'FINBERT_BATCH_SIZE': (int, lambda x: x > 0)
        }
        
        type_errors = []
        
        for config_name, (expected_type, validator) in type_tests.items():
            try:
                value = get_config(config_name)
                if value is None:
                    continue  # Already caught in previous test
                
                if not isinstance(value, expected_type):
                    type_errors.append(f"{config_name}: expected {expected_type}, got {type(value)}")
                elif not validator(value):
                    type_errors.append(f"{config_name}: value {value} failed validation")
                else:
                    print(f"  ✅ {config_name}: {type(value).__name__} = {self._format_config_value(value)}")
                    
            except Exception as e:
                type_errors.append(f"{config_name}: error getting value: {e}")
        
        if type_errors:
            self._add_error(f"Type validation errors: {type_errors}")
        else:
            self._add_success(f"All configuration types valid")
        
        self.validation_results['type_errors'] = type_errors
    
    def _test_production_readiness(self):
        """Test production deployment readiness"""
        print("\n🧪 Test 6: Production Readiness")
        
        if not HAS_CONFIG_MANAGER:
            self._add_error("Skipping - configuration manager not available")
            return
        
        try:
            config_manager = get_config_manager()
            readiness = config_manager.validate_environment_readiness()
            
            print(f"  Environment: {readiness['environment']}")
            print(f"  Ready: {readiness['ready']}")
            print(f"  Configuration count: {readiness['config_count']}")
            
            if readiness['issues']:
                print(f"  ❌ Issues:")
                for issue in readiness['issues']:
                    print(f"    - {issue}")
                self._add_error(f"Production readiness issues: {readiness['issues']}")
            
            if readiness['warnings']:
                print(f"  ⚠️ Warnings:")
                for warning in readiness['warnings']:
                    print(f"    - {warning}")
                self._add_warning(f"Production warnings: {readiness['warnings']}")
            
            if readiness['ready']:
                self._add_success("System ready for production deployment")
            
            self.validation_results['production_readiness'] = readiness
            
        except Exception as e:
            self._add_error(f"Production readiness check failed: {e}")
    
    def _test_integration_functionality(self):
        """Test integration with other system components"""
        print("\n🧪 Test 7: Integration Testing")
        
        # Test Redis configuration integration
        try:
            from Friren_V1.infrastructure.configuration_manager import get_redis_config
            redis_config = get_redis_config()
            print(f"  ✅ Redis config integration: {redis_config['host']}:{redis_config['port']}")
        except Exception as e:
            self._add_error(f"Redis config integration failed: {e}")
        
        # Test XGBoost configuration integration
        try:
            from Friren_V1.infrastructure.configuration_manager import get_xgboost_config
            xgboost_config = get_xgboost_config()
            print(f"  ✅ XGBoost config integration: {Path(xgboost_config['model_path']).name}")
        except Exception as e:
            self._add_error(f"XGBoost config integration failed: {e}")
        
        # Test FinBERT configuration integration
        try:
            from Friren_V1.infrastructure.configuration_manager import get_finbert_config
            finbert_config = get_finbert_config()
            print(f"  ✅ FinBERT config integration: {finbert_config['model_name']}")
        except Exception as e:
            self._add_error(f"FinBERT config integration failed: {e}")
        
        # Test memory configuration integration
        try:
            from Friren_V1.infrastructure.configuration_manager import get_memory_config
            memory_config = get_memory_config()
            print(f"  ✅ Memory config integration: {memory_config['threshold_mb']}MB threshold")
        except Exception as e:
            self._add_error(f"Memory config integration failed: {e}")
        
        if not self.errors:
            self._add_success("All integration tests passed")
    
    def _format_config_value(self, value: Any) -> str:
        """Format configuration value for display"""
        if isinstance(value, list):
            if len(value) <= 3:
                return str(value)
            else:
                return f"[{', '.join(map(str, value[:2]))}, +{len(value)-2} more]"
        elif isinstance(value, str) and len(value) > 50:
            return value[:47] + "..."
        else:
            return str(value)
    
    def _add_success(self, message: str):
        """Add success message"""
        print(f"  ✅ {message}")
    
    def _add_warning(self, message: str):
        """Add warning message"""
        print(f"  ⚠️ {message}")
        self.warnings.append(message)
    
    def _add_error(self, message: str):
        """Add error message"""
        print(f"  ❌ {message}")
        self.errors.append(message)
        self.success = False
    
    def _generate_validation_report(self) -> Dict[str, Any]:
        """Generate comprehensive validation report"""
        print("\n" + "=" * 60)
        print("📊 CONFIGURATION VALIDATION REPORT")
        print("=" * 60)
        
        # Summary
        total_tests = 7
        error_count = len(self.errors)
        warning_count = len(self.warnings)
        
        if self.success:
            print("🎉 VALIDATION PASSED")
            print(f"✅ All {total_tests} test categories completed successfully")
        else:
            print("❌ VALIDATION FAILED")
            print(f"❌ {error_count} errors found across {total_tests} test categories")
        
        if warning_count > 0:
            print(f"⚠️ {warning_count} warnings found")
        
        # Details
        if self.errors:
            print(f"\n❌ ERRORS ({len(self.errors)}):")
            for i, error in enumerate(self.errors, 1):
                print(f"  {i}. {error}")
        
        if self.warnings:
            print(f"\n⚠️ WARNINGS ({len(self.warnings)}):")
            for i, warning in enumerate(self.warnings, 1):
                print(f"  {i}. {warning}")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        if not HAS_CONFIG_MANAGER:
            print("  1. Fix configuration manager import issues")
            print("  2. Ensure all required dependencies are installed")
        elif self.errors:
            print("  1. Set missing environment variables using .env file")
            print("  2. Copy .env.production.template to .env and configure values")
            print("  3. Validate configuration file paths exist")
        else:
            print("  1. System ready for production deployment")
            print("  2. Consider monitoring configuration changes")
        
        # Generate report data
        report = {
            'validation_passed': self.success,
            'total_tests': total_tests,
            'error_count': error_count,
            'warning_count': warning_count,
            'errors': self.errors,
            'warnings': self.warnings,
            'validation_results': self.validation_results,
            'timestamp': str(datetime.now()),
            'recommendations': []
        }
        
        return report


def main():
    """Main validation function"""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run validation
    validator = ConfigurationValidator()
    report = validator.validate_all()
    
    # Save report
    report_file = "configuration_validation_report.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\n📄 Detailed report saved to: {report_file}")
    
    # Exit with appropriate code
    sys.exit(0 if report['validation_passed'] else 1)


if __name__ == "__main__":
    main()