#!/usr/bin/env python3
"""
Trading System Requirements Validator

This script validates that all required dependencies and environment variables
are properly configured before running the trading system.
"""

import os
import sys
import importlib
from pathlib import Path
from typing import List, Tuple, Dict


class RequirementsValidator:
    """Validates trading system requirements"""

    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.passed_checks: List[str] = []

    def check_python_version(self) -> bool:
        """Check Python version compatibility"""
        required_version = (3, 8)
        current_version = sys.version_info[:2]

        if current_version >= required_version:
            self.passed_checks.append(f" Python {sys.version} (compatible)")
            return True
        else:
            self.errors.append(f" Python {current_version} detected, requires >= {required_version}")
            return False

    def check_critical_packages(self) -> bool:
        """Check critical Python packages"""
        critical_packages = [
            'django',
            'psycopg2',
            'pandas',
            'numpy',
            'requests',
            'dotenv',
        ]

        all_available = True

        for package in critical_packages:
            try:
                importlib.import_module(package)
                self.passed_checks.append(f" {package} installed")
            except ImportError:
                self.errors.append(f" {package} not installed")
                all_available = False

        return all_available

    def check_optional_packages(self) -> bool:
        """Check optional packages (with warnings)"""
        optional_packages = [
            ('sklearn', 'scikit-learn - needed for ML features'),
            ('torch', 'PyTorch - needed for FinBERT sentiment analysis'),
            ('transformers', 'Transformers - needed for FinBERT'),
            ('xgboost', 'XGBoost - needed for conflict resolution'),
            ('matplotlib', 'Matplotlib - needed for visualization'),
            ('yfinance', 'yfinance - needed for price data'),
        ]

        for package, description in optional_packages:
            try:
                importlib.import_module(package)
                self.passed_checks.append(f" {package} installed")
            except ImportError:
                self.warnings.append(f" {package} not installed - {description}")

        return True  # Optional packages don't fail validation

    def check_environment_variables(self) -> bool:
        """Check critical environment variables"""
        critical_env_vars = [
            'SECRET_KEY',
            'RDS_ENDPOINT',
            'RDS_USERNAME',
            'RDS_PASSWORD',
            'RDS_DBNAME',
        ]

        optional_env_vars = [
            'NEWS_API_KEY',
            'ALPACA_API_KEY',
            'ALPACA_SECRET',
            'REDDIT_CLIENT_ID',
            'FMP_API_KEY',
        ]

        all_critical_present = True

        # Check critical variables
        for var in critical_env_vars:
            value = os.getenv(var)
            if value:
                self.passed_checks.append(f" {var} configured")
            else:
                self.errors.append(f" {var} environment variable not set")
                all_critical_present = False

        # Check optional variables
        for var in optional_env_vars:
            value = os.getenv(var)
            if value:
                self.passed_checks.append(f" {var} configured")
            else:
                self.warnings.append(f" {var} not configured - some features may be limited")

        return all_critical_present

    def check_file_structure(self) -> bool:
        """Check critical file structure"""
        # Try to detect if we're inside Friren_V1 or in the parent directory
        current_dir = Path(__file__).parent

        # Check if we're already in Friren_V1 directory
        if (current_dir / 'config' / 'settings.py').exists():
            # We're in Friren_V1, check relative to current directory
            critical_paths = [
                'config/settings.py',
                'manage.py',
                'trading_engine/',
                'multiprocess_infrastructure/',
            ]
            project_root = current_dir
        else:
            # We're in parent directory, check relative to Friren_V1
            critical_paths = [
                'Friren_V1/config/settings.py',
                'Friren_V1/manage.py',
                'Friren_V1/trading_engine/',
                'Friren_V1/multiprocess_infrastructure/',
            ]
            project_root = current_dir

        all_present = True

        for path_str in critical_paths:
            path = project_root / path_str
            if path.exists():
                self.passed_checks.append(f" {path_str} found")
            else:
                self.errors.append(f" {path_str} missing")
                all_present = False

        return all_present

    def check_database_connectivity(self) -> bool:
        """Test database connectivity"""
        try:
            import psycopg2

            # Get database config from environment
            host = os.getenv('RDS_ENDPOINT')
            port = os.getenv('RDS_PORT', '5432')
            dbname = os.getenv('RDS_DBNAME')
            user = os.getenv('RDS_USERNAME')
            password = os.getenv('RDS_PASSWORD')

            if not all([host, dbname, user, password]):
                self.warnings.append(" Database credentials incomplete - skipping connectivity test")
                return True

            # Test connection
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
                connect_timeout=10
            )
            conn.close()

            self.passed_checks.append(" Database connectivity successful")
            return True

        except ImportError:
            self.warnings.append(" psycopg2 not available - skipping database test")
            return True
        except Exception as e:
            self.warnings.append(f" Database connectivity failed: {e}")
            return True  # Non-critical for validation

    def validate_all(self) -> bool:
        """Run all validation checks"""
        print(" Trading System Requirements Validation")
        print("=" * 50)

        checks = [
            ("Python Version", self.check_python_version),
            ("Critical Packages", self.check_critical_packages),
            ("Optional Packages", self.check_optional_packages),
            ("Environment Variables", self.check_environment_variables),
            ("File Structure", self.check_file_structure),
            ("Database Connectivity", self.check_database_connectivity),
        ]

        all_passed = True

        for check_name, check_func in checks:
            print(f"\n {check_name}:")
            try:
                result = check_func()
                if not result:
                    all_passed = False
            except Exception as e:
                self.errors.append(f" {check_name} check failed: {e}")
                all_passed = False

        # Print results
        print("\n" + "=" * 50)
        print(" VALIDATION RESULTS")
        print("=" * 50)

        if self.passed_checks:
            print(f"\n PASSED ({len(self.passed_checks)} checks):")
            for check in self.passed_checks:
                print(f"   {check}")

        if self.warnings:
            print(f"\n WARNINGS ({len(self.warnings)} items):")
            for warning in self.warnings:
                print(f"   {warning}")

        if self.errors:
            print(f"\n ERRORS ({len(self.errors)} items):")
            for error in self.errors:
                print(f"   {error}")

        print("\n" + "=" * 50)

        if all_passed and not self.errors:
            print(" VALIDATION PASSED - Trading system ready to run!")
            print("\n Next steps:")
            print("   1. Run: python manage.py migrate")
            print("   2. Start trading: python gui_interface.py")
            return True
        else:
            print(" VALIDATION FAILED - Please fix errors before running")
            print("\n Recommended fixes:")
            if self.errors:
                print("   1. Install missing packages: pip install -r requirements.txt")
                print("   2. Set missing environment variables in .env file")
                print("   3. Verify database configuration")
            return False


def main():
    """Main validation function"""
    # Load environment variables from .env if available
    try:
        from dotenv import load_dotenv
        env_file = Path(__file__).parent / '.env'
        if env_file.exists():
            load_dotenv(env_file)
            print(f" Loaded environment from {env_file}")
        else:
            print(" No .env file found - using system environment variables")
    except ImportError:
        print(" python-dotenv not available - using system environment variables")

    validator = RequirementsValidator()
    success = validator.validate_all()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
