"""
Django Configuration Safety Utils
=================================

Provides safe Django configuration for use in subprocess contexts
where Django settings may not be properly initialized.
"""

import os
import logging
from typing import Optional


def safe_django_setup():
    """
    Safely configure Django for subprocess context
    
    Returns:
        True if Django is configured successfully, False otherwise
    """
    try:
        import django
        from django.conf import settings
        
        if not settings.configured:
            # Set Django settings module
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Friren_V1.config.settings')
            django.setup()
            
        # Test that Django is working
        from django.apps import apps
        apps.check_apps_ready()
        
        return True
        
    except Exception as e:
        logging.getLogger(__name__).error(f"Django configuration failed: {e}")
        return False


def safe_model_import():
    """
    Safely import Django models with configuration check
    
    Returns:
        Tuple of (CurrentHoldings, TransactionHistory, success)
    """
    try:
        # Ensure Django is configured
        if not safe_django_setup():
            return None, None, False
            
        from Friren_V1.infrastructure.database.models import CurrentHoldings, TransactionHistory
        return CurrentHoldings, TransactionHistory, True
        
    except Exception as e:
        logging.getLogger(__name__).error(f"Django model import failed: {e}")
        return None, None, False


def safe_database_operation(operation_name: str, operation_func, *args, **kwargs):
    """
    Execute database operation with Django safety checks
    
    Args:
        operation_name: Name of operation for logging
        operation_func: Function to execute
        *args, **kwargs: Arguments for operation_func
        
    Returns:
        Operation result or None if failed
    """
    try:
        # Ensure Django is configured
        if not safe_django_setup():
            logging.getLogger(__name__).warning(f"Database operation '{operation_name}' skipped - Django not available")
            return None
            
        return operation_func(*args, **kwargs)
        
    except Exception as e:
        logging.getLogger(__name__).error(f"Database operation '{operation_name}' failed: {e}")
        return None