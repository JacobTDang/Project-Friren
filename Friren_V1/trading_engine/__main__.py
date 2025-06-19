#!/usr/bin/env python3
"""
trading_engine/__main__.py

Main entry point for Friren Trading Engine
Handles command line arguments and system initialization
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Configure Django BEFORE any other imports
def setup_django():
    """Setup Django configuration"""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Friren_V1.config.settings')
    try:
        import django
        django.setup()
        print("Django configuration successful")
        return True
    except Exception as e:
        print(f"Django configuration failed: {e}")
        return False

# Setup Django first
django_success = setup_django()

try:
    from .orchestrator import UnifiedOrchestrator
    ORCHESTRATOR_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import UnifiedOrchestrator: {e}")
    ORCHESTRATOR_AVAILABLE = False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Friren Trading Engine')
    parser.add_argument('--symbols', type=str, default='AAPL,MSFT,GOOGL,TSLA,NVDA',
                        help='Comma-separated list of symbols to trade')
    parser.add_argument('--cycle-interval', type=int, default=30,
                        help='Trading cycle interval in seconds')
    parser.add_argument('--mode', type=str, default='paper_trading',
                        choices=['paper_trading', 'live_trading'],
                        help='Trading mode')
    parser.add_argument('--multiprocess', action='store_true',
                        help='Enable multiprocessing mode')

    args = parser.parse_args()

    # Parse symbols
    symbols = [s.strip().upper() for s in args.symbols.split(',')]

    print("=" * 60)
    print("Starting Friren Trading Engine")
    print("=" * 60)
    print(f"Symbols: {symbols}")
    print(f"Cycle Interval: {args.cycle_interval}s")
    print(f"Mode: {args.mode}")
    print("=" * 60)

    if not django_success:
        print("Error: Django configuration failed. Cannot start trading system.")
        return 1

    if not ORCHESTRATOR_AVAILABLE:
        print("Error: Trading orchestrator not available. Cannot start trading system.")
        return 1

    try:
        # Create and start orchestrator
        orchestrator = UnifiedOrchestrator(
            symbols=symbols,
            cycle_interval_seconds=args.cycle_interval,
            mode=args.mode
        )

        # Start the system
        orchestrator.start_trading_system()

    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Error starting trading system: {e}")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
