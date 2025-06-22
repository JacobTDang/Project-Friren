#!/usr/bin/env python3
"""
Main entry point for Friren Trading System
Simple orchestrator startup
"""

import logging
import sys
import os
import time
import multiprocessing as mp

# Windows-specific multiprocessing configuration
if sys.platform == "win32":
    try:
        # Set spawn method for Windows (only if not already set)
        if mp.get_start_method(allow_none=True) is None:
            mp.set_start_method('spawn', force=False)
    except RuntimeError:
        # Already set, ignore
        pass
    # Enable freeze support for Windows executables
    mp.freeze_support()

# Add Friren_V1 path to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'Friren_V1'))

from trading_engine.portfolio_manager.orchestrator import MainOrchestrator

def setup_logging():
    """Setup basic logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('friren_trading.log')
        ]
    )

def main():
    """Main entry point - just start the orchestrator"""
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)

        logger.info("Starting Friren Trading System...")

        # Create orchestrator
        orchestrator = MainOrchestrator()

        # Initialize system
        logger.info("Initializing system...")
        orchestrator.initialize_system()

        # Start system
        logger.info("Starting orchestrator...")
        orchestrator.start_system()

                # Keep main thread alive while orchestrator runs
        logger.info("Orchestrator running. Will auto-stop after 5 minutes or on errors...")
        try:
            start_time = time.time()
            timeout_seconds = 300  # 5 minutes for debugging
            error_count = 0
            max_errors = 10  # Break after 10 errors

            while True:
                elapsed = time.time() - start_time

                # Check for timeout
                if elapsed > timeout_seconds:
                    logger.info("DEBUG TIMEOUT: Stopping after 5 minutes for review")
                    break

                # Check orchestrator status and errors
                try:
                    status = orchestrator.get_system_status()

                    # Break if system is in error state
                    if status.get('state') == 'error':
                        logger.error("SYSTEM ERROR STATE DETECTED - Breaking out of loop")
                        break

                    # Break if processes keep failing
                    failed_processes = status.get('failed_processes', 0)
                    if failed_processes > 0:
                        error_count += 1
                        logger.warning(f"Process failures detected: {failed_processes} (error count: {error_count})")
                        if error_count >= max_errors:
                            logger.error("TOO MANY PROCESS FAILURES - Breaking out of loop")
                            break
                    else:
                        error_count = 0  # Reset on success

                except Exception as e:
                    error_count += 1
                    logger.error(f"Error checking system status: {e} (error count: {error_count})")
                    if error_count >= max_errors:
                        logger.error("TOO MANY STATUS CHECK ERRORS - Breaking out of loop")
                        break

                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutdown signal received")

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
        if 'orchestrator' in locals():
            orchestrator.stop_system()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        if 'orchestrator' in locals():
            orchestrator.emergency_stop_all_trades()
        sys.exit(1)

if __name__ == "__main__":
    main()
