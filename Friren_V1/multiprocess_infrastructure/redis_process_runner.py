#!/usr/bin/env python3
"""
Redis process runner script

This script is launched as a subprocess and runs individual trading system 
processes using Redis for communication instead of multiprocessing queues.
"""

import sys
import os
import json
import logging
import importlib
import time
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

def setup_logging(process_id: str):
    """Setup logging for the spawned process"""
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(project_root, 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Setup logging
    log_file = os.path.join(logs_dir, f'{process_id}_redis_subprocess.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(f"redis_subprocess.{process_id}")
    return logger

def load_process_class(module_path: str, class_name: str):
    """Dynamically load a process class"""
    try:
        module = importlib.import_module(module_path)
        process_class = getattr(module, class_name)
        return process_class
    except Exception as e:
        raise ImportError(f"Could not load class {class_name} from {module_path}: {e}")

def create_redis_compatible_process(original_class, process_id: str, process_args: dict, heartbeat_interval: int):
    """Create a Redis-compatible wrapper for existing process classes"""
    from Friren_V1.multiprocess_infrastructure.redis_base_process import RedisBaseProcess, ProcessState
    
    class RedisCompatibleProcess(RedisBaseProcess):
        """Redis-compatible wrapper for existing process classes"""
        
        def __init__(self, process_id: str, heartbeat_interval: int = 30, **kwargs):
            super().__init__(process_id, heartbeat_interval)
            
            # Create the original process instance
            try:
                self.original_process = original_class(process_id=process_id, **kwargs)
                self.logger.info(f"Created original process instance: {original_class.__name__}")
            except Exception as e:
                self.logger.error(f"Failed to create original process: {e}")
                raise
        
        def _initialize(self):
            """Initialize the wrapped process"""
            try:
                # If original process has an initialize method, call it
                if hasattr(self.original_process, '_initialize'):
                    self.original_process._initialize()
                elif hasattr(self.original_process, 'initialize'):
                    self.original_process.initialize()
                
                self.logger.info(f"Process {self.process_id} initialized successfully")
            except Exception as e:
                self.logger.error(f"Error initializing process: {e}")
                raise
        
        def _execute(self):
            """Execute the main process logic"""
            try:
                # Call the original process's main logic
                if hasattr(self.original_process, '_execute'):
                    self.original_process._execute()
                elif hasattr(self.original_process, 'execute'):
                    self.original_process.execute()
                elif hasattr(self.original_process, 'run'):
                    self.original_process.run()
                elif hasattr(self.original_process, 'process'):
                    self.original_process.process()
                else:
                    # Fallback - just sleep to prevent spinning
                    time.sleep(1)
                    
            except Exception as e:
                self.logger.error(f"Error in execute: {e}")
                self.error_count += 1
                
                # Brief pause on error to prevent spinning
                time.sleep(1)
        
        def _cleanup(self):
            """Cleanup the wrapped process"""
            try:
                # Call original process cleanup if available
                if hasattr(self.original_process, '_cleanup'):
                    self.original_process._cleanup()
                elif hasattr(self.original_process, 'cleanup'):
                    self.original_process.cleanup()
                elif hasattr(self.original_process, 'stop'):
                    self.original_process.stop()
                
                super()._cleanup()
                self.logger.info(f"Process {self.process_id} cleanup completed")
            except Exception as e:
                self.logger.error(f"Error in cleanup: {e}")
    
    return RedisCompatibleProcess

def main():
    """Main entry point for Redis subprocess"""
    if len(sys.argv) != 2:
        print("ERROR: Usage: python redis_process_runner.py <config_file>", file=sys.stderr)
        sys.exit(1)
    
    config_file = sys.argv[1]
    
    try:
        # Load configuration
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        process_id = config['process_id']
        module_path = config['module_path']
        class_name = config['class_name']
        process_args = config['process_args']
        heartbeat_interval = config.get('heartbeat_interval', 30)
        
        # Setup logging
        logger = setup_logging(process_id)
        logger.info(f"REDIS_SUBPROCESS: Starting process {process_id}")
        logger.info(f"REDIS_SUBPROCESS: Module: {module_path}, Class: {class_name}")
        logger.info(f"REDIS_SUBPROCESS: Args: {process_args}")
        
        # Load the original process class
        logger.info(f"REDIS_SUBPROCESS: Loading process class...")
        original_class = load_process_class(module_path, class_name)
        logger.info(f"REDIS_SUBPROCESS: Process class loaded successfully")
        
        # Create Redis-compatible wrapper
        logger.info(f"REDIS_SUBPROCESS: Creating Redis-compatible wrapper...")
        redis_process_class = create_redis_compatible_process(
            original_class, 
            process_id, 
            process_args, 
            heartbeat_interval
        )
        
        # Create process instance
        logger.info(f"REDIS_SUBPROCESS: Creating process instance...")
        
        # Filter out process_id from args since we pass it separately
        filtered_args = {k: v for k, v in process_args.items() if k != 'process_id'}
        
        process_instance = redis_process_class(
            process_id=process_id,
            heartbeat_interval=heartbeat_interval,
            **filtered_args
        )
        
        logger.info(f"REDIS_SUBPROCESS: Process instance created")
        
        # Start the process
        logger.info(f"REDIS_SUBPROCESS: Starting process main loop...")
        process_instance.start()
        
        # Keep the process alive
        logger.info(f"REDIS_SUBPROCESS: Process {process_id} running, entering main loop...")
        
        try:
            # Wait for the process to complete
            if process_instance._main_thread:
                process_instance._main_thread.join()
        except KeyboardInterrupt:
            logger.info(f"REDIS_SUBPROCESS: Received interrupt signal")
        except Exception as e:
            logger.error(f"REDIS_SUBPROCESS: Error in main execution: {e}")
        
        # Stop the process
        logger.info(f"REDIS_SUBPROCESS: Stopping process {process_id}")
        try:
            process_instance.stop()
        except Exception as e:
            logger.error(f"REDIS_SUBPROCESS: Error stopping process: {e}")
        
        logger.info(f"REDIS_SUBPROCESS: Process {process_id} finished")
        
    except Exception as e:
        error_msg = f"REDIS_SUBPROCESS: Fatal error in process runner: {e}"
        print(error_msg, file=sys.stderr)
        if 'logger' in locals():
            logger.error(error_msg)
        sys.exit(1)
    
    finally:
        # Cleanup config file
        try:
            if os.path.exists(config_file):
                os.unlink(config_file)
        except Exception as e:
            print(f"REDIS_SUBPROCESS: Could not remove config file: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()