"""
Windows process runner script

This script is launched as a separate subprocess and loads/runs
individual trading system processes without serialization issues.
"""

import sys
import os
import json
import logging
import importlib
import time
from datetime import datetime
from dataclasses import is_dataclass

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
    log_file = os.path.join(logs_dir, f'{process_id}_subprocess.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger = logging.getLogger(f"subprocess.{process_id}")
    return logger

def load_process_class(module_path: str, class_name: str):
    """Dynamically load a process class"""
    try:
        module = importlib.import_module(module_path)
        process_class = getattr(module, class_name)
        return process_class
    except Exception as e:
        raise ImportError(f"Could not load class {class_name} from {module_path}: {e}")

def main():
    """Main entry point for subprocess"""
    if len(sys.argv) != 2:
        print("ERROR: Usage: python windows_process_runner.py <config_file>", file=sys.stderr)
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

        # Setup logging
        logger = setup_logging(process_id)
        logger.info(f"SUBPROCESS: Starting process {process_id}")
        logger.info(f"SUBPROCESS: Module: {module_path}, Class: {class_name}")
        logger.info(f"SUBPROCESS: Args: {process_args}")

        # Load the process class
        logger.info(f"SUBPROCESS: Loading process class...")
        process_class = load_process_class(module_path, class_name)
        logger.info(f"SUBPROCESS: Process class loaded successfully")

        # Create shared resources inside this process
        logger.info(f"SUBPROCESS: Creating shared resources...")
        try:
            from Friren_V1.multiprocess_infrastructure.shared_state_manager import SharedStateManager
            from Friren_V1.multiprocess_infrastructure.queue_manager import QueueManager

            shared_state = SharedStateManager()
            queue_manager = QueueManager()
            logger.info(f"SUBPROCESS: Shared resources created")
        except Exception as e:
            logger.error(f"SUBPROCESS: Could not create shared resources: {e}")
            # Continue without shared resources for now
            shared_state = None
            queue_manager = None

        # Create process instance with proper dataclass reconstruction
        logger.info(f"SUBPROCESS: Creating process instance...")

        # Reconstruct dataclass objects if needed
        reconstructed_args = {}
        for key, value in process_args.items():
            if key == 'config' and isinstance(value, dict):
                # Try to reconstruct PipelineConfig from dict
                try:
                    from Friren_V1.trading_engine.portfolio_manager.processes.enhanced_news_pipeline_process import PipelineConfig
                    if all(field in value for field in ['cycle_interval_minutes', 'batch_size', 'max_memory_mb']):
                        reconstructed_args[key] = PipelineConfig(**value)
                        logger.info(f"SUBPROCESS: Reconstructed PipelineConfig from dict")
                    else:
                        reconstructed_args[key] = value
                except Exception as e:
                    logger.warning(f"SUBPROCESS: Could not reconstruct PipelineConfig: {e}")
                    reconstructed_args[key] = value
            else:
                reconstructed_args[key] = value

        if 'process_id' in reconstructed_args:
            # Remove process_id from args since we'll pass it directly
            args_copy = reconstructed_args.copy()
            args_copy.pop('process_id', None)
            process_instance = process_class(process_id=process_id, **args_copy)
        else:
            process_instance = process_class(process_id=process_id, **reconstructed_args)

        logger.info(f"SUBPROCESS: Process instance created")

        # Set up shared resources if available
        if shared_state and queue_manager:
            process_instance.shared_state = shared_state
            process_instance.priority_queue = queue_manager.priority_queue
            process_instance.health_queue = queue_manager.health_queue
            logger.info(f"SUBPROCESS: Shared resources attached")

        # Start the process
        logger.info(f"SUBPROCESS: Starting process main loop...")
        process_instance.start()

        # Keep the process alive
        logger.info(f"SUBPROCESS: Process {process_id} running, entering main loop...")

        try:
            # Main loop - keep process alive until stopped
            while hasattr(process_instance, 'state') and process_instance.state.value != 'stopped':
                time.sleep(1)

                # Basic health check
                if hasattr(process_instance, 'error_count') and process_instance.error_count >= 10:
                    logger.error(f"SUBPROCESS: Too many errors, stopping process {process_id}")
                    break

        except KeyboardInterrupt:
            logger.info(f"SUBPROCESS: Received interrupt signal")
        except Exception as e:
            logger.error(f"SUBPROCESS: Error in main loop: {e}")

        # Stop the process
        logger.info(f"SUBPROCESS: Stopping process {process_id}")
        try:
            process_instance.stop()
        except Exception as e:
            logger.error(f"SUBPROCESS: Error stopping process: {e}")

        logger.info(f"SUBPROCESS: Process {process_id} finished")

    except Exception as e:
        error_msg = f"SUBPROCESS: Fatal error in process runner: {e}"
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
            print(f"SUBPROCESS: Could not remove config file: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
