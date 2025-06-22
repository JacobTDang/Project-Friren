"""
Windows-compatible process launcher for trading system

This module provides a Windows-safe way to spawn processes without
serializing complex objects that contain threading primitives.
"""

import sys
import os
import subprocess
import time
import logging
from typing import Dict, Any, Type
from multiprocessing import Queue, Event
import tempfile
import json
from dataclasses import is_dataclass, asdict

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

logger = logging.getLogger(__name__)

class WindowsProcessLauncher:
    """
    Windows-compatible process launcher that avoids serialization issues
    by spawning processes through subprocess instead of multiprocessing
    """

    def __init__(self):
        self.processes = {}
        self.process_configs = {}

    def register_process(self, process_id: str, process_class: Type, process_args: Dict[str, Any] = None):
        """Register a process configuration"""
        self.process_configs[process_id] = {
            'process_class': process_class,
            'process_args': process_args or {},
            'module_path': process_class.__module__,
            'class_name': process_class.__name__
        }
        logger.info(f"WINDOWS_LAUNCHER: Registered process {process_id}")

    def start_process(self, process_id: str) -> bool:
        """Start a process using subprocess to avoid serialization issues"""
        try:
            if process_id not in self.process_configs:
                logger.error(f"WINDOWS_LAUNCHER: Unknown process {process_id}")
                return False

            config = self.process_configs[process_id]

            # Create a temporary config file for the process
            config_data = {
                'process_id': process_id,
                'module_path': config['module_path'],
                'class_name': config['class_name'],
                'process_args': config['process_args']
            }

            # Write config to temp file with proper dataclass serialization
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)

            def serialize_dataclass(obj):
                """Custom serializer for dataclasses and other objects"""
                if is_dataclass(obj):
                    return asdict(obj)
                elif hasattr(obj, '__dict__'):
                    return obj.__dict__
                else:
                    return str(obj)

            json.dump(config_data, temp_file, default=serialize_dataclass, indent=2)
            temp_file.close()

            # Create the process launcher script path
            launcher_script = os.path.join(os.path.dirname(__file__), 'windows_process_runner.py')

            # Launch process via subprocess
            cmd = [sys.executable, launcher_script, temp_file.name]

            logger.info(f"WINDOWS_LAUNCHER: Starting process {process_id} with command: {' '.join(cmd)}")

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=project_root
            )

            self.processes[process_id] = {
                'process': process,
                'config_file': temp_file.name,
                'start_time': time.time()
            }

            # Give process a moment to start
            time.sleep(1)

            # Check if process is still running
            if process.poll() is None:
                logger.info(f"WINDOWS_LAUNCHER: Process {process_id} started successfully (PID: {process.pid})")
                return True
            else:
                # Process died immediately, get error output
                stdout, stderr = process.communicate()
                logger.error(f"WINDOWS_LAUNCHER: Process {process_id} died immediately")
                logger.error(f"STDOUT: {stdout}")
                logger.error(f"STDERR: {stderr}")
                self._cleanup_process(process_id)
                return False

        except Exception as e:
            logger.error(f"WINDOWS_LAUNCHER: Error starting process {process_id}: {e}")
            return False

    def stop_process(self, process_id: str):
        """Stop a process"""
        if process_id not in self.processes:
            return

        try:
            process_info = self.processes[process_id]
            process = process_info['process']

            if process.poll() is None:  # Process is still running
                logger.info(f"WINDOWS_LAUNCHER: Terminating process {process_id}")
                process.terminate()

                # Wait for graceful shutdown
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.warning(f"WINDOWS_LAUNCHER: Force killing process {process_id}")
                    process.kill()
                    process.wait()

            self._cleanup_process(process_id)
            logger.info(f"WINDOWS_LAUNCHER: Process {process_id} stopped")

        except Exception as e:
            logger.error(f"WINDOWS_LAUNCHER: Error stopping process {process_id}: {e}")

    def _cleanup_process(self, process_id: str):
        """Cleanup process resources"""
        if process_id in self.processes:
            process_info = self.processes[process_id]

            # Remove temp config file
            try:
                if os.path.exists(process_info['config_file']):
                    os.unlink(process_info['config_file'])
            except Exception as e:
                logger.warning(f"WINDOWS_LAUNCHER: Could not remove temp file: {e}")

            del self.processes[process_id]

    def get_process_status(self, process_id: str) -> Dict[str, Any]:
        """Get process status"""
        if process_id not in self.processes:
            return {'status': 'not_found'}

        process_info = self.processes[process_id]
        process = process_info['process']

        if process.poll() is None:
            return {
                'status': 'running',
                'pid': process.pid,
                'uptime': time.time() - process_info['start_time']
            }
        else:
            return {
                'status': 'stopped',
                'exit_code': process.returncode,
                'uptime': time.time() - process_info['start_time']
            }

    def stop_all_processes(self):
        """Stop all processes"""
        process_ids = list(self.processes.keys())
        for process_id in process_ids:
            self.stop_process(process_id)

    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all processes"""
        return {
            process_id: self.get_process_status(process_id)
            for process_id in self.processes.keys()
        }
