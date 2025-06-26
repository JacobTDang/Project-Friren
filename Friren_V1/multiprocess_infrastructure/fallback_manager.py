"""
Fallback manager when Redis is not available
Uses basic Python multiprocessing instead
"""

import logging
import multiprocessing as mp
import time
import threading
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ProcessStatus:
    """Basic process status"""
    process_id: str
    is_running: bool = False
    is_healthy: bool = False
    start_time: Optional[datetime] = None

class FallbackManager:
    """
    Fallback manager when Redis is not available
    Provides minimal functionality for testing
    """
    
    def __init__(self):
        self.logger = logging.getLogger("fallback_manager")
        self.shared_state: Dict[str, Any] = {}
        self.processes: Dict[str, ProcessStatus] = {}
        self.logger.info("FallbackManager initialized (Redis not available)")
    
    def set_shared_state(self, key: str, value: Any, namespace: str = "general") -> bool:
        """Set shared state value"""
        full_key = f"{namespace}:{key}"
        self.shared_state[full_key] = value
        return True
    
    def get_shared_state(self, key: str, namespace: str = "general", default: Any = None) -> Any:
        """Get shared state value"""
        full_key = f"{namespace}:{key}"
        return self.shared_state.get(full_key, default)
    
    def update_shared_state(self, updates: Dict[str, Any], namespace: str = "general") -> bool:
        """Update multiple shared state values"""
        for key, value in updates.items():
            self.set_shared_state(key, value, namespace)
        return True
    
    def update_position(self, symbol: str, position_data: Dict[str, Any]) -> bool:
        """Update position data"""
        self.set_shared_state(symbol, position_data, "positions")
        return True
    
    def get_position(self, symbol: str) -> Dict[str, Any]:
        """Get position data"""
        return self.get_shared_state(symbol, "positions", {})
    
    def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        """Get all positions"""
        positions = {}
        for key, value in self.shared_state.items():
            if key.startswith("positions:"):
                symbol = key.split(":", 1)[1]
                positions[symbol] = value
        return positions
    
    def update_process_health(self, process_id: str, health_data: Dict[str, Any]) -> bool:
        """Update process health"""
        if process_id not in self.processes:
            self.processes[process_id] = ProcessStatus(process_id=process_id)
        
        status = self.processes[process_id]
        status.is_healthy = health_data.get('status') == 'healthy'
        status.is_running = True
        return True
    
    def get_process_health(self, process_id: str) -> Dict[str, Any]:
        """Get process health"""
        if process_id in self.processes:
            status = self.processes[process_id]
            return {
                'status': 'healthy' if status.is_healthy else 'unhealthy',
                'is_running': status.is_running,
                'last_heartbeat': datetime.now().isoformat()
            }
        return {}
    
    def get_all_process_health(self) -> Dict[str, Dict[str, Any]]:
        """Get all process health"""
        return {
            pid: self.get_process_health(pid)
            for pid in self.processes.keys()
        }
    
    def update_symbol_coordination(self, coordination_data: Dict[str, Any]) -> bool:
        """Update symbol coordination"""
        self.set_shared_state("coordination", coordination_data, "system")
        return True
    
    def get_symbol_coordination(self) -> Dict[str, Any]:
        """Get symbol coordination"""
        return self.get_shared_state("coordination", "system", {})
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get system status"""
        return {
            'status': 'running',
            'manager_type': 'fallback',
            'redis_available': False,
            'processes': len(self.processes),
            'timestamp': datetime.now().isoformat()
        }
    
    def publish_broadcast(self, message: str, data: Dict[str, Any] = None) -> bool:
        """Publish broadcast (no-op in fallback)"""
        self.logger.info(f"Broadcast: {message}")
        return True
    
    def publish_emergency(self, message: str, data: Dict[str, Any] = None) -> bool:
        """Publish emergency (no-op in fallback)"""
        self.logger.warning(f"Emergency: {message}")
        return True
    
    def cleanup_all_data(self) -> bool:
        """Cleanup all data"""
        self.shared_state.clear()
        self.processes.clear()
        return True

# Global fallback instance
_fallback_manager = None

def get_fallback_manager() -> FallbackManager:
    """Get fallback manager instance"""
    global _fallback_manager
    if _fallback_manager is None:
        _fallback_manager = FallbackManager()
    return _fallback_manager