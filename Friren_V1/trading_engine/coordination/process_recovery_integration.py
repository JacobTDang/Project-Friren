"""
Process Recovery Integration Manager
==================================

Coordinates between ProcessRecoveryManager and RedisProcessManager to prevent conflicts.
Establishes clear authority delegation and unified process lifecycle management.

Integration Strategy:
- ProcessRecoveryManager: High-level strategic recovery decisions and priority management
- RedisProcessManager: Low-level subprocess execution and resource management
- This integration layer: Coordination and conflict prevention
"""

import time
import threading
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Set, Any
from dataclasses import dataclass
from enum import Enum

from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority
)


class RecoveryAuthority(Enum):
    """Authority levels for process recovery actions"""
    STRATEGIC = "strategic"      # ProcessRecoveryManager handles
    OPERATIONAL = "operational"  # RedisProcessManager handles  
    COORDINATED = "coordinated"  # Requires coordination


@dataclass
class ProcessRecoveryState:
    """Unified process recovery state"""
    process_id: str
    authority: RecoveryAuthority
    strategic_manager_active: bool = False
    operational_manager_active: bool = False
    last_strategic_action: Optional[datetime] = None
    last_operational_action: Optional[datetime] = None
    coordination_lock: bool = False
    coordination_lock_holder: Optional[str] = None
    restart_count: int = 0
    last_restart_reason: Optional[str] = None


class ProcessRecoveryIntegrationManager:
    """
    Coordinates process recovery between strategic and operational managers
    
    Authority Delegation Rules:
    1. CRITICAL processes (position_health_monitor): Always STRATEGIC authority
    2. HIGH priority processes: STRATEGIC for failures, OPERATIONAL for restarts
    3. NORMAL processes: COORDINATED - both managers can participate
    4. LOW priority processes: OPERATIONAL authority only
    
    Conflict Resolution:
    - Use Redis-based coordination locks
    - Priority-based authority arbitration
    - Timeout-based lock release
    """

    def __init__(self, redis_manager=None):
        self.redis_manager = redis_manager or get_trading_redis_manager()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # State tracking
        self.process_states: Dict[str, ProcessRecoveryState] = {}
        self.authority_mapping: Dict[str, RecoveryAuthority] = {}
        
        # Coordination settings
        self.coordination_lock_timeout = 30  # seconds
        self.strategic_priority_processes = {
            'position_health_monitor', 'decision_engine', 'risk_manager'
        }
        self.operational_only_processes = {
            'system_monitor', 'health_monitor', 'debug_monitor'
        }
        
        # Thread safety
        self._lock = threading.RLock()
        self._monitoring_active = False
        self._monitoring_thread = None
        
        self.logger.info("Process Recovery Integration Manager initialized")
        self._initialize_authority_mapping()

    def _initialize_authority_mapping(self):
        """Initialize authority mapping for all known processes"""
        # CRITICAL processes - strategic authority only
        critical_processes = ['position_health_monitor']
        for process_id in critical_processes:
            self.authority_mapping[process_id] = RecoveryAuthority.STRATEGIC
            
        # HIGH priority processes - strategic for failures
        high_priority_processes = ['decision_engine', 'risk_manager']
        for process_id in high_priority_processes:
            self.authority_mapping[process_id] = RecoveryAuthority.STRATEGIC
            
        # NORMAL processes - coordinated authority
        normal_processes = ['enhanced_news_pipeline', 'strategy_analyzer']
        for process_id in normal_processes:
            self.authority_mapping[process_id] = RecoveryAuthority.COORDINATED
            
        # LOW priority processes - operational authority only
        low_priority_processes = ['system_monitor', 'health_monitor', 'debug_monitor']
        for process_id in low_priority_processes:
            self.authority_mapping[process_id] = RecoveryAuthority.OPERATIONAL
            
        self.logger.info(f"Authority mapping initialized for {len(self.authority_mapping)} processes")

    def request_recovery_authority(self, process_id: str, requesting_manager: str, 
                                 action_type: str) -> bool:
        """
        Request authority to perform recovery action on a process
        
        Args:
            process_id: Target process ID
            requesting_manager: 'strategic' or 'operational'
            action_type: 'restart', 'stop', 'kill', etc.
            
        Returns:
            True if authority granted, False if denied
        """
        with self._lock:
            try:
                # Get or create process state
                if process_id not in self.process_states:
                    authority = self.authority_mapping.get(process_id, RecoveryAuthority.COORDINATED)
                    self.process_states[process_id] = ProcessRecoveryState(
                        process_id=process_id,
                        authority=authority
                    )
                
                state = self.process_states[process_id]
                
                # Check coordination lock
                if state.coordination_lock:
                    if state.coordination_lock_holder != requesting_manager:
                        # Check if lock has expired
                        if (state.last_strategic_action and 
                            (datetime.now() - state.last_strategic_action).total_seconds() > self.coordination_lock_timeout):
                            self._release_coordination_lock(process_id)
                        else:
                            self.logger.debug(f"Recovery authority denied for {process_id}: coordination lock held by {state.coordination_lock_holder}")
                            return False
                
                # Authority decision matrix
                authority_granted = self._evaluate_authority_request(state, requesting_manager, action_type)
                
                if authority_granted:
                    # Grant authority and update state
                    self._grant_recovery_authority(state, requesting_manager, action_type)
                    self.logger.info(f"Recovery authority GRANTED: {requesting_manager} can {action_type} {process_id}")
                else:
                    self.logger.info(f"Recovery authority DENIED: {requesting_manager} cannot {action_type} {process_id}")
                    
                return authority_granted
                
            except Exception as e:
                self.logger.error(f"Error in recovery authority request: {e}")
                return False

    def _evaluate_authority_request(self, state: ProcessRecoveryState, 
                                  requesting_manager: str, action_type: str) -> bool:
        """Evaluate whether to grant recovery authority"""
        
        # Authority level decision matrix
        if state.authority == RecoveryAuthority.STRATEGIC:
            # Only strategic manager allowed
            return requesting_manager == 'strategic'
            
        elif state.authority == RecoveryAuthority.OPERATIONAL:
            # Only operational manager allowed
            return requesting_manager == 'operational'
            
        elif state.authority == RecoveryAuthority.COORDINATED:
            # Both managers allowed, but coordinated
            
            # Strategic manager has priority for critical actions
            if action_type in ['recover', 'emergency_restart', 'priority_restart']:
                if requesting_manager == 'strategic':
                    return True
                elif requesting_manager == 'operational':
                    # Allow if strategic manager isn't active
                    return not state.strategic_manager_active
                    
            # Normal restarts can be handled by either
            elif action_type in ['restart', 'stop']:
                return True
                
            # Unknown action types - default to strategic
            else:
                return requesting_manager == 'strategic'
        
        return False

    def _grant_recovery_authority(self, state: ProcessRecoveryState, 
                                requesting_manager: str, action_type: str):
        """Grant recovery authority and update state"""
        
        # Set coordination lock for critical actions
        if action_type in ['recover', 'emergency_restart', 'priority_restart']:
            state.coordination_lock = True
            state.coordination_lock_holder = requesting_manager
            
        # Update manager activity state
        if requesting_manager == 'strategic':
            state.strategic_manager_active = True
            state.last_strategic_action = datetime.now()
        else:
            state.operational_manager_active = True
            state.last_operational_action = datetime.now()
            
        # Update restart tracking
        if action_type in ['restart', 'recover', 'emergency_restart', 'priority_restart']:
            state.restart_count += 1
            state.last_restart_reason = action_type

    def release_recovery_authority(self, process_id: str, releasing_manager: str):
        """Release recovery authority after action completion"""
        with self._lock:
            try:
                if process_id not in self.process_states:
                    return
                    
                state = self.process_states[process_id]
                
                # Update manager activity state
                if releasing_manager == 'strategic':
                    state.strategic_manager_active = False
                else:
                    state.operational_manager_active = False
                    
                # Release coordination lock if held by releasing manager
                if (state.coordination_lock and 
                    state.coordination_lock_holder == releasing_manager):
                    self._release_coordination_lock(process_id)
                    
                self.logger.debug(f"Recovery authority released: {releasing_manager} for {process_id}")
                
            except Exception as e:
                self.logger.error(f"Error releasing recovery authority: {e}")

    def _release_coordination_lock(self, process_id: str):
        """Release coordination lock for a process"""
        if process_id in self.process_states:
            state = self.process_states[process_id]
            state.coordination_lock = False
            state.coordination_lock_holder = None
            self.logger.debug(f"Coordination lock released for {process_id}")

    def report_recovery_action(self, process_id: str, manager: str, action: str, 
                             success: bool, details: Optional[Dict[str, Any]] = None):
        """Report the result of a recovery action"""
        try:
            # Send coordination message
            message_data = {
                'process_id': process_id,
                'manager': manager,
                'action': action,
                'success': success,
                'timestamp': datetime.now().isoformat(),
                'details': details or {}
            }
            
            message = create_process_message(
                sender='recovery_integration',
                recipient='coordination_log',
                message_type='RECOVERY_ACTION_REPORT',
                data=message_data,
                priority=MessagePriority.NORMAL
            )
            
            self.redis_manager.send_message(message, 'coordination_queue')
            
            # Update local state
            with self._lock:
                if process_id in self.process_states:
                    state = self.process_states[process_id]
                    if not success:
                        # Failed recovery - may need escalation
                        self._handle_recovery_failure(state, manager, action)
                        
            self.logger.info(f"Recovery action reported: {manager} {action} {process_id} - {'SUCCESS' if success else 'FAILED'}")
            
        except Exception as e:
            self.logger.error(f"Error reporting recovery action: {e}")

    def _handle_recovery_failure(self, state: ProcessRecoveryState, manager: str, action: str):
        """Handle recovery action failure - possibly escalate"""
        
        # Escalation rules
        if manager == 'operational' and state.restart_count < 3:
            # Escalate to strategic manager
            self.logger.warning(f"Escalating {state.process_id} recovery to strategic manager")
            
            escalation_message = create_process_message(
                sender='recovery_integration',
                recipient='strategic_recovery_manager',
                message_type='RECOVERY_ESCALATION',
                data={
                    'process_id': state.process_id,
                    'failed_manager': manager,
                    'failed_action': action,
                    'restart_count': state.restart_count,
                    'escalation_reason': 'operational_recovery_failed'
                },
                priority=MessagePriority.HIGH
            )
            
            self.redis_manager.send_message(escalation_message, 'strategic_recovery_queue')

    def get_recovery_status(self, process_id: str = None) -> Dict[str, Any]:
        """Get recovery status for process(es)"""
        with self._lock:
            if process_id:
                if process_id in self.process_states:
                    state = self.process_states[process_id]
                    return {
                        'process_id': state.process_id,
                        'authority': state.authority.value,
                        'strategic_active': state.strategic_manager_active,
                        'operational_active': state.operational_manager_active,
                        'coordination_lock': state.coordination_lock,
                        'lock_holder': state.coordination_lock_holder,
                        'restart_count': state.restart_count,
                        'last_restart_reason': state.last_restart_reason
                    }
                else:
                    return {'process_id': process_id, 'status': 'unknown'}
            else:
                # Return status for all processes
                return {
                    pid: {
                        'authority': state.authority.value,
                        'strategic_active': state.strategic_manager_active,
                        'operational_active': state.operational_manager_active,
                        'coordination_lock': state.coordination_lock,
                        'restart_count': state.restart_count
                    }
                    for pid, state in self.process_states.items()
                }

    def start_monitoring(self):
        """Start background monitoring for expired locks and coordination issues"""
        if self._monitoring_active:
            return
            
        self._monitoring_active = True
        self._monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self._monitoring_thread.start()
        self.logger.info("Recovery integration monitoring started")

    def stop_monitoring(self):
        """Stop background monitoring"""
        self._monitoring_active = False
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5)
        self.logger.info("Recovery integration monitoring stopped")

    def _monitoring_loop(self):
        """Background monitoring loop"""
        while self._monitoring_active:
            try:
                with self._lock:
                    current_time = datetime.now()
                    
                    # Check for expired coordination locks
                    for process_id, state in self.process_states.items():
                        if state.coordination_lock and state.last_strategic_action:
                            lock_age = (current_time - state.last_strategic_action).total_seconds()
                            if lock_age > self.coordination_lock_timeout:
                                self.logger.warning(f"Releasing expired coordination lock for {process_id}")
                                self._release_coordination_lock(process_id)
                
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Error in recovery integration monitoring: {e}")
                time.sleep(5)


# Global instance
_recovery_integration_manager = None


def get_recovery_integration_manager(redis_manager=None):
    """Get the global recovery integration manager instance"""
    global _recovery_integration_manager
    if _recovery_integration_manager is None:
        _recovery_integration_manager = ProcessRecoveryIntegrationManager(redis_manager)
    return _recovery_integration_manager


def request_recovery_authority(process_id: str, requesting_manager: str, action_type: str) -> bool:
    """Request recovery authority through the integration manager"""
    manager = get_recovery_integration_manager()
    return manager.request_recovery_authority(process_id, requesting_manager, action_type)


def release_recovery_authority(process_id: str, releasing_manager: str):
    """Release recovery authority through the integration manager"""
    manager = get_recovery_integration_manager()
    manager.release_recovery_authority(process_id, releasing_manager)


def report_recovery_action(process_id: str, manager: str, action: str, success: bool, details=None):
    """Report recovery action through the integration manager"""
    manager = get_recovery_integration_manager()
    manager.report_recovery_action(process_id, manager, action, success, details)