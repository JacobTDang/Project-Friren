"""
Main Terminal Bridge - Routes subprocess output to main terminal
"""

import logging
import sys
from typing import Optional

class MainTerminalBridge:
    """Bridge for routing subprocess output to main terminal"""
    
    def __init__(self):
        self.logger = logging.getLogger("main_terminal_bridge")
        
    def route_subprocess_output(self, process_name: str, output: str):
        """Route subprocess output to main terminal"""
        if output and output.strip():
            print(f"[{process_name}] {output.strip()}")
            
    def start_output_routing(self):
        """Start output routing system"""
        self.logger.info("Terminal bridge started - routing subprocess output to main terminal")
        
    def stop_output_routing(self):
        """Stop output routing system"""
        self.logger.info("Terminal bridge stopped")
        
    def start_monitoring(self):
        """Start monitoring and output routing"""
        self.start_output_routing()
        self.logger.info("Terminal bridge monitoring started")


def send_colored_business_output(process_id, message, output_type):
    """Send colored business output directly to main terminal"""
    
    # Color codes for different output types
    colors = {
        "news": "\033[96m",      # Cyan
        "finbert": "\033[93m",   # Yellow
        "xgboost": "\033[92m",   # Green
        "recommendation": "\033[95m", # Magenta
        "decision": "\033[94m",   # Blue
        "position": "\033[91m",   # Red
        "execution": "\033[95m",  # Magenta
        "risk": "\033[93m",      # Yellow
        "strategy": "\033[92m",   # Green
    }
    
    reset = "\033[0m"
    color = colors.get(output_type.lower(), "\033[94m")  # Default to blue
    
    # Print the colored business output directly to main terminal
    print(f"{color}[{process_id.upper()}] {message}{reset}")
    
    # Also log it
    logging.getLogger("main_terminal_bridge").info(f"[{process_id.upper()}] {message}")


# Global instance
_terminal_bridge = MainTerminalBridge()

def get_terminal_bridge():
    """Get the global terminal bridge instance"""
    return _terminal_bridge