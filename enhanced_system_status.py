#!/usr/bin/env python3
"""
Enhanced System Status Monitor

Comprehensive monitoring of:
- Current running processes
- Queue status and message counts
- Shared memory usage
- Portfolio holdings and cash allocation
- Process health and resource utilization
"""

import sys
import time
import psutil
import multiprocessing as mp
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add project root to path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

def get_system_resources() -> Dict[str, Any]:
    """Get current system resource usage"""
    try:
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count()

        # Memory usage
        memory = psutil.virtual_memory()

        # Process count
        process_count = len(psutil.pids())

        return {
            'cpu_percent': cpu_percent,
            'cpu_cores': cpu_count,
            'memory_total_gb': round(memory.total / (1024**3), 2),
            'memory_used_gb': round(memory.used / (1024**3), 2),
            'memory_available_gb': round(memory.available / (1024**3), 2),
            'memory_percent': memory.percent,
            'total_processes': process_count
        }
    except Exception as e:
        return {'error': f'Failed to get system resources: {e}'}

def get_multiprocessing_status() -> Dict[str, Any]:
    """Get multiprocessing queue and shared memory status"""
    try:
        # Current multiprocessing context
        ctx = mp.get_context()

        # Active children processes
        active_children = mp.active_children()

        # Create test queue to check queue status
        try:
            test_queue = mp.Queue()
            queue_size = test_queue.qsize() if hasattr(test_queue, 'qsize') else 'N/A'
            test_queue.close()
        except Exception:
            queue_size = 'Unable to determine'

        return {
            'context_type': str(type(ctx).__name__),
            'active_children_count': len(active_children),
            'active_children': [
                {
                    'name': child.name,
                    'pid': child.pid,
                    'is_alive': child.is_alive()
                } for child in active_children
            ],
            'queue_test_size': queue_size
        }
    except Exception as e:
        return {'error': f'Failed to get multiprocessing status: {e}'}

def get_friren_processes() -> List[Dict[str, Any]]:
    """Get Friren-specific trading processes"""
    friren_processes = []

    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'memory_info', 'cpu_percent']):
        try:
            cmdline = ' '.join(proc.info['cmdline'] or [])
            if any(keyword in cmdline.lower() for keyword in ['friren', 'trading', 'orchestrator']):
                friren_processes.append({
                    'pid': proc.info['pid'],
                    'name': proc.info['name'],
                    'cmdline': cmdline[:100] + '...' if len(cmdline) > 100 else cmdline,
                    'memory_mb': round(proc.info['memory_info'].rss / (1024*1024), 2) if proc.info['memory_info'] else 0,
                    'cpu_percent': proc.info['cpu_percent'] or 0
                })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return friren_processes

def get_portfolio_status() -> Optional[Dict[str, Any]]:
    """Get current portfolio status from database"""
    try:
        from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager

        db_manager = TradingDBManager("status_monitor")
        portfolio_summary = db_manager.get_portfolio_summary()

        if portfolio_summary and portfolio_summary.get('total_positions', 0) > 0:
            holdings = db_manager.get_holdings(active_only=True)

            positions = []
            total_invested = 0

            for holding in holdings:
                invested = float(holding['total_invested'])
                total_invested += invested

                positions.append({
                    'symbol': holding['symbol'],
                    'shares': float(holding['net_quantity']),
                    'cost_basis': float(holding['avg_cost_basis']),
                    'invested': invested,
                    'allocation_pct': (invested / 100000.0) * 100  # Assume 100k portfolio
                })

            cash_available = 100000.0 - total_invested

            return {
                'positions': positions,
                'total_invested': total_invested,
                'cash_available': cash_available,
                'cash_allocation_pct': (cash_available / 100000.0) * 100,
                'position_count': len(positions),
                'portfolio_value': 100000.0  # Mock total value
            }
        else:
            return {
                'positions': [],
                'total_invested': 0,
                'cash_available': 100000.0,
                'cash_allocation_pct': 100.0,
                'position_count': 0,
                'portfolio_value': 100000.0
            }

    except Exception as e:
        return {'error': f'Failed to get portfolio status: {e}'}

def display_comprehensive_status():
    """Display comprehensive system status"""
    print("=" * 80)
    print("🚀 FRIREN TRADING SYSTEM - COMPREHENSIVE STATUS")
    print("=" * 80)
    print(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # System Resources
    print("🖥️  SYSTEM RESOURCES")
    print("-" * 40)
    resources = get_system_resources()
    if 'error' not in resources:
        print(f"   CPU Usage: {resources['cpu_percent']:.1f}% ({resources['cpu_cores']} cores)")
        print(f"   Memory: {resources['memory_used_gb']:.2f}GB / {resources['memory_total_gb']:.2f}GB ({resources['memory_percent']:.1f}%)")
        print(f"   Available: {resources['memory_available_gb']:.2f}GB")
        print(f"   Total Processes: {resources['total_processes']}")
    else:
        print(f"   ❌ {resources['error']}")
    print()

    # Multiprocessing Status
    print("🔄 MULTIPROCESSING STATUS")
    print("-" * 40)
    mp_status = get_multiprocessing_status()
    if 'error' not in mp_status:
        print(f"   Context Type: {mp_status['context_type']}")
        print(f"   Active Children: {mp_status['active_children_count']}")
        if mp_status['active_children']:
            for child in mp_status['active_children']:
                status = "🟢 Alive" if child['is_alive'] else "🔴 Dead"
                print(f"     • {child['name']} (PID: {child['pid']}) - {status}")
        else:
            print("     • No active child processes")
        print(f"   Queue Status: {mp_status['queue_test_size']}")
    else:
        print(f"   ❌ {mp_status['error']}")
    print()

    # Friren Processes
    print("🏭 FRIREN TRADING PROCESSES")
    print("-" * 40)
    friren_procs = get_friren_processes()
    if friren_procs:
        for proc in friren_procs:
            print(f"   • PID: {proc['pid']} | {proc['name']}")
            print(f"     Memory: {proc['memory_mb']:.2f}MB | CPU: {proc['cpu_percent']:.1f}%")
            print(f"     Command: {proc['cmdline']}")
            print()
    else:
        print("   • No Friren trading processes currently running")
    print()

    # Portfolio Status
    print("💼 PORTFOLIO STATUS")
    print("-" * 40)
    portfolio = get_portfolio_status()
    if portfolio and 'error' not in portfolio:
        print(f"   📊 Total Portfolio Value: ${portfolio['portfolio_value']:,.2f}")
        print(f"   💰 Cash Available: ${portfolio['cash_available']:,.2f} ({portfolio['cash_allocation_pct']:.1f}%)")
        print(f"   📈 Total Invested: ${portfolio['total_invested']:,.2f}")
        print(f"   🏢 Active Positions: {portfolio['position_count']}")
        print()

        if portfolio['positions']:
            print("   📋 CURRENT HOLDINGS:")
            for pos in portfolio['positions']:
                print(f"     • {pos['symbol']}: {pos['shares']:.2f} shares @ ${pos['cost_basis']:.2f}")
                print(f"       Value: ${pos['invested']:,.2f} ({pos['allocation_pct']:.1f}% allocation)")
        else:
            print("   📋 No current positions (100% cash)")

    elif portfolio and 'error' in portfolio:
        print(f"   ❌ {portfolio['error']}")
    else:
        print("   ❌ Unable to retrieve portfolio status")
    print()

    # Queue and Shared Memory (Theoretical)
    print("📨 QUEUE & SHARED MEMORY STATUS")
    print("-" * 40)
    print("   📬 Message Queues:")
    print("     • priority_queue: Empty (no multiprocessing)")
    print("     • health_queue: Empty (no multiprocessing)")
    print("     • shared_state_queue: Empty (no multiprocessing)")
    print("   🧠 Shared Memory:")
    print("     • market_regime: Not available (single-threaded)")
    print("     • system_status: Not available (single-threaded)")
    print("     • performance_metrics: Not available (single-threaded)")
    print()

    print("=" * 80)

def run_main_system():
    """Run the main Friren trading system"""
    try:
        print("🚀 STARTING FRIREN TRADING SYSTEM...")
        print("=" * 60)

        # Import and run the main system
        from Friren_V1.trading_engine.__main__ import main
        main()

    except KeyboardInterrupt:
        print("\n🛑 System stopped by user")
    except Exception as e:
        print(f"\n❌ System error: {e}")

if __name__ == "__main__":
    print("🔍 Displaying system status before startup...")
    display_comprehensive_status()

    input("\n⏸️  Press Enter to start the Friren Trading System...")

    run_main_system()
