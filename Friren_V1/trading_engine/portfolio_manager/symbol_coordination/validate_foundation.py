"""
Foundation Validation Script

Step-by-step validation of symbol coordination classes to ensure
they work correctly before proceeding with orchestrator integration.
"""

def validate_step_1():
    """Step 1: Test basic imports"""
    print("Step 1: Testing basic imports...")
    try:
        from symbol_config import MonitoringIntensity, SymbolHealth
        print("  MonitoringIntensity and SymbolHealth imported successfully")

        from symbol_config import SymbolMonitoringConfig, SymbolResourceBudget, SymbolState
        print("  SymbolMonitoringConfig, SymbolResourceBudget, SymbolState imported successfully")

        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        return False

def validate_step_2():
    """Step 2: Test configuration creation"""
    print("Step 2: Testing configuration creation...")
    try:
        from symbol_config import SymbolMonitoringConfig, MonitoringIntensity

        config = SymbolMonitoringConfig(
            symbol="AAPL",
            monitoring_intensity=MonitoringIntensity.ACTIVE
        )
        print(f"  Created config for {config.symbol}")
        print(f"  Intensity: {config.monitoring_intensity.value}")
        print(f"  Update frequency: {config.update_frequency} seconds")
        print(f"  API budget: {config.api_call_budget} calls/hour")

        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def validate_step_3():
    """Step 3: Test resource budget"""
    print("Step 3: Testing resource budget...")
    try:
        from symbol_config import SymbolResourceBudget

        budget = SymbolResourceBudget(
            api_calls_per_hour=100,
            cpu_allocation_pct=20.0,
            memory_allocation_mb=200.0
        )

        print(f"  Created budget: {budget.api_calls_per_hour} API calls/hour")
        print(f"  CPU allocation: {budget.cpu_allocation_pct}%")
        print(f"  Memory allocation: {budget.memory_allocation_mb}MB")

        # Test API tracking
        print(f"  Initial remaining calls: {budget.get_remaining_api_calls()}")
        budget.record_api_call()
        print(f"  After 1 call: {budget.get_remaining_api_calls()}")

        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def validate_step_4():
    """Step 4: Test symbol state"""
    print("Step 4: Testing symbol state...")
    try:
        from symbol_config import SymbolMonitoringConfig, SymbolState

        config = SymbolMonitoringConfig(symbol="TEST")
        state = SymbolState(symbol="TEST", config=config)

        print(f"  Created state for {state.symbol}")
        print(f"  Initial health: {state.health_status.value}")
        print(f"  Total cycles: {state.total_cycles}")

        # Test recording results
        state.record_successful_cycle(1.5)
        print(f"  After successful cycle: {state.successful_cycles} successful")

        state.record_error("Test error")
        print(f"  After error: {state.error_count} errors")

        success_rate = state.get_success_rate()
        print(f"  Success rate: {success_rate}%")

        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def validate_step_5():
    """Step 5: Test coordinator creation only"""
    print("Step 5: Testing coordinator creation...")
    try:
        # Import without relative import
        import sys
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)

        # Try to import the coordinator class manually
        exec(open('symbol_coordinator.py').read(), globals())
        print("  symbol_coordinator.py loaded successfully")

        # Test basic creation
        coordinator = SymbolCoordinator(total_api_budget=400)
        print(f"  Created coordinator with budget {coordinator.total_api_budget}")
        print(f"  Max intensive symbols: {coordinator.max_intensive_symbols}")

        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run validation steps"""
    print("Symbol Coordination Foundation Validation")
    print("=" * 50)

    steps = [
        validate_step_1,
        validate_step_2,
        validate_step_3,
        validate_step_4,
        validate_step_5
    ]

    all_passed = True
    for i, step in enumerate(steps, 1):
        try:
            result = step()
            if result:
                print(f"  Step {i}: PASSED")
            else:
                print(f"  Step {i}: FAILED")
                all_passed = False
        except Exception as e:
            print(f"  Step {i}: CRITICAL ERROR - {e}")
            all_passed = False
        print()

    print("=" * 50)
    if all_passed:
        print("FOUNDATION VALIDATION SUCCESSFUL")
        print("All core classes work correctly")
        print("Ready to proceed with orchestrator integration")
    else:
        print("FOUNDATION VALIDATION FAILED")
        print("Fix issues before proceeding")

    return all_passed

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
