"""
API Resilience Monitor - Testing and Monitoring Tool

Provides comprehensive testing and monitoring capabilities for the API resilience system.
This tool can be used to:
- Test circuit breaker functionality
- Monitor API health status
- Simulate failure scenarios
- Generate resilience reports
"""

import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
import threading
import random

# Import the resilience system
try:
    from Friren_V1.infrastructure.api_resilience import (
        APIResilienceManager, APIServiceType, CircuitState, get_resilience_manager
    )
    HAS_RESILIENCE = True
except ImportError:
    HAS_RESILIENCE = False
    print("❌ API Resilience system not available")

class ResilienceTestSuite:
    """
    Test suite for API resilience functionality
    """
    
    def __init__(self):
        self.logger = logging.getLogger("resilience_test")
        self.manager = get_resilience_manager() if HAS_RESILIENCE else None
        
        if not self.manager:
            raise RuntimeError("Resilience manager not available")
        
        self.test_results = []
        
    def run_all_tests(self):
        """Run comprehensive resilience test suite"""
        print("🔧 Starting API Resilience Test Suite...")
        
        # Test 1: Basic resilient call
        self._test_basic_resilient_call()
        
        # Test 2: Retry logic
        self._test_retry_logic()
        
        # Test 3: Circuit breaker functionality
        self._test_circuit_breaker()
        
        # Test 4: Health monitoring
        self._test_health_monitoring()
        
        # Test 5: Concurrent access
        self._test_concurrent_access()
        
        print("\n📊 Test Results Summary:")
        for result in self.test_results:
            status = "✅ PASS" if result['passed'] else "❌ FAIL"
            print(f"{status} {result['test_name']}: {result['message']}")
        
        passed = sum(1 for r in self.test_results if r['passed'])
        total = len(self.test_results)
        print(f"\nOverall: {passed}/{total} tests passed")
        
        return passed == total
    
    def _test_basic_resilient_call(self):
        """Test basic resilient call functionality"""
        print("\n🧪 Test 1: Basic resilient call")
        
        def test_function():
            return "success"
        
        try:
            result = self.manager.resilient_call(
                test_function, APIServiceType.ALPACA_TRADING, "test_operation"
            )
            
            if result == "success":
                self.test_results.append({
                    'test_name': 'Basic Resilient Call',
                    'passed': True,
                    'message': 'Successfully executed resilient call'
                })
                print("✅ Basic resilient call successful")
            else:
                self.test_results.append({
                    'test_name': 'Basic Resilient Call',
                    'passed': False,
                    'message': f'Unexpected result: {result}'
                })
                print(f"❌ Unexpected result: {result}")
                
        except Exception as e:
            self.test_results.append({
                'test_name': 'Basic Resilient Call',
                'passed': False,
                'message': f'Exception: {e}'
            })
            print(f"❌ Exception: {e}")
    
    def _test_retry_logic(self):
        """Test retry logic with temporary failures"""
        print("\n🧪 Test 2: Retry logic")
        
        attempt_count = 0
        
        def failing_function():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise Exception(f"Temporary failure {attempt_count}")
            return "success_after_retries"
        
        try:
            start_time = time.time()
            result = self.manager.resilient_call(
                failing_function, APIServiceType.YAHOO_FINANCE, "test_retry"
            )
            elapsed = time.time() - start_time
            
            if result == "success_after_retries" and attempt_count == 3:
                self.test_results.append({
                    'test_name': 'Retry Logic',
                    'passed': True,
                    'message': f'Succeeded after {attempt_count} attempts in {elapsed:.2f}s'
                })
                print(f"✅ Retry logic successful: {attempt_count} attempts, {elapsed:.2f}s")
            else:
                self.test_results.append({
                    'test_name': 'Retry Logic',
                    'passed': False,
                    'message': f'Unexpected behavior: result={result}, attempts={attempt_count}'
                })
                print(f"❌ Unexpected behavior: result={result}, attempts={attempt_count}")
                
        except Exception as e:
            self.test_results.append({
                'test_name': 'Retry Logic',
                'passed': False,
                'message': f'Exception: {e}'
            })
            print(f"❌ Exception: {e}")
    
    def _test_circuit_breaker(self):
        """Test circuit breaker functionality"""
        print("\n🧪 Test 3: Circuit breaker")
        
        def always_failing_function():
            raise Exception("Persistent failure")
        
        try:
            service = APIServiceType.NEWS_API
            circuit_breaker = self.manager.circuit_breakers[service]
            
            # Reset circuit breaker
            circuit_breaker.force_close()
            
            # Cause failures to open circuit
            failure_count = 0
            for i in range(10):
                try:
                    self.manager.resilient_call(
                        always_failing_function, service, f"test_failure_{i}"
                    )
                except:
                    failure_count += 1
                    
                # Check if circuit opened
                if circuit_breaker.get_state() == CircuitState.OPEN:
                    break
            
            if circuit_breaker.get_state() == CircuitState.OPEN:
                self.test_results.append({
                    'test_name': 'Circuit Breaker',
                    'passed': True,
                    'message': f'Circuit opened after {failure_count} failures'
                })
                print(f"✅ Circuit breaker opened after {failure_count} failures")
            else:
                self.test_results.append({
                    'test_name': 'Circuit Breaker',
                    'passed': False,
                    'message': f'Circuit did not open (state: {circuit_breaker.get_state()})'
                })
                print(f"❌ Circuit did not open (state: {circuit_breaker.get_state()})")
                
        except Exception as e:
            self.test_results.append({
                'test_name': 'Circuit Breaker',
                'passed': False,
                'message': f'Exception: {e}'
            })
            print(f"❌ Exception: {e}")
    
    def _test_health_monitoring(self):
        """Test health monitoring functionality"""
        print("\n🧪 Test 4: Health monitoring")
        
        try:
            # Make some successful calls
            for i in range(5):
                self.manager.resilient_call(
                    lambda: "success", APIServiceType.REDIS, f"health_test_{i}"
                )
            
            # Get health status
            health = self.manager.get_service_health(APIServiceType.REDIS)
            
            if health.success_rate > 0.8 and health.is_healthy:
                self.test_results.append({
                    'test_name': 'Health Monitoring',
                    'passed': True,
                    'message': f'Health monitoring working (success rate: {health.success_rate:.2f})'
                })
                print(f"✅ Health monitoring working (success rate: {health.success_rate:.2f})")
            else:
                self.test_results.append({
                    'test_name': 'Health Monitoring',
                    'passed': False,
                    'message': f'Unexpected health status: rate={health.success_rate:.2f}, healthy={health.is_healthy}'
                })
                print(f"❌ Unexpected health status: rate={health.success_rate:.2f}, healthy={health.is_healthy}")
                
        except Exception as e:
            self.test_results.append({
                'test_name': 'Health Monitoring',
                'passed': False,
                'message': f'Exception: {e}'
            })
            print(f"❌ Exception: {e}")
    
    def _test_concurrent_access(self):
        """Test concurrent access to resilience system"""
        print("\n🧪 Test 5: Concurrent access")
        
        results = []
        threads = []
        
        def worker(worker_id):
            try:
                for i in range(10):
                    result = self.manager.resilient_call(
                        lambda: f"worker_{worker_id}_call_{i}", 
                        APIServiceType.DATABASE, 
                        f"concurrent_test_{worker_id}_{i}"
                    )
                    results.append(result)
                    time.sleep(0.01)  # Small delay
            except Exception as e:
                results.append(f"error_{worker_id}: {e}")
        
        try:
            # Start multiple worker threads
            for i in range(5):
                thread = threading.Thread(target=worker, args=(i,))
                threads.append(thread)
                thread.start()
            
            # Wait for all threads
            for thread in threads:
                thread.join()
            
            success_count = sum(1 for r in results if r.startswith("worker_"))
            error_count = sum(1 for r in results if r.startswith("error_"))
            
            if error_count == 0 and success_count == 50:  # 5 workers * 10 calls each
                self.test_results.append({
                    'test_name': 'Concurrent Access',
                    'passed': True,
                    'message': f'All {success_count} concurrent calls successful'
                })
                print(f"✅ Concurrent access successful: {success_count} calls")
            else:
                self.test_results.append({
                    'test_name': 'Concurrent Access',
                    'passed': False,
                    'message': f'Failures in concurrent access: {error_count} errors, {success_count} successes'
                })
                print(f"❌ Concurrent access issues: {error_count} errors, {success_count} successes")
                
        except Exception as e:
            self.test_results.append({
                'test_name': 'Concurrent Access',
                'passed': False,
                'message': f'Exception: {e}'
            })
            print(f"❌ Exception: {e}")


class ResilienceMonitor:
    """
    Monitor for API resilience system status
    """
    
    def __init__(self):
        self.logger = logging.getLogger("resilience_monitor")
        self.manager = get_resilience_manager() if HAS_RESILIENCE else None
        
        if not self.manager:
            raise RuntimeError("Resilience manager not available")
    
    def print_health_status(self):
        """Print current health status of all services"""
        print("\n🏥 API Health Status Report")
        print("=" * 60)
        
        health_summary = self.manager.get_health_summary()
        
        print(f"Overall Health Score: {health_summary['overall_health_score']:.2f}")
        print(f"Overall Success Rate: {health_summary['overall_success_rate']:.2f}")
        print(f"Healthy Services: {health_summary['healthy_services']}/{health_summary['total_services']}")
        print(f"Total API Calls (Last Hour): {health_summary['total_api_calls_last_hour']}")
        
        if health_summary['open_circuit_breakers']:
            print(f"⚠️ Open Circuit Breakers: {', '.join(health_summary['open_circuit_breakers'])}")
        else:
            print("✅ All Circuit Breakers Closed")
        
        print("\nService Details:")
        print("-" * 60)
        
        for service_name, details in health_summary['service_details'].items():
            status = "🟢" if details['is_healthy'] else "🔴"
            circuit_status = "🔵" if details['circuit_state'] == 'closed' else "🔴"
            
            print(f"{status} {service_name.upper():<15} | "
                  f"Success: {details['success_rate']:.2f} | "
                  f"Avg Response: {details['avg_response_time_ms']:.1f}ms | "
                  f"Circuit: {circuit_status} {details['circuit_state'].upper()} | "
                  f"Failures: {details['consecutive_failures']}")
    
    def export_health_report(self, filepath: str):
        """Export comprehensive health report to JSON"""
        health_summary = self.manager.get_health_summary()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'health_summary': health_summary,
            'detailed_health': {}
        }
        
        # Get detailed health for each service
        for service in APIServiceType:
            report['detailed_health'][service.value] = {
                'service_health': self.manager.get_service_health(service).__dict__,
                'circuit_state': self.manager.circuit_breakers[service].get_state().value
            }
        
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"📄 Health report exported to: {filepath}")
    
    def simulate_failure_scenario(self, service: APIServiceType, failure_count: int = 5):
        """Simulate failure scenario for testing"""
        print(f"\n⚠️ Simulating {failure_count} failures for {service.value}")
        
        def failing_function():
            raise Exception("Simulated failure")
        
        for i in range(failure_count):
            try:
                self.manager.resilient_call(
                    failing_function, service, f"simulated_failure_{i}"
                )
            except:
                pass
            
            circuit_state = self.manager.circuit_breakers[service].get_state()
            print(f"Failure {i+1}: Circuit state = {circuit_state.value}")
            
            if circuit_state == CircuitState.OPEN:
                print(f"🔴 Circuit breaker opened after {i+1} failures")
                break


def main():
    """Main function for resilience monitoring tool"""
    if not HAS_RESILIENCE:
        print("❌ API Resilience system not available")
        return
    
    print("🛡️ API Resilience Monitor")
    print("=" * 40)
    
    monitor = ResilienceMonitor()
    
    while True:
        print("\nSelect an option:")
        print("1. View current health status")
        print("2. Run resilience test suite")
        print("3. Export health report")
        print("4. Simulate failure scenario")
        print("5. Exit")
        
        choice = input("\nEnter choice (1-5): ").strip()
        
        if choice == '1':
            monitor.print_health_status()
        elif choice == '2':
            test_suite = ResilienceTestSuite()
            test_suite.run_all_tests()
        elif choice == '3':
            filename = f"health_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            monitor.export_health_report(filename)
        elif choice == '4':
            print("\nSelect service to simulate failures:")
            services = list(APIServiceType)
            for i, service in enumerate(services):
                print(f"{i+1}. {service.value}")
            
            try:
                service_choice = int(input("Enter service number: ")) - 1
                if 0 <= service_choice < len(services):
                    monitor.simulate_failure_scenario(services[service_choice])
                else:
                    print("Invalid service selection")
            except ValueError:
                print("Invalid input")
        elif choice == '5':
            print("👋 Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    main()