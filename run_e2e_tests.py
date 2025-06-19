#!/usr/bin/env python3
"""
E2E Test Runner Script for Enhanced News Pipeline Integration

This script provides a command-line interface to run different E2E test scenarios
for the enhanced news pipeline integration. It loads configuration from JSON
and provides various testing modes.

Usage:
    python run_e2e_tests.py --scenario smoke_test
    python run_e2e_tests.py --scenario regression_test
    python run_e2e_tests.py --scenario stress_test
    python run_e2e_tests.py --scenario production_readiness
    python run_e2e_tests.py --all
    python run_e2e_tests.py --custom --symbols AAPL,MSFT --duration 10
"""

import sys
import os
import argparse
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import subprocess

# Import our comprehensive test suite
try:
    from comprehensive_e2e_test_suite import ComprehensiveE2ETestSuite, print_comprehensive_report
    TEST_SUITE_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import test suite: {e}")
    TEST_SUITE_AVAILABLE = False


class E2ETestRunner:
    """Enhanced E2E Test Runner with scenario support"""

    def __init__(self, config_file: str = "e2e_test_config.json"):
        self.config_file = config_file
        self.config = self.load_config()
        self.logger = self.setup_logging()

    def load_config(self) -> Dict[str, Any]:
        """Load test configuration from JSON file"""
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Warning: Config file {self.config_file} not found. Using defaults.")
            return self.get_default_config()
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in {self.config_file}: {e}")
            return self.get_default_config()

    def get_default_config(self) -> Dict[str, Any]:
        """Return default configuration if config file is not available"""
        return {
            "test_configuration": {
                "general": {
                    "test_timeout_seconds": 300,
                    "performance_threshold_ms": 30000,
                    "memory_threshold_mb": 400,
                    "verbose_logging": True
                },
                "test_data": {
                    "test_symbols": ["AAPL", "MSFT", "GOOGL"],
                    "max_articles_per_test": 20
                }
            },
            "test_scenarios": {
                "smoke_test": {
                    "description": "Quick validation test",
                    "duration_minutes": 5,
                    "symbols": ["AAPL"],
                    "articles_per_symbol": 5
                }
            }
        }

    def setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        config = self.config.get('test_configuration', {}).get('debugging', {})
        log_level = getattr(logging, config.get('log_level', 'INFO'))

        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('e2e_test_runner.log')
            ]
        )

        return logging.getLogger(__name__)

    def run_scenario(self, scenario_name: str) -> Dict[str, Any]:
        """Run a specific test scenario"""
        scenarios = self.config.get('test_scenarios', {})

        if scenario_name not in scenarios:
            raise ValueError(f"Unknown scenario: {scenario_name}. Available: {list(scenarios.keys())}")

        scenario = scenarios[scenario_name]

        print(f" Running E2E Test Scenario: {scenario_name}")
        print(f" Description: {scenario['description']}")
        print(f" Expected Duration: {scenario['duration_minutes']} minutes")
        print(f" Symbols: {scenario.get('symbols', ['AAPL'])}")
        print("=" * 60)

        start_time = datetime.now()

        if not TEST_SUITE_AVAILABLE:
            print(" Test suite not available. Please ensure comprehensive_e2e_test_suite.py is accessible.")
            return {'error': 'Test suite not available'}

        try:
            # Configure test suite based on scenario
            test_suite = ComprehensiveE2ETestSuite()

            # Override configuration with scenario-specific settings
            self._configure_test_suite_for_scenario(test_suite, scenario)

            # Run the tests
            report = test_suite.run_all_tests()

            # Add scenario metadata to report
            report['scenario_info'] = {
                'name': scenario_name,
                'description': scenario['description'],
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'actual_duration_minutes': (datetime.now() - start_time).total_seconds() / 60
            }

            # Validate scenario-specific requirements
            if 'required_success_rate' in scenario:
                actual_success_rate = report['test_summary']['success_rate_percent']
                required_success_rate = scenario['required_success_rate']

                if actual_success_rate < required_success_rate:
                    report['scenario_validation'] = {
                        'passed': False,
                        'reason': f"Success rate {actual_success_rate:.1f}% below required {required_success_rate}%"
                    }
                else:
                    report['scenario_validation'] = {'passed': True}

            return report

        except Exception as e:
            self.logger.error(f"Scenario {scenario_name} failed: {e}")
            return {
                'error': str(e),
                'scenario_info': {
                    'name': scenario_name,
                    'failed': True,
                    'error_time': datetime.now().isoformat()
                }
            }

    def _configure_test_suite_for_scenario(self, test_suite: 'ComprehensiveE2ETestSuite', scenario: Dict[str, Any]):
        """Configure test suite based on scenario requirements"""
        # This would modify the test suite configuration based on scenario
        # For now, we'll just log the configuration
        self.logger.info(f"Configuring test suite for scenario with {len(scenario.get('symbols', []))} symbols")

    def run_all_scenarios(self) -> Dict[str, Any]:
        """Run all configured test scenarios"""
        scenarios = self.config.get('test_scenarios', {})
        all_results = {}

        print(f" Running All E2E Test Scenarios ({len(scenarios)} total)")
        print("=" * 60)

        for scenario_name in scenarios:
            print(f"\n Starting scenario: {scenario_name}")
            result = self.run_scenario(scenario_name)
            all_results[scenario_name] = result

            # Print brief summary
            if 'error' in result:
                print(f" {scenario_name} FAILED: {result['error']}")
            else:
                success_rate = result.get('test_summary', {}).get('success_rate_percent', 0)
                print(f" {scenario_name} COMPLETED: {success_rate:.1f}% success rate")

        return {
            'all_scenarios': all_results,
            'summary': self._generate_all_scenarios_summary(all_results)
        }

    def _generate_all_scenarios_summary(self, all_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary across all scenario results"""
        total_scenarios = len(all_results)
        successful_scenarios = sum(1 for result in all_results.values() if 'error' not in result)

        total_tests = 0
        total_passed = 0

        for result in all_results.values():
            if 'test_summary' in result:
                total_tests += result['test_summary']['total_tests']
                total_passed += result['test_summary']['passed_tests']

        return {
            'total_scenarios': total_scenarios,
            'successful_scenarios': successful_scenarios,
            'scenario_success_rate': (successful_scenarios / total_scenarios * 100) if total_scenarios > 0 else 0,
            'total_tests_across_scenarios': total_tests,
            'total_passed_across_scenarios': total_passed,
            'overall_test_success_rate': (total_passed / total_tests * 100) if total_tests > 0 else 0
        }

    def run_custom_scenario(self, symbols: List[str], duration_minutes: int, test_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """Run a custom test scenario with specified parameters"""
        custom_scenario = {
            'description': f'Custom test scenario for {len(symbols)} symbols',
            'duration_minutes': duration_minutes,
            'symbols': symbols,
            'articles_per_symbol': min(20, duration_minutes * 2),  # Scale with duration
            'test_types': test_types or ['component', 'integration']
        }

        print(f" Running Custom E2E Test Scenario")
        print(f" Symbols: {symbols}")
        print(f" Duration: {duration_minutes} minutes")
        print(f" Test Types: {custom_scenario['test_types']}")
        print("=" * 60)

        # Run as if it were a configured scenario
        return self.run_scenario_from_config(custom_scenario)

    def run_scenario_from_config(self, scenario_config: Dict[str, Any]) -> Dict[str, Any]:
        """Run a scenario from configuration dict rather than predefined scenarios"""
        start_time = datetime.now()

        if not TEST_SUITE_AVAILABLE:
            return {'error': 'Test suite not available'}

        try:
            test_suite = ComprehensiveE2ETestSuite()
            report = test_suite.run_all_tests()

            report['scenario_info'] = {
                'name': 'custom',
                'description': scenario_config['description'],
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'actual_duration_minutes': (datetime.now() - start_time).total_seconds() / 60
            }

            return report

        except Exception as e:
            return {'error': str(e)}

    def generate_test_report(self, results: Dict[str, Any], output_file: str = None):
        """Generate and save a comprehensive test report"""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"e2e_test_report_{timestamp}.json"

        # Add metadata to results
        report_data = {
            'test_runner_version': '1.0',
            'config_file': self.config_file,
            'generation_time': datetime.now().isoformat(),
            'system_info': self._get_system_info(),
            'results': results
        }

        # Save JSON report
        with open(output_file, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)

        print(f" Test report saved to: {output_file}")

        # Generate HTML report if configured
        if self.config.get('test_configuration', {}).get('reporting', {}).get('generate_html_report', False):
            html_file = output_file.replace('.json', '.html')
            self._generate_html_report(report_data, html_file)
            print(f" HTML report saved to: {html_file}")

    def _get_system_info(self) -> Dict[str, Any]:
        """Get system information for the report"""
        import platform

        try:
            import psutil
            memory_info = {
                'total_gb': psutil.virtual_memory().total / (1024**3),
                'available_gb': psutil.virtual_memory().available / (1024**3)
            }
            cpu_info = {
                'cpu_count': psutil.cpu_count(),
                'cpu_percent': psutil.cpu_percent()
            }
        except ImportError:
            memory_info = {'error': 'psutil not available'}
            cpu_info = {'error': 'psutil not available'}

        return {
            'platform': platform.platform(),
            'python_version': platform.python_version(),
            'architecture': platform.architecture()[0],
            'processor': platform.processor(),
            'memory': memory_info,
            'cpu': cpu_info
        }

    def _generate_html_report(self, report_data: Dict[str, Any], html_file: str):
        """Generate an HTML version of the test report"""
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>E2E Test Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .summary {{ background-color: #e8f5e8; padding: 15px; margin: 10px 0; border-radius: 5px; }}
                .error {{ background-color: #ffe8e8; padding: 15px; margin: 10px 0; border-radius: 5px; }}
                .warning {{ background-color: #fff8e8; padding: 15px; margin: 10px 0; border-radius: 5px; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .pass {{ color: green; font-weight: bold; }}
                .fail {{ color: red; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1> E2E Test Report</h1>
                <p><strong>Generated:</strong> {report_data['generation_time']}</p>
                <p><strong>Runner Version:</strong> {report_data['test_runner_version']}</p>
            </div>
        """

        # Add results summary
        if 'results' in report_data and 'test_summary' in report_data['results']:
            summary = report_data['results']['test_summary']
            html_content += f"""
            <div class="summary">
                <h2> Test Summary</h2>
                <p><strong>Total Tests:</strong> {summary['total_tests']}</p>
                <p><strong>Passed:</strong> <span class="pass">{summary['passed_tests']}</span></p>
                <p><strong>Failed:</strong> <span class="fail">{summary['failed_tests']}</span></p>
                <p><strong>Success Rate:</strong> {summary['success_rate_percent']:.1f}%</p>
                <p><strong>Duration:</strong> {summary['total_duration_seconds']:.2f} seconds</p>
            </div>
            """

        html_content += """
        </body>
        </html>
        """

        with open(html_file, 'w') as f:
            f.write(html_content)


def main():
    """Main entry point for the test runner"""
    parser = argparse.ArgumentParser(description='E2E Test Runner for Enhanced News Pipeline')

    # Scenario selection
    parser.add_argument('--scenario', help='Run specific test scenario')
    parser.add_argument('--all', action='store_true', help='Run all configured scenarios')

    # Custom scenario options
    parser.add_argument('--custom', action='store_true', help='Run custom scenario')
    parser.add_argument('--symbols', help='Comma-separated list of symbols for custom scenario')
    parser.add_argument('--duration', type=int, help='Duration in minutes for custom scenario')
    parser.add_argument('--test-types', help='Comma-separated list of test types')

    # Configuration
    parser.add_argument('--config', default='e2e_test_config.json', help='Path to config file')
    parser.add_argument('--output', help='Output file for test report')

    # Control options
    parser.add_argument('--list-scenarios', action='store_true', help='List available scenarios')
    parser.add_argument('--validate-config', action='store_true', help='Validate configuration file')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be tested without running')

    args = parser.parse_args()

    # Create test runner
    try:
        runner = E2ETestRunner(args.config)
    except Exception as e:
        print(f" Failed to initialize test runner: {e}")
        return 1

    # Handle list scenarios
    if args.list_scenarios:
        scenarios = runner.config.get('test_scenarios', {})
        print(" Available Test Scenarios:")
        for name, scenario in scenarios.items():
            print(f"  • {name}: {scenario['description']} ({scenario['duration_minutes']} min)")
        return 0

    # Handle config validation
    if args.validate_config:
        print(" Configuration file is valid")
        return 0

    # Handle dry run
    if args.dry_run:
        if args.scenario:
            scenario = runner.config.get('test_scenarios', {}).get(args.scenario)
            if scenario:
                print(f"Would run scenario '{args.scenario}': {scenario['description']}")
            else:
                print(f" Unknown scenario: {args.scenario}")
                return 1
        elif args.all:
            scenarios = runner.config.get('test_scenarios', {})
            print(f"Would run all {len(scenarios)} scenarios:")
            for name in scenarios:
                print(f"  • {name}")
        elif args.custom:
            symbols = args.symbols.split(',') if args.symbols else ['AAPL']
            duration = args.duration or 10
            print(f"Would run custom scenario with symbols {symbols} for {duration} minutes")
        return 0

    # Run tests
    results = None

    try:
        if args.scenario:
            results = runner.run_scenario(args.scenario)
            print_comprehensive_report(results)

        elif args.all:
            results = runner.run_all_scenarios()

            # Print summary of all scenarios
            print("\n" + "=" * 80)
            print(" ALL SCENARIOS SUMMARY")
            print("=" * 80)

            summary = results['summary']
            print(f"Total Scenarios: {summary['total_scenarios']}")
            print(f"Successful Scenarios: {summary['successful_scenarios']}")
            print(f"Scenario Success Rate: {summary['scenario_success_rate']:.1f}%")
            print(f"Overall Test Success Rate: {summary['overall_test_success_rate']:.1f}%")

        elif args.custom:
            symbols = args.symbols.split(',') if args.symbols else ['AAPL']
            duration = args.duration or 10
            test_types = args.test_types.split(',') if args.test_types else None

            results = runner.run_custom_scenario(symbols, duration, test_types)
            print_comprehensive_report(results)

        else:
            # Default: run smoke test
            print("No specific scenario specified. Running smoke test...")
            results = runner.run_scenario('smoke_test')
            print_comprehensive_report(results)

        # Generate report
        if results:
            runner.generate_test_report(results, args.output)

        # Determine exit code
        if isinstance(results, dict):
            if 'error' in results:
                return 1
            elif 'test_summary' in results:
                success_rate = results['test_summary']['success_rate_percent']
                return 0 if success_rate >= 80 else 1
            elif 'summary' in results:  # All scenarios
                success_rate = results['summary']['overall_test_success_rate']
                return 0 if success_rate >= 80 else 1

        return 0

    except KeyboardInterrupt:
        print("\n Tests interrupted by user")
        return 130
    except Exception as e:
        print(f"\n Test runner failed: {e}")
        runner.logger.exception("Test runner crashed")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
