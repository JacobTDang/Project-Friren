import unittest
import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the classes we're testing
from entropy_regime_detector import EntropyRegimeDetector, EntropyConfig

class TestEntropyRegimeDetector(unittest.TestCase):
    """
    Comprehensive test suite for EntropyRegimeDetector
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        print(f"\n{'='*60}")
        print(f"Setting up test: {self._testMethodName}")
        print(f"{'='*60}")

        # Create test configuration
        self.config = EntropyConfig(
            price_entropy_window=10,  # Smaller for testing
            volume_entropy_window=8,
            volatility_entropy_window=6,
            distribution_window=25,
            confidence_threshold=0.5,
            regime_persistence=2
        )

        # Initialize detector
        self.detector = EntropyRegimeDetector(self.config)

        # Create comprehensive test data
        self.test_data = self._create_comprehensive_test_data()

        print(f" Test data created: {len(self.test_data)} rows")
        print(f" Date range: {self.test_data.index[0]} to {self.test_data.index[-1]}")

    def _create_comprehensive_test_data(self) -> pd.DataFrame:
        """
        Create realistic market data with different regime patterns
        """
        np.random.seed(42)  # For reproducible tests

        # Create 252 days of data (1 trading year)
        dates = pd.date_range('2023-01-01', periods=252, freq='D')

        # Initialize with base price
        base_price = 100.0
        prices = [base_price]
        volumes = []

        # Create different market regimes throughout the year
        for i in range(1, len(dates)):
            day_of_year = i

            # TRENDING REGIME (days 1-60): Strong uptrend with low entropy
            if day_of_year <= 60:
                trend = 0.001  # 0.1% daily trend
                volatility = 0.008  # Low volatility
                volume_base = 1000000

            # HIGH ENTROPY REGIME (days 61-120): Chaotic, high volatility
            elif day_of_year <= 120:
                trend = 0.0002
                volatility = 0.025  # High volatility
                volume_base = 1500000

            # MEAN REVERTING REGIME (days 121-180): Oscillating around mean
            elif day_of_year <= 180:
                trend = -0.0001 if i % 10 < 5 else 0.0001  # Alternating
                volatility = 0.012
                volume_base = 800000

            # STABLE REGIME (days 181-220): Low volatility, minimal movement
            elif day_of_year <= 220:
                trend = 0.0001
                volatility = 0.005  # Very low volatility
                volume_base = 600000

            # VOLATILE REGIME (days 221-252): High volatility regime change
            else:
                trend = 0.002 if np.random.random() > 0.7 else -0.002
                volatility = 0.03  # Very high volatility
                volume_base = 2000000

            # Generate price movement
            random_shock = np.random.normal(0, volatility)
            price_change = trend + random_shock
            new_price = prices[-1] * (1 + price_change)
            prices.append(new_price)

            # Generate volume with realistic patterns
            volume_noise = np.random.normal(1, 0.3)
            volume = max(int(volume_base * volume_noise), 100000)
            volumes.append(volume)

        # Add initial volume
        volumes.insert(0, 1000000)

        # Create OHLC data
        data = []
        for i, (date, close, volume) in enumerate(zip(dates, prices, volumes)):
            # Generate realistic OHLC based on close price
            daily_range = abs(np.random.normal(0, 0.015)) * close

            open_price = close + np.random.normal(0, 0.005) * close
            high = max(open_price, close) + np.random.uniform(0, daily_range)
            low = min(open_price, close) - np.random.uniform(0, daily_range)

            data.append({
                'Date': date,
                'Open': round(open_price, 2),
                'High': round(high, 2),
                'Low': round(low, 2),
                'Close': round(close, 2),
                'Volume': volume
            })

        df = pd.DataFrame(data)
        df.set_index('Date', inplace=True)

        print(f"Created test data with regimes:")
        print(f"  Days 1-60: TRENDING (low entropy)")
        print(f"  Days 61-120: HIGH_ENTROPY (chaotic)")
        print(f"  Days 121-180: MEAN_REVERTING")
        print(f"  Days 181-220: STABLE (low volatility)")
        print(f"  Days 221-252: VOLATILE")

        return df

    def test_config_initialization(self):
        """Test that configuration is properly initialized"""
        print("Testing configuration initialization...")

        # Test default config
        default_config = EntropyConfig()
        self.assertEqual(default_config.price_entropy_window, 20)
        self.assertEqual(default_config.confidence_threshold, 0.6)
        self.assertIsNotNone(default_config.lookback_periods)

        # Test custom config
        self.assertEqual(self.config.price_entropy_window, 10)
        self.assertEqual(self.config.confidence_threshold, 0.5)

        print(" Configuration initialization passed")

    def test_detector_initialization(self):
        """Test that detector initializes correctly"""
        print("Testing detector initialization...")

        self.assertIsNotNone(self.detector.config)
        self.assertIsNotNone(self.detector.SDT)
        self.assertEqual(len(self.detector.regime_history), 0)
        self.assertEqual(len(self.detector.entropy_history), 0)

        print(" Detector initialization passed")

    def test_market_data_preparation(self):
        """Test basic market data preparation"""
        print("Testing market data preparation...")

        prepared_data = self.detector._prepare_market_data(self.test_data)

        # Check that required columns are added
        required_columns = ['Returns', 'Log_Returns', 'Range', 'Body',
                           'Upper_Shadow', 'Lower_Shadow', 'Volume_Ratio', 'Price_Volume']

        for col in required_columns:
            self.assertIn(col, prepared_data.columns, f"Missing column: {col}")

        # Check data quality
        self.assertFalse(prepared_data['Returns'].isna().all(), "All returns are NaN")
        self.assertTrue((prepared_data['Range'] >= 0).all(), "Range values should be non-negative")
        self.assertTrue((prepared_data['Volume_Ratio'] > 0).any(), "Volume ratio should have positive values")

        print(f" Market data preparation passed")
        print(f"  Added {len(required_columns)} derived columns")
        print(f"  Returns range: {prepared_data['Returns'].min():.4f} to {prepared_data['Returns'].max():.4f}")

    def test_entropy_indicators(self):
        """Test entropy indicator calculations"""
        print("Testing entropy indicator calculations...")

        df_with_entropy = self.detector.add_entropy_indicators(self.test_data)

        # Check that entropy columns are created
        entropy_patterns = ['_raw', '_pct', '_ewm', '_zscore']
        entropy_base_names = ['Direction_Entropy', 'Return_Magnitude_Entropy',
                             'Volume_Entropy', 'Vol_Entropy', 'Distribution_Entropy']

        for base_name in entropy_base_names:
            for pattern in entropy_patterns:
                col_name = base_name + pattern
                if col_name in df_with_entropy.columns:
                    # Check that entropy values are reasonable
                    col_data = df_with_entropy[col_name].dropna()
                    if len(col_data) > 0:
                        self.assertTrue(col_data.min() >= 0, f"{col_name} has negative values")
                        print(f"   {col_name}: {len(col_data)} values, range [{col_data.min():.3f}, {col_data.max():.3f}]")

        # Check composite entropy
        self.assertIn('Composite_Entropy_raw', df_with_entropy.columns)
        self.assertIn('Composite_Entropy_ewm', df_with_entropy.columns)
        self.assertIn('Composite_Entropy_slope', df_with_entropy.columns)

        composite_data = df_with_entropy['Composite_Entropy_ewm'].dropna()
        print(f"   Composite entropy: {len(composite_data)} values")

        print(" Entropy indicators calculation passed")

    def test_regime_detection(self):
        """Test full regime detection process"""
        print("Testing regime detection...")

        df_with_regimes = self.detector.detect_regime_history(self.test_data)

        # Check that regime columns are created
        regime_columns = ['Entropy_Regime_raw', 'Entropy_Confidence', 'Entropy_Regime']
        for col in regime_columns:
            self.assertIn(col, df_with_regimes.columns, f"Missing regime column: {col}")

        # Check regime history
        self.assertGreater(len(self.detector.regime_history), 0, "No regime history generated")

        # Analyze detected regimes
        regimes_detected = df_with_regimes['Entropy_Regime'].dropna()
        regime_counts = regimes_detected.value_counts()

        print(f"   Regime detection completed")
        print(f"  Regimes detected: {len(regimes_detected)} periods")
        print(f"  Regime distribution:")
        for regime, count in regime_counts.items():
            pct = (count / len(regimes_detected)) * 100
            print(f"    {regime}: {count} periods ({pct:.1f}%)")

        # Check confidence scores
        confidence_data = df_with_regimes['Entropy_Confidence'].dropna()
        avg_confidence = confidence_data.mean()
        print(f"  Average confidence: {avg_confidence:.3f}")

        self.assertTrue(0 <= avg_confidence <= 1, "Confidence should be between 0 and 1")

        print(" Regime detection passed")

    def test_signal_generation(self):
        """Test trading signal generation"""
        print("Testing signal generation...")

        signals_df = self.detector.get_signals(self.test_data)

        # Check signal dataframe structure
        required_signal_columns = ['Regime', 'Regime_Confidence', 'Signal',
                                  'Signal_Strength', 'Signal_Grade']
        for col in required_signal_columns:
            self.assertIn(col, signals_df.columns, f"Missing signal column: {col}")

        # Analyze signal distribution
        signal_counts = signals_df['Signal'].value_counts()
        print(f"   Signal generation completed")
        print(f"  Signal distribution:")
        for signal, count in signal_counts.items():
            pct = (count / len(signals_df)) * 100
            print(f"    {signal}: {count} periods ({pct:.1f}%)")

        # Check signal grades
        grade_counts = signals_df['Signal_Grade'].value_counts()
        print(f"  Signal grade distribution:")
        for grade, count in grade_counts.items():
            pct = (count / len(signals_df)) * 100
            print(f"    {grade}: {count} periods ({pct:.1f}%)")

        # Verify signal strength is reasonable
        strength_data = signals_df['Signal_Strength'].dropna()
        if len(strength_data) > 0:
            print(f"  Signal strength range: {strength_data.min():.3f} to {strength_data.max():.3f}")

        print(" Signal generation passed")

    def test_transition_detection(self):
        """Test regime transition detection"""
        print("Testing transition detection...")

        transition_df = self.detector.get_regime_transition_signals(self.test_data)

        # Check transition dataframe structure
        required_transition_columns = ['Transition_Warning', 'Transition_Type',
                                     'Entropy_Slope', 'Entropy_Acceleration']
        for col in required_transition_columns:
            self.assertIn(col, transition_df.columns, f"Missing transition column: {col}")

        # Analyze transitions
        warnings_detected = transition_df['Transition_Warning'].sum()
        transition_types = transition_df['Transition_Type'].value_counts()

        print(f"   Transition detection completed")
        print(f"  Transition warnings detected: {warnings_detected}")
        print(f"  Transition types:")
        for trans_type, count in transition_types.items():
            if trans_type != 'NONE':
                print(f"    {trans_type}: {count}")

        print(" Transition detection passed")

    def test_regime_summary(self):
        """Test regime summary statistics"""
        print("Testing regime summary...")

        summary = self.detector.get_entropy_regime_summary(self.test_data)

        # Check summary structure
        required_summary_keys = ['regime_distribution', 'average_confidence',
                               'entropy_statistics', 'regime_transitions']
        for key in required_summary_keys:
            self.assertIn(key, summary, f"Missing summary key: {key}")

        print("   Regime summary generated")
        print("  Summary contents:")
        print(f"    Regime distribution: {len(summary['regime_distribution'])} regimes")
        print(f"    Entropy statistics: {len(summary['entropy_statistics'])} measures")

        if 'current_regime' in summary and summary['current_regime']:
            current = summary['current_regime']
            print(f"    Current regime: {current['regime']} (confidence: {current['confidence']:.3f})")

        print(" Regime summary passed")

    def test_entropy_calculations(self):
        """Test individual entropy calculation methods"""
        print("Testing entropy calculation methods...")

        # Test categorical entropy
        categorical_data = ['A', 'B', 'A', 'C', 'B', 'A']
        cat_entropy = self.detector._calculate_categorical_entropy(categorical_data)
        self.assertGreater(cat_entropy, 0, "Categorical entropy should be positive")
        print(f"   Categorical entropy: {cat_entropy:.3f}")

        # Test continuous entropy
        continuous_data = np.random.normal(0, 1, 100)
        cont_entropy = self.detector._calculate_continuous_entropy(continuous_data)
        self.assertGreater(cont_entropy, 0, "Continuous entropy should be positive")
        print(f"   Continuous entropy: {cont_entropy:.3f}")

        # Test distribution entropy
        return_data = np.random.normal(0.001, 0.02, 50)
        dist_entropy = self.detector._calculate_distribution_entropy(return_data)
        self.assertGreaterEqual(dist_entropy, 0, "Distribution entropy should be non-negative")
        print(f"   Distribution entropy: {dist_entropy:.3f}")

        # Test tail entropy
        tail_entropy = self.detector._calculate_tail_entropy(return_data)
        self.assertGreaterEqual(tail_entropy, 0, "Tail entropy should be non-negative")
        print(f"   Tail entropy: {tail_entropy:.3f}")

        print(" Entropy calculation methods passed")

    def test_edge_cases(self):
        """Test edge cases and error handling"""
        print("Testing edge cases...")

        # Test with minimal data
        minimal_data = self.test_data.head(10)
        try:
            minimal_regimes = self.detector.detect_regime_history(minimal_data)
            print("   Minimal data handling passed")
        except Exception as e:
            self.fail(f"Failed on minimal data: {e}")

        # Test with NaN values
        nan_data = self.test_data.copy()
        nan_data.loc[nan_data.index[10:15], 'Close'] = np.nan
        try:
            nan_regimes = self.detector.detect_regime_history(nan_data)
            print("   NaN data handling passed")
        except Exception as e:
            self.fail(f"Failed on NaN data: {e}")

        # Test with constant prices
        constant_data = self.test_data.copy()
        constant_data['Close'] = 100.0
        constant_data['High'] = 100.0
        constant_data['Low'] = 100.0
        constant_data['Open'] = 100.0
        try:
            constant_regimes = self.detector.detect_regime_history(constant_data)
            print("   Constant price handling passed")
        except Exception as e:
            self.fail(f"Failed on constant prices: {e}")

        print(" Edge cases passed")

    def test_performance_metrics(self):
        """Test performance and timing"""
        print("Testing performance...")

        import time

        # Time the full detection process
        start_time = time.time()
        df_with_regimes = self.detector.detect_regime_history(self.test_data)
        detection_time = time.time() - start_time

        print(f"   Full regime detection: {detection_time:.3f} seconds")
        print(f"   Processing rate: {len(self.test_data)/detection_time:.1f} rows/second")

        # Time signal generation
        start_time = time.time()
        signals = self.detector.get_signals(self.test_data)
        signal_time = time.time() - start_time

        print(f"   Signal generation: {signal_time:.3f} seconds")

        self.assertLess(detection_time, 30, "Detection should complete within 30 seconds")
        self.assertLess(signal_time, 30, "Signal generation should complete within 30 seconds")

        print(" Performance tests passed")

def run_entropy_regime_tests():
    """
    Run the complete test suite with detailed output
    """
    print(" ENTROPY REGIME DETECTOR TEST SUITE")
    print("="*80)

    # Create test suite
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestEntropyRegimeDetector)

    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(test_suite)

    # Print summary
    print("\n" + "="*80)
    print(" TEST SUMMARY")
    print("="*80)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    if result.failures:
        print("\n FAILURES:")
        for test, traceback in result.failures:
            print(f"  {test}: {traceback}")

    if result.errors:
        print("\n ERRORS:")
        for test, traceback in result.errors:
            print(f"  {test}: {traceback}")

    if result.wasSuccessful():
        print("\n ALL TESTS PASSED! ")
        print("Your Entropy Regime Detector is working correctly!")
    else:
        print("\n  SOME TESTS FAILED")
        print("Check the failures and errors above.")

    return result.wasSuccessful()

# Quick test function for individual components
def quick_test():
    """
    Quick test of core functionality
    """
    print(" QUICK TEST")
    print("="*40)

    try:
        # Initialize
        detector = EntropyRegimeDetector()
        print(" Detector initialized")

        # Create minimal test data
        dates = pd.date_range('2023-01-01', periods=100, freq='D')
        np.random.seed(42)

        prices = [100]
        for i in range(99):
            change = np.random.normal(0.001, 0.02)
            prices.append(prices[-1] * (1 + change))

        test_data = pd.DataFrame({
            'Open': prices,
            'High': [p * (1 + abs(np.random.normal(0, 0.01))) for p in prices],
            'Low': [p * (1 - abs(np.random.normal(0, 0.01))) for p in prices],
            'Close': prices,
            'Volume': [int(np.random.uniform(500000, 1500000)) for _ in prices]
        }, index=dates)

        print(" Test data created")

        # Test entropy indicators
        df_entropy = detector.add_entropy_indicators(test_data)
        print(f" Entropy indicators added: {len([c for c in df_entropy.columns if 'Entropy' in c])} columns")

        # Test regime detection
        df_regimes = detector.detect_regime_history(test_data)
        regimes = df_regimes['Entropy_Regime'].dropna()
        print(f" Regimes detected: {regimes.value_counts().to_dict()}")

        # Test signals
        signals = detector.get_signals(test_data)
        signal_counts = signals['Signal'].value_counts()
        print(f" Signals generated: {len(signal_counts)} types")

        print("\n QUICK TEST PASSED!")
        return True

    except Exception as e:
        print(f"\n QUICK TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Run quick test first
    if quick_test():
        print("\n" + "="*80)
        # Run full test suite
        success = run_entropy_regime_tests()

        if success:
            print("\n READY FOR PRODUCTION! ")
        else:
            print("\n NEEDS SOME FIXES")
    else:
        print("\n BASIC FUNCTIONALITY BROKEN - CHECK YOUR CODE")
