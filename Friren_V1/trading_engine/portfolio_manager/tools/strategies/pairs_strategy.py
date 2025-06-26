import pandas as pd
import numpy as np
from scipy.stats import linregress
from .base_strategy import BaseStrategy, StrategySignal, StrategyMetadata
from trading_engine.data.data_utils import StockDataTools
from Friren_V1.trading_engine.data.yahoo_price import YahooFinancePriceData

class PairsStrategy(BaseStrategy):
    """
    Cointegration Pairs Trading Strategy

    Extracted from conPair_bt.py - CointegrationPairsBacktest.backtest_pairs_strategy()

    Logic:
    - Identifies cointegrated stock pairs
    - BUY spread when Z-score <= -entry_threshold (spread oversold)
    - SELL spread when Z-score >= entry_threshold (spread overbought)
    - Uses hedge ratio from linear regression

    Note: This strategy requires two symbols and returns signals for both
    """

    def __init__(self, entry_threshold: float = 2.0, exit_threshold: float = 0.5,
                 lookback_window: int = 60, min_cointegration_periods: int = 100):
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold
        self.lookback_window = lookback_window
        self.min_cointegration_periods = min_cointegration_periods
        self.data_tools = StockDataTools()
        self.data_fetcher = StockDataFetcher()
        super().__init__()

    def generate_pairs_signal(self, symbol1: str, symbol2: str, df1: pd.DataFrame,
                            df2: pd.DataFrame, sentiment1: float = 0.0, sentiment2: float = 0.0,
                            confidence_threshold: float = 60.0) -> dict:
        """
        Generate pairs trading signals for two symbols

        Returns:
            dict: {
                'symbol1_signal': StrategySignal,
                'symbol2_signal': StrategySignal,
                'spread_info': dict
            }
        """

        # Validate input data
        if not self.validate_data(df1, ['Close']) or not self.validate_data(df2, ['Close']):
            return {
                'symbol1_signal': StrategySignal('HOLD', 0, 'Insufficient data for pairs analysis'),
                'symbol2_signal': StrategySignal('HOLD', 0, 'Insufficient data for pairs analysis'),
                'spread_info': {}
            }

        # Align dataframes by date
        common_dates = df1.index.intersection(df2.index)
        if len(common_dates) < self.min_cointegration_periods:
            return {
                'symbol1_signal': StrategySignal('HOLD', 0, f'Insufficient common data: {len(common_dates)} < {self.min_cointegration_periods}'),
                'symbol2_signal': StrategySignal('HOLD', 0, f'Insufficient common data: {len(common_dates)} < {self.min_cointegration_periods}'),
                'spread_info': {}
            }

        df1_aligned = df1.loc[common_dates]
        df2_aligned = df2.loc[common_dates]

        # Calculate hedge ratio and spread
        hedge_ratio = self._calculate_hedge_ratio(df1_aligned['Close'], df2_aligned['Close'])
        spread_data = self._calculate_spread_features(df1_aligned['Close'], df2_aligned['Close'], hedge_ratio)

        if spread_data.empty or len(spread_data) < self.lookback_window:
            return {
                'symbol1_signal': StrategySignal('HOLD', 0, 'Insufficient data for spread calculation'),
                'symbol2_signal': StrategySignal('HOLD', 0, 'Insufficient data for spread calculation'),
                'spread_info': {}
            }

        # Get current spread values
        current = spread_data.iloc[-1]
        current_spread = current['spread']
        current_zscore = current['spread_zscore']
        spread_mean = current['spread_mean']
        spread_std = current['spread_std']

        current_price1 = df1_aligned['Close'].iloc[-1]
        current_price2 = df2_aligned['Close'].iloc[-1]

        # Calculate base signals
        signal1, signal2, base_confidence = self._calculate_pairs_signals(
            current_zscore, symbol1, symbol2, hedge_ratio
        )

        # Apply sentiment filters for both symbols
        final_signal1, final_confidence1 = self.apply_pairs_sentiment_filter(
            signal1, base_confidence, sentiment1, sentiment2
        )
        final_signal2, final_confidence2 = self.apply_pairs_sentiment_filter(
            signal2, base_confidence, sentiment1, sentiment2
        )

        # Check confidence thresholds
        if final_confidence1 < confidence_threshold:
            final_signal1 = 'HOLD'
        if final_confidence2 < confidence_threshold:
            final_signal2 = 'HOLD'

        # Generate reasoning
        reasoning1 = self._generate_pairs_reasoning(
            symbol1, current_price1, final_signal1, current_zscore, hedge_ratio, sentiment1, sentiment2
        )
        reasoning2 = self._generate_pairs_reasoning(
            symbol2, current_price2, final_signal2, current_zscore, hedge_ratio, sentiment1, sentiment2
        )

        spread_info = {
            'spread': current_spread,
            'zscore': current_zscore,
            'hedge_ratio': hedge_ratio,
            'spread_mean': spread_mean,
            'spread_std': spread_std,
            'price1': current_price1,
            'price2': current_price2
        }

        return {
            'symbol1_signal': StrategySignal(
                final_signal1, final_confidence1, reasoning1,
                metadata={'pair_symbol': symbol2, 'hedge_ratio': hedge_ratio, 'spread_zscore': current_zscore}
            ),
            'symbol2_signal': StrategySignal(
                final_signal2, final_confidence2, reasoning2,
                metadata={'pair_symbol': symbol1, 'hedge_ratio': hedge_ratio, 'spread_zscore': current_zscore}
            ),
            'spread_info': spread_info
        }

    def generate_signal(self, df: pd.DataFrame, sentiment: float = 0.0,
                       confidence_threshold: float = 60.0) -> StrategySignal:
        """
        Standard interface - but pairs strategy needs two symbols
        This will return a message explaining how to use the pairs strategy
        """
        return StrategySignal(
            'HOLD',
            0,
            'Pairs strategy requires two symbols. Use generate_pairs_signal() method instead.'
        )

    def get_signal_confidence(self, df: pd.DataFrame) -> float:
        """Calculate confidence - not applicable for pairs without second symbol"""
        return 0.0

    def _calculate_hedge_ratio(self, prices1: pd.Series, prices2: pd.Series) -> float:
        """Calculate optimal hedge ratio using linear regression"""
        try:
            slope, intercept, r_value, p_value, std_err = linregress(prices2.values, prices1.values)
            return slope
        except Exception as e:
            print(f"Error calculating hedge ratio: {e}")
            return 1.0  # Default to 1:1 ratio

    def _calculate_spread_features(self, prices1: pd.Series, prices2: pd.Series, hedge_ratio: float) -> pd.DataFrame:
        """Calculate spread and related features"""

        # Calculate spread
        spread = prices1 - hedge_ratio * prices2

        # Create dataframe with spread features
        spread_df = pd.DataFrame(index=prices1.index)
        spread_df['spread'] = spread

        # Rolling statistics
        spread_df['spread_mean'] = spread.rolling(self.lookback_window).mean()
        spread_df['spread_std'] = spread.rolling(self.lookback_window).std()

        # Z-score
        spread_df['spread_zscore'] = (spread - spread_df['spread_mean']) / spread_df['spread_std']

        # Remove rows with insufficient data
        spread_df = spread_df.dropna()

        return spread_df

    def _calculate_pairs_signals(self, current_zscore: float, symbol1: str, symbol2: str,
                               hedge_ratio: float) -> tuple[str, str, float]:
        """Calculate base pairs trading signals"""

        base_confidence = 0.0
        signal1 = 'HOLD'
        signal2 = 'HOLD'

        # Long spread (buy symbol1, sell symbol2) when spread is oversold
        if current_zscore <= -self.entry_threshold:
            signal1 = 'BUY'   # Buy the first symbol
            signal2 = 'SELL'  # Sell the second symbol (short)
            zscore_magnitude = abs(current_zscore)
            base_confidence = min(100, 50 + (zscore_magnitude - self.entry_threshold) * 25)

        # Short spread (sell symbol1, buy symbol2) when spread is overbought
        elif current_zscore >= self.entry_threshold:
            signal1 = 'SELL'  # Sell the first symbol (short)
            signal2 = 'BUY'   # Buy the second symbol
            zscore_magnitude = abs(current_zscore)
            base_confidence = min(100, 50 + (zscore_magnitude - self.entry_threshold) * 25)

        # Exit signal when spread reverts to mean
        elif abs(current_zscore) <= self.exit_threshold:
            # This would close existing positions - for signal generation, treat as low confidence
            base_confidence = 20

        return signal1, signal2, base_confidence

    def apply_pairs_sentiment_filter(self, base_signal: str, base_confidence: float,
                                   sentiment1: float, sentiment2: float) -> tuple[str, float]:
        """Apply sentiment filtering specific to pairs trading"""

        adjusted_confidence = base_confidence

        # Calculate relative sentiment
        sentiment_divergence = abs(sentiment1 - sentiment2)
        avg_sentiment = (sentiment1 + sentiment2) / 2

        # Pairs trading works better when sentiments diverge
        if sentiment_divergence > 0.3:
            # High sentiment divergence supports pairs trading
            adjusted_confidence = min(100, adjusted_confidence * 1.2)
        elif sentiment_divergence < 0.1:
            # Low sentiment divergence (correlated sentiment) reduces pairs effectiveness
            adjusted_confidence *= 0.8

        # Strong overall sentiment can overwhelm pairs relationships
        if abs(avg_sentiment) > 0.5:
            adjusted_confidence *= 0.9  # Reduce confidence in high overall sentiment

        # Check if sentiment supports the direction of the signal
        if base_signal == 'BUY' and sentiment1 > sentiment2:
            adjusted_confidence = min(100, adjusted_confidence * 1.1)
        elif base_signal == 'SELL' and sentiment1 < sentiment2:
            adjusted_confidence = min(100, adjusted_confidence * 1.1)

        return base_signal, adjusted_confidence

    def _generate_pairs_reasoning(self, symbol: str, current_price: float, signal: str,
                                current_zscore: float, hedge_ratio: float,
                                sentiment1: float, sentiment2: float) -> str:
        """Generate reasoning for pairs trading signal"""

        if signal == 'BUY':
            if current_zscore <= -self.entry_threshold:
                reason = f"Pairs long: spread oversold (Z={current_zscore:.2f}), buy {symbol}"
            else:
                reason = f"Pairs exit: buy {symbol} to close short position"

        elif signal == 'SELL':
            if current_zscore >= self.entry_threshold:
                reason = f"Pairs short: spread overbought (Z={current_zscore:.2f}), sell {symbol}"
            else:
                reason = f"Pairs exit: sell {symbol} to close long position"
        else:
            reason = f"Pairs neutral: spread Z-score {current_zscore:.2f} in neutral range"

        # Add hedge ratio context
        reason += f", hedge ratio: {hedge_ratio:.3f}"

        # Add sentiment context
        sentiment_diff = abs(sentiment1 - sentiment2)
        if sentiment_diff > 0.3:
            reason += f", sentiment divergence supports pairs trading"
        elif sentiment_diff < 0.1:
            reason += f", low sentiment divergence may reduce pairs effectiveness"

        return reason

    def find_cointegrated_pairs(self, symbols: list, period: str = "1y") -> list:
        """
        Find potentially cointegrated pairs from a list of symbols

        Returns:
            list: Sorted list of pairs with cointegration statistics
        """
        pairs_data = []

        print(f"Testing {len(symbols)} symbols for cointegration...")

        # Get data for all symbols
        symbol_data = {}
        for symbol in symbols:
            try:
                data = self.data_fetcher.extract_data(symbol, period=period)
                if not data.empty and len(data) > self.min_cointegration_periods:
                    symbol_data[symbol] = data['Close']
                    print(f"   {symbol}: {len(data)} data points")
                else:
                    print(f"   {symbol}: insufficient data")
            except Exception as e:
                print(f"   {symbol}: error - {e}")

        # Test all pairs
        symbols_with_data = list(symbol_data.keys())
        for i in range(len(symbols_with_data)):
            for j in range(i + 1, len(symbols_with_data)):
                symbol1, symbol2 = symbols_with_data[i], symbols_with_data[j]

                try:
                    # Align data
                    common_dates = symbol_data[symbol1].index.intersection(symbol_data[symbol2].index)
                    if len(common_dates) < self.min_cointegration_periods:
                        continue

                    prices1 = symbol_data[symbol1].loc[common_dates]
                    prices2 = symbol_data[symbol2].loc[common_dates]

                    # Calculate hedge ratio and spread
                    hedge_ratio = self._calculate_hedge_ratio(prices1, prices2)
                    spread = prices1 - hedge_ratio * prices2

                    # Simple cointegration test (Augmented Dickey-Fuller test substitute)
                    # For simplicity, we'll use spread mean reversion as a proxy
                    spread_mean = spread.mean()
                    spread_std = spread.std()

                    # Calculate half-life of mean reversion
                    half_life = self._calculate_half_life(spread)

                    # Calculate correlation
                    correlation = prices1.corr(prices2)

                    # Score based on spread stability and mean reversion
                    if spread_std > 0 and not pd.isna(half_life) and half_life > 0:
                        stability_score = 1.0 / (spread_std / abs(spread_mean) + 0.01)
                        mean_reversion_score = 1.0 / (half_life + 1)
                        total_score = stability_score * mean_reversion_score * correlation

                        pairs_data.append({
                            'symbol1': symbol1,
                            'symbol2': symbol2,
                            'hedge_ratio': hedge_ratio,
                            'correlation': correlation,
                            'spread_mean': spread_mean,
                            'spread_std': spread_std,
                            'half_life': half_life,
                            'total_score': total_score,
                            'data_points': len(common_dates)
                        })

                except Exception as e:
                    print(f"  Error testing {symbol1}-{symbol2}: {e}")
                    continue

        # Sort by total score
        pairs_data.sort(key=lambda x: x['total_score'], reverse=True)

        print(f"\nFound {len(pairs_data)} potential pairs")
        if pairs_data:
            print("Top 5 pairs:")
            for i, pair in enumerate(pairs_data[:5]):
                print(f"  {i+1}. {pair['symbol1']}-{pair['symbol2']}: score={pair['total_score']:.3f}, "
                      f"corr={pair['correlation']:.3f}, half-life={pair['half_life']:.1f}")

        return pairs_data

    def _calculate_half_life(self, spread: pd.Series) -> float:
        """Calculate half-life of mean reversion"""
        try:
            spread_lag = spread.shift(1).dropna()
            spread_diff = spread.diff().dropna()

            # Align the series
            aligned_data = pd.concat([spread_diff, spread_lag], axis=1).dropna()
            if len(aligned_data) < 10:
                return np.nan

            y = aligned_data.iloc[:, 0].values
            x = aligned_data.iloc[:, 1].values

            # Simple OLS regression: Δy = α + βy_{t-1} + ε
            x_with_const = np.column_stack([np.ones(len(x)), x])
            coeffs = np.linalg.lstsq(x_with_const, y, rcond=None)[0]
            beta = coeffs[1]

            if beta >= 0:  # No mean reversion
                return np.nan

            half_life = -np.log(2) / np.log(1 + beta)
            return half_life

        except Exception:
            return np.nan

    def get_required_indicators(self) -> list[str]:
        """Return required indicators (computed internally for pairs)"""
        return ['spread', 'spread_zscore', 'hedge_ratio']

    def _get_strategy_metadata(self) -> StrategyMetadata:
        """Return strategy metadata"""
        return StrategyMetadata(
            name="Cointegration Pairs Trading",
            category="PAIRS",
            typical_holding_days=12,
            works_best_in="Markets with stable correlations between related stocks",
            min_confidence=70.0,
            max_positions=2,  # Always involves two positions
            requires_pairs=True
        )
