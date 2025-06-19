import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from scipy import stats
import sys
import os

# Add the parent directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))  # Go up two levels
sys.path.insert(0, parent_dir)
import warnings
warnings.filterwarnings('ignore')
from trading_engine.data.data_utils import StockDataTools

class PCAFactorAnalyzer:
    """
    PCA Factor Analysis for identifying systematic and idiosyncratic components
    """
    def __init__(self, n_components=5):
        self.n_components = n_components
        self.pca = PCA(n_components=n_components)
        self.scaler = StandardScaler()
        self.factor_loadings = None
        self.factor_returns = None
        self.explained_variance_ratio = None
        self.fitted = False

    def fit(self, returns_matrix):
        """
        Fit PCA model to returns matrix
        returns_matrix: DataFrame with returns for different assets as columns
        """
        # Remove any rows with NaN values
        clean_returns = returns_matrix.dropna()

        if len(clean_returns) < 50:
            raise ValueError("Need at least 50 observations to fit PCA model")

        # Standardize returns
        scaled_returns = self.scaler.fit_transform(clean_returns)

        # Fit PCA
        self.pca.fit(scaled_returns)

        # Calculate factor loadings (how much each asset loads on each factor)
        self.factor_loadings = pd.DataFrame(
            self.pca.components_.T,
            index=clean_returns.columns,
            columns=[f'Factor_{i+1}' for i in range(self.n_components)]
        )

        # Calculate factor returns (time series of factor values)
        self.factor_returns = pd.DataFrame(
            self.pca.transform(scaled_returns),
            index=clean_returns.index,
            columns=[f'Factor_{i+1}' for i in range(self.n_components)]
        )

        self.explained_variance_ratio = self.pca.explained_variance_ratio_
        self.fitted = True

        print(f"PCA Model fitted successfully:")
        print(f"  Factors explain {sum(self.explained_variance_ratio):.1%} of total variance")
        for i, var_ratio in enumerate(self.explained_variance_ratio):
            print(f"  Factor {i+1}: {var_ratio:.1%}")

        return self

    def transform_new_data(self, new_returns):
        """Transform new returns data using fitted PCA model"""
        if not self.fitted:
            raise ValueError("Model must be fitted first")

        # Align columns
        aligned_returns = new_returns.reindex(columns=self.factor_loadings.index).dropna()

        if aligned_returns.empty:
            return pd.DataFrame()

        # Scale and transform
        scaled_returns = self.scaler.transform(aligned_returns)
        factor_scores = self.pca.transform(scaled_returns)

        return pd.DataFrame(
            factor_scores,
            index=aligned_returns.index,
            columns=[f'Factor_{i+1}' for i in range(self.n_components)]
        )

    def calculate_factor_exposures(self, asset_returns):
        """Calculate how much an asset is exposed to each factor"""
        if not self.fitted:
            raise ValueError("Model must be fitted first")

        exposures = {}

        for asset in self.factor_loadings.index:
            if asset in asset_returns.columns:
                exposures[asset] = {
                    'market_factor': self.factor_loadings.loc[asset, 'Factor_1'],  # First factor usually market
                    'factor_2': self.factor_loadings.loc[asset, 'Factor_2'],
                    'factor_3': self.factor_loadings.loc[asset, 'Factor_3'] if self.n_components >= 3 else 0,
                    'total_systematic': np.sum(self.factor_loadings.loc[asset] ** 2),
                    'idiosyncratic': 1 - np.sum(self.factor_loadings.loc[asset] ** 2)
                }

        return exposures

    def calculate_factor_momentum(self, lookback_days=20):
        """Calculate momentum in each factor"""
        if not self.fitted or self.factor_returns is None:
            return {}

        factor_momentum = {}

        for factor in self.factor_returns.columns:
            recent_returns = self.factor_returns[factor].tail(lookback_days)

            if len(recent_returns) >= lookback_days:
                # Calculate momentum as cumulative return and trend strength
                cumulative_return = recent_returns.sum()
                trend_strength = stats.linregress(range(len(recent_returns)), recent_returns)[0]
                volatility = recent_returns.std()

                factor_momentum[factor] = {
                    'cumulative_return': cumulative_return,
                    'trend_strength': trend_strength,
                    'volatility': volatility,
                    'momentum_score': trend_strength / (volatility + 1e-8)  # Risk-adjusted momentum
                }

        return factor_momentum

class PCAFactorBacktest:
    def __init__(self, starting_cash=25000):
        self.starting_cash = starting_cash
        self.cash = starting_cash
        self.positions = {}  # Track positions for multiple assets
        self.portfolio_value = []
        self.trades = []
        self.SDT = StockDataTools()
        self.pca_analyzer = PCAFactorAnalyzer(n_components=5)
        self.universe = []

    def build_universe(self, symbols=None):
        """Build trading universe and fit PCA model"""
        if symbols is None:
            # Use a diversified set of stocks from different sectors
            symbols = [
                # Tech
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META',
                # Finance
                'JPM', 'BAC', 'GS', 'WFC',
                # Healthcare
                'JNJ', 'PFE', 'UNH',
                # Consumer
                'PG', 'KO', 'WMT',
                # Energy
                'XOM', 'CVX',
                # Industrial
                'BA', 'CAT'
            ]

        self.universe = symbols
        print(f"Building universe with {len(symbols)} stocks...")

        # Get historical data for all stocks
        all_returns = pd.DataFrame()

        for symbol in symbols:
            try:
                data = self.SDT.extract_data(symbol, period="2y", interval="1d")
                returns = data['Close'].pct_change()
                all_returns[symbol] = returns
                print(f"  Fetched data for {symbol}")
            except Exception as e:
                print(f"  Failed to fetch {symbol}: {e}")
                continue

        # Remove stocks with insufficient data
        min_observations = len(all_returns) * 0.8  # Require 80% data availability
        valid_stocks = all_returns.columns[all_returns.count() >= min_observations]
        all_returns = all_returns[valid_stocks]

        print(f"Using {len(valid_stocks)} stocks with sufficient data")

        # Fit PCA model
        if len(all_returns.columns) >= 5:
            self.pca_analyzer.fit(all_returns)
            return all_returns
        else:
            raise ValueError("Need at least 5 stocks to fit PCA model")

    def add_factor_features(self, df, symbol, universe_returns):
        """Add factor-based features to individual stock data"""
        df_enhanced = df.copy()

        # Calculate returns
        df_enhanced['returns'] = df_enhanced['Close'].pct_change()

        # Get factor exposures for this stock
        exposures = self.pca_analyzer.calculate_factor_exposures(universe_returns)

        if symbol not in exposures:
            print(f"Warning: {symbol} not in factor model")
            return df_enhanced

        stock_exposures = exposures[symbol]

        # Add static exposure features
        df_enhanced['market_factor_exposure'] = stock_exposures['market_factor']
        df_enhanced['factor_2_exposure'] = stock_exposures['factor_2']
        df_enhanced['factor_3_exposure'] = stock_exposures['factor_3']
        df_enhanced['total_systematic_exposure'] = stock_exposures['total_systematic']
        df_enhanced['idiosyncratic_component'] = stock_exposures['idiosyncratic']

        # Calculate rolling factor momentum
        df_enhanced['factor_momentum_1'] = np.nan
        df_enhanced['factor_momentum_2'] = np.nan
        df_enhanced['factor_momentum_3'] = np.nan
        df_enhanced['systematic_return'] = np.nan
        df_enhanced['idiosyncratic_return'] = np.nan

        # Rolling calculations
        for i in range(60, len(df_enhanced)):  # Need sufficient history
            try:
                # Get recent period
                end_date = df_enhanced.index[i]
                start_date = df_enhanced.index[max(0, i-60)]

                # Get factor returns for this period
                period_factor_returns = self.pca_analyzer.factor_returns.loc[start_date:end_date]

                if len(period_factor_returns) >= 20:
                    # Calculate factor momentum
                    factor_momentum = self.pca_analyzer.calculate_factor_momentum(lookback_days=20)

                    if 'Factor_1' in factor_momentum:
                        df_enhanced.iloc[i, df_enhanced.columns.get_loc('factor_momentum_1')] = \
                            factor_momentum['Factor_1']['momentum_score']

                    if 'Factor_2' in factor_momentum:
                        df_enhanced.iloc[i, df_enhanced.columns.get_loc('factor_momentum_2')] = \
                            factor_momentum['Factor_2']['momentum_score']

                    if 'Factor_3' in factor_momentum and self.pca_analyzer.n_components >= 3:
                        df_enhanced.iloc[i, df_enhanced.columns.get_loc('factor_momentum_3')] = \
                            factor_momentum['Factor_3']['momentum_score']

                    # Decompose return into systematic and idiosyncratic components
                    current_return = df_enhanced['returns'].iloc[i]
                    if not pd.isna(current_return):
                        # Approximate systematic return using factor loadings
                        systematic_return = (
                            stock_exposures['market_factor'] * period_factor_returns['Factor_1'].iloc[-1] +
                            stock_exposures['factor_2'] * period_factor_returns['Factor_2'].iloc[-1] +
                            stock_exposures['factor_3'] * period_factor_returns['Factor_3'].iloc[-1]
                        )

                        idiosyncratic_return = current_return - systematic_return

                        df_enhanced.iloc[i, df_enhanced.columns.get_loc('systematic_return')] = systematic_return
                        df_enhanced.iloc[i, df_enhanced.columns.get_loc('idiosyncratic_return')] = idiosyncratic_return

            except Exception as e:
                continue

        return df_enhanced

    def backtest_factor_strategy(self, universe_returns, strategy_type='factor_momentum',
                                factor_threshold=0.5, rebalance_frequency=20):
        """
        Backtest factor-based strategies

        strategy_type options:
        - 'factor_momentum': Buy stocks with positive factor momentum
        - 'idiosyncratic': Buy stocks with strong idiosyncratic returns
        - 'low_systematic': Buy stocks with low systematic risk
        """
        print(f"Running {strategy_type} factor strategy...")

        # Track portfolio
        portfolio_weights = {}
        rebalance_counter = 0

        # Get all stock data with factor features
        stock_data = {}
        for symbol in self.universe:
            try:
                data = self.SDT.extract_data(symbol, period="1y", interval="1d")
                enhanced_data = self.add_factor_features(data, symbol, universe_returns)
                stock_data[symbol] = enhanced_data
            except Exception as e:
                print(f"Failed to enhance {symbol}: {e}")
                continue

        if not stock_data:
            print("No valid stock data available")
            return {}

        # Find common date range
        common_dates = None
        for symbol, data in stock_data.items():
            if common_dates is None:
                common_dates = data.index
            else:
                common_dates = common_dates.intersection(data.index)

        if len(common_dates) < 100:
            print("Insufficient common date range")
            return {}

        common_dates = sorted(common_dates)[60:]  # Start after warm-up period

        # Backtesting loop
        for i, date in enumerate(common_dates):
            # Rebalance portfolio periodically
            if i % rebalance_frequency == 0:
                new_weights = self._calculate_portfolio_weights(
                    stock_data, date, strategy_type, factor_threshold
                )
                portfolio_weights = new_weights
                rebalance_counter += 1

            # Calculate portfolio value
            total_value = self.cash
            for symbol, weight in portfolio_weights.items():
                if symbol in stock_data and date in stock_data[symbol].index:
                    price = stock_data[symbol].loc[date, 'Close']
                    shares = weight * self.starting_cash / price
                    total_value += shares * (price - stock_data[symbol].loc[common_dates[0], 'Close'])

            self.portfolio_value.append(total_value)

        print(f"Completed backtest with {rebalance_counter} rebalances")
        return self.calculate_performance()

    def _calculate_portfolio_weights(self, stock_data, date, strategy_type, threshold):
        """Calculate portfolio weights based on factor strategy"""
        scores = {}

        for symbol, data in stock_data.items():
            if date not in data.index:
                continue

            row = data.loc[date]

            # Skip if we don't have factor features
            if pd.isna(row.get('factor_momentum_1', np.nan)):
                continue

            if strategy_type == 'factor_momentum':
                # Score based on factor momentum
                score = (
                    row.get('factor_momentum_1', 0) * 0.5 +  # Market factor weight
                    row.get('factor_momentum_2', 0) * 0.3 +  # Second factor weight
                    row.get('factor_momentum_3', 0) * 0.2    # Third factor weight
                )

            elif strategy_type == 'idiosyncratic':
                # Score based on idiosyncratic component strength
                idio_return = row.get('idiosyncratic_return', 0)
                idio_component = row.get('idiosyncratic_component', 0)
                score = idio_return * idio_component  # Strong idiosyncratic positive returns

            elif strategy_type == 'low_systematic':
                # Score based on low systematic risk (defensive strategy)
                systematic_exposure = row.get('total_systematic_exposure', 1)
                recent_vol = data['returns'].rolling(20).std().loc[date] if len(data) > 20 else 0.02
                score = -systematic_exposure / (recent_vol + 1e-8)  # Prefer low systematic risk

            else:
                score = 0

            scores[symbol] = score

        if not scores:
            return {}

        # Select top stocks above threshold
        if strategy_type == 'low_systematic':
            # For defensive strategy, take highest scores (least negative)
            selected_stocks = {k: v for k, v in scores.items() if v > np.percentile(list(scores.values()), 70)}
        else:
            # For growth strategies, take stocks above threshold
            selected_stocks = {k: v for k, v in scores.items() if v > threshold}

        if not selected_stocks:
            return {}

        # Equal weight selected stocks
        n_stocks = len(selected_stocks)
        weights = {symbol: 1.0 / n_stocks for symbol in selected_stocks}

        return weights

    def calculate_performance(self):
        """Calculate strategy performance metrics"""
        if not self.portfolio_value:
            return {}

        final_value = self.portfolio_value[-1]
        total_return = (final_value - self.starting_cash) / self.starting_cash

        # Calculate basic metrics
        portfolio_series = pd.Series(self.portfolio_value)
        daily_returns = portfolio_series.pct_change().dropna()

        # Sharpe ratio
        sharpe_ratio = daily_returns.mean() / daily_returns.std() * np.sqrt(252) if daily_returns.std() > 0 else 0

        # Maximum drawdown
        rolling_max = portfolio_series.expanding().max()
        drawdown = (portfolio_series - rolling_max) / rolling_max
        max_drawdown = drawdown.min()

        # Volatility
        annualized_volatility = daily_returns.std() * np.sqrt(252)

        results = {
            'starting_cash': self.starting_cash,
            'final_value': final_value,
            'total_return': total_return * 100,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown * 100,
            'annualized_volatility': annualized_volatility * 100,
            'portfolio_values': self.portfolio_value
        }

        return results

def run_pca_factor_backtest_example():
    """Example of how to run PCA factor strategy backtest"""
    print("Running PCA Factor Strategy Backtest...")

    # Initialize backtester
    backtester = PCAFactorBacktest(starting_cash=100000)

    # Build universe and fit PCA model
    try:
        universe_returns = backtester.build_universe()
    except Exception as e:
        print(f"Failed to build universe: {e}")
        return {}

    # Test different factor strategies
    strategies = ['factor_momentum', 'idiosyncratic', 'low_systematic']
    all_results = {}

    for strategy in strategies:
        print(f"\nTesting {strategy} strategy...")

        # Create new backtester instance for each strategy
        strategy_backtester = PCAFactorBacktest(starting_cash=100000)
        strategy_backtester.pca_analyzer = backtester.pca_analyzer  # Reuse fitted model
        strategy_backtester.universe = backtester.universe

        try:
            results = strategy_backtester.backtest_factor_strategy(
                universe_returns=universe_returns,
                strategy_type=strategy,
                factor_threshold=0.3 if strategy != 'low_systematic' else -0.5,
                rebalance_frequency=20
            )

            all_results[strategy] = results

        except Exception as e:
            print(f"Failed to run {strategy} strategy: {e}")
            continue

    # Print comparison results
    print("\n" + "="*80)
    print("PCA FACTOR STRATEGIES COMPARISON")
    print("="*80)
    print(f"{'Strategy':<20} {'Return':<10} {'Sharpe':<8} {'MaxDD':<8} {'Volatility':<10}")
    print("-" * 80)

    for strategy, results in all_results.items():
        if results:
            print(f"{strategy:<20} {results['total_return']:>7.1f}% "
                  f"{results['sharpe_ratio']:>7.2f} "
                  f"{results['max_drawdown']:>6.1f}% "
                  f"{results['annualized_volatility']:>8.1f}%")

    return all_results

def analyze_factor_structure_example():
    """Example of analyzing factor structure of the market"""
    print("Analyzing Factor Structure...")

    # Build universe
    backtester = PCAFactorBacktest()
    universe_returns = backtester.build_universe()

    # Analyze factor loadings
    print("\n" + "="*60)
    print("FACTOR LOADINGS ANALYSIS")
    print("="*60)

    loadings = backtester.pca_analyzer.factor_loadings

    # Show top loadings for each factor
    for factor in loadings.columns:
        print(f"\n{factor} (explains {backtester.pca_analyzer.explained_variance_ratio[int(factor.split('_')[1])-1]:.1%} of variance):")
        top_positive = loadings[factor].nlargest(5)
        top_negative = loadings[factor].nsmallest(5)

        print("  Top Positive Loadings:")
        for stock, loading in top_positive.items():
            print(f"    {stock}: {loading:.3f}")

        print("  Top Negative Loadings:")
        for stock, loading in top_negative.items():
            print(f"    {stock}: {loading:.3f}")

    # Calculate factor momentum
    print("\n" + "="*60)
    print("CURRENT FACTOR MOMENTUM")
    print("="*60)

    factor_momentum = backtester.pca_analyzer.calculate_factor_momentum(lookback_days=20)

    for factor, momentum_data in factor_momentum.items():
        print(f"\n{factor}:")
        print(f"  Cumulative Return (20d): {momentum_data['cumulative_return']:>6.3f}")
        print(f"  Trend Strength: {momentum_data['trend_strength']:>6.3f}")
        print(f"  Volatility: {momentum_data['volatility']:>6.3f}")
        print(f"  Momentum Score: {momentum_data['momentum_score']:>6.3f}")

    return backtester.pca_analyzer

def compare_individual_vs_factor_strategy():
    """Compare individual stock picking vs factor-based approach"""
    print("Comparing Individual Stock Selection vs Factor-Based Approach...")

    # Simple buy-and-hold benchmark
    sdt = StockDataTools()
    spy_data = sdt.extract_data('SPY', period='1y', interval='1d')
    spy_return = (spy_data['Close'].iloc[-1] / spy_data['Close'].iloc[0] - 1) * 100

    print(f"\nSPY Benchmark Return: {spy_return:.1f}%")

    # Run factor strategies
    results = run_pca_factor_backtest_example()

    print(f"\nStrategy Performance vs SPY:")
    print("-" * 40)
    for strategy, result in results.items():
        if result:
            excess_return = result['total_return'] - spy_return
            print(f"{strategy:<20}: {excess_return:>+6.1f}% excess return")

    return results

if __name__ == "__main__":
    # Step 1: Analyze factor structure
    print("Step 1: Factor Structure Analysis")
    factor_analyzer = analyze_factor_structure_example()

    # Step 2: Run factor strategies
    print("\nStep 2: Factor Strategy Backtesting")
    strategy_results = run_pca_factor_backtest_example()

    # Step 3: Compare vs benchmark
    print("\nStep 3: Benchmark Comparison")
    comparison = compare_individual_vs_factor_strategy()
