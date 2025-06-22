import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from scipy import stats
from typing import Dict, List, Optional, Tuple, Any
import warnings
warnings.filterwarnings('ignore')

from .base_strategy import BaseStrategy, StrategySignal, StrategyMetadata
from trading_engine.data.data_utils import StockDataTools
from Friren_V1.trading_engine.data.yahoo_price import YahooFinancePriceData

class PCAAnalyzer:
    """
    IMPROVED: Enhanced PCA Factor Analyzer with better stability and caching
    """

    def __init__(self, n_components: int = 5, min_variance_explained: float = 0.7):
        self.n_components = n_components
        self.min_variance_explained = min_variance_explained
        self.pca = PCA(n_components=n_components)
        self.scaler = StandardScaler()
        self.factor_loadings = None
        self.factor_returns = None
        self.explained_variance_ratio = None
        self.fitted = False
        self.universe_symbols = []
        self.fit_date = None

        # IMPROVEMENT: Add stability tracking
        self.factor_stability_scores = {}
        self.loading_history = []

    def fit(self, returns_matrix: pd.DataFrame, min_observations: int = 100):
        """
        IMPROVED: Fit PCA model with better validation and stability checks
        """
        # Remove any rows with NaN values
        clean_returns = returns_matrix.dropna()

        if len(clean_returns) < min_observations:
            raise ValueError(f"Need at least {min_observations} observations to fit PCA model")

        # IMPROVEMENT: Check for sufficient variance in data
        if clean_returns.std().min() < 1e-6:
            raise ValueError("Insufficient variance in some assets - check for constant prices")

        # Store universe info
        self.universe_symbols = clean_returns.columns.tolist()
        self.fit_date = clean_returns.index[-1]

        # Standardize returns
        scaled_returns = self.scaler.fit_transform(clean_returns)

        # Fit PCA
        self.pca.fit(scaled_returns)

        # IMPROVEMENT: Check if we explain enough variance
        total_variance_explained = sum(self.pca.explained_variance_ratio_)
        if total_variance_explained < self.min_variance_explained:
            print(f"Warning: Only explaining {total_variance_explained:.1%} of variance with {self.n_components} factors")

        # Calculate factor loadings
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

        # IMPROVEMENT: Calculate factor stability
        self._calculate_factor_stability()

        # Store loading history for stability tracking
        self.loading_history.append({
            'date': self.fit_date,
            'loadings': self.factor_loadings.copy()
        })

        print(f"PCA Model fitted successfully:")
        print(f"  Factors explain {total_variance_explained:.1%} of total variance")
        for i, var_ratio in enumerate(self.explained_variance_ratio):
            print(f"  Factor {i+1}: {var_ratio:.1%}")

        return self

    def _calculate_factor_stability(self):
        """
        IMPROVEMENT: Calculate factor stability scores
        """
        if len(self.loading_history) < 2:
            # Initialize stability scores
            for i in range(self.n_components):
                self.factor_stability_scores[f'Factor_{i+1}'] = 1.0
            return

        # Compare with previous loadings
        prev_loadings = self.loading_history[-2]['loadings']
        current_loadings = self.factor_loadings

        for factor in current_loadings.columns:
            if factor in prev_loadings.columns:
                # Calculate correlation between loadings
                correlation = current_loadings[factor].corr(prev_loadings[factor])
                self.factor_stability_scores[factor] = abs(correlation)
            else:
                self.factor_stability_scores[factor] = 0.5  # Unknown stability

    def get_factor_interpretation(self) -> Dict[str, str]:
        """
        IMPROVEMENT: Automatically interpret factors based on loadings
        """
        interpretations = {}

        if not self.fitted:
            return interpretations

        for factor in self.factor_loadings.columns:
            loadings = self.factor_loadings[factor].abs()
            top_stocks = loadings.nlargest(5).index.tolist()

            # Simple sector classification based on stock symbols
            tech_stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META']
            finance_stocks = ['JPM', 'BAC', 'GS', 'WFC']
            energy_stocks = ['XOM', 'CVX']

            tech_count = sum(1 for stock in top_stocks if stock in tech_stocks)
            finance_count = sum(1 for stock in top_stocks if stock in finance_stocks)
            energy_count = sum(1 for stock in top_stocks if stock in energy_stocks)

            if factor == 'Factor_1':
                interpretations[factor] = "Market Factor (broad market exposure)"
            elif tech_count >= 3:
                interpretations[factor] = "Technology Sector Factor"
            elif finance_count >= 2:
                interpretations[factor] = "Financial Sector Factor"
            elif energy_count >= 1:
                interpretations[factor] = "Energy/Commodity Factor"
            else:
                interpretations[factor] = f"Mixed Factor (top: {', '.join(top_stocks[:3])})"

        return interpretations

    def calculate_factor_scores(self, asset_returns: pd.Series, lookback_days: int = 20) -> Dict[str, float]:
        """
        IMPROVEMENT: Calculate current factor scores and momentum
        """
        if not self.fitted or len(self.factor_returns) < lookback_days:
            return {}

        scores = {}
        recent_factor_returns = self.factor_returns.tail(lookback_days)

        for factor in self.factor_returns.columns:
            factor_series = recent_factor_returns[factor]

            # Factor momentum (trend strength)
            if len(factor_series) >= 10:
                slope, _, r_value, _, _ = stats.linregress(range(len(factor_series)), factor_series)
                momentum = slope * r_value  # Trend strength weighted by R-squared
            else:
                momentum = 0

            # Factor volatility
            volatility = factor_series.std()

            # Factor mean reversion potential
            current_level = factor_series.iloc[-1]
            historical_mean = self.factor_returns[factor].mean()
            z_score = (current_level - historical_mean) / (self.factor_returns[factor].std() + 1e-8)

            scores[factor] = {
                'momentum': momentum,
                'volatility': volatility,
                'current_level': current_level,
                'z_score': z_score,
                'stability': self.factor_stability_scores.get(factor, 0.5)
            }

        return scores

class PCAStrategy(BaseStrategy):
    """
    IMPROVED: PCA Factor Strategy converted to production format

    Multiple sub-strategies:
    1. Factor Momentum: Buy assets with positive factor momentum exposure
    2. Factor Mean Reversion: Buy assets when factors are oversold
    3. Low Beta: Buy assets with low systematic risk
    4. Idiosyncratic Alpha: Focus on stock-specific opportunities
    """

    def __init__(self, strategy_type: str = 'factor_momentum',
                 n_factors: int = 5, refit_frequency: int = 60,
                 universe_size: int = 20):
        """
        Args:
            strategy_type: 'factor_momentum', 'factor_mean_reversion', 'low_beta', 'idiosyncratic'
            n_factors: Number of PCA factors to use
            refit_frequency: How often to refit PCA model (days)
            universe_size: Number of stocks in universe
        """
        self.strategy_type = strategy_type
        self.n_factors = n_factors
        self.refit_frequency = refit_frequency
        self.universe_size = universe_size

        self.data_tools = StockDataTools()
        self.data_fetcher = StockDataFetcher()
        self.pca_analyzer = PCAAnalyzer(n_components=n_factors)

        # IMPROVEMENT: Cache universe data to avoid repeated fetches
        self.universe_cache = {}
        self.last_universe_update = None

        super().__init__()

    def generate_signal(self, df: pd.DataFrame, sentiment: float = 0.0,
                       confidence_threshold: float = 60.0) -> StrategySignal:
        """Generate PCA-based signal for a single symbol"""

        # Validate required data
        if not self.validate_data(df, ['Close']):
            return StrategySignal('HOLD', 0, 'Insufficient data for PCA analysis')

        # Get symbol from dataframe attributes or use default
        symbol = getattr(df, 'attrs', {}).get('symbol', 'UNKNOWN')

        try:
            # Build/update universe if needed
            universe_returns = self._get_universe_returns()

            if universe_returns.empty:
                return StrategySignal('HOLD', 0, 'Unable to build factor universe')

            # Check if symbol is in our universe
            if symbol not in universe_returns.columns and symbol != 'UNKNOWN':
                return StrategySignal('HOLD', 30, f'{symbol} not in factor universe')

            # Calculate factor exposures and signals
            factor_scores = self.pca_analyzer.calculate_factor_scores(df['Close'])

            if not factor_scores:
                return StrategySignal('HOLD', 0, 'Unable to calculate factor scores')

            # Generate signal based on strategy type
            base_signal, base_confidence = self._calculate_factor_signal(
                symbol, df, factor_scores, universe_returns
            )

            # Apply sentiment filter
            final_signal, final_confidence = self.apply_sentiment_filter(
                base_signal, base_confidence, sentiment
            )

            # Check confidence threshold
            if final_confidence < confidence_threshold:
                return StrategySignal(
                    'HOLD',
                    final_confidence,
                    f'Factor signal below threshold: {final_confidence:.1f}% < {confidence_threshold}%'
                )

            # Generate reasoning
            reasoning = self._generate_factor_reasoning(
                symbol, final_signal, factor_scores, self.strategy_type
            )

            return StrategySignal(
                final_signal,
                final_confidence,
                reasoning,
                metadata={
                    'strategy_type': self.strategy_type,
                    'factor_scores': factor_scores,
                    'universe_size': len(universe_returns.columns),
                    'model_age_days': self._get_model_age_days()
                }
            )

        except Exception as e:
            return StrategySignal('HOLD', 0, f'PCA analysis error: {str(e)}')

    def get_signal_confidence(self, df: pd.DataFrame) -> float:
        """Calculate confidence based on factor model quality and signal strength"""

        try:
            if not self.pca_analyzer.fitted:
                return 0.0

            # Base confidence from variance explained
            variance_explained = sum(self.pca_analyzer.explained_variance_ratio)
            base_confidence = variance_explained * 80  # Max 80% from model quality

            # Factor stability bonus
            avg_stability = np.mean(list(self.pca_analyzer.factor_stability_scores.values()))
            stability_bonus = avg_stability * 15  # Max 15% from stability

            # Model age penalty (older models are less reliable)
            age_penalty = min(10, self._get_model_age_days() * 0.1)  # Max 10% penalty

            final_confidence = base_confidence + stability_bonus - age_penalty

            return max(0, min(100, final_confidence))

        except Exception:
            return 0.0

    def _get_universe_returns(self) -> pd.DataFrame:
        """
        IMPROVEMENT: Get universe returns with caching and smart updates
        """
        # Check if we need to update universe
        if (self.last_universe_update is None or
            (pd.Timestamp.now() - self.last_universe_update).days >= self.refit_frequency):

            self._update_universe()

        # Return cached universe returns
        if 'returns_matrix' in self.universe_cache:
            return self.universe_cache['returns_matrix']
        else:
            return pd.DataFrame()

    def _update_universe(self):
        """Update universe data and refit PCA model"""

        print(f"Updating PCA universe...")

        # IMPROVEMENT: Use a more sophisticated universe selection
        universe_symbols = self._select_universe_symbols()

        # Get returns data
        all_returns = pd.DataFrame()

        for symbol in universe_symbols:
            try:
                data = self.data_fetcher.extract_data(symbol, period="1y", interval="1d")
                if len(data) > 100:  # Minimum data requirement
                    returns = data['Close'].pct_change()
                    all_returns[symbol] = returns
            except Exception as e:
                print(f"Failed to fetch {symbol}: {e}")
                continue

        # Filter for data quality
        min_observations = len(all_returns) * 0.8
        valid_stocks = all_returns.columns[all_returns.count() >= min_observations]
        all_returns = all_returns[valid_stocks]

        if len(all_returns.columns) >= self.n_factors:
            # Fit PCA model
            self.pca_analyzer.fit(all_returns)

            # Cache results
            self.universe_cache = {
                'returns_matrix': all_returns,
                'symbols': valid_stocks.tolist(),
                'update_date': pd.Timestamp.now()
            }
            self.last_universe_update = pd.Timestamp.now()

            print(f"Universe updated with {len(valid_stocks)} stocks")
        else:
            print(f"Insufficient stocks for PCA: {len(all_returns.columns)} < {self.n_factors}")

    def _select_universe_symbols(self) -> List[str]:
        """
        IMPROVEMENT: Smarter universe selection based on liquidity and diversification
        """
        # Use a diversified, liquid universe
        liquid_universe = [
            # Large Cap Tech
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX',
            # Finance
            'JPM', 'BAC', 'WFC', 'GS', 'MS', 'BLK', 'AXP',
            # Healthcare
            'JNJ', 'PFE', 'UNH', 'ABBV', 'MRK', 'TMO',
            # Consumer
            'PG', 'KO', 'PEP', 'WMT', 'COST', 'HD', 'NKE', 'MCD',
            # Industrial
            'BA', 'CAT', 'GE', 'HON', 'MMM', 'RTX',
            # Energy
            'XOM', 'CVX', 'COP', 'EOG',
            # Communication
            'VZ', 'T', 'DIS', 'CMCSA'
        ]

        # Return up to universe_size symbols
        return liquid_universe[:self.universe_size]

    def _calculate_factor_signal(self, symbol: str, df: pd.DataFrame,
                               factor_scores: Dict, universe_returns: pd.DataFrame) -> Tuple[str, float]:
        """Calculate signal based on factor analysis"""

        base_signal = 'HOLD'
        base_confidence = 0.0

        if symbol == 'UNKNOWN' or symbol not in universe_returns.columns:
            return base_signal, base_confidence

        # Get factor exposures for this symbol
        if symbol in self.pca_analyzer.factor_loadings.index:
            exposures = self.pca_analyzer.factor_loadings.loc[symbol]
        else:
            return base_signal, base_confidence

        if self.strategy_type == 'factor_momentum':
            # Buy if exposed to factors with positive momentum
            momentum_score = 0
            for factor, scores in factor_scores.items():
                factor_num = int(factor.split('_')[1]) - 1
                exposure = exposures.iloc[factor_num]
                momentum_score += exposure * scores['momentum']

            if momentum_score > 0.001:  # Positive momentum threshold
                base_signal = 'BUY'
                base_confidence = min(100, abs(momentum_score) * 5000)
            elif momentum_score < -0.001:
                base_signal = 'SELL'
                base_confidence = min(100, abs(momentum_score) * 5000)

        elif self.strategy_type == 'factor_mean_reversion':
            # Buy if factors are oversold (negative Z-scores)
            reversion_score = 0
            for factor, scores in factor_scores.items():
                factor_num = int(factor.split('_')[1]) - 1
                exposure = exposures.iloc[factor_num]
                reversion_score += exposure * (-scores['z_score'])  # Negative for contrarian

            if reversion_score > 0.5:  # Oversold threshold
                base_signal = 'BUY'
                base_confidence = min(100, reversion_score * 30)
            elif reversion_score < -0.5:
                base_signal = 'SELL'
                base_confidence = min(100, abs(reversion_score) * 30)

        elif self.strategy_type == 'low_beta':
            # Buy if low systematic risk (defensive strategy)
            systematic_risk = sum(exposures ** 2)
            if systematic_risk < 0.7:  # Low beta threshold
                base_signal = 'BUY'
                base_confidence = min(100, (1 - systematic_risk) * 100)

        elif self.strategy_type == 'idiosyncratic':
            # Buy if high idiosyncratic component
            idiosyncratic_weight = 1 - sum(exposures ** 2)
            if idiosyncratic_weight > 0.4:  # High idiosyncratic threshold
                # Check recent price momentum for stock-specific opportunities
                recent_returns = df['Close'].pct_change().tail(10)
                if len(recent_returns.dropna()) > 5:
                    momentum = recent_returns.mean()
                    if momentum > 0.002:  # Positive idiosyncratic momentum
                        base_signal = 'BUY'
                        base_confidence = min(100, idiosyncratic_weight * momentum * 5000)

        return base_signal, base_confidence

    def _generate_factor_reasoning(self, symbol: str, signal: str,
                                 factor_scores: Dict, strategy_type: str) -> str:
        """Generate human-readable reasoning for factor signal"""

        if strategy_type == 'factor_momentum':
            dominant_factor = max(factor_scores.keys(),
                                key=lambda f: abs(factor_scores[f]['momentum']))
            momentum = factor_scores[dominant_factor]['momentum']

            interpretations = self.pca_analyzer.get_factor_interpretation()
            factor_desc = interpretations.get(dominant_factor, dominant_factor)

            reason = f"Factor momentum: {factor_desc} shows "
            reason += f"{'positive' if momentum > 0 else 'negative'} momentum ({momentum:.4f})"

        elif strategy_type == 'factor_mean_reversion':
            extreme_factor = max(factor_scores.keys(),
                               key=lambda f: abs(factor_scores[f]['z_score']))
            z_score = factor_scores[extreme_factor]['z_score']

            reason = f"Factor mean reversion: {extreme_factor} at "
            reason += f"{z_score:.2f} standard deviations from mean"

        elif strategy_type == 'low_beta':
            reason = f"Low beta strategy: {symbol} has low systematic risk exposure"

        elif strategy_type == 'idiosyncratic':
            reason = f"Idiosyncratic strategy: {symbol} shows stock-specific opportunity"

        else:
            reason = f"PCA factor analysis suggests {signal.lower()} signal"

        return reason

    def _get_model_age_days(self) -> int:
        """Get age of current PCA model in days"""
        if self.pca_analyzer.fit_date is None:
            return 999  # Very old

        return (pd.Timestamp.now() - self.pca_analyzer.fit_date).days

    def apply_sentiment_filter(self, base_signal: str, base_confidence: float,
                             sentiment: float) -> Tuple[str, float]:
        """Apply PCA-specific sentiment filtering"""

        adjusted_confidence = base_confidence

        # Factor strategies are generally less sensitive to sentiment
        # but extreme sentiment can override factor signals

        if abs(sentiment) > 0.6:  # Extreme sentiment
            if base_signal == 'BUY' and sentiment < -0.6:
                adjusted_confidence *= 0.7  # Reduce bullish confidence in extreme negativity
            elif base_signal == 'SELL' and sentiment > 0.6:
                adjusted_confidence *= 0.7  # Reduce bearish confidence in extreme positivity

        # Factor momentum strategies benefit from sentiment alignment
        if self.strategy_type == 'factor_momentum':
            if base_signal == 'BUY' and sentiment > 0.2:
                adjusted_confidence = min(100, adjusted_confidence * 1.1)
            elif base_signal == 'SELL' and sentiment < -0.2:
                adjusted_confidence = min(100, adjusted_confidence * 1.1)

        return base_signal, adjusted_confidence

    def get_required_indicators(self) -> List[str]:
        """Return required indicators (computed internally)"""
        return ['Close', 'factor_exposures', 'factor_scores']

    def _get_strategy_metadata(self) -> StrategyMetadata:
        """Return strategy metadata"""

        category_map = {
            'factor_momentum': 'MOMENTUM',
            'factor_mean_reversion': 'MEAN_REVERSION',
            'low_beta': 'DEFENSIVE',
            'idiosyncratic': 'ALPHA'
        }

        return StrategyMetadata(
            name=f"PCA {self.strategy_type.replace('_', ' ').title()}",
            category=category_map.get(self.strategy_type, 'FACTOR'),
            typical_holding_days=15,
            works_best_in=f"Diversified markets with clear factor structure",
            min_confidence=65.0,
            max_positions=1,
            requires_pairs=False
        )
