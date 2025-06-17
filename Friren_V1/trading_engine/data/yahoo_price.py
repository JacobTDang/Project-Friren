"""
Yahoo Finance Pure Price Data Fetcher

Handles ONLY price/volume data from Yahoo Finance.
No news scraping or sentiment analysis - pure OHLCV focus.

Enhanced for multiprocess trading system with Alpaca websocket integration.
"""

import yfinance as yf
import pandas as pd
import time
import random
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
import logging

class YahooFinancePriceData:
    """
    Pure OHLCV data extraction from Yahoo Finance API

    Focus: Historical price and volume data only
    No news content, no sentiment, no external dependencies
    Optimized for multiprocess trading system usage
    """

    def __init__(self, max_retries: int = 3, rate_limit_delay: float = 0.5):
        self.max_retries = max_retries
        self.rate_limit_delay = rate_limit_delay
        self.logger = logging.getLogger(f"{__name__}.YahooFinancePriceData")

        # Cache for ticker info to reduce API calls
        self._ticker_info_cache = {}
        self._cache_expiry = {}
        self._cache_duration = timedelta(hours=1)  # Cache ticker info for 1 hour

    def extract_data(self, symbol: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
        """
        Extract raw OHLCV data from Yahoo Finance for a single symbol

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            period: Data period ('1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max')
            interval: Data interval ('1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo')

        Returns:
            pd.DataFrame: OHLCV data with datetime index
        """
        symbol = symbol.upper().strip()

        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    delay = random.uniform(1, 3)
                    self.logger.info(f"Retrying {symbol} in {delay:.1f}s (attempt {attempt + 1})")
                    time.sleep(delay)

                ticker = yf.Ticker(symbol)
                df = ticker.history(period=period, interval=interval)

                if df.empty:
                    self.logger.warning(f"No data available for {symbol}")
                    if attempt == self.max_retries - 1:
                        return pd.DataFrame()
                    continue

                # Clean the data
                df = df.dropna(how='all')
                df = df.ffill().bfill()

                # Add metadata attributes for downstream usage
                df.attrs['symbol'] = symbol
                df.attrs['period'] = period
                df.attrs['interval'] = interval
                df.attrs['fetch_time'] = datetime.now()

                # Try to get current price information
                try:
                    current_price = self._get_current_price(ticker)
                    df.attrs['current_price'] = current_price
                except Exception as e:
                    self.logger.debug(f"Could not get current price for {symbol}: {e}")
                    df.attrs['current_price'] = df['Close'].iloc[-1] if not df.empty else None

                self.logger.debug(f"Successfully fetched {symbol}: {len(df)} rows")
                return df

            except Exception as e:
                self.logger.warning(f"Error fetching {symbol} (attempt {attempt + 1}): {str(e)}")
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Failed to fetch {symbol} after {self.max_retries} attempts")
                    return pd.DataFrame()

        return pd.DataFrame()

    def get_multiple_stocks(self, symbol_list: List[str], period: str = "1y",
                          interval: str = "1d", show_progress: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Fetch OHLCV data for multiple symbols with rate limiting

        Args:
            symbol_list: List of stock symbols
            period: Data period
            interval: Data interval
            show_progress: Whether to print progress updates

        Returns:
            Dict[str, pd.DataFrame]: Symbol -> OHLCV DataFrame mapping
        """
        data = {}
        failed_symbols = []

        for i, symbol in enumerate(symbol_list):
            if show_progress:
                self.logger.info(f'Fetching {symbol} ({i+1}/{len(symbol_list)})')

            # Rate limiting
            if i > 0:
                time.sleep(random.uniform(self.rate_limit_delay, self.rate_limit_delay * 2))

            stock_data = self.extract_data(symbol, period, interval)

            if stock_data is not None and not stock_data.empty:
                data[symbol] = stock_data
                if show_progress:
                    self.logger.debug(f'Successfully fetched {symbol}')
            else:
                failed_symbols.append(symbol)
                if show_progress:
                    self.logger.warning(f'Failed to fetch {symbol}')

        if failed_symbols:
            self.logger.warning(f"Failed to fetch data for: {failed_symbols}")

        return data

    def get_market_data(self, period: str = "1y", interval: str = "1d") -> Dict[str, pd.DataFrame]:
        """Get major market indices and VIX data"""
        market_symbols = ['SPY', '^VIX', 'QQQ', '^GSPC', '^DJI', '^IXIC']
        self.logger.info("Fetching market data...")
        return self.get_multiple_stocks(market_symbols, period, interval)

    def get_sector_etfs(self, period: str = "1y", interval: str = "1d") -> Dict[str, pd.DataFrame]:
        """Get sector ETF data for market analysis"""
        sector_symbols = [
            'XLK', 'XLF', 'XLV', 'XLE', 'XLI', 'XLY', 'XLP', 'XLB', 'XLU', 'XLRE', 'XLC'
        ]
        self.logger.info("Fetching sector ETF data...")
        return self.get_multiple_stocks(sector_symbols, period, interval)

    def get_top_stocks(self, period: str = "1y", interval: str = "1d") -> Dict[str, pd.DataFrame]:
        """Get data for major large-cap stocks"""
        major_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B',
            'UNH', 'XOM', 'JNJ', 'JPM', 'V', 'PG', 'MA', 'CVX', 'HD', 'PFE',
            'ABBV', 'BAC', 'KO', 'AVGO', 'PEP', 'TMO', 'COST', 'WMT', 'DIS',
            'ABT', 'CRM', 'ACN', 'VZ', 'ADBE', 'DHR', 'TXN', 'NEE', 'NKE',
            'LIN', 'RTX', 'QCOM', 'MCD', 'UPS', 'PM', 'T', 'HON', 'AMGN',
            'LOW', 'COP', 'IBM', 'ELV'
        ]
        self.logger.info(f"Fetching {len(major_symbols)} major stocks...")
        return self.get_multiple_stocks(major_symbols, period, interval)

    def get_international_data(self, period: str = "1y", interval: str = "1d") -> Dict[str, pd.DataFrame]:
        """Get international market data"""
        international_symbols = [
            'EFA', 'EEM', 'VGK', 'VWO', 'FXI', 'EWJ', '^FTSE', '^N225'
        ]
        self.logger.info("Fetching international market data...")
        return self.get_multiple_stocks(international_symbols, period, interval)

    def get_commodity_data(self, period: str = "1y", interval: str = "1d") -> Dict[str, pd.DataFrame]:
        """Get commodity and currency proxy data"""
        commodity_symbols = [
            'GLD', 'SLV', 'USO', 'UNG', 'DBA', 'UUP', 'TLT', 'HYG'
        ]
        self.logger.info("Fetching commodity and currency data...")
        return self.get_multiple_stocks(commodity_symbols, period, interval)

    def get_ticker_info(self, symbol: str, use_cache: bool = True) -> Dict:
        """
        Get ticker information (company details, financials)

        Args:
            symbol: Stock symbol
            use_cache: Whether to use cached data if available

        Returns:
            Dict: Ticker information
        """
        symbol = symbol.upper().strip()

        # Check cache first
        if use_cache and symbol in self._ticker_info_cache:
            cache_time = self._cache_expiry.get(symbol, datetime.min)
            if datetime.now() - cache_time < self._cache_duration:
                return self._ticker_info_cache[symbol]

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            # Cache the result
            if use_cache:
                self._ticker_info_cache[symbol] = info
                self._cache_expiry[symbol] = datetime.now()

            return info
        except Exception as e:
            self.logger.error(f"Error fetching info for {symbol}: {e}")
            return {}

    def get_earnings_dates(self, symbol: str) -> pd.DataFrame:
        """Get earnings calendar for symbol"""
        try:
            ticker = yf.Ticker(symbol)
            return ticker.calendar
        except Exception as e:
            self.logger.error(f"Error fetching earnings for {symbol}: {e}")
            return pd.DataFrame()

    def get_options_data(self, symbol: str, expiry_date: Optional[str] = None) -> Dict:
        """
        Get options chain data for symbol

        Args:
            symbol: Stock symbol
            expiry_date: Specific expiry date (YYYY-MM-DD) or None for nearest

        Returns:
            Dict: Options data with 'calls', 'puts', and 'expiry' keys
        """
        try:
            ticker = yf.Ticker(symbol)

            if expiry_date:
                options = ticker.option_chain(expiry_date)
            else:
                expiry_dates = ticker.options
                if expiry_dates:
                    options = ticker.option_chain(expiry_dates[0])
                else:
                    return {}

            return {
                'calls': options.calls,
                'puts': options.puts,
                'expiry': expiry_date or expiry_dates[0] if expiry_dates else None
            }
        except Exception as e:
            self.logger.error(f"Error fetching options for {symbol}: {e}")
            return {}

    def validate_data(self, df: pd.DataFrame, symbol: str) -> tuple[bool, List[str]]:
        """
        Validate OHLCV data quality

        Args:
            df: DataFrame to validate
            symbol: Symbol name for logging

        Returns:
            Tuple[bool, List[str]]: (is_valid, list_of_issues)
        """
        issues = []

        if df.empty:
            return False, ["DataFrame is empty"]

        # Check required columns
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, [f"Missing required columns: {missing_cols}"]

        # Check for invalid prices (negative or zero)
        for col in ['Open', 'High', 'Low', 'Close']:
            if (df[col] <= 0).any():
                count = (df[col] <= 0).sum()
                issues.append(f"Invalid prices in {col}: {count} rows")

        # Check OHLC relationships
        ohlc_violations = (df['High'] < df['Low']).sum()
        if ohlc_violations > 0:
            issues.append(f"High < Low violations: {ohlc_violations} rows")

        # Check for extreme daily moves (>50% in a day)
        daily_returns = df['Close'].pct_change().abs()
        extreme_moves = (daily_returns > 0.5).sum()
        if extreme_moves > 0:
            issues.append(f"{extreme_moves} days with >50% price moves (review for data quality)")

        # Check volume
        negative_volume = (df['Volume'] < 0).sum()
        if negative_volume > 0:
            issues.append(f"Negative volume detected: {negative_volume} rows")

        # Check for significant missing values
        null_counts = df.isnull().sum()
        significant_nulls = null_counts[null_counts > len(df) * 0.05]
        if len(significant_nulls) > 0:
            issues.append(f"Significant missing values: {significant_nulls.to_dict()}")

        # Check data freshness
        if len(df) > 0:
            last_date = df.index[-1]
            if hasattr(last_date, 'tz') and last_date.tz:
                days_old = (datetime.now(last_date.tz) - last_date).days
            else:
                days_old = (datetime.now() - last_date.replace(tzinfo=None)).days

            if days_old > 7:
                issues.append(f"Data may be outdated: last date is {days_old} days old")

        # Determine if data is valid (critical issues only)
        critical_issues = [issue for issue in issues if
                          "DataFrame is empty" in issue or
                          "Missing required columns" in issue or
                          "Invalid prices" in issue]

        is_valid = len(critical_issues) == 0

        # Log issues if any
        if issues and self.logger.isEnabledFor(logging.INFO):
            self.logger.info(f"Data quality report for {symbol}:")
            for issue in issues:
                severity = "CRITICAL" if any(crit in issue for crit in ["empty", "Missing required", "Invalid prices"]) else "WARNING"
                self.logger.info(f"  {severity}: {issue}")

        return is_valid, issues

    def test_connection(self) -> bool:
        """Test Yahoo Finance connection with a simple request"""
        self.logger.info("Testing Yahoo Finance connection...")
        test_data = self.extract_data('AAPL', period='5d', interval='1d')

        if not test_data.empty:
            self.logger.info("Connection successful!")
            self.logger.info(f"Sample data shape: {test_data.shape}")
            self.logger.info(f"Date range: {test_data.index[0]} to {test_data.index[-1]}")
            return True
        else:
            self.logger.error("Connection failed!")
            return False

    def get_real_time_price(self, symbol: str) -> Optional[float]:
        """
        Get current/real-time price for symbol
        Note: This is still limited by Yahoo Finance's rate limits

        Args:
            symbol: Stock symbol

        Returns:
            Optional[float]: Current price or None if unavailable
        """
        try:
            ticker = yf.Ticker(symbol)
            current_price = self._get_current_price(ticker)
            return current_price
        except Exception as e:
            self.logger.error(f"Error getting real-time price for {symbol}: {e}")
            return None

    def _get_current_price(self, ticker) -> Optional[float]:
        """Helper method to extract current price from ticker info"""
        try:
            info = ticker.info
            # Try multiple fields for current price
            price_fields = ['currentPrice', 'regularMarketPrice', 'previousClose', 'ask', 'bid']
            for field in price_fields:
                if field in info and info[field] is not None:
                    return float(info[field])
            return None
        except Exception:
            return None

    def get_websocket_compatible_symbols(self, symbols: List[str]) -> List[str]:
        """
        Filter symbols to ensure they're compatible with Alpaca websocket
        (This is preparation for integration with Alpaca real-time feeds)

        Args:
            symbols: List of symbols to check

        Returns:
            List[str]: Filtered list of compatible symbols
        """
        # For now, just clean the symbols - later this can be enhanced
        # to check against Alpaca's supported symbol list
        compatible = []
        for symbol in symbols:
            cleaned = symbol.upper().strip()
            # Remove Yahoo-specific prefixes (^) and suffixes that Alpaca doesn't support
            if not cleaned.startswith('^') and '.' not in cleaned:
                compatible.append(cleaned)

        return compatible


# Convenience functions for backwards compatibility
def extract_data(symbol: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    """Backwards compatible function"""
    fetcher = YahooFinancePriceData()
    return fetcher.extract_data(symbol, period, interval)

def get_multiple_stocks(symbol_list: List[str], period: str = "1y", interval: str = "1d") -> Dict[str, pd.DataFrame]:
    """Backwards compatible function"""
    fetcher = YahooFinancePriceData()
    return fetcher.get_multiple_stocks(symbol_list, period, interval)

def get_market_data(period: str = "1y", interval: str = "1d") -> Dict[str, pd.DataFrame]:
    """Backwards compatible function"""
    fetcher = YahooFinancePriceData()
    return fetcher.get_market_data(period, interval)


# Example usage and testing
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("YAHOO FINANCE PURE PRICE DATA TESTER")
    print("=" * 60)

    try:
        fetcher = YahooFinancePriceData()

        if fetcher.test_connection():
            print("\nYahoo Finance connection successful!")

            print("\nTesting single stock fetch...")
            aapl_data = fetcher.extract_data('AAPL', period='1mo')
            if not aapl_data.empty:
                print(f"AAPL data shape: {aapl_data.shape}")
                print(f"Date range: {aapl_data.index[0]} to {aapl_data.index[-1]}")
                print(f"Current price: ${aapl_data.attrs.get('current_price', 'N/A')}")
            else:
                print("Failed to fetch AAPL data")

            print("\nTesting multiple stocks fetch...")
            tech_stocks = ['AAPL', 'MSFT', 'GOOGL']
            tech_data = fetcher.get_multiple_stocks(tech_stocks, period='1mo')
            print(f"Fetched {len(tech_data)} out of {len(tech_stocks)} tech stocks")

            print("\nTesting market data fetch...")
            market_data = fetcher.get_market_data(period='1mo')
            print(f"Fetched {len(market_data)} market instruments")

            if 'AAPL' in tech_data:
                print("\nTesting data validation...")
                is_valid, issues = fetcher.validate_data(tech_data['AAPL'], 'AAPL')
                print(f"AAPL data validation: {'Valid' if is_valid else 'Invalid'}")
                if issues:
                    for issue in issues:
                        print(f"  Issue: {issue}")

            print("\nTesting real-time price...")
            current_price = fetcher.get_real_time_price('AAPL')
            print(f"AAPL current price: ${current_price}")

            print("\nAll tests completed successfully.")

        else:
            print("\nCannot proceed - Yahoo Finance connection failed")
            print("\nTroubleshooting tips:")
            print("1. Check your internet connection")
            print("2. Try updating yfinance: pip install --upgrade yfinance")
            print("3. If behind corporate firewall, check proxy settings")

    except Exception as e:
        print(f"\nTest failed with error: {e}")
        print("\nTry these solutions:")
        print("1. Update yfinance: pip install --upgrade yfinance")
        print("2. Install missing dependencies: pip install pandas numpy")
        print("3. Check internet connection")

    print("\n" + "=" * 60)
