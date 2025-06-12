import yfinance as yf
import pandas as pd
import time
import random

class StockDataFetcher:
    """
    Pure data extraction class focused on fetching data from Yahoo Finance API
    All data processing/indicator methods moved to StockDataTools

    Note: Yahoo Finance now handles sessions internally, so we don't create custom sessions
    """

    def __init__(self, max_retries=3):
        self.max_retries = max_retries

    def extract_data(self, symbol, period="1y", interval="1d"):
        """
        Extract raw OHLCV data from Yahoo Finance for a single symbol
        """
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    delay = random.uniform(1, 3)
                    print(f"  Retrying {symbol} in {delay:.1f}s (attempt {attempt + 1})")
                    time.sleep(delay)

                ticker = yf.Ticker(symbol)
                df = ticker.history(period=period, interval=interval)

                if df.empty:
                    print(f"  No data available for {symbol}")
                    continue

                df = df.dropna(how='all')
                df = df.ffill().bfill()

                try:
                    current_info = ticker.info
                    current_price = current_info.get('currentPrice', current_info.get('regularMarketPrice'))
                    df.attrs['current_price'] = current_price
                    df.attrs['symbol'] = symbol
                except:
                    df.attrs['current_price'] = df['Close'].iloc[-1] if not df.empty else None
                    df.attrs['symbol'] = symbol

                return df

            except Exception as e:
                print(f"  Error fetching {symbol} (attempt {attempt + 1}): {str(e)}")
                if attempt == self.max_retries - 1:
                    print(f"  Failed to fetch {symbol} after {self.max_retries} attempts")
                    return pd.DataFrame()

        return pd.DataFrame()

    def get_multiple_stocks(self, symbol_list, period="1y", interval="1d"):
        data = {}
        failed_symbols = []

        for i, symbol in enumerate(symbol_list):
            print(f'Fetching data from {symbol} ({i+1}/{len(symbol_list)})')

            if i > 0:
                time.sleep(random.uniform(0.5, 1.5))

            stock_data = self.extract_data(symbol, period, interval)

            if stock_data is not None and not stock_data.empty:
                data[symbol] = stock_data
                print(f'  Successfully fetched {symbol}')
            else:
                failed_symbols.append(symbol)
                print(f'  Failed to fetch {symbol}')

        if failed_symbols:
            print(f"\nFailed to fetch data for: {failed_symbols}")

        return data

    def get_top_stocks(self, period="1y", interval="1d"):
        major_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B',
            'UNH', 'XOM', 'JNJ', 'JPM', 'V', 'PG', 'MA', 'CVX', 'HD', 'PFE',
            'ABBV', 'BAC', 'KO', 'AVGO', 'PEP', 'TMO', 'COST', 'WMT', 'DIS',
            'ABT', 'CRM', 'ACN', 'VZ', 'ADBE', 'DHR', 'TXN', 'NEE', 'NKE',
            'LIN', 'RTX', 'QCOM', 'MCD', 'UPS', 'PM', 'T', 'HON', 'AMGN',
            'LOW', 'COP', 'IBM', 'ELV'
        ]

        print(f"Fetching {len(major_symbols)} major stocks...")
        return self.get_multiple_stocks(major_symbols, period, interval)

    def get_market_data(self, period="1y", interval="1d"):
        market_symbols = ['SPY', '^VIX', 'QQQ', '^GSPC', '^DJI', '^IXIC']
        print("Fetching market data...")
        return self.get_multiple_stocks(market_symbols, period, interval)

    def get_sector_etfs(self, period="1y", interval="1d"):
        sector_symbols = [
            'XLK', 'XLF', 'XLV', 'XLE', 'XLI', 'XLY', 'XLP', 'XLB', 'XLU', 'XLRE', 'XLC'
        ]

        print("Fetching sector ETF data...")
        return self.get_multiple_stocks(sector_symbols, period, interval)

    def get_international_data(self, period="1y", interval="1d"):
        international_symbols = [
            'EFA', 'EEM', 'VGK', 'VWO', 'FXI', 'EWJ', '^FTSE', '^N225'
        ]

        print("Fetching international market data...")
        return self.get_multiple_stocks(international_symbols, period, interval)

    def get_commodity_data(self, period="1y", interval="1d"):
        commodity_symbols = [
            'GLD', 'SLV', 'USO', 'UNG', 'DBA', 'UUP', 'TLT', 'HYG'
        ]

        print("Fetching commodity and currency data...")
        return self.get_multiple_stocks(commodity_symbols, period, interval)

    def validate_data(self, df, symbol):
        issues = []

        if df.empty:
            return False, ["DataFrame is empty"]

        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, [f"Missing required columns: {missing_cols}"]

        for col in ['Open', 'High', 'Low', 'Close']:
            if (df[col] <= 0).any():
                count = (df[col] <= 0).sum()
                issues.append(f"Invalid prices in {col}: {count} rows")

        ohlc_violations = (df['High'] < df['Low']).sum()
        if ohlc_violations > 0:
            issues.append(f"High < Low violations: {ohlc_violations} rows")

        daily_returns = df['Close'].pct_change().abs()
        extreme_moves = (daily_returns > 0.5).sum()
        if extreme_moves > 0:
            issues.append(f"{extreme_moves} days with >50% price moves (review for data quality)")

        negative_volume = (df['Volume'] < 0).sum()
        if negative_volume > 0:
            issues.append(f"Negative volume detected: {negative_volume} rows")

        null_counts = df.isnull().sum()
        significant_nulls = null_counts[null_counts > len(df) * 0.05]
        if len(significant_nulls) > 0:
            issues.append(f"Significant missing values: {significant_nulls.to_dict()}")

        if len(df) > 0:
            last_date = df.index[-1]
            import datetime
            days_old = (datetime.datetime.now(last_date.tz) - last_date).days
            if days_old > 7:
                issues.append(f"Data may be outdated: last date is {days_old} days old")

        critical_issues = [issue for issue in issues if
                          "DataFrame is empty" in issue or
                          "Missing required columns" in issue or
                          "Invalid prices" in issue]

        is_valid = len(critical_issues) == 0

        if issues:
            print(f"Data quality report for {symbol}:")
            for issue in issues:
                severity = "CRITICAL" if any(crit in issue for crit in ["empty", "Missing required", "Invalid prices"]) else "WARNING"
                print(f"  {severity}: {issue}")

        return is_valid, issues

    def test_connection(self):
        print("Testing Yahoo Finance connection...")
        test_data = self.extract_data('AAPL', period='5d', interval='1d')

        if not test_data.empty:
            print("Connection successful!")
            print(f"  Sample data shape: {test_data.shape}")
            print(f"  Date range: {test_data.index[0]} to {test_data.index[-1]}")
            return True
        else:
            print("Connection failed!")
            return False

    def get_ticker_info(self, symbol):
        try:
            ticker = yf.Ticker(symbol)
            return ticker.info
        except Exception as e:
            print(f"Error fetching info for {symbol}: {e}")
            return {}

    def get_earnings_dates(self, symbol):
        try:
            ticker = yf.Ticker(symbol)
            return ticker.calendar
        except Exception as e:
            print(f"Error fetching earnings for {symbol}: {e}")
            return pd.DataFrame()

    def get_options_data(self, symbol, expiry_date=None):
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
            print(f"Error fetching options for {symbol}: {e}")
            return {}


# Convenience functions
def extract_data(symbol, period="1y", interval="1d"):
    fetcher = StockDataFetcher()
    return fetcher.extract_data(symbol, period, interval)

def get_multiple_stocks(symbol_list, period="1y", interval="1d"):
    fetcher = StockDataFetcher()
    return fetcher.get_multiple_stocks(symbol_list, period, interval)

def get_market_data(period="1y", interval="1d"):
    fetcher = StockDataFetcher()
    return fetcher.get_market_data(period, interval)


# Example usage
if __name__ == "__main__":
    print("=" * 60)
    print("YAHOO FINANCE DATA FETCHER TEST")
    print("=" * 60)

    try:
        fetcher = StockDataFetcher()

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

            print("\nAll tests completed.")

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
