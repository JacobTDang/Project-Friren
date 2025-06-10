import yfinance as yf
import pandas as pd
msft = yf.Ticker("MSFT")

# functions to build
# get_multiple_stocks, get_sp500_stocks, get_market_data, get_vix_data
# add_moving_averages, add_rsi, add_bollinger_bands
# save_to_database, load_from_database

# get_live_price, stream_prices - maybe for later on HFT implementation (that is like v2)

class StockDataExtractor:
  """
  Fetch data with yahoo finance api
  """

  def __init__(self):
    pass

  def extract_data(self, symbol, period="1y", interval = "1d"):
    """
    Extract data from selected symbol, period, interval
    returns stock data with ohlcv columns
    """

    ticker = yf.Ticker(symbol)
    # get historical data
    df = ticker.history(period=period, interval=interval)

    # get current data

    current_info = ticker.info
    current_price = current_info.get('currentPrice', current_info.get('regularMarketPrice'))
    df.attrs['current_price'] = current_price

    return df

  if __name__ == "__main__":
    print(msft.info)
