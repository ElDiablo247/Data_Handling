import yfinance as yf
import pandas as pd
from datetime import datetime


ticker_symbol = yf.Ticker("AAPL")
data = ticker_symbol.info
market_state = data.get('marketState')
print(market_state)
price = data.get('regularMarketPrice')
unix_timestamp = data.get('regularMarketTime')
trade_time = datetime.fromtimestamp(unix_timestamp)
print(price, trade_time)