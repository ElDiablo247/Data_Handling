import yfinance as yf
import pandas as pd
from datetime import datetime


ticker_symbol = yf.Ticker("BTC-USD")
data = ticker_symbol.info
market_state = data.get('marketState')
print(market_state)

price = data.get('regularMarketPrice')

print(price)

