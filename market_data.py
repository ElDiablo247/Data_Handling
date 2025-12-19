import yfinance as yf
from datetime import datetime


class MarketData:
    def __init__(self, ticker_symbol: str):
        """
        Initializes the MarketData object for a given ticker symbol.

        This constructor sets up the instance by initializing data fields to None
        and then immediately calls a helper method to fetch, process, and populate
        the asset data.

        Args:
            ticker_symbol (str): The ticker symbol of the asset (e.g., 'AAPL').
        """
        self.ticker_name = ticker_symbol
        self.ticker = yf.Ticker(ticker_symbol)
        # Validate that the ticker object contains information and extract it.
        if not self.ticker.info:
            raise ValueError(f"Asset '{ticker_symbol}' not found or no data available.")
        self.asset_info = self.ticker.info


    def get_price(self) -> float:
        """Returns the validated asset price."""
        asset_price = self.asset_info.get('regularMarketPrice', None)
        if asset_price is None:
            raise ValueError(f"Market price for '{self.ticker_name}' is missing or not provided by the API.")
        return asset_price

    def get_asset_type(self) -> str:
        """Returns the asset's quote type."""
        asset_type = self.asset_info.get('quoteType', "N/A")
        return asset_type

    def get_sector(self) -> str:
        """Returns the asset's sector."""
        asset_sector = self.asset_info.get('sector', "N/A")
        return asset_sector
    
    def get_datetime(self) -> datetime:
        """Returns the datetime when the asset data was fetched."""
        datetime_variable = datetime.now()
        return datetime_variable