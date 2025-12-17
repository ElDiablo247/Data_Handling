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
        self._asset_price = None
        self._asset_type = None
        self._sector = None
        self._datetime = None
        self._get_asset_data(ticker_symbol)

    
    def _get_asset_data(self, ticker_symbol: str):
        """
        Fetches, processes, and stores market data for the given asset.

        This internal method makes a single API call to yfinance, validates the
        retrieved data, and populates the instance's data fields.

        Args:
            ticker_symbol (str): The ticker symbol of the asset to fetch.

        Raises:
            ValueError: If the asset is not found, the market state is invalid,
                        or the price is not a positive number.
        """
        # Initialize the yfinance Ticker object.
        ticker = yf.Ticker(ticker_symbol)

        # Validate that the ticker object contains information and extract it.
        if not ticker.info:
            raise ValueError(f"Asset '{ticker_symbol}' not found or no data available.")
        asset_info = ticker.info

        asset_price = asset_info.get('regularMarketPrice', None)
        if asset_price is None:
            raise ValueError(f"Market price for '{ticker_symbol}' is missing or not provided by the API.")
        
        datetime_now = datetime.now()
        self._asset_price = float(asset_price)
        self._datetime = datetime_now
        self._asset_type = asset_info.get('quoteType', "N/A")
        self._sector = asset_info.get('sector', "N/A")

    def get_price(self) -> float:
        """Returns the validated asset price."""
        return self._asset_price

    def get_asset_type(self) -> str:
        """Returns the asset's quote type."""
        return self._asset_type

    def get_sector(self) -> str:
        """Returns the asset's sector."""
        return self._sector
    
    def get_datetime(self) -> datetime:
        """Returns the datetime when the asset data was fetched."""
        return self._datetime