class Position:
    def __init__(self):
        """
        Initializes a new Position object with empty attributes.
        This allows the object to be used for both creating new positions
        and loading existing ones.
        """
        self.position_id = None
        self.user_id = None
        self.trade_id = None
        self.ticker = None
        self.amount = None
        self.open_price = None
        self.close_price = None
        self.asset_share = None
        self.asset_type = None
        self.sector = None
        self.open_datetime = None
        self.close_datetime = None
        self.loss_profit = None
        self.state = None

    def set_open_position_data(self, position_id: str, user_id: str, trade_id: str, ticker: str, amount: float, open_price: float, asset_type: str, sector: str, open_datetime):
        """
        Populates the position object with data for opening a new position.
        Calculates shares based on the provided price.
        
        Returns:
            dict: A dictionary ready for the backend to insert.
        """
        self.position_id = position_id
        self.user_id = user_id
        self.trade_id = trade_id
        self.ticker = ticker
        self.amount = amount
        self.open_price = open_price
        self.asset_type = asset_type
        self.sector = sector
        self.open_datetime = open_datetime
        self.state = 'BUY'

        # Perform the math
        self.asset_share = round(self.amount / self.open_price, 8)


    def to_position_dict(self) -> dict:
        """Returns the dictionary format expected by the BackendManager for the positions table."""
        return {
            'position_id': self.position_id,
            'user_id': self.user_id,
            'position_ticker': self.ticker,
            'position_amount': self.amount,
            'open_price': self.open_price,
            'asset_share': self.asset_share,
            'asset_type': self.asset_type,
            'sector': self.sector,
            'open_datetime': self.open_datetime
        }

    def to_trade_dict(self) -> dict:
        """Returns the dictionary format expected by the BackendManager for the trades table."""
        return {
            'trade_id': self.trade_id,
            'position_id': self.position_id,
            'user_id': self.user_id,
            'position_ticker': self.ticker,
            'position_amount': self.amount,
            'open_price': self.open_price,
            'close_price': self.close_price,
            'loss_profit': self.loss_profit,
            'asset_share': self.asset_share,
            'asset_type': self.asset_type,
            'sector': self.sector,
            'open_datetime': self.open_datetime,
            'close_datetime': self.close_datetime,
            'state': self.state
        }