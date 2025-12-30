import yfinance as yf
import pandas as pd
from decimal import Decimal
import random
import string
from backend_manager import BackendManager
from user import User
from market_data import MarketData
from position import Position


class System:
    def __init__(self, backend_manager: BackendManager):
        """Initializes the System with a dependency on the BackendManager."""
        self.backend_manager = backend_manager


    def get_account_balance(self, user_id: str) -> float:
        """
        Calls the BackendManager to get the account balance for a given user ID.
        
        Args:
            user_id (str): The unique identifier of the user.
        
        Returns:
            float: A float representing the current account balance of the user with the given user ID.
        """
        local_account_balance = self.backend_manager.get_account_balance(user_id)
        if not local_account_balance:
            raise ValueError(f"No user with user ID '{user_id}' found.")
        return float(local_account_balance[0])
    
    def top_up_account_balance(self, user: User, amount: float):
        """
        Tops up the account balance for the logged-in user by a specified amount.

        This function validates the input amount to ensure it is a positive number.
        It then delegates the database update operation to the BackendManager.

        Args:
            user (User): The currently logged-in user object.
            amount (float): The amount to add to the user's account balance.

        Raises:
            ValueError: If the input amount is not a positive number.
        """
        if amount < 1:
            raise ValueError("Top-up amount must be a minimum of 1.")
        
        user_id = user.get_user_id()
        self.backend_manager.increase_user_balance(user_id, Decimal(str(amount)))

    def open_position(self, user: User, ticker_symbol: str, position_amount: float):
        """
        Orchestrates opening a new position for the logged-in user.

        This function handles the entire process of opening a position in a single atomic transaction:
        1. Validates the input amount and user's available funds.
        2. Fetches live market data for the asset via an API call.
        3. Calculates the number of shares based on the current price.
        4. Generates unique IDs for the position and the trade event.
        5. Inserts records into the 'positions' and 'trades' tables.
        6. Deducts the cost from the user's account balance.

        Args:
            user (User): The currently logged-in user object.
            ticker_symbol (str): The ticker of the asset to buy (e.g., 'AAPL').
            position_amount (float): The amount of cash to invest in this position.
        """
        user_id = user.get_user_id()
        
        # Checks for valid inputs
        if not isinstance(ticker_symbol, str): 
            raise TypeError("Ticker symbol must be a string.")
        if self.get_account_balance(user_id) < position_amount:
            raise ValueError(f"Insufficient funds for this operation. Increase your balance or reduce the position amount.")
        
        # Retrieve ticker symbol data using a MarketData instance
        market_data = MarketData(ticker_symbol)
        current_asset_price = market_data.get_price()
        asset_type = market_data.get_asset_type()
        asset_sector = market_data.get_sector() 
        datetime_now = market_data.get_datetime()
        position = Position()

        with self.backend_manager.engine.begin() as connection:
            new_position_id = self.generate_position_id(connection=connection)
            new_trade_id = self.generate_trade_id(connection=connection)
        
            # Assemble a dictionary with the acquired data for the new position.
            position.set_open_position_data(
                position_id=new_position_id,
                user_id=user_id,
                trade_id=new_trade_id,
                ticker=ticker_symbol,
                amount=position_amount,
                open_price=current_asset_price,
                asset_type=asset_type,
                sector=asset_sector,
                datetime=datetime_now
            )

            # Delegate database operations to the BackendManager.
            self.backend_manager.insert_position(position, connection=connection)
            self.backend_manager.insert_buy_trade(new_trade_id, position, connection=connection)
            self.backend_manager.decrease_user_balance(user_id, position.amount, connection=connection)

    def generate_position_id(self, connection=None) -> str:
        """
        Generates a unique, random ID for a new position.

        This function repeatedly generates an 8-character ID and checks for its
        uniqueness in the 'trades' table until a free ID is found. The 'trades'
        table is used for the check as it is the master ledger of all position
        IDs that have ever existed. This process is designed to run within a
        larger transaction to prevent race conditions.

        Args:
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the uniqueness check. Defaults to None.

        Returns:
            str: A guaranteed unique 8-character position ID.
        """
        while True:
            # Generate parts according to format: Letter Letter Number Number Letter Number Number Number
            letter_1 = random.choice(string.ascii_uppercase)
            letter_2 = random.choice(string.ascii_uppercase)
            two_digits = f"{random.randint(0, 99):02d}"  # Two digits (00-99)
            letter_3 = random.choice(string.ascii_uppercase)
            three_digits = f"{random.randint(0, 999):03d}" # Three digits (000-999)

            combined_id = f"{letter_1}{letter_2}{two_digits}{letter_3}{three_digits}"

            # Check if this position_id already exists
            position_id_exists = self.backend_manager.position_id_exists(combined_id, connection=connection)
            
            if not position_id_exists:
                return combined_id

    def generate_trade_id(self, connection=None) -> str:
        """
        Generates a unique, random ID for a new trade event.

        This function repeatedly generates a 12-character ID and checks for its
        uniqueness in the 'trades' table until a free ID is found. This process
        is designed to run within a larger transaction to prevent race conditions.

        Args:
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the uniqueness check. Defaults to None.

        Returns:
            str: A guaranteed unique 12-character trade ID.
        """
        while True:
            # Generate parts according to format: 4 digits, 2 letters, 3 digits, 3 letters
            four_digits = f"{random.randint(0, 9999):04d}"
            two_letters = "".join(random.choices(string.ascii_uppercase, k=2))
            three_digits = f"{random.randint(0, 999):03d}"
            three_letters = "".join(random.choices(string.ascii_uppercase, k=3))
            combined_id = f"{four_digits}{two_letters}{three_digits}{three_letters}"
            
            # Check if this trade_id already exists
            trade_id_exists = self.backend_manager.trade_id_exists(combined_id, connection=connection)   

            if not trade_id_exists:
                return combined_id

    def close_position(self, user: User, position_id: str):
        """
        Orchestrates closing an existing position for the logged-in user.

        This function handles the entire process of closing a position in a single atomic transaction:
        1. Deletes the position from the active 'positions' table and retrieves its data.
        2. Fetches live market data for the asset to get the closing price.
        3. Calculates the final value of the position and the resulting profit or loss.
        4. Generates a unique ID for the 'SELL' trade event.
        5. Inserts a historical record into the 'trades' table.
        6. Adds the proceeds from the sale back to the user's account balance.

        Args:
            user (User): The currently logged-in user object.
            position_id (str): The ID of the position to close.

        Raises:
            ValueError: If the position ID does not exist or does not belong to the user.
        """
        position = Position()
        user_id = user.get_user_id()

        with self.backend_manager.engine.begin() as connection:
            # Generate a unique ID for this 'SELL' trade event within the transaction
            trade_id = self.generate_trade_id(connection=connection)
            
            closed_position_data = self.backend_manager.delete_position_db(user_id, position_id, connection=connection)
            market_data = MarketData(closed_position_data.position_ticker)
            current_asset_price = market_data.get_price()
            datetime_now = market_data.get_datetime()

            position.set_close_position_data(closed_position_data, trade_id, current_asset_price, datetime_now, state='SELL'
            )

            self.backend_manager.insert_sell_trade(position, connection=connection)

            # Return the proceeds from the sale to the user's account balance
            closing_value = position.asset_share * position.close_price
            self.backend_manager.increase_user_balance(user_id, closing_value, connection=connection)