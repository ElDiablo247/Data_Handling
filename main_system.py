from sqlalchemy import create_engine, text
import random
import string
import bcrypt
from functools import wraps
import yfinance as yf
import pandas as pd
import datetime
import os
from dotenv import load_dotenv

class System:
    def __init__(self):
        
            
    def get_funds_db(self) -> float:
        """
        Function that gets the account balance from the database.
        
        Args:
            None
        
        Returns:
            float: A float representing the current funds in the user's account.
        """
        query = "SELECT funds FROM users WHERE user_id = :user_id"
        params = {"user_id": self.user_id}
        result = self.execute_query(query, params, fetch="one")
        if not result:
            raise ValueError("User id not found.")
        return float(result[0])
    
    def modify_funds_db(self, amount: float, connection=None):
        """
        Updates the logged-in user's funds in the database.
        Can increase or decrease based on the amount if possitve or negative.

        Args:
            amount (float): Positive or negative amount to change the balance by.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.

        Returns:
            None: Only prints the change in funds and the new balance.
        """
        if amount != 0:
            query = """
            UPDATE users 
            SET funds = funds + :delta 
            WHERE user_id = :uid
            RETURNING funds;
            """
            params = {"delta": amount, "uid": self.user_id}
            result = self.execute_query(query, params, fetch="one", connection=connection)

            # Check if the result is None which indicates error somewhere.
            if result is None:
                raise RuntimeError("Failed to update account balance. User ID may not exist.")
            new_balance = float(result[0])
            print(f"Balance updated by {amount}$. New balance: ${new_balance}")
        else:
            print(f"Amount was 0 so balance was not changed.")

    def open_position(self, asset_name: str, position_amount: float):
        """
        Orchestrates opening a new position for the logged-in user.

        This function handles the entire process of opening a position:
        1. Validates the input amount and user's available funds.
        2. Fetches live market data for the asset via an API call.
        3. Calculates the number of shares based on the current price.
        4. Atomically inserts the new position into the 'positions' table, logs the
           event to the 'user_history' table, and deducts the cost from the user's
           funds in a single database transaction.

        Args:
            asset_name (str): The name/ticker of the asset to buy (e.g., 'AAPL').
            position_amount (float): The amount of cash to invest in this position.

        Returns:
            None: On success, prints a confirmation message. Raises an error on failure.
        """
        # Checks for valid inputs
        if position_amount < 10:
            raise ValueError("Minimum amount to open a position is 10.")
        if not isinstance(asset_name, str): 
            raise TypeError("Asset name must be a string.")
        if self.get_funds_db() < position_amount:
            raise ValueError(f"Insufficient funds to open {asset_name} worth {position_amount}$.")
        
        # Retrieve asset data using the function get_asset_data (which also does validity checks)
        asset_data = self.get_asset_data_api(asset_name)
        local_position_id = self.id_generator("position")
        local_asset_price = asset_data[0]
        local_asset_share = self.calculate_asset_shares(local_asset_price, position_amount)
        local_asset_type = asset_data[1]
        local_asset_sector = asset_data[2]

        with self.engine.begin() as connection:
            # Insert the new position into the database
            query = """
            INSERT INTO positions (position_id, user_id, position_name, position_amount, open_price, asset_share, asset_type, sector)
            VALUES (:position_id, :user_id, :position_name, :position_amount, :open_price, :asset_share, :asset_type, :sector)
            RETURNING *;
            """
            params = {
                'position_id': local_position_id,
                'user_id': self.user_id,
                'position_name': asset_name,
                'position_amount': position_amount,
                'open_price': local_asset_price,
                'asset_share': local_asset_share,
                'asset_type': local_asset_type,
                'sector': local_asset_sector
            }
            position_object = self.execute_query(query, params, fetch="one", connection=connection)

            pos_amount = -float(position_amount) # Make the amount negative to be deducted from funds
            self.log_to_history('OPEN', position_object, connection=connection)

            self.modify_funds_db(pos_amount, connection=connection)

        print(f"Bought asset {asset_name} with position ID {local_position_id} at price {local_asset_price}$ and {local_asset_share} shares in sector {local_asset_sector}.")

    def calculate_asset_shares(self, asset_price: float, asset_amount: float) -> float:
        """
        Function that calculates the number of shares that can be bought with a given amount of money at a specific asset price.
        For example, if the asset price is $150 and the user wants to invest $300, the function will return 2.0 shares.

        Args:
            asset_price (float): The current price of the asset.
            asset_amount (float): The amount of money the investor wants to invest in the asset.
        
        Returns:
            float: The number of shares that can be bought, rounded to 8 decimal places.
        """
        shares = asset_amount / asset_price 
        return round(shares, 8)

    def close_asset(self, position_id: str = None, asset_name: str = None):
        """
        Orchestrates the closing of one or more positions in a single, atomic transaction.

        This function acts as a "smart manager" that determines what to close based
        on the provided arguments. It handles fetching the necessary position data
        and current asset price, then wraps all database modifications (logging
        transactions, deleting positions, and updating funds) in one transaction
        to ensure data integrity.

        - To close a single position, provide the `position_id`.
        - To close all positions for an asset, provide the `asset_name`.

        Args:
            position_id (str, optional): The unique ID of a single position to close.
            asset_name (str, optional): The name of an asset to close all positions for.

        Returns:
            None: Executes the closing process and prints confirmation messages.
        """
        # Validate that the function is called correctly with exclusive arguments.
        if not position_id and not asset_name:
            raise ValueError("You must provide either a position_id or an asset_name.")
        if position_id and asset_name:
            raise ValueError("Provide either a position_id or an asset_name, not both.")

        positions_list = []
        asset_current_price = None

        if position_id:
            position = self.get_position_db(position_id) # Store the position object in a variable
            position_name = position[2] 
            asset_current_price = self.get_asset_current_price(position_name)
            positions_list.append(position)
        else:
            query = "SELECT * FROM positions WHERE user_id = :user_id AND position_name = :asset_name;"
            params = {"user_id": self.user_id, "asset_name": asset_name}
            results = self.execute_query(query, params, fetch="all")
            if not results:
                raise ValueError(f"No open positions found for asset '{asset_name}' for user '{self.user_name}'.")
            positions_list = results
            asset_current_price = self.get_asset_current_price(asset_name)

        # Open a single transaction to ensure all operations succeed or fail together.
        with self.engine.begin() as connection:
            return_balance = self.close_position(positions_list, asset_current_price, connection)
            self.modify_funds_db(return_balance, connection)

    def close_position(self, positions_list: list, current_price: float, connection=None) -> float:
        """
        Processes a list of positions to be closed within an existing transaction.

        This function acts as an internal "worker" for the closing process. It
        iterates through a list of positions and performs the following steps for each:
        1. Calculate the profit or loss for each position.
        2. Log the completed trade in the transactions history.
        3. Log the closure in the user history table.
        4. Delete the position from the active positions table.

        Args:
            positions_list (list): A list of position tuples to be closed.
            current_price (float): The pre-fetched current price of the asset.
            connection (sqlalchemy.engine.Connection): An active SQLAlchemy connection.

        Returns:
            float: The total amount to be returned to the user's funds from all closed positions.
        """
        
        return_amount = 0.0

        # Iterate through each position provided in the list and extract position data for clarity
        for db_position_object in positions_list:  
            db_pos_id = db_position_object[0]
            db_pos_amount = float(db_position_object[3])
            db_pos_cost = float(db_position_object[4])
            db_asset_share = float(db_position_object[5])            
            asset_price = current_price

            # Calculate profit/loss and the total amount to be returned for this single position.
            profit_loss = round((asset_price - db_pos_cost) * (db_asset_share), 2)
            return_amount += db_pos_amount + profit_loss
            
            # Log the completed trade in transactions and history tables, then delete it from the positions table.
            self.complete_transaction(db_position_object, asset_price, profit_loss, connection=connection)
            self.log_to_history('CLOSED', db_position_object, connection=connection)
            self.delete_position_db(db_pos_id, connection=connection)
        
        return return_amount
        
    def get_position_db(self, position_id: str) -> tuple:
        """
        Retrieves all data for a single position from the 'positions' table.

        Args:
            position_id (str): The unique identifier for the position.

        Returns:
            tuple: A tuple containing all columns for the found position row, or raises a ValueError.
        """
        query = """
        SELECT position_id, user_id, position_name, position_amount, open_price, asset_share, asset_type, sector, open_datetime
        FROM positions
        WHERE position_id = :pos_id AND user_id = :user_id;
        """
        params = {"pos_id": position_id, "user_id": self.user_id}
        result = self.execute_query(query, params, fetch="one")
        if not result:
            raise ValueError(f"No position found with ID '{position_id}' for user '{self.user_name}'.")

        return result

    def complete_transaction(self, position_object: tuple, asset_price_at_close: float, profit_loss: float, connection=None):
        """
        Logs a completed trade into the transactions table.

        This helper function takes all the necessary data for a closed position
        and inserts it into the `transactions` table to create a permanent
        historical record. It can operate within a larger transaction.

        Args:
            position_object (tuple): The original position data from the database.
            asset_price_at_close (float): The asset's market price at the time of closing.
            profit_loss (float): The calculated profit or loss for the trade.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.

        Returns:
            None: Prints a confirmation message of the completed transaction.
        """
        transaction_id = position_object[0]  
        db_pos_name = position_object[2]
        db_pos_open_datetime = position_object[8]
        db_pos_amount, db_pos_open_price = map(float, (position_object[3], position_object[4]))

        query = """
        INSERT INTO transactions (transaction_id, user_id, position_name, position_amount, open_price, close_price, loss_profit, open_datetime)
        VALUES (:transaction_id, :user_id, :position_name, :position_amount, :open_price, :close_price, :loss_profit, :open_datetime);
        """
        params = {
            'transaction_id': transaction_id,
            'user_id': self.user_id,
            'position_name': db_pos_name,
            'position_amount': db_pos_amount,
            'open_price': db_pos_open_price,
            'close_price': asset_price_at_close,
            'loss_profit': profit_loss,
            'open_datetime': db_pos_open_datetime
        }
        self.execute_query(query, params, connection=connection)

        print(f"User -{self.user_name}-, completed a transaction with ID: {transaction_id}, for asset {db_pos_name}, at the current price of {asset_price_at_close}$. The invested amount was {db_pos_amount}$ and the profit/loss is {profit_loss}$.")
    
    def delete_position_db(self, position_id: str, connection=None):
        """
        Deletes a single position from the active positions table.

        This helper executes a DELETE statement for a given position ID and user
        ID. It uses the RETURNING clause to confirm a row was deleted and can
        operate within a larger transaction.

        Args:
            position_id (str): The unique identifier of the position to delete.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.

        Returns:
            None: Prints a confirmation message of the deletion.
        """
        # Delete the position from the database query, and store the result in a variable
        query = """
        DELETE FROM positions
        WHERE position_id = :pos_id AND user_id = :user_id
        RETURNING position_id, position_name;
        """
        params = {"pos_id": position_id, "user_id": self.user_id}
        result = self.execute_query(query, params, fetch="one", connection=connection) 

        # If no rows were returned, raise an error, else print confirmation message!
        if not result:
            raise ValueError(f"No position found with ID '{position_id}' for user '{self.user_name}'.")
        print(f"Closed position with position ID: {position_id}")

    def log_to_history(self, state: str, position_object: tuple, connection=None):
        """
        Logs an entry to the 'user_history' table based on an event (OPEN or CLOSED).
        
        This function has two distinct behaviors based on the 'state':
        - For an 'OPEN' event, it copies the newly created position's data directly
          from the 'positions' table. Columns related to closing (e.g., close_price)
          are left as NULL.
        - For a 'CLOSED' event, it copies the completed trade's data from the
          'transactions' table. It then supplements this with data that is not
          present in the transactions record (asset_share, asset_type, sector)
          by extracting it from the provided 'position_object', which holds the
          original position's details.

        This function uses an INSERT...SELECT statement to copy data from either
        the 'positions' or 'transactions' table into the 'user_history' table,
        populating a new row with a given state.

        Args:
            state (str): The state of the event, either 'OPEN' or 'CLOSED'.
            position_object (tuple): The full data object for the position being logged.
            connection (sqlalchemy.engine.Connection, optional): An existing database 
                connection to use for the operation. Defaults to None.
        """
        position_id = position_object[0]

        if state == 'OPEN':         
            query = """
            INSERT INTO user_history (position_id, user_id, position_name, position_amount, open_price, asset_share, asset_type, sector, open_datetime, state, close_price, loss_profit, close_datetime)
            SELECT position_id, user_id, position_name, position_amount, open_price, asset_share, asset_type, sector, open_datetime, :state, NULL, NULL, NULL
            FROM positions WHERE position_id = :position_id;
            """
            params = {'state': state, 'position_id': position_id}
        elif state == 'CLOSED':
            asset_share = position_object[5]
            asset_type = position_object[6]
            sector = position_object[7]           
            query = """
            INSERT INTO user_history (position_id, user_id, position_name, position_amount, open_price, close_price, loss_profit, open_datetime, close_datetime, state, asset_share, asset_type, sector)
            SELECT transaction_id, user_id, position_name, position_amount, open_price, close_price, loss_profit, open_datetime, close_datetime, :state, :asset_share, :asset_type, :sector
            FROM transactions WHERE transaction_id = :pos_id;
            """
            params = {
                'state': state, 
                'pos_id': position_id,
                'asset_share': asset_share,
                'asset_type': asset_type,
                'sector': sector
            }
        else:
            raise ValueError("State for history log must be 'OPEN' or 'CLOSED'.")

        result = self.execute_query(query, params, fetch='proxy', connection=connection)

        if result.rowcount == 0:
            # This is an important check. If nothing was inserted, it means the source record wasn't found.
            raise RuntimeError(f"Failed to log to history: Source record with ID '{position_id}' not found for state '{state}'.")

        print(f"Logged event '{state}' for ID '{position_id}' to history.")
        
    def get_asset_data_api(self, asset_name: str) -> list:
        """
        Retrieves live market data for a given financial asset.

        This function uses the yfinance library to fetch an asset's current
        market price, type, and sector. It includes robust checks to handle
        different market states (e.g., open, closed, post-market) and validates
        that the retrieved price is a valid, positive number before returning.

        Args:
            asset_name (str): The ticker symbol of the asset (e.g., 'AAPL').

        Returns:
            list: A list containing the [price, asset_type, sector].
        """
        # Initialize the yfinance Ticker object and increment the API call counter.
        ticker = yf.Ticker(asset_name)
        self.api_calls += 1 

        # Validate that the ticker object contains information and extract it.
        if not ticker.info:
            raise ValueError(f"Asset '{asset_name}' not found or no data available.")
        asset_info = ticker.info
        market_state = asset_info.get("marketState", None)
        asset_price = None
        
        # Determine the asset price based on the current market state and validate it.
        if market_state in ["CLOSED", "PRE", "POST", "POSTPOST"]:
            asset_price = asset_info.get('previousClose', 0.0) # Use the previous closing price if the market is not open.
        elif market_state == "REGULAR":
            asset_price = asset_info.get('regularMarketPrice', 0.0) # Use the regular market price for an open market.
        else:
            raise ValueError(f"Current market state is -{market_state}- and this Market state is not recognized or unsupported.")  
        if not asset_price or asset_price <= 0.0:
            raise ValueError(f"Current price for '{asset_name}' is {asset_price} so either a negative number or doesn't exist. The process cannot continue.")
        
        # Retrieve the asset's type and sector, then compile and return all data.
        asset_type = asset_info.get('quoteType', "N/A")
        sector = asset_info.get('sector', "N/A")
        asset_data = [asset_price, asset_type, sector]
        return asset_data 
    
    def get_asset_current_price(self, asset_name: str) -> float:
        """
        Retrieves the current market price for a specified asset.

        This function is a simple wrapper around `get_asset_data_api` to fetch
        just the current price of an asset. It ensures that the user is logged
        in before making the API call.

        Args:
            asset_name (str): The ticker symbol of the asset (e.g., 'AAPL').

        Returns:
            float: The current market price of the asset.
        """
        if not self.signed_in:
            raise PermissionError("You must be logged in to perform this action.")
        asset_data = self.get_asset_data_api(asset_name)
        asset_price = asset_data[0]
        return asset_price 
    
    def get_portfolio_info(self, source: str) -> pd.DataFrame:
        """
        Retrieves data for the logged-in user from a specified table and returns it as a Pandas DataFrame.
        If no records are found, it prints a message and returns an empty DataFrame.

        Args:
            source (str, optional): The table to fetch data from. Valid options are
                'positions', 'transactions', or 'user_history'
        
        Returns:
            pd.DataFrame: A DataFrame containing the requested data, or an empty DataFrame.
        """
        
        if source == 'positions':
            table_name = 'positions'
        elif source == 'transactions':
            table_name = 'transactions'
        elif source == 'history':
            table_name = 'user_history'
        else:
            raise ValueError(f"Invalid source '{source}'. Please choose from 'positions', 'transactions', or 'history'.")

        query = f"SELECT * FROM {table_name} WHERE user_id = :user_id;"
        params = {"user_id": self.user_id}
        result_proxy = self.execute_query(query, params, fetch='proxy') 

        column_names = list(result_proxy.keys())
        results = result_proxy.fetchall()

        if not results:
            print(f"No records found in '{table_name}' for user '{self.user_name}'.")
            return pd.DataFrame()
        
        local_df = pd.DataFrame(results, columns=column_names)
        return local_df
