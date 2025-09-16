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

class Portofolio:
    def __init__(self):
        # Load variables from .env file
        load_dotenv()

        # Read the variables
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')

        # Validate that all variables are present
        if not all([db_user, db_password, db_host, db_name]):
            raise ValueError("One or more required database environment variables are not set in your .env file.")

        # Construct the connection string
        connection_string = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}'
        
        self.engine = create_engine(connection_string)
        self.user_id = None
        self.user_name = None 
        self.signed_in = False
        self.db_calls = 0
        self.api_calls = 0
        self.create_empty()
        
        
    def create_empty(self):
        """
        Creates the necessary database tables if they do not already exist.
        This function sets up the 'users', 'positions', and 'transactions' tables
        with the required columns and constraints. It also resets the session's
        database and API call counters.
        
        Args:
            None

        Returns:
            None: Executes SQL CREATE TABLE statements in the connected database.
        """
        queries = [
            """CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(50) NOT NULL PRIMARY KEY,
                user_name VARCHAR(50) NOT NULL UNIQUE,
                password VARCHAR(80) NOT NULL,
                funds NUMERIC(12,2) NOT NULL DEFAULT 0
            );""",
            """CREATE TABLE IF NOT EXISTS positions (
                position_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL REFERENCES users(user_id),
                position_name VARCHAR(50) NOT NULL,
                position_amount NUMERIC(12,2) NOT NULL,
                open_price NUMERIC(12,2) NOT NULL DEFAULT 0,
                asset_share NUMERIC(18,8) NOT NULL DEFAULT 0,
                asset_type VARCHAR(50) NOT NULL DEFAULT 'N/A',
                sector VARCHAR(50) NOT NULL DEFAULT 'N/A',
                open_datetime TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
            );""",
            """CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR(50) NOT NULL PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL REFERENCES users(user_id),
                position_name VARCHAR(50) NOT NULL,
                position_amount NUMERIC(12,2) NOT NULL,
                open_price NUMERIC(12,2) NOT NULL,
                close_price NUMERIC(12,2) NOT NULL,
                loss_profit NUMERIC(12,2) NOT NULL,
                open_datetime TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                close_datetime TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
            );"""  
        ]
        for query in queries:
            self.execute_query(query)
        self.db_calls = 0
        self.api_calls = 0

    def execute_query(self, query: str, params=None, fetch=None, connection=None):
        """
        Executes a single SQL query with optional parameters and optional result fetching.
        The function uses bound parameters to avoid SQL injection and can return either
        all rows, a single row, or nothing depending on the 'fetch' argument.

        Args:
            query (str): A valid SQL query string with named parameters (e.g., :name).
            params (dict, optional): A mapping of parameter names to values. Defaults to {}.
            fetch (str, optional): Set to 'all' to fetch all rows, 'one' to fetch a single row,
                or leave as None to execute without fetching. Defaults to None.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection. If provided, the query is executed within the context of this
                connection's transaction. If None, a new transaction is created.
                Defaults to None.

        Returns:
            Any: When fetch is 'all' returns a list of rows; when 'one' returns a single row;
            otherwise returns None.
        """
        self.db_calls += 1 # Increment db calls by one, each time a db call is made
        # This helper contains the core execution and fetching logic. It runs the query
        # on a given connection, whether it's new or part of an existing transaction.
        def _execute_and_fetch(conn):
            result = conn.execute(text(query), params or {})
            if fetch == 'all':
                return result.fetchall()
            elif fetch == 'one':
                return result.fetchone()
            elif fetch == 'proxy':
                return result
            return None
        
        if connection:
            return _execute_and_fetch(connection)
        with self.engine.begin() as conn:
            return _execute_and_fetch(conn)

    def requires_login(func):
        """
        Decorator to ensure that a user is logged in before executing a method.
        If the user is not logged in, a PermissionError is raised.
        
        Args:
            func (callable): The function to be decorated.
        
        Returns:
            wrapper: A wrapper function that checks login status before executing the original function.
        """
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.signed_in:
                raise PermissionError("You must be logged in to perform this action.")
            return func(self, *args, **kwargs)
        return wrapper

    def id_generator(self, id_type: str) -> str:
        """
        Generates a unique, random ID for either a 'user' or a 'position' depending on the 'id_type' argument.

        The function ensures that the generated ID does not already exist in the
        relevant database tables before returning it (users or positions).
        - For 'user', it generates a 5-character ID of 2 letters followed by 3 numbers (e.g., 'AB123') and checks for
          uniqueness in the 'users' table.
        - For 'position', it generates a 10-character alphanumeric ID and checks for
          uniqueness across both the 'positions' and 'transactions' tables to
          prevent collisions when a position is closed.

        Args:
            id_type (str): The type of ID to generate, either "user" or "position".

        Returns:
            str: A unique ID.
        """
        if id_type == "user":
            while True:
                # Generate a new user_id
                letter_1 = random.choice(string.ascii_uppercase)
                letter_2 = random.choice(string.ascii_uppercase)
                numbers = f"{random.randint(0, 999):03d}"
                generated_user_id = f"{letter_1}{letter_2}{numbers}"

                query = "SELECT 1 FROM users WHERE user_id = :id"
                if not self.execute_query(query, {"id": generated_user_id}, fetch="one"):
                    return generated_user_id  # Return only if it's unique, else the loop continues
        elif id_type == "position":
            chars = string.ascii_uppercase + string.digits
            while True:
                local_id = ''.join(random.choices(chars, k=10))   
                query = """
                SELECT 1 FROM positions WHERE position_id = :id
                UNION ALL
                SELECT 1 FROM transactions WHERE transaction_id = :id
                """
                if not self.execute_query(query, {"id": local_id}, fetch="one"):
                    return local_id  # Returns the new ID only if it's unique, else the loop continues        
            
    def insert_new_user_db(self, user_id: str, user_name: str, password: str, account_funds: float):
        """
        Inserts a new user into the database.
        This function adds a new record to the 'users' table with the provided user details,
        including a unique user ID, username, hashed password, and initial account funds.

        Args:
            user_id (str): A unique identifier for the new user.
            user_name (str): The username of the new account.
            password (str): The hashed password to store securely in the database.
            account_funds (float): The initial amount of funds for the account.

        Returns:
            None: Executes the SQL insert statement and prints a confirmation message.
        """
        query = """
        INSERT INTO users (user_id, user_name, password, funds) 
        VALUES (:user_id, :user_name, :password, :funds);
        """
        params = {'user_id': user_id, 'user_name': user_name, 'password': password, 'funds': account_funds}
        self.execute_query(query, params)
        print(f"New user '{user_name}' added to the database with ID: {user_id}")            

    def register_user(self, user_name: str, password: str):
        """
        Registers a new user in the system.
        The function checks if the username already exists, hashes the password, 
        generates a unique user ID, and stores the new user's details in the database.

        Args:
            user_name (str): The desired username for the new account.
            password (str): The plain-text password to hash and store securely.

        Raises:
            PermissionError: If a user is already logged in.
            ValueError: If the username already exists in the database.

        Returns:
            None: Adds the new user to the database or raises an error if the username is taken.
        """
        if self.signed_in == True:
            raise PermissionError("You are already logged in. To register a new account, please log out first.")
        local_username = user_name.lower()

        query = """
        SELECT 1
        FROM users
        WHERE user_name = :username
        """ 
        params = {"username": local_username}
        result = self.execute_query(query, params, fetch="one") 

        if result: # If result is found, it means the username already exists
            raise ValueError(f"Username '{local_username}' already exists. Try another one.") 
        
        hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode() # Hash the password before storing
        local_user_id = self.id_generator("user")  # Generate a unique user ID
        local_funds = 0.0
        self.insert_new_user_db(local_user_id, local_username, hashed_password, local_funds)

    def log_in_user(self, user_name: str, password: str):
        """
        Authenticates a user by verifying their username and password.
        If the credentials are correct, the user's ID and username are loaded into memory.

        Args:
            user_name (str): The username of the account to log in.
            password (str): The plain-text password to verify against the stored hash.

        Raises:
            PermissionError: If a user is already logged in.
            ValueError: If the username is not found or the password is incorrect.

        Returns:
            None: Updates the object's user-related attributes and prints login confirmation.
        """
        if self.signed_in == True:
            raise PermissionError("You are already logged in. To log in with another account, please log out first.")
        local_user_name = user_name.lower()
        query = """
        SELECT user_id, user_name, password
        FROM users
        WHERE user_name = :u
        """
        params = {"u": local_user_name}
        result = self.execute_query(query, params, fetch="one")

        if not result:
            raise ValueError(f"The username '{local_user_name}' was not found.") 
            
        stored_user_id, stored_user_name, stored_hash = result

        if not bcrypt.checkpw(password.encode(), stored_hash.encode()):
            raise ValueError("Incorrect password.")

        self.user_id = stored_user_id
        self.user_name = stored_user_name
        self.signed_in = True
        print(f"Logged in as {self.user_name} (ID: {self.user_id})")

    def log_out_user(self):
        """
        Logs out the currently logged-in user by resetting user-related attributes.
        This function clears the user ID and username from the instance and sets
        the login status to False.

        Args:
            None

        Raises:
            PermissionError: If no user is currently logged in.
        Returns:
            None: Resets the user's session data.
        """
        if not self.signed_in:
            raise PermissionError("You are not logged in.")
        self.user_id = None
        self.user_name = None
        self.signed_in = False
        print("Logged out successfully.")
            
    @requires_login
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
    
    @requires_login
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

    @requires_login
    def get_account_info(self):
        """
        Retrieves and displays the current account information for the logged-in user.
        This includes the user ID, username, and the total funds available in the account.

        Args:
            None

        Returns:
            None: Prints the account information directly to the console.
        """       
        local_funds = self.get_funds_db()
        print(f"User ID: {self.user_id}, User Name: {self.user_name}, Account Balance: {local_funds}")

    @requires_login
    def open_position(self, asset_name: str, position_amount: float):
        """
        Orchestrates opening a new position for the logged-in user.

        This function handles the entire process of opening a position:
        1. Validates the input amount and user's available funds.
        2. Fetches live market data for the asset via an API call.
        3. Calculates the number of shares based on the current price.
        4. Atomically inserts the new position to the 'positions' table and deducts the cost from the
           user's funds in a single database transaction.

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
        asset_data = self.get_asset_data(asset_name)
        local_position_id = self.id_generator("position")
        local_asset_price = asset_data[0]
        local_asset_share = self.calculate_asset_shares(local_asset_price, position_amount)
        local_asset_type = asset_data[1]
        local_asset_sector = asset_data[2]

        with self.engine.begin() as connection:
            # Insert the new position into the database
            query = """
            INSERT INTO positions (position_id, user_id, position_name, position_amount, open_price, asset_share, asset_type, sector) 
            VALUES (:position_id, :user_id, :position_name, :position_amount, :open_price, :asset_share, :asset_type, :sector);
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
            self.execute_query(query, params, connection=connection)

            pos_amount = -float(position_amount) # Make the amount negative so that it gets deducted from user's account balance.     
            self.modify_funds_db(pos_amount, connection=connection)

        print(f"Bought asset {asset_name} with position ID {local_position_id} at price {local_asset_price}$ and {local_asset_share} shares in sector {local_asset_sector}.")

    @requires_login
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

    @requires_login
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

        # Determine which positions to close based on the provided arguments.
        positions_list = []
        asset_current_data = None
        if position_id:
            # If a position_id is given, fetch that single position's data.
            position = self.get_position_db(position_id)
            position_name = position[2] # Get the asset name from the position data to look up the price.
            asset_current_data = self.get_asset_data(position_name)
            positions_list.append(position)
        else:
            # If an asset_name is given, fetch all positions matching that name for the user.
            query = "SELECT * FROM positions WHERE user_id = :user_id AND position_name = :asset_name;"
            params = {"user_id": self.user_id, "asset_name": asset_name}
            results = self.execute_query(query, params, fetch="all")
            if not results:
                raise ValueError(f"No open positions found for asset '{asset_name}' for user '{self.user_name}'.")
            positions_list = results
            asset_current_data = self.get_asset_data(asset_name)
        
        # Fetch the current market price for the asset(s) being closed.
        current_price = asset_current_data[0]

        # Open a single transaction to ensure all operations succeed or fail together.
        with self.engine.begin() as connection:
            # Call the worker function to process the positions and get the total return amount.
            return_balance = self.close_position(positions_list, current_price, connection)
            # Update the user's funds with the final calculated amount.
            self.modify_funds_db(return_balance, connection)

    @requires_login
    def close_position(self, positions_list: list, current_price: float, connection=None) -> float:
        """
        Processes a list of positions to be closed within an existing transaction.

        This function acts as an internal "worker" for the closing process. It
        iterates through a list of positions and performs the following steps for each:
        1. Calculate the profit or loss for each position.
        2. Log the completed trade in the transactions history.
        3. Delete the position from the active positions table.

        Args:
            positions_list (list): A list of position tuples to be closed.
            current_price (float): The pre-fetched current price of the asset.
            connection (sqlalchemy.engine.Connection): An active SQLAlchemy connection.

        Returns:
            float: The total amount to be returned to the user's funds from all closed positions.
        """
        # Initialize a variable to accumulate the total return from all positions.
        return_amount = 0.0
        # Iterate through each position provided in the list.
        for db_position_object in positions_list:
            # Extract position data for clarity.
            db_pos_id = db_position_object[0]
            db_pos_amount = float(db_position_object[3])
            db_pos_cost = float(db_position_object[4])
            db_asset_share = float(db_position_object[5])            
            asset_price = current_price

            # Calculate profit/loss and the total amount to be returned for this single position.
            profit_loss = round((asset_price - db_pos_cost) * (db_asset_share), 2)
            return_amount += db_pos_amount + profit_loss
            
            # Log the completed trade and delete the original position from the active table.
            self.complete_transaction(db_position_object, asset_price, profit_loss, connection=connection)
            self.delete_position_db(db_pos_id, connection=connection)
        # Return the total accumulated amount to the calling function.
        return return_amount
        
    @requires_login
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

    @requires_login
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
        db_pos_id = position_object[0]
        db_pos_name = position_object[2]
        db_pos_open_datetime = position_object[8]
        db_pos_amount, db_pos_open_price = map(float, (position_object[3], position_object[4]))

        query = """
        INSERT INTO transactions (transaction_id, user_id, position_name, position_amount, open_price, close_price, loss_profit, open_datetime)
        VALUES (:transaction_id, :user_id, :position_name, :position_amount, :open_price, :close_price, :loss_profit, :open_datetime);
        """
        params = {
            'transaction_id': db_pos_id,
            'user_id': self.user_id,
            'position_name': db_pos_name,
            'position_amount': db_pos_amount,
            'open_price': db_pos_open_price,
            'close_price': asset_price_at_close,
            'loss_profit': profit_loss,
            'open_datetime': db_pos_open_datetime
        }
        self.execute_query(query, params, connection=connection)   

        print(f"User -{self.user_name}-, completed a transaction with ID: {db_pos_id}, for asset {db_pos_name}, at the current price of {asset_price_at_close}$. The invested amount was {db_pos_amount}$ and the profit/loss is {profit_loss}$.")
    
    @requires_login
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
        
    @requires_login
    def get_asset_data(self, asset_name: str) -> list:
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
    
    @requires_login
    def get_all_positions_df(self) -> pd.DataFrame:
        """
        Retrieves all open positions for the logged-in user and returns them as a Pandas DataFrame.
        If no positions are found, it prints a message to the console and returns an
        empty DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing all position data, or an empty DataFrame.
        """
        query = "SELECT * FROM positions WHERE user_id = :user_id;"
        params = {"user_id": self.user_id}
        result_proxy = self.execute_query(query, params, fetch='proxy') # Get the result proxy object

        column_names = list(result_proxy.keys())
        results = result_proxy.fetchall()

        # 3. Now, check if the results list is empty
        if not results:
            print(f"No positions found for user '{self.user_name}'.")
            return pd.DataFrame()
        
        # 4. Create the DataFrame
        local_df = pd.DataFrame(results, columns=column_names)
        return local_df
    
    def show_db_api_calls(self):
        """
        Displays the total number of database and API calls and resets the counters.

        This function prints the cumulative count of database and API calls made
        during the current session (since the last reset). After displaying the
        counts, it resets both counters to zero, allowing for fresh tracking of
        subsequent operations.

        Args:
            None

        Returns:
            None: Prints the call counts to the console.
        """
        print(f"Total DB calls are '{self.db_calls}' and total API calls are '{self.api_calls}.")
        # Reset the counters for database and API calls
        self.api_calls = 0
        self.db_calls = 0