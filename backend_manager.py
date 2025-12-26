from sqlalchemy import create_engine, text
import os
from decimal import Decimal
from dotenv import load_dotenv
from position import Position


class BackendManager:

    def __init__(self):
        """Initializes the BackendManager with no backend set."""
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
        self.create_empty()


    def create_empty(self):
        """
        Creates the necessary database tables if they do not already exist.
        This function sets up the 'users', 'positions', 'transactions', and 'user_history'
        tables with the required columns and constraints. It also resets the
        session's database and API call counters.

        Args:
            None

        Returns:
            None: Executes SQL CREATE TABLE statements in the connected database.
        """
        queries = [
            """CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(50) NOT NULL PRIMARY KEY,
                user_name VARCHAR(50) NOT NULL UNIQUE,
                hash_password VARCHAR(80) NOT NULL,
                account_balance NUMERIC(12,2) NOT NULL DEFAULT 0
            );""",
            """CREATE TABLE IF NOT EXISTS positions (
                position_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL REFERENCES users(user_id),
                position_ticker VARCHAR(50) NOT NULL,
                position_amount NUMERIC(12,2) NOT NULL,
                open_price NUMERIC(12,2) NOT NULL DEFAULT 0,
                asset_share NUMERIC(18,8) NOT NULL DEFAULT 0,
                asset_type VARCHAR(50) NOT NULL DEFAULT 'N/A',
                sector VARCHAR(50) NOT NULL DEFAULT 'N/A',
                open_datetime TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
            );""",
            """CREATE TABLE IF NOT EXISTS trades (
                trade_id VARCHAR(50) NOT NULL PRIMARY KEY,
                position_id VARCHAR(50) NOT NULL,
                user_id VARCHAR(50) NOT NULL REFERENCES users(user_id),
                position_ticker VARCHAR(50) NOT NULL,
                position_amount NUMERIC(12,2) NOT NULL,
                open_price NUMERIC(12,2),
                close_price NUMERIC(12,2),
                loss_profit NUMERIC(12,2),
                asset_share NUMERIC(18,8),
                asset_type VARCHAR(50) NOT NULL DEFAULT 'N/A',
                sector VARCHAR(50) NOT NULL DEFAULT 'N/A',
                open_datetime TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                close_datetime TIMESTAMP(0) WITHOUT TIME ZONE,
                state VARCHAR(50) NOT NULL
            );""",
            """CREATE INDEX IF NOT EXISTS idx_trades_position_id ON trades (position_id);"""
        ]
        for query in queries:
            self.execute_query(query)
        
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
        
        def _execute_and_fetch(conn): # Helper function to avoid code duplication
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
        
    def username_exists(self, username: str, connection=None):
        """
        Checks if a given username already exists in the 'users' table.

        Args:
            username (str): The username to check for existence.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.

        Returns:
            A Row object if the user exists, otherwise None.
        """
        query = """SELECT 1 FROM users WHERE user_name = :username"""
        params = {"username": username}
        result = self.execute_query(query, params, fetch="one", connection=connection)
        return result
    
    def user_id_exists(self, user_id: str, connection=None):
        """
        Checks if a given user ID already exists in the 'users' table.

        Args:
            user_id (str): The user ID to check for existence.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.

        Returns:
            A Row object if the ID exists, otherwise None.
        """
        query = """SELECT 1 FROM users WHERE user_id = :user_id"""
        params = {"user_id": user_id}
        result = self.execute_query(query, params, fetch="one", connection=connection)
        return result
    
    def insert_user_db(self, user_id: str, user_name: str, hash_password: str, connection=None):
        """
        Inserts a new user record into the 'users' table.

        Args:
            user_id (str): The unique ID for the new user.
            user_name (str): The username for the new account.
            hash_password (str): The securely hashed password for the user.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.
        """
        query = """
        INSERT INTO users (user_id, user_name, hash_password) 
        VALUES (:user_id, :user_name, :hash_password);
        """
        params = {'user_id': user_id, 'user_name': user_name, 'hash_password': hash_password}
        self.execute_query(query, params, connection=connection)

    def retrieve_user_by_username(self, username: str, connection=None):
        """
        Retrieves a user record from the 'users' table by username.

        Args:
            username (str): The username of the user to retrieve.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.

        Returns:
            A Row object containing the user record, or None if not found.
        """
        query = """
        SELECT user_id, user_name, hash_password
        FROM users
        WHERE user_name = :username;
        """
        params = {"username": username}
        result = self.execute_query(query, params, fetch="one", connection=connection)
        return result
    
    def get_account_balance(self, user_id: str):
        """
        Function that gets the account balance from the database for the given user_id.
        
        Args:
            user_id (str): The unique identifier of the user whose funds are to be retrieved.
        
        Returns:
            A Row object containing the user's funds, or None if the user is not found.
        """
        query = "SELECT account_balance FROM users WHERE user_id = :user_id"
        params = {"user_id": user_id}
        result = self.execute_query(query, params, fetch="one")
        return result
    
    def position_id_exists(self, position_id: str, connection=None):
        """
        Checks if a given position ID already exists in the 'trades' table.

        This check is performed against the 'trades' table as it is the master
        ledger of all position IDs that have ever existed.

        Args:
            position_id (str): The position ID to check for existence.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.

        Returns:
            A Row object if the ID exists, otherwise None.
        """
        query = """SELECT 1 FROM trades WHERE position_id = :position_id"""
        params = {"position_id": position_id}
        result = self.execute_query(query, params, fetch="one", connection=connection)
        return result
    
    def trade_id_exists(self, trade_id: str, connection=None):
        """
        Checks if a given trade ID already exists in the 'trades' table.

        Args:
            trade_id (str): The trade ID to check for existence.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.

        Returns:
            A Row object if the ID exists, otherwise None.
        """
        query = """SELECT 1 FROM trades WHERE trade_id = :trade_id"""
        params = {"trade_id": trade_id}
        result = self.execute_query(query, params, fetch="one", connection=connection)
        return result
    
    def insert_position(self, position: Position, connection=None):
        """
        Inserts a new record into the 'positions' table.

        This function takes a dictionary of position data and inserts it into the
        'positions' table, which tracks currently active holdings. It is designed
        to be called within a larger database transaction.

        Args:
            position_data (dict): A dictionary containing all necessary data for
                the new position, with keys matching the table's column names.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.
        """
        position_data_dict = position.to_position_dict()
        query = """
        INSERT INTO positions (position_id, user_id, position_ticker, position_amount, open_price, asset_share, asset_type, sector, open_datetime)
        VALUES (:position_id, :user_id, :position_ticker, :position_amount, :open_price, :asset_share, :asset_type, :sector, :open_datetime);
        """
        self.execute_query(query, position_data_dict, connection=connection)

    def insert_buy_trade(self, trade_id: str, position: Position, connection=None):
        """
        Inserts a new 'OPEN' trade record into the 'trades' table.

        This function takes a dictionary of trade data and a unique trade ID,
        then inserts a new record into the 'trades' table with a state of 'OPEN'.
        This creates a permanent, historical record of the buy event.

        Args:
            trade_id (str): The unique ID for this specific trade event.
            trade_data (dict): A dictionary containing all common data for the
                trade, with keys matching the table's column names.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.
        """
        trade_data_dict = position.to_trade_dict()
        query = """
        INSERT INTO trades (trade_id, position_id, user_id, position_ticker, position_amount, open_price, asset_share, asset_type, sector, open_datetime, state)
        VALUES (:trade_id, :position_id, :user_id, :position_ticker, :position_amount, :open_price, :asset_share, :asset_type, :sector, :open_datetime, 'BUY');
        """
        # Combine the main data dictionary with the specific trade_id
        params = {**trade_data_dict, 'trade_id': trade_id}
        self.execute_query(query, params, connection=connection)

    def insert_sell_trade(self, position: Position, connection=None):
        """
        Inserts a new 'SELL' trade record into the 'trades' table.

        This function takes a populated Position object representing a closed trade
        and inserts its data into the 'trades' table. This creates a permanent,
        historical record of the sell event, including profit/loss.

        Args:
            position (Position): A Position object containing all data for the
                closed trade, including closing price and profit/loss.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.
        """
        trade_data_dict = position.to_trade_dict()
        query = """
        INSERT INTO trades (trade_id, position_id, user_id, position_ticker, position_amount, open_price, close_price, loss_profit, asset_share, asset_type, sector, open_datetime, close_datetime, state)
        VALUES (:trade_id, :position_id, :user_id, :position_ticker, :position_amount, :open_price, :close_price, :loss_profit, :asset_share, :asset_type, :sector, :open_datetime, :close_datetime, :state);
        """
        self.execute_query(query, trade_data_dict, connection=connection)

    def increase_user_balance(self, user_id: str, amount: Decimal, connection=None):
        """
        Increases a user's account balance by a specified amount.

        This function executes an UPDATE statement to add the given amount to the
        user's current account balance.

        Args:
            user_id (str): The ID of the user whose balance will be increased.
            amount (Decimal): The positive amount to add to the balance.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.
        """
        query = """
        UPDATE users
        SET account_balance = account_balance + :amount
        WHERE user_id = :user_id;
        """
        params = {'user_id': user_id, 'amount': amount}
        self.execute_query(query, params, connection=connection)
        
    def decrease_user_balance(self, user_id: str, amount: Decimal, connection=None):
        """
        Decreases a user's account balance by a specified amount.

        This function executes an UPDATE statement to subtract the given amount from
        the user's current account balance.

        Args:
            user_id (str): The ID of the user whose balance will be decreased.
            amount (Decimal): The positive amount to subtract from the balance.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.
        """
        query = """
        UPDATE users
        SET account_balance = account_balance - :amount
        WHERE user_id = :user_id;
        """
        params = {'user_id': user_id, 'amount': amount}
        self.execute_query(query, params, connection=connection)

    def delete_position_db(self, user_id: str, position_id: str, connection=None):
        """
        Deletes a position from the 'positions' table for a given user and position ID,
        returning the data of the deleted row.

        Args:
            user_id (str): The ID of the user who owns the position.
            position_id (str): The ID of the position to delete.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.

        Returns:
            A Row object containing the data of the deleted position.

        Raises:
            ValueError: If no matching position is found to delete.
        """
        query = """
        DELETE FROM positions
        WHERE position_id = :pos_id AND user_id = :user_id
        RETURNING *;
        """
        params = {"pos_id": position_id, "user_id": user_id}
        result = self.execute_query(query, params, fetch="one", connection=connection)
        return result

    def retrieve_user_positions(self, user_id: str, connection=None):
        """
        Retrieves all positions for a specific user from the 'positions' table.

        Args:
            user_id (str): The user ID to fetch positions for.
            connection (sqlalchemy.engine.Connection, optional): An existing database connection.

        Returns:
            list: A list of Row objects representing the user's positions.
        """
        query = "SELECT * FROM positions WHERE user_id = :user_id"
        params = {"user_id": user_id}
        return self.execute_query(query, params, fetch="all", connection=connection)
    
    def retrieve_user_trades(self, user_id: str, connection=None):
        """
        Retrieves all trade history for a specific user from the 'trades' table.

        Args:
            user_id (str): The user ID to fetch trade history for.
            connection (sqlalchemy.engine.Connection, optional): An existing database connection.   
        Returns:
            list: A list of Row objects representing the user's trade history.
        """
        query = "SELECT * FROM trades WHERE user_id = :user_id"
        params = {"user_id": user_id}
        return self.execute_query(query, params, fetch="all", connection=connection)