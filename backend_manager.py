from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

class BackendManager:

    def __init__(self):
        """Initializes the BackendManager with no backend set."""
        self._backend = None
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
            );""",
            """CREATE TABLE IF NOT EXISTS user_history (
                action_id SERIAL PRIMARY KEY,
                position_id VARCHAR(50) NOT NULL,
                user_id VARCHAR(50) NOT NULL REFERENCES users(user_id),
                position_name VARCHAR(50) NOT NULL,
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
            );"""
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
    
    def insert_user_db(self, user_id: str, user_name: str, password: str, connection=None):
        """
        Inserts a new user record into the 'users' table.

        Args:
            user_id (str): The unique ID for the new user.
            user_name (str): The username for the new account.
            password (str): The securely hashed password for the user.
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the operation. Defaults to None.
        """
        query = """
        INSERT INTO users (user_id, user_name, password) 
        VALUES (:user_id, :user_name, :password);
        """
        params = {'user_id': user_id, 'user_name': user_name, 'password': password}
        self.execute_query(query, params, connection=connection)