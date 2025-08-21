from sqlalchemy import create_engine, text
import random
import string
import bcrypt
from functools import wraps



class Portofolio:
    def __init__(self):
        # Initialize SQLAlchemy engine
        self.engine = create_engine('postgresql+psycopg2://postgres:6987129457@localhost/assets_project_db_2')
        self.user_id = None
        self.user_name = None 
        self.signed_in = False
        self.create_empty()
        
    def create_empty(self):
        """
        Creates the necessary database tables if they do not already exist.
        This function sets up the 'positions' and 'users' tables with the required
        columns and constraints for storing portfolio data and user information.

        Args:
            None

        Returns:
            None: Executes SQL CREATE TABLE statements in the connected database.
        """
        queries = [
            """CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(50) NOT NULL PRIMARY KEY,
                user_name VARCHAR(50) NOT NULL,
                password VARCHAR(80) NOT NULL,
                funds NUMERIC(12,2) NOT NULL DEFAULT 0
            );""",
            """CREATE TABLE IF NOT EXISTS positions (
                position_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL REFERENCES users(user_id),
                position_name VARCHAR(50) NOT NULL,
                position_amount NUMERIC(12,2) NOT NULL,
                asset_type VARCHAR(50) NOT NULL
            );"""    
        ]
        for query in queries:
            self.execute_query(query)

    def execute_query(self, query: str, params=None, fetch=None):
        """
        Executes a single SQL query with optional parameters and optional result fetching.
        The function uses bound parameters to avoid SQL injection and can return either
        all rows, a single row, or nothing depending on the 'fetch' argument.

        Args:
            query (str): A valid SQL query string with named parameters (e.g., :name).
            params (dict, optional): A mapping of parameter names to values. Defaults to {}.
            fetch (str, optional): Set to 'all' to fetch all rows, 'one' to fetch a single row,
                or leave as None to execute without fetching. Defaults to None.

        Returns:
            Any: When fetch is 'all' returns a list of rows; when 'one' returns a single row;
            otherwise returns None.
        """
        with self.engine.begin() as connection:
            result = connection.execute(text(query), params or {})
            if fetch == 'all':
                return result.fetchall()
            elif fetch == 'one':
                return result.fetchone()
            return None
    
    def execute_many(self, query: str, params_list: list):
        """
        Executes the same SQL statement multiple times with different parameter sets
        inside a single transaction. Each item in 'params_list' is executed once.

        Args:
            query (str): A valid SQL query string with named parameters.
            params_list (list): A list of dictionaries, each providing values for the query parameters.

        Returns:
            None: Executes all statements within a transaction without returning results.
        """
        with self.engine.connect() as connection:
            with connection.begin():  # Automatically handles transactions
                for params in params_list:
                    connection.execute(text(query), params)

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

    def user_id_generator(self, user_name: str) -> str:
        """
        Generates a unique user ID based on the username.
        The ID is composed of the first letter of the username (uppercase), 
        a random uppercase letter, and a three-digit random number.
        The function ensures that the generated ID does not already exist in the database.

        Args:
            user_name (str): The username from which to derive the first letter for the ID.

        Returns:
            str: A unique user ID that does not exist in the 'users' table.
        """
        while True:
            # Generate a new user_id
            letter_1 = user_name[0].upper()
            letter_2 = random.choice(string.ascii_uppercase)
            numbers = f"{random.randint(0, 999):03d}"
            generated_user_id = f"{letter_1}{letter_2}{numbers}"

            query = "SELECT 1 FROM users WHERE user_id = :id"
            if not self.execute_query(query, {"id": generated_user_id}, fetch="one"):
                return generated_user_id  # Return only if it's unique

    @requires_login        
    def position_id_generator(self) -> str:
        """
        Generates a unique 10-character alphanumeric position ID (user must be logged in).
        The ID space is large enough to avoid collisions even with billions of positions.

        Args:
            None

        Returns:
            str: A unique position identifier.
        """
        chars = string.ascii_uppercase + string.digits
        while True:
            local_id = ''.join(random.choices(chars, k=10))   
            query = "SELECT 1 FROM positions WHERE position_id = :id"
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

        Returns:
            None: Adds the new user to the database or raises an error if the username is taken.
        """
        if self.signed_in == True:
            raise PermissionError("You are already logged in. To register a new account, please log out first.")
        local_username = user_name.lower()
        query = """
        SELECT 1
        FROM users
        WHERE user_name = :u
        """ # Check if the username already exists
        params = {"u": local_username}
        if self.execute_query(query, params, fetch="one"): # if a row has been found, then the username already exists
            raise ValueError(f"Username '{local_username}' already exists. Try another one.") # and if not, then an error is raised!     
        # Hash the password before storing
        hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        local_user_id = self.user_id_generator(local_username)  # Generate a guaranteed unique ID
        local_funds = 0.0
        self.insert_new_user_db(local_user_id, local_username, hashed_password, local_funds)

    def log_in_user(self, user_name: str, password: str):
        """
        Authenticates a user by verifying their username and password.
        If the credentials are correct, the user's ID and username are loaded into memory.

        Args:
            user_name (str): The username of the account to log in.
            password (str): The plain-text password to verify against the stored hash.

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
        This function clears the user ID, username, and account funds, effectively logging out the user.

        Args:
            None

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
        return float(self.execute_query(
            "SELECT funds FROM users WHERE user_id = :user_id",
            {"user_id": self.user_id}, fetch="one"
        )[0])
    
    @requires_login
    def modify_funds_db(self, amount: float, action: str):
        """
        Updates the logged-in user's funds in the database.
        Can increase or decrease based on the action.

        Args:
            amount (float): Positive amount to change the balance by.
            action (str): 'increase' or 'decrease'. If decrease, the amount becomes negative.

        Returns:
            None: Only prints the new balance.
        """
        if amount <= 0:
            raise ValueError("Amount must be positive.")
        if action not in ("increase", "decrease"):
            raise ValueError("Action must be 'increase' or 'decrease'.")

        if action == "increase":
            delta = amount
        else:
            delta = -amount

        query = """
        UPDATE users 
        SET funds = funds + :delta 
        WHERE user_id = :uid
        RETURNING funds;
        """
        params = {"delta": delta, "uid": self.user_id}

        result = self.execute_query(query, params, fetch="one")
        new_balance = float(result[0])
        print(f"Balance updated. New balance: ${new_balance}")

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
    def open_position(self, asset_name: str, asset_amount: int, asset_type: str):
        """
        Opens a new position for the current user by first verifying if account balance in the database is sufficient and then decreases the user's account balance by the position ammount in the database.

        Args:
            asset_name (str): The name/ticker of the asset to buy.
            asset_amount (int): The amount (cash) to allocate to this position.
            asset_type (str): The asset type ('stock', 'etf', or 'crypto').

        Returns:
            None: Creates the position row and updates the user's account balance and prints a confirmation message.
        """
        if asset_amount < 10:
            raise ValueError("Minimum amount to open a position is 10.")

        # 1) Check latest funds in DB
        if self.get_funds_db() < asset_amount:
            raise ValueError(f"Insufficient funds to open {asset_name} worth {asset_amount}$.")
        
        # 2) Insert position in DB
        local_position_id = self.position_id_generator()
        query = """
        INSERT INTO positions (position_id, user_id, position_name, position_amount, asset_type)
        VALUES (:pos_id, :user_id, :pos_name, :pos_amount, :asset_type);
        """
        params = {
            "pos_id": local_position_id,
            "user_id": self.user_id,
            "pos_name": asset_name,
            "pos_amount": asset_amount,
            "asset_type": asset_type,
        }
        self.execute_query(query, params)

        # 3) Deduct funds in DB
        self.modify_funds_db(asset_amount, "decrease")
        print(f"Opened position: {asset_name} ({asset_type}) with ID {local_position_id} for ${asset_amount}.")

    @requires_login
    def close_position(self, position_id: str):
        """
        Searches an existing position by its ID and if found, it deletes it from the database. After that it updates the user's account balance and outputs a confirmation message to the user.
        
        Args:   
            position_id (str): The unique identifier of the position to close.
        
        Returns:
            None: Only prints a confirmation message.
        """
        query = """
        DELETE FROM positions
        WHERE position_id = :pos_id AND user_id = :user_id
        RETURNING position_name, position_amount, position_id;
        """
        params = {"pos_id": position_id, "user_id": self.user_id}
        result = self.execute_query(query, params, fetch="one")
        # If no position was found, raise an error
        if not result:
            raise ValueError(f"No position found with ID {position_id} for user {self.user_name}.")
        
        local_pos_name, local_pos_amount, local_pos_id = result # Unpack the result in variables
        local_pos_amount = float(local_pos_amount)        
        self.modify_funds_db(local_pos_amount, "increase") # Increase account balance by position's value'
        print(f"User -{self.user_name}- closed position -{local_pos_name}- worth {local_pos_amount}$ with position ID: {local_pos_id}")
        

    def close_asset(self, asset_name: str):
        """
        This function closes all positions of a specific asset for the logged-in user, based on user's input.
        If user enters an asset name ("AAPL), it deletes all positions of Apple from the database and then increases the user's account balance by the total amount of money that was invested in that asset.
        
        Args:
            asset_name (str): The name of the asset to close (e.g., 'AAPL', 'SPY').
        
        Returns:
            None: Only prints a confirmation message with the number of positions closed and the total amount returned to the user's account.
        """
        query = """
        DELETE FROM positions
        WHERE position_name = :pos_name AND user_id = :user_id
        RETURNING position_id, position_amount;
        """
        params = {"pos_name": asset_name, "user_id": self.user_id}
        rows = self.execute_query(query, params, fetch="all")

        if not rows: # If no rows were returned, raise an error
            raise ValueError(f"No asset found named '{asset_name}' for user {self.user_name}.")
        
        pos_ids_list = [] # List to store closed position IDs
        total_amount = 0.0 # Stores total amount of money returned to the user's account
        for row in rows: # Loop to find all positions of the asset and total amount of money
            pos_ids_list.append(row[0])
            total_amount += float(row[1])

        self.modify_funds_db(total_amount, "increase")  # Increase account balance by total
        print(f"User -{self.user_name}- closed asset -{asset_name}- with {len(pos_ids_list)} position IDs: {', '.join(pos_ids_list)} worth {total_amount:.2f}$") # Confirmation message
