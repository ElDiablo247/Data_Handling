from sqlalchemy import create_engine, text
import pandas as pd
import random
import string
import re
import bcrypt



class Portofolio:
    def __init__(self):
        # Initialize SQLAlchemy engine
        self.engine = create_engine('postgresql+psycopg2://postgres:6987129457@localhost/assets_project_db_2')
        self.user_id = None
        self.user_name = None 
        self.account_funds = None
        self.create_empty()
        

    def create_empty(self):
        queries = [
            """CREATE TABLE IF NOT EXISTS positions (
                position_id VARCHAR(50) PRIMARY KEY,
                position_name VARCHAR(50) NOT NULL,
                position_amount INT NOT NULL,
                asset_type VARCHAR(50) NOT NULL,
                sector VARCHAR(50) NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(50) NOT NULL PRIMARY KEY,
                user_name VARCHAR(50) NOT NULL,
                password VARCHAR(80) NOT NULL,
                funds INT NOT NULL DEFAULT 0
            );"""
        ]
        for query in queries:
            self.execute_query(query)



    def log_in_user(self, user_name: str, password: str):
        query = """
        SELECT user_id, user_name, password, funds
        FROM users
        WHERE user_name = :u
        """
        params = {"u": user_name}
        result = self.execute_query(query, params, fetch="one")

        if not result:
            raise ValueError(f"The username '{user_name}' was not found.") 
            
        stored_user_id, stored_user_name, stored_hash, stored_funds = result

        if not bcrypt.checkpw(password.encode(), stored_hash.encode()):
            raise ValueError("Incorrect password.")

        self.user_id = stored_user_id
        self.user_name = stored_user_name
        self.account_funds = float(stored_funds) if stored_funds is not None else 0.0

        print(f"Logged in as {self.user_name} (ID: {self.user_id}) with funds: {self.account_funds}")

            

    def register_user(self, user_name: str, password: str):
        # Check if username already exists in SQL
        query = """
        SELECT 1
        FROM users
        WHERE user_name = :u
        """
        params = {"u": user_name}
        if self.execute_query(query, params, fetch="one"): # if a row has been found, then the username already exists
            raise ValueError(f"Username '{user_name}' already exists. Try another one.") # and if not, then an error is raised!
        
        # Hash the password before storing
        hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

        local_user_id = self.user_id_generator(user_name)  # Generate a guaranteed unique ID
        local_funds = 0.0

        self.insert_new_user_db(local_user_id, user_name, hashed_password, local_funds)

            
    def user_id_generator(self, user_name: str) -> str:
        while True:
            # Generate a new user_id
            letter_1 = user_name[0].upper()
            letter_2 = random.choice(string.ascii_uppercase)
            numbers = f"{random.randint(0, 999):03d}"
            generated_user_id = f"{letter_1}{letter_2}{numbers}"

            query = "SELECT 1 FROM users WHERE user_id = :id"
            if not self.execute_query(query, {"id": generated_user_id}, fetch="one"):
                return generated_user_id  # Return only if it's unique
            
    def insert_new_user_db(self, user_id: str, user_name: str, password: str, account_funds: float):
        """Inserts a new user into the database."""
        query = """
        INSERT INTO users (user_id, user_name, password, funds) 
        VALUES (:user_id, :user_name, :password, :funds);
        """
        params = {'user_id': user_id, 'user_name': user_name, 'password': password, 'funds': account_funds}
        self.execute_query(query, params)
        print(f"New user '{user_name}' added to the database with ID: {user_id}")


    def execute_query(self, query: str, params=None, fetch='False'):
        """ Function that is called to execute an SQL query. It takes a string parameter which is a valid SQL query, and also a list of parameters if any. FOR EXAMPLE

        query = '''
        INSERT INTO stocks (stock_name, total_amount, sector)
        VALUES (:name, :amount, :sector);
        '''
        params = {'name': 'AAPL', 'amount': 50, 'sector': 'Technology'}
        master.execute_query(query, params)'''

        query is the variable that contains the SQL query. Values are masked with (:) to avoid SQL injection. Params is a dictionary of key values to replace the masked values. At the end the query is executed.

        If fetch is set to True then the value at the end is shown to the user.

        Args:
            query (str): A string SQL query
            params (type depends, optional): Parameters could be anything but usually a string or integer. Defaults to None
            fetch (bool, optional): If specifically set to True then the results are fetched and showed to the user. Defaults to False.

        Returns:
            _type_: Type depends on the type of the result (integer, string) and returns that value
        """
        with self.engine.begin() as connection:
            result = connection.execute(text(query), params or {})
            if fetch == 'all':
                return result.fetchall()
            elif fetch == 'one':
                return result.fetchone()
            return None
    
    def execute_many(self, query: str, params_list: list):
        """ Function that executes many queries at once. A query must be first created and then a list of values is fed to this function as a parameter. The function then iterates over each query and executes them.

        Args:
            query (str): An SQL string query
            params_list (list): A list of parameters. For further information look into execute_query function documentation
        """
        with self.engine.connect() as connection:
            with connection.begin():  # Automatically handles transactions
                for params in params_list:
                    connection.execute(text(query), params)

    def get_account_info(self):
        print(f"User ID: {self.user_id}, User Name: {self.user_name}, Account Funds: {self.account_funds}")


pf = Portofolio()
pf.register_user("JohnDoe", "securepassword123")
pf.register_user("JaneSmith", "anothersecurepassword456")
pf.register_user("AliceJohnson", "yetanotherpassword789")
pf.get_account_info()
pf.log_in_user("JohnDoe", "securepassword123")
pf.get_account_info()

