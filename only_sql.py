from sqlalchemy import create_engine, text
import pandas as pd
import random
import string
import re


class Portofolio:
    def __init__(self, user_name: str):
        # Initialize SQLAlchemy engine
        self.engine = create_engine('postgresql+psycopg2://postgres:6987129457@localhost/assets_project_db_2')
        self.create_empty()
        self.user_name = None
        self.user_id = None
        self.account_funds = 0
        self.set_user_id(user_name)
        

    def create_empty(self):
        queries = [
            """CREATE TABLE IF NOT EXISTS stocks (
                stock_name VARCHAR(50) NOT NULL PRIMARY KEY,
                total_amount INT NOT NULL,
                sector VARCHAR(50) NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS etfs (
                etf_name VARCHAR(50) NOT NULL PRIMARY KEY,
                total_amount INT NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS crypto (
                crypto_name VARCHAR(50) NOT NULL PRIMARY KEY,
                total_amount INT NOT NULL
            );""",
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
                funds INT NOT NULL
            );"""
        ]
        for query in queries:
            self.execute_query(query)


    def set_user_id(self, user_name: str):
        # Query the database to check if the username exists
        query = """SELECT user_id FROM users WHERE user_name = :user_name;"""
        params = {'user_name': user_name}
        result = self.execute_query(query, params, fetch=True)

        if result:  # If user exists, set user_id from the database
            self.user_id = result[0][0]  # Extract user_id from the first tuple
            print(f"User '{user_name}' found!")
            user_input = input("Now enter your user ID to confirm your identity: ").strip()
            if user_input != self.user_id:
                raise ValueError("Incorrect User ID! Access denied.")  # Stop execution if wrong
            print(f"Access granted! Using existing ID: {self.user_id}")
        else:  # If user does not exist, create a new one
            self.user_name = user_name
            self.user_id = self.user_id_generator()  # Generate a guaranteed unique ID
            self.insert_new_user(self.user_id, user_name)
            print(f"New user '{user_name}' created with ID: {self.user_id}")
            
    def user_id_generator(self):
        """Generates a unique user_id by checking against existing IDs in the database."""
        query = """SELECT user_id FROM users;"""
        result = self.execute_query(query, fetch=True)
        
        # Store all existing user_ids in a set for fast lookup
        existing_user_ids = {row[0] for row in result}  

        while True:
            # Generate a new user_id
            letter_1 = self.user_name[0]
            letter_2 = random.choice(string.ascii_uppercase)
            numbers = f"{random.randint(0, 999):03d}"
            user_id = f"{letter_1}{letter_2}{numbers}"

            # Check if it's unique
            if user_id not in existing_user_ids:
                return user_id  # Return only if it's unique
            
    def insert_new_user(self, user_id: str, user_name: str):
        """Inserts a new user into the database."""
        query = """
        INSERT INTO users (user_id, user_name, funds) 
        VALUES (:user_id, :user_name, :funds);
        """
        params = {'user_id': user_id, 'user_name': user_name, 'funds': 0}  # Default funds = 0
        self.execute_query(query, params)
        print(f"New user '{user_name}' added to the database with ID: {user_id}")



    def execute_query(self, query: str, params=None, fetch=False):
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
            if fetch:
                return result.fetchall()  # Fetch results if requested
    
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

    

raul = Portofolio("Raul_Birta")
anna = Portofolio("Anna")