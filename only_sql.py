from sqlalchemy import create_engine, text
import pandas as pd
import random
import string
import re


class Portofolio:
    def __init__(self, user_name: str, password: str, action: str):
        # Initialize SQLAlchemy engine
        self.engine = create_engine('postgresql+psycopg2://postgres:6987129457@localhost/assets_project_db_2')
        self.create_empty()
        self.user_names = set() 
        self.user_ids = set()
        self.user_name = None
        self.user_id = None
        self.account_funds = None
        self.fetch_users_ids()
        self.execute_action(user_name, password, action)
        

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
                password VARCHAR(50) NOT NULL,
                funds INT NOT NULL DEFAULT 0
            );"""
        ]
        for query in queries:
            self.execute_query(query)

    def fetch_users_ids(self):
        query = """
        SELECT user_id, user_name FROM users;
        """
        result = self.execute_query(query, fetch='all')
        if result:
            for each in result:
                self.user_ids.add(each[0])
                self.user_names.add(each[1])


    def execute_action(self, user_name: str, password: str, action: str):
        if action == "log in":
            self.log_in_user(user_name, password)
        elif action == "register":
            self.create_new_user(user_name, password)
        else:
            raise ValueError("You have to type 'log in' if you are trying to log in, or 'register' if you are a new user.")

    def log_in_user(self, user_name: str, password: str):
        query = """
        SELECT user_id, user_name, funds FROM users WHERE user_name = :user_name_input AND password = :password_input;
        """
        params = {'user_name_input':user_name, 'password_input': password}
        result = self.execute_query(query, params, fetch='one')
        if result:
            self.user_id, self.user_name, self.funds = result
            print(f"User '{user_name}' was found and logged in.")
        else:
            raise ValueError(f"The password was wrong for '{user_name}'.  You have to try again.")
            

    def create_new_user(self, user_name: str, password: str):
        if user_name in self.user_names:
            print(f"Username '{user_name}' already exists. Try another one.")
            username_input = input("Type another user_name: ")
            self.create_new_user(username_input, password)
        else:
            self.user_name = user_name
            local_user_id = self.user_id_generator()  # Generate a guaranteed unique ID
            self.user_id = local_user_id
            self.insert_new_user_db(local_user_id, user_name, password)
            print(f"New user '{user_name}' created with ID: {self.user_id}")
            
    def user_id_generator(self):
        while True:
            # Generate a new user_id
            letter_1 = self.user_name[0]
            letter_2 = random.choice(string.ascii_uppercase)
            numbers = f"{random.randint(0, 999):03d}"
            generated_user_id = f"{letter_1}{letter_2}{numbers}"

            if generated_user_id not in self.user_ids:
                return generated_user_id  # Return only if it's unique
            
    def insert_new_user_db(self, user_id: str, user_name: str, password: str):
        """Inserts a new user into the database."""
        query = """
        INSERT INTO users (user_id, user_name, password) 
        VALUES (:user_id, :user_name, :password);
        """
        params = {'user_id': user_id, 'user_name': user_name, 'password': password}  # Default funds = 0
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




raul = Portofolio("Raul_Birta", "password1", "log in")
anna = Portofolio("Anna", "password2", "log in")
user = Portofolio("Blake", "password134", "log in")