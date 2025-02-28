from sqlalchemy import create_engine, text
import pandas as pd
import random
import string
import re


class Asset_Manager:
    def __init__(self):
        # Initialize SQLAlchemy engine
        self.engine = create_engine('postgresql+psycopg2://postgres:6987129457@localhost/assets_project_db')
        self.create_empty()
        self.unique_codes = set()
        self.account_amount = 0

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
    
    def open_position(self, asset_name: str, amount: int, asset_type: str, asset_sector="-"):
        """This function opens a new asset position and stores it both in memory (via a pandas DataFrame) and in the SQL database.
        
        If the asset_type is stock, the user is prompted to enter the sector the stock belongs to (e.g., APPLE belongs to the Technology sector).
        A unique ID is generated for the position using the generate_unique_id() function.
        The asset's details are compiled into a dictionary (new_entry) containing all relevant data.
        This entry is appended to the positions_df DataFrame in memory.
        Finally, the append_position_to_db() function is called to save the entry to the SQL database.
        
        Args:
            asset_name (str): The name  of the asset
            amount (int): Amount of position
            asset_type (str): Type of the asset (stock, ETF or crypto)
        """
        if amount < self.account_amount:
            sector = asset_sector
            if sector == None:
                sector = "-"
            if asset_type.lower() == 'stock':
                if sector == "-":
                    sector = input("This position is for a stock so a sector must be entered(Technology, Finance etc.): ").upper()  
                    self.add_stock_memory(asset_name, amount, sector)  
                else:
                    self.add_stock_memory(asset_name, amount, sector)             
            if asset_type.lower() == 'etf':
                self.add_etf_memory(asset_name, amount)
            if asset_type.lower() == 'crypto':
                self.add_crypto_memory(asset_name, amount)
            #a unique id is generated to represent each new position
            unique_id = self.generate_unique_id()
            new_entry = {'position_id': unique_id, 'position_name': asset_name, 'position_amount': amount, 'asset_type': asset_type, 'sector': sector}
            self.positions_df = pd.concat([self.positions_df, pd.DataFrame([new_entry])], ignore_index=True)
            self.append_position_to_db(unique_id, asset_name, amount, asset_type, sector)
        else:
            raise ValueError("Your account has ")

    def add_stock_memory(self, stock_name: str, amount: int, stock_sector: str):
        """ Function that adds a stock asset to memory dataframe. If the stock already exists in the `stocks_df` DataFrame, its `total_amount` is increased by the specified `amount`, and the change is reflected in the SQL database. If the stock does not exist, a new entry is created and added to both the in-memory DataFrame and the database.

        Args:
            stock_name (str): Name of the stock of type string
            amount (int): The amount of the stock position
            stock_sector (str): The sector in which the stock belongs (technology if stock is microsoft for example etc.)
        """
        if stock_name in self.stocks_df['stock_name'].values:
            self.stocks_df.loc[self.stocks_df['stock_name'] == stock_name, 'total_amount'] += amount
            print("Total amount of ", stock_name, " was increased by ", amount)
            self.increase_asset_amount_db(stock_name, amount, 'stock')
        else:
            new_entry = {'stock_name': stock_name, 'total_amount': amount, 'sector': stock_sector}
            self.stocks_df = pd.concat([self.stocks_df, pd.DataFrame([new_entry])], ignore_index=True)
            print("New entry in stocks. ", stock_name, " with an amount of ", amount)     
            self.insert_new_asset_db(stock_name, amount, 'stock', stock_sector)

    def add_etf_memory(self, etf_name: str, amount: int):
        """Function that adds an ETF asset to memory dataframe. If the ETF already exists in the `etfs_df` DataFrame, its `total_amount` is increased by the specified `amount`, and the change is reflected in the SQL database. If the ETF does not exist, a new entry is created and added to both the in-memory DataFrame and the database.

        Args:
            etf_name (str): Name of the ETF asset
            amount (int): Amount of the ETF asset
        """
        if etf_name in self.etfs_df['etf_name'].values:
            self.etfs_df.loc[self.etfs_df['etf_name'] == etf_name, 'total_amount'] += amount
            print("Total amount of ", etf_name, " was increased by ", amount)          
            self.increase_asset_amount_db(etf_name, amount, 'etf')
        else:
            new_entry = {'etf_name': etf_name, 'total_amount': amount}
            self.etfs_df = pd.concat([self.etfs_df, pd.DataFrame([new_entry])], ignore_index=True)
            print("New entry in ETFs. ", etf_name, " with an amount of ", amount)           
            self.insert_new_asset_db(etf_name, amount, 'etf')

    def add_crypto_memory(self, crypto_name: str, amount: int):
        """Function that adds a crypto asset to the memory dataframe. If the cryptocurrency already exists in the `crypto_df` DataFrame, its `total_amount` is increased by the specified `amount`, and the change is reflected in the SQL database. If the cryptocurrency does not exist, a new entry is created and added to both the in-memory DataFrame and the database.

        Args:
            crypto_name (str): Name of the crypto asset.
            amount (int): Amount of the crypto asset.
        """
        if crypto_name in self.crypto_df['crypto_name'].values:
            self.crypto_df.loc[self.crypto_df['crypto_name'] == crypto_name, 'total_amount'] += amount
            print("Total amount of ", crypto_name, " was increased by ", amount)
            self.increase_asset_amount_db(crypto_name, amount, 'crypto')
        else:
            new_entry = {'crypto_name': crypto_name, 'total_amount': amount}
            self.crypto_df = pd.concat([self.crypto_df, pd.DataFrame([new_entry])], ignore_index=True)
            print("New entry in crypto. ", crypto_name, " with an amount of ", amount) 
            self.insert_new_asset_db(crypto_name, amount, 'crypto')