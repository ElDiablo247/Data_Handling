from sqlalchemy import create_engine, text
import pandas as pd
import random
import string


class DatabaseManipulation:
    def __init__(self):
        # Initialize SQLAlchemy engine
        self.engine = create_engine('postgresql+psycopg2://postgres:6987129457@localhost/assets_project_db')
        self.create_empty()
        self.stocks_df = pd.DataFrame()
        self.etfs_df = pd.DataFrame()
        self.crypto_df = pd.DataFrame()
        self.positions_df = pd.DataFrame()
        self.unique_codes = set()
        self.load_data_from_db_to_memory()

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

    def open_position(self, asset_name: str, amount: int, asset_type: str):
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
        sector = None
        if asset_type.lower() == 'stock':
            sector = input("This position is for a stock so a sector must be entered(Technology, Finance etc.): ").upper()
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

    def add_stock_memory(self, stock_name: str, amount: int, stock_sector: str):
        """ Function that adds a stock asset to memory dataframe. If the stock already exists in the `stocks_df` DataFrame, its `total_amount` is increased by the specified `amount`, and the change is reflected in the SQL database. If the stock does not exist, a new entry is created and added to both the in-memory DataFrame and the database.

        Args:
            stock_name (str): Name of the stock of type string
            amount (int): The amount of the stock position
            stock_sector (str): The sector in which the stock belongs (technology if stock is microsoft for example etc.)
        """
        if stock_name in self.stocks_df['stock_name'].values:
            self.stocks_df.loc[self.stocks_df['stock_name'] == stock_name, 'total_amount'] += amount
            self.increase_asset_amount_db(stock_name, amount, 'stock')
        else:
            new_entry = {'stock_name': stock_name, 'total_amount': amount, 'sector': stock_sector}
            self.stocks_df = pd.concat([self.stocks_df, pd.DataFrame([new_entry])], ignore_index=True)     
            self.insert_new_asset_db(stock_name, amount, 'stock', stock_sector)

    def add_etf_memory(self, etf_name: str, amount: int):
        """Function that adds an ETF asset to memory dataframe. If the ETF already exists in the `etfs_df` DataFrame, its `total_amount` is increased by the specified `amount`, and the change is reflected in the SQL database. If the ETF does not exist, a new entry is created and added to both the in-memory DataFrame and the database.

        Args:
            etf_name (str): Name of the ETF asset
            amount (int): Amount of the ETF asset
        """
        if etf_name in self.etfs_df['etf_name'].values:
            self.etfs_df.loc[self.etfs_df['etf_name'] == etf_name, 'total_amount'] += amount           
            self.increase_asset_amount_db(etf_name, amount, 'etf')
        else:
            new_entry = {'etf_name': etf_name, 'total_amount': amount}
            self.etfs_df = pd.concat([self.etfs_df, pd.DataFrame([new_entry])], ignore_index=True)          
            self.insert_new_asset_db(etf_name, amount, 'etf')

    def add_crypto_memory(self, crypto_name: str, amount: int):
        """Function that adds a crypto asset to the memory dataframe. If the cryptocurrency already exists in the `crypto_df` DataFrame, its `total_amount` is increased by the specified `amount`, and the change is reflected in the SQL database. If the cryptocurrency does not exist, a new entry is created and added to both the in-memory DataFrame and the database.

        Args:
            crypto_name (str): Name of the crypto asset.
            amount (int): Amount of the crypto asset.
        """
        if crypto_name in self.crypto_df['crypto_name'].values:
            self.crypto_df.loc[self.crypto_df['crypto_name'] == crypto_name, 'total_amount'] += amount
            self.increase_asset_amount_db(crypto_name, amount, 'crypto')
        else:
            new_entry = {'crypto_name': crypto_name, 'total_amount': amount}
            self.crypto_df = pd.concat([self.crypto_df, pd.DataFrame([new_entry])], ignore_index=True)
            self.insert_new_asset_db(crypto_name, amount, 'crypto')


    def close_position(self, position_id: str):
        """This function removes the specified position from the `positions_df` DataFrame in memory and the SQL database based on the user input unique position_id. It also adjusts the relevant asset amounts in their respective DataFrames (`stocks_df`, `etfs_df`, `crypto_df`) based on the asset type (stock, ETF, or crypto). If the user input does not exist, it raises a KeyError to notify the user.

        Args:
            position_id (str): A string representing the unique id of the asset to be deleted

        """
        if position_id in self.positions_df['position_id'].values:
            self.delete_position_from_memory(position_id)

            position_row = self.positions_df[self.positions_df['position_id'] == position_id]
            asset_name = position_row['position_name'].iloc[0]
            amount = int(position_row['position_amount'].iloc[0])
            asset_type = position_row['asset_type'].iloc[0]
            if asset_type == 'stock':
                self.remove_stock_memory(asset_name, amount)
            if asset_type == 'etf':
                self.remove_etf_memory(asset_name, amount)
            if asset_type == 'crypto':
                self.remove_crypto_memory(asset_name, amount)  
        else:
            raise KeyError('position_id does not exist.')

    def remove_stock_memory(self, stock_name: str, amount: int):
        if stock_name in self.stocks_df['stock_name'].values:
            self.stocks_df.loc[self.stocks_df['stock_name'] == stock_name, 'total_amount'] -= amount
            self.decrease_asset_amount_db(stock_name, amount, 'stock')
        else:
            raise KeyError('Stock name ', stock_name, ' does not exist.')

    def remove_etf_memory(self, etf_name: str, amount: int):
        if etf_name in self.etfs_df['etf_name'].values:
            self.etfs_df.loc[self.etfs_df['etf_name'] == etf_name, 'total_amount'] -= amount
            self.decrease_asset_amount_db(etf_name, amount, 'etf')
        else:
            raise KeyError('ETF name ', etf_name, ' does not exist.')

    def remove_crypto_memory(self, crypto_name: str, amount: int):
        if crypto_name in self.crypto_df['crypto_name'].values:
            self.crypto_df.loc[self.crypto_df['crypto_name'] == crypto_name, 'total_amount'] -= amount
            self.decrease_asset_amount_db(crypto_name, amount, 'crypto')
        else:
            raise KeyError('Crypto name ', crypto_name, ' does not exist.')
    
    def delete_position_from_memory(self, position_id):
        to_drop = self.positions_df[self.positions_df['position_id'] == position_id].index
        self.positions_df.drop(to_drop, inplace=True)
        self.delete_position_db(position_id)

    def generate_unique_id(self):
        flag = False
        while flag == False:
            letter = random.choice(string.ascii_uppercase)
            numbers = f"{random.randint(0, 999):03d}"
            unique_id = f"{letter}{numbers}"
            if unique_id not in self.unique_codes:
                self.unique_codes.add(unique_id)
                flag = True
                return unique_id
    
    def load_data_from_db_to_memory(self):
        with self.engine.connect() as connection:
            self.stocks_df = pd.read_sql_table('stocks', connection)
            self.etfs_df = pd.read_sql_table('etfs', connection)
            self.crypto_df = pd.read_sql_table('crypto', connection)
            self.positions_df = pd.read_sql_table('positions', connection)
            self.unique_codes = set(self.positions_df['position_id'])
            self.show_all_assets_in_memory()

    def show_all_assets_in_memory(self):
        print("Stocks")
        print(self.stocks_df)
        print()
        
        print("ETFs")
        print(self.etfs_df)
        print()
        
        print("Crypto")
        print(self.crypto_df)
        print()
        
        print("Positions")
        print(self.positions_df)

    def upload_tables_to_db(self):
        self.stocks_df.to_sql('stocks', con=self.engine, if_exists='replace', index=False)
        self.etfs_df.to_sql('etfs', con=self.engine, if_exists='replace', index=False)
        self.crypto_df.to_sql('crypto', con=self.engine, if_exists='replace', index=False)
        self.positions_df.to_sql('positions', con=self.engine, if_exists='replace', index=False)

        
    def create_empty(self):
        queries = [
            """CREATE TABLE IF NOT EXISTS stocks (
                stock_name VARCHAR(255) NOT NULL PRIMARY KEY,
                total_amount INT NOT NULL,
                sector VARCHAR(255) NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS etfs (
                etf_name VARCHAR(255) NOT NULL PRIMARY KEY,
                total_amount INT NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS crypto (
                crypto_name VARCHAR(255) NOT NULL PRIMARY KEY,
                total_amount INT NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS positions (
                position_id INT PRIMARY KEY,
                position_name VARCHAR(255) NOT NULL,
                position_amount INT NOT NULL,
                asset_type VARCHAR(255) NOT NULL,
                sector VARCHAR(255) NOT NULL
            );"""
        ]
        for query in queries:
            self.execute_query(query)

    def append_position_to_db(self, position_id: str, asset_name: str, amount: int, asset_type: str, sector: str):
        query = """
        INSERT INTO positions(position_id, position_name, position_amount, asset_type, sector)
        VALUES(:id, :asset_name, :amount, :asset_type, :sector);
        """
        params = {'id': position_id, 'asset_name': asset_name, 'amount': amount, 'asset_type': asset_type, 'sector': sector}
        self.execute_query(query, params)

    def increase_asset_amount_db(self, asset_name: str, amount: int, asset_type: str):
        if asset_type == 'stock':
            query = """
            UPDATE stocks 
            SET total_amount = total_amount + :amount 
            WHERE stock_name = :name;
            """
            params = {'amount': amount, 'name': asset_name}
            self.execute_query(query, params)
            print('Stock total amount increased')
        if asset_type == 'etf':
            query = """
            UPDATE etfs 
            SET total_amount = total_amount + :amount 
            WHERE etf_name = :name;
            """
            params = {'amount': amount, 'name': asset_name}
            self.execute_query(query, params)
            print('ETF total amount increased')
        if asset_type == 'crypto':
            query = """
            UPDATE crypto 
            SET total_amount = total_amount + :amount 
            WHERE crypto_name = :name;
            """
            params = {'amount': amount, 'name': asset_name}
            self.execute_query(query, params)
            print('Crypto total amount increased')


    def insert_new_asset_db(self, asset_name: str, amount: int, asset_type: str, sector=None):
        if asset_type == 'stock':
            query = """
            INSERT INTO stocks(stock_name, total_amount, sector)
            VALUES(:name, :amount, :sector);
            """
            params = {'name': asset_name, 'amount': amount, 'sector': sector}
            self.execute_query(query, params)
            print('New stock created')
        if asset_type == 'etf':
            query = """
            INSERT INTO etfs(etf_name, total_amount)
            VALUES(:name, :amount);
            """
            params = {'name': asset_name, 'amount': amount}
            self.execute_query(query, params)
            print('New ETF created')
        if asset_type == 'crypto':
            query = """
            INSERT INTO crypto(crypto_name, total_amount)
            VALUES(:name, :amount);
            """
            params = {'name': asset_name, 'amount': amount}
            self.execute_query(query, params)
            print('New crypto asset created')

    def delete_position_db(self, position_id: str):
        query = """
        DELETE FROM positions
        WHERE position_id = :pos_id;
        """
        params = {'pos_id': position_id}
        self.execute_query(query, params)
        print('Position with id ', position_id, ' was deleted.')

    def decrease_asset_amount_db(self, asset_name: str, amount: int, asset_type: str):
        if asset_type == 'stock':
            query = """
            UPDATE stocks 
            SET total_amount = total_amount - :amount 
            WHERE stock_name = :name;
            """
            params = {'amount': amount, 'name': asset_name}
            self.execute_query(query, params)
            print('Stock amount decreased')
        if asset_type == 'etf':
            query = """
            UPDATE etfs 
            SET total_amount = total_amount - :amount 
            WHERE etf_name = :name;
            """
            params = {'amount': amount, 'name': asset_name}
            self.execute_query(query, params)
            print('ETF amount decreased')
        if asset_type == 'crypto':
            query = """
            UPDATE crypto 
            SET total_amount = total_amount - :amount 
            WHERE crypto_name = :name;
            """
            params = {'amount': amount, 'name': asset_name}
            self.execute_query(query, params)
            print('Crypto amount decreased')

    def show_total_invested(self):
        stocks_total = self.stocks_df['total_amount'].sum()
        etfs_total = self.etfs_df['total_amount'].sum()
        crypto_total = self.crypto_df['total_amount'].sum()
        grand_total = stocks_total + etfs_total + crypto_total
        data = {'stocks_total': [stocks_total], 'etfs_total': [etfs_total], 'crypto_total': [crypto_total], 'grand_total': [grand_total]}
        total_assets_df = pd.DataFrame(data)
        print(total_assets_df)