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


    def close_position(self, position_id: str):
        """This function removes the specified position from the `positions_df` DataFrame in memory and the SQL database based on the user input unique position_id. It also adjusts the relevant asset amounts in their respective DataFrames (`stocks_df`, `etfs_df`, `crypto_df`) based on the asset type (stock, ETF, or crypto). If the user input does not exist, it raises a KeyError to notify the user.

        Args:
            position_id (str): A string representing the unique id of the asset to be deleted

        """
        if position_id in self.positions_df['position_id'].values:
            position_row = self.positions_df[self.positions_df['position_id'] == position_id]
            asset_name = position_row['position_name'].iloc[0]
            amount = int(position_row['position_amount'].iloc[0])
            asset_type = position_row['asset_type'].iloc[0]

            if asset_type == 'stock':
                self.decrease_stock_amount_memory(asset_name, amount)
            if asset_type == 'etf':
                self.decrease_etf_amount_memory(asset_name, amount)
            if asset_type == 'crypto':
                self.decrease_crypto_amount_memory(asset_name, amount)  
            
            self.delete_position_from_memory(position_id) #remove the position

        else:
            raise KeyError('position_id does not exist.')

    def decrease_stock_amount_memory(self, stock_name: str, amount: int):
        """This function is called from close_position function if the position exists, and should not be directly called by the user because of data integrity. This function subtracts the position amount from the stock asset (self.stocks) and if it reaches 0 then completely deletes the asset.

        Args:
            stock_name (str): The name of the stock of type string ("Apple")
            amount (int): The amount of the stock position at time of purchase

        Raises:
            KeyError: Stock name does not exist.
        """
        if stock_name in self.stocks_df['stock_name'].values:
            self.stocks_df.loc[self.stocks_df['stock_name'] == stock_name, 'total_amount'] -= amount          
            self.decrease_stock_amount_db(stock_name, amount)

            new_amount = self.stocks_df.loc[self.stocks_df['stock_name'] == stock_name, 'total_amount'].values[0]
            if new_amount == 0:
                self.stocks_df.drop(self.stocks_df[self.stocks_df['stock_name'] == stock_name].index, inplace=True)
                print("Stock asset ", stock_name, " reached total amount 0 and was removed from memory")
        else:
            raise KeyError('Stock name ', stock_name, ' does not exist.')

    def decrease_etf_amount_memory(self, etf_name: str, amount: int):
        """This function is called from close_position function if the position exists, and should not be directly called by the user because of data integrity. This function subtracts the position amount from the ETF asset (self.etfs) and if it reaches 0 then completely deletes the asset.

        Args:
            etf_name (str): The name of the ETF of type string ("VOO")
            amount (int): The amount of the ETF position at time of purchase

        Raises:
            KeyError: ETF name does not exist
        """
        if etf_name in self.etfs_df['etf_name'].values:
            self.etfs_df.loc[self.etfs_df['etf_name'] == etf_name, 'total_amount'] -= amount
            self.decrease_etf_amount_db(etf_name, amount)

            new_amount = self.etfs_df.loc[self.etfs_df['etf_name'] == etf_name, 'total_amount'].values[0]
            if new_amount == 0:
                self.etfs_df.drop(self.etfs_df[self.etfs_df['etf_name'] == etf_name].index, inplace=True)
                print("ETF asset ", etf_name, " reached total amount 0 and was removed from memory")
        else:
            raise KeyError('ETF name ', etf_name, ' does not exist.')

    def decrease_crypto_amount_memory(self, crypto_name: str, amount: int):
        """This function is called from close_position function if the position exists, and should not be directly called by the user because of data integrity. This function subtracts the position amount from the crypto asset (self.crypto) and if it reaches 0 then completely deletes the asset.

        Args:
            crypto_name (str): The name of the crypto of type string ("Bitcoin")
            amount (int): The amount of the crypto position at time of purchase

        Raises:
            KeyError: Crypto name does not exist
        """
        if crypto_name in self.crypto_df['crypto_name'].values:
            self.crypto_df.loc[self.crypto_df['crypto_name'] == crypto_name, 'total_amount'] -= amount
            self.decrease_crypto_amount_db(crypto_name, amount)

            new_amount = self.crypto_df.loc[self.crypto_df['crypto_name'] == crypto_name, 'total_amount'].values[0]
            if new_amount == 0:
                self.crypto_df.drop(self.crypto_df[self.crypto_df['crypto_name'] == crypto_name].index, inplace=True)
                print("Crypto asset ", crypto_name, " reached total amount 0 and was removed from memory")
        else:
            raise KeyError('Crypto name ', crypto_name, ' does not exist.')
    
    def delete_position_from_memory(self, position_id: str):
        """Function that deletes a position from memory (self.positions) by using the position_id string parameter to search for it. Here we do not have to implement any error checking because it has
        already been checked that the position exists, so coming so far means that the position exist in self.positions or else an error would have been raised sooner.

        Args:
            position_id (str): The string position_id to look for ("J444")
        """
        to_drop = self.positions_df[self.positions_df['position_id'] == position_id].index
        self.positions_df.drop(to_drop, inplace=True)
        print('Position with id ', position_id, ' was deleted from memory.')
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
                position_id VARCHAR(255) PRIMARY KEY,
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
        if asset_type == 'etf':
            query = """
            UPDATE etfs 
            SET total_amount = total_amount + :amount 
            WHERE etf_name = :name;
            """
            params = {'amount': amount, 'name': asset_name}
            self.execute_query(query, params)
        if asset_type == 'crypto':
            query = """
            UPDATE crypto 
            SET total_amount = total_amount + :amount 
            WHERE crypto_name = :name;
            """
            params = {'amount': amount, 'name': asset_name}
            self.execute_query(query, params)


    def insert_new_asset_db(self, asset_name: str, amount: int, asset_type: str, sector=None):
        if asset_type == 'stock':
            query = """
            INSERT INTO stocks(stock_name, total_amount, sector)
            VALUES(:name, :amount, :sector);
            """
            params = {'name': asset_name, 'amount': amount, 'sector': sector}
            self.execute_query(query, params)
        if asset_type == 'etf':
            query = """
            INSERT INTO etfs(etf_name, total_amount)
            VALUES(:name, :amount);
            """
            params = {'name': asset_name, 'amount': amount}
            self.execute_query(query, params)
        if asset_type == 'crypto':
            query = """
            INSERT INTO crypto(crypto_name, total_amount)
            VALUES(:name, :amount);
            """
            params = {'name': asset_name, 'amount': amount}
            self.execute_query(query, params)

    def delete_position_db(self, position_id: str):
        query = """
        DELETE FROM positions
        WHERE position_id = :pos_id;
        """
        params = {'pos_id': position_id}
        self.execute_query(query, params)
        print('Position with id ', position_id, ' was deleted from database.')

    def show_total_invested(self):
        stocks_total = self.stocks_df['total_amount'].sum()
        etfs_total = self.etfs_df['total_amount'].sum()
        crypto_total = self.crypto_df['total_amount'].sum()
        grand_total = stocks_total + etfs_total + crypto_total
        data = {'stocks_total': [stocks_total], 'etfs_total': [etfs_total], 'crypto_total': [crypto_total], 'grand_total': [grand_total]}
        total_assets_df = pd.DataFrame(data)
        print(total_assets_df)

    def decrease_stock_amount_db(self, asset_name: str, amount: int):
        query = """
        UPDATE stocks 
        SET total_amount = total_amount - :amount 
        WHERE stock_name = :name;

        DELETE FROM stocks 
        WHERE stock_name = :name AND total_amount <= 0;
        """
        params = {'amount': amount, 'name': asset_name}
        self.execute_query(query, params)
        print('Stock amount decreased')

    def decrease_etf_amount_db(self, asset_name: str, amount: int):
        query = """
        UPDATE etfs 
        SET total_amount = total_amount - :amount 
        WHERE etf_name = :name;

        DELETE FROM etfs 
        WHERE etf_name = :name AND total_amount <= 0;
        """
        params = {'amount': amount, 'name': asset_name}
        self.execute_query(query, params)
        print('ETF amount decreased')

    def decrease_crypto_amount_db(self, asset_name: str, amount: int):
        query = """
        UPDATE crypto 
        SET total_amount = total_amount - :amount 
        WHERE crypto_name = :name;

        DELETE FROM crypto
        WHERE crypto_name = :name AND total_amount <= 0;
        """
        params = {'amount': amount, 'name': asset_name}
        self.execute_query(query, params)
        print('Crypto amount decreased')

    

    def remove_asset(self, asset_name: str):
        position_id_list = []
        for index, row in self.positions_df.iterrows():
            if row['position_name'] == asset_name:
                position_id_list.append(row['position_id'])
            else:
                print("Position with asset name ", asset_name, " does not exist.")
        for id in position_id_list:
            self.close_position(id)

    def close_all_positions(self):
        self.stocks_df = pd.DataFrame()
        self.etfs_df = pd.DataFrame()
        self.crypto_df = pd.DataFrame()
        self.positions_df = pd.DataFrame()
        
        query = """
        DROP TABLE IF EXISTS stocks, etfs, crypto, positions;
        """
        
        self.execute_query(query)
        self.create_empty()
        print("All tables in memory and database have been reset")



master = DatabaseManipulation()
master.show_total_invested()

"""
assets = [
    # BTC (Bitcoin) - Total: $2,159.49
    ('BTC', 800.00, 'crypto'),
    ('BTC', 500.00, 'crypto'),
    ('BTC', 300.00, 'crypto'),
    ('BTC', 200.00, 'crypto'),
    ('BTC', 159.49, 'crypto'),
    ('BTC', 100.00, 'crypto'),
    ('BTC', 100.00, 'crypto'),

    # ETH (Ethereum) - Total: $999.90
    ('ETH', 400.00, 'crypto'),
    ('ETH', 300.00, 'crypto'),
    ('ETH', 150.00, 'crypto'),
    ('ETH', 100.00, 'crypto'),
    ('ETH', 49.90, 'crypto'),

    # SOL (Solana) - Total: $781.73
    ('SOL', 300.00, 'crypto'),
    ('SOL', 200.00, 'crypto'),
    ('SOL', 150.00, 'crypto'),
    ('SOL', 100.00, 'crypto'),
    ('SOL', 31.73, 'crypto'),

    # AAPL (Apple) - Total: $489.00
    ('AAPL', 200.00, 'stock', 'Technology'),
    ('AAPL', 150.00, 'stock', 'Technology'),
    ('AAPL', 100.00, 'stock', 'Technology'),
    ('AAPL', 39.00, 'stock', 'Technology'),

    # GOOG (Alphabet) - Total: $600.00
    ('GOOG', 250.00, 'stock', 'Technology'),
    ('GOOG', 150.00, 'stock', 'Technology'),
    ('GOOG', 100.00, 'stock', 'Technology'),
    ('GOOG', 50.00, 'stock', 'Technology'),
    ('GOOG', 50.00, 'stock', 'Technology'),

    # META (Meta Platforms Inc) - Total: $810.00
    ('META', 300.00, 'stock', 'Technology'),
    ('META', 200.00, 'stock', 'Technology'),
    ('META', 150.00, 'stock', 'Technology'),
    ('META', 100.00, 'stock', 'Technology'),
    ('META', 60.00, 'stock', 'Technology'),

    # MSFT (Microsoft) - Total: $640.00
    ('MSFT', 250.00, 'stock', 'Technology'),
    ('MSFT', 150.00, 'stock', 'Technology'),
    ('MSFT', 100.00, 'stock', 'Technology'),
    ('MSFT', 80.00, 'stock', 'Technology'),
    ('MSFT', 60.00, 'stock', 'Technology'),

    # AMZN (Amazon) - Total: $980.00
    ('AMZN', 400.00, 'stock', 'Consumer Discretionary'),
    ('AMZN', 300.00, 'stock', 'Consumer Discretionary'),
    ('AMZN', 150.00, 'stock', 'Consumer Discretionary'),
    ('AMZN', 100.00, 'stock', 'Consumer Discretionary'),
    ('AMZN', 30.00, 'stock', 'Consumer Discretionary'),

    # JPM (JPMorgan Chase) - Total: $495.32
    ('JPM', 200.00, 'stock', 'Financials'),
    ('JPM', 150.00, 'stock', 'Financials'),
    ('JPM', 100.00, 'stock', 'Financials'),
    ('JPM', 45.32, 'stock', 'Financials'),

    # KO (Coca-Cola) - Total: $685.09
    ('KO', 300.00, 'stock', 'Consumer Staples'),
    ('KO', 200.00, 'stock', 'Consumer Staples'),
    ('KO', 100.00, 'stock', 'Consumer Staples'),
    ('KO', 85.09, 'stock', 'Consumer Staples'),

    # WMT (Walmart) - Total: $250.00
    ('WMT', 100.00, 'stock', 'Consumer Staples'),
    ('WMT', 75.00, 'stock', 'Consumer Staples'),
    ('WMT', 50.00, 'stock', 'Consumer Staples'),
    ('WMT', 25.00, 'stock', 'Consumer Staples'),

    # MA (Mastercard) - Total: $804.89
    ('MA', 300.00, 'stock', 'Financials'),
    ('MA', 200.00, 'stock', 'Financials'),
    ('MA', 150.00, 'stock', 'Financials'),
    ('MA', 100.00, 'stock', 'Financials'),
    ('MA', 54.89, 'stock', 'Financials'),

    # V (Visa) - Total: $1,049.87
    ('V', 400.00, 'stock', 'Financials'),
    ('V', 300.00, 'stock', 'Financials'),
    ('V', 200.00, 'stock', 'Financials'),
    ('V', 100.00, 'stock', 'Financials'),
    ('V', 49.87, 'stock', 'Financials'),

    # TSLA (Tesla) - Total: $590.34
    ('TSLA', 250.00, 'stock', 'Automotive'),
    ('TSLA', 150.00, 'stock', 'Automotive'),
    ('TSLA', 100.00, 'stock', 'Automotive'),
    ('TSLA', 90.34, 'stock', 'Automotive'),

    # BRK.B (Berkshire Hathaway) - Total: $1,360.00
    ('BRK.B', 500.00, 'stock', 'Financials'),
    ('BRK.B', 400.00, 'stock', 'Financials'),
    ('BRK.B', 300.00, 'stock', 'Financials'),
    ('BRK.B', 100.00, 'stock', 'Financials'),
    ('BRK.B', 60.00, 'stock', 'Financials'),

    # NFLX (Netflix) - Total: $441.24
    ('NFLX', 200.00, 'stock', 'Technology'),
    ('NFLX', 150.00, 'stock', 'Technology'),
    ('NFLX', 50.00, 'stock', 'Technology'),
    ('NFLX', 41.24, 'stock', 'Technology'),

    # NVDA (Nvidia) - Total: $1,724.81
    ('NVDA', 600.00, 'stock', 'Technology'),
    ('NVDA', 500.00, 'stock', 'Technology'),
    ('NVDA', 300.00, 'stock', 'Technology'),
    ('NVDA', 200.00, 'stock', 'Technology'),
    ('NVDA', 100.00, 'stock', 'Technology'),
    ('NVDA', 24.81, 'stock', 'Technology'),

    # SPOT (Spotify) - Total: $110.00
    ('SPOT', 50.00, 'stock', 'Technology'),
    ('SPOT', 30.00, 'stock', 'Technology'),
    ('SPOT', 20.00, 'stock', 'Technology'),
    ('SPOT', 10.00, 'stock', 'Technology'),

    # AIR.PA (Airbus) - Total: $480.00
    ('AIR.PA', 200.00, 'stock', 'Industrials'),
    ('AIR.PA', 150.00, 'stock', 'Industrials'),
    ('AIR.PA', 100.00, 'stock', 'Industrials'),
    ('AIR.PA', 30.00, 'stock', 'Industrials'),

    # ARM (ARM Holdings) - Total: $554.00
    ('ARM', 200.00, 'stock', 'Technology'),
    ('ARM', 150.00, 'stock', 'Technology'),
    ('ARM', 100.00, 'stock', 'Technology'),
    ('ARM', 54.00, 'stock', 'Technology'),

    # COST (Costco) - Total: $610.00
    ('COST', 250.00, 'stock', 'Consumer Staples'),
    ('COST', 150.00, 'stock', 'Consumer Staples'),
    ('COST', 100.00, 'stock', 'Consumer Staples'),
    ('COST', 60.00, 'stock', 'Consumer Staples'),
    ('COST', 50.00, 'stock', 'Consumer Staples'),

    # PYPL (PayPal) - Total: $680.00
    ('PYPL', 300.00, 'stock', 'Financials'),
    ('PYPL', 200.00, 'stock', 'Financials'),
    ('PYPL', 100.00, 'stock', 'Financials'),
    ('PYPL', 80.00, 'stock', 'Financials'),

    # QCOM (Qualcomm) - Total: $570.00
    ('QCOM', 250.00, 'stock', 'Technology'),
    ('QCOM', 150.00, 'stock', 'Technology'),
    ('QCOM', 100.00, 'stock', 'Technology'),
    ('QCOM', 70.00, 'stock', 'Technology'),

    # BLK (BlackRock) - Total: $240.00
    ('BLK', 100.00, 'stock', 'Financials'),
    ('BLK', 75.00, 'stock', 'Financials'),
    ('BLK', 50.00, 'stock', 'Financials'),
    ('BLK', 15.00, 'stock', 'Financials'),

    # AMD (Advanced Micro Devices) - Total: $949.93
    ('AMD', 400.00, 'stock', 'Technology'),
    ('AMD', 300.00, 'stock', 'Technology'),
    ('AMD', 150.00, 'stock', 'Technology'),
    ('AMD', 99.93, 'stock', 'Technology'),

    # RACE (Ferrari) - Total: $390.00
    ('RACE', 150.00, 'stock', 'Automotive'),
    ('RACE', 100.00, 'stock', 'Automotive'),
    ('RACE', 75.00, 'stock', 'Automotive'),
    ('RACE', 65.00, 'stock', 'Automotive'),

    # CMG (Chipotle) - Total: $561.00
    ('CMG', 200.00, 'stock', 'Consumer Discretionary'),
    ('CMG', 150.00, 'stock', 'Consumer Discretionary'),
    ('CMG', 100.00, 'stock', 'Consumer Discretionary'),
    ('CMG', 111.00, 'stock', 'Consumer Discretionary'),

    # SPY (S&P 500 ETF) - Total: $690.00
    ('SPY', 300.00, 'etf'),
    ('SPY', 200.00, 'etf'),
    ('SPY', 100.00, 'etf'),
    ('SPY', 90.00, 'etf'),

    # QQQ (Invesco QQQ) - Total: $829.99
    ('QQQ', 400.00, 'etf'),
    ('QQQ', 200.00, 'etf'),
    ('QQQ', 150.00, 'etf'),
    ('QQQ', 79.99, 'etf'),

    # VTI (Vanguard Total Stock) - Total: $400.19
    ('VTI', 200.00, 'etf'),
    ('VTI', 100.00, 'etf'),
    ('VTI', 50.00, 'etf'),
    ('VTI', 50.19, 'etf'),

    # VOO (Vanguard S&P 500) - Total: $460.00
    ('VOO', 200.00, 'etf'),
    ('VOO', 150.00, 'etf'),
    ('VOO', 100.00, 'etf'),
    ('VOO', 10.00, 'etf'),

    # TSM (Taiwan Semiconductor) - Total: $1,025.03
    ('TSM', 400.00, 'stock', 'Technology'),
    ('TSM', 300.00, 'stock', 'Technology'),
    ('TSM', 200.00, 'stock', 'Technology'),
    ('TSM', 100.00, 'stock', 'Technology'),
    ('TSM', 25.03, 'stock', 'Technology'),

    # LII (Lennox International) - Total: $325.08
    ('LII', 150.00, 'stock', 'Industrials'),
    ('LII', 100.00, 'stock', 'Industrials'),
    ('LII', 50.00, 'stock', 'Industrials'),
    ('LII', 25.08, 'stock', 'Industrials'),
]

positions = []
for asset in assets:
    positions.append(list(asset)) 

for position in positions:
    if len(position) == 4:
        master.open_position(position[0], position[1], position[2], position[3])
    else:
        master.open_position(position[0], position[1], position[2])

"""