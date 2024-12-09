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
        self.sql_to_df()

    def execute_many(self, query, params_list):
        with self.engine.connect() as connection:
            with connection.begin():  # Automatically handles transactions
                for params in params_list:
                    connection.execute(text(query), params)

    def execute_query(self, query, params=None, fetch=False):
        with self.engine.begin() as connection:
            result = connection.execute(text(query), params or {})
            if fetch:
                return result.fetchall()  # Fetch results if requested

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

    def open_position(self, asset_name: str, amount: int, asset_type: str):
        sector = None
        if asset_type.lower() == 'stock':
            sector = input("This position is for a stock so a sector must be entered(Technology, Finance etc.): ").upper()
            self.add_stock_position(asset_name, amount, sector)           
        
        if asset_type.lower() == 'etf':
            self.add_etf_position(asset_name, amount)
        
        if asset_type.lower() == 'crypto':
            self.add_crypto_position(asset_name, amount)

        #a unique id is generated to represent each new position
        unique_id = self.generate_unique_id()
        new_entry = {'position_id': unique_id, 'position_name': asset_name, 'position_amount': amount, 'asset_type': asset_type, 'sector': sector}
        self.positions_df = pd.concat([self.positions_df, pd.DataFrame([new_entry])], ignore_index=True)
        self.append_position_to_db_query(unique_id, asset_name, amount, asset_type, sector)

    def add_stock_position(self, stock_name: str, amount: int, stock_sector: str):     
        if stock_name in self.stocks_df['stock_name'].values:
            self.stocks_df.loc[self.stocks_df['stock_name'] == stock_name, 'total_amount'] += amount
            self.update_asset_amount_query(stock_name, amount, 'stock')
        else:
            new_entry = {'stock_name': stock_name, 'total_amount': amount, 'sector': stock_sector}
            self.stocks_df = pd.concat([self.stocks_df, pd.DataFrame([new_entry])], ignore_index=True)     
            self.insert_new_asset_query(stock_name, amount, 'stock', stock_sector)

    def add_etf_position(self, etf_name: str, amount: int):
        if etf_name in self.etfs_df['etf_name'].values:
            self.etfs_df.loc[self.etfs_df['etf_name'] == etf_name, 'total_amount'] += amount           
            self.update_asset_amount_query(etf_name, amount, 'etf')
        else:
            new_entry = {'etf_name': etf_name, 'total_amount': amount}
            self.etfs_df = pd.concat([self.etfs_df, pd.DataFrame([new_entry])], ignore_index=True)          
            self.insert_new_asset_query(etf_name, amount, 'etf')

    def add_crypto_position(self, crypto_name: str, amount: int):
        if crypto_name in self.crypto_df['crypto_name'].values:
            self.crypto_df.loc[self.crypto_df['crypto_name'] == crypto_name, 'total_amount'] += amount
            self.update_asset_amount_query(crypto_name, amount, 'crypto')
        else:
            new_entry = {'crypto_name': crypto_name, 'total_amount': amount}
            self.crypto_df = pd.concat([self.crypto_df, pd.DataFrame([new_entry])], ignore_index=True)
            self.insert_new_asset_query(crypto_name, amount, 'crypto')

    def update_asset_amount_query(self, asset_name: str, amount: int, asset_type: str):
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

    def insert_new_asset_query(self, asset_name: str, amount: int, asset_type: str, sector=None):
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

    def append_position_to_db_query(self, position_id: str, asset_name: str, amount: int, asset_type: str, sector: str):
        query = """
        INSERT INTO positions(position_id, position_name, position_amount, asset_type, sector)
        VALUES(:id, :asset_name, :amount, :asset_type, :sector);
        """
        params = {'id': position_id, 'asset_name': asset_name, 'amount': amount, 'asset_type': asset_type, 'sector': sector}
        self.execute_query(query, params)

    def close_position(self, position_id: str):
        if position_id in self.positions_df['position_id'].values:
            position_row = self.positions_df[self.positions_df['position_id'] == position_id]
            asset_name = position_row['position_name'].iloc[0]
            amount = int(position_row['position_amount'].iloc[0])
            asset_type = position_row['asset_type'].iloc[0]
            if asset_type == 'stock':
                self.stocks_df.loc[self.stocks_df['stock_name'] == asset_name, 'total_amount'] -= amount
                self.decrease_asset_amount_query(asset_name, amount, asset_type)
            if asset_type == 'etf':
                self.stocks_df.loc[self.etfs_df['etf_name'] == asset_name, 'total_amount'] -= amount
                self.decrease_asset_amount_query(asset_name, amount, asset_type)
            if asset_type == 'crypto':
                self.crypto_df.loc[self.crypto_df['crypto_name'] == asset_name, 'total_amount'] -= amount
                self.decrease_asset_amount_query(asset_name, amount, asset_type)
            self.delete_position_from_memory(position_id)
        else:
            raise KeyError('position_id does not exist.')
            

    def decrease_asset_amount_query(self, asset_name: str, amount: int, asset_type: str):
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

    def delete_position_from_memory(self, position_id):
        to_drop = self.positions_df[self.positions_df['position_id'] == position_id].index
        self.positions_df.drop(to_drop, inplace=True)
        self.delete_position_query(position_id)

    def delete_position_query(self, position_id: str):
        query = """
        DELETE FROM positions
        WHERE position_id = :pos_id;
        """
        params = {'pos_id': position_id}
        self.execute_query(query, params)
        print('Position with id ', position_id, ' was deleted.')


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
    
    def show_tables_db(self):
        stocks_result = self.execute_query("""SELECT * FROM stocks""", fetch=True)
        print("Stocks")
        for entry in stocks_result:
            print(entry)
        print()
        etfs_result = self.execute_query("""SELECT * FROM etfs""", fetch=True)
        print("ETFs")
        for entry in etfs_result:
            print(entry)
        print()
        crypto_results = self.execute_query("""SELECT * FROM crypto""", fetch=True)
        print("Crypto")
        for entry in crypto_results:
            print(entry)
        print()
        positions_results = self.execute_query("""SELECT * FROM positions""", fetch=True)
        print("Positions")
        for entry in positions_results:
            print(entry)

    def sql_to_df(self):
        with self.engine.connect() as connection:
            self.stocks_df = pd.read_sql_table('stocks', connection)
            self.etfs_df = pd.read_sql_table('etfs', connection)
            self.crypto_df = pd.read_sql_table('crypto', connection)
            self.positions_df = pd.read_sql_table('positions', connection)
            self.unique_codes = set(self.positions_df['position_id'])
            self.show_tables_db()
            
    def upload_tables_to_db(self):
        self.stocks_df.to_sql('stocks', con=self.engine, if_exists='replace', index=False)
        self.etfs_df.to_sql('etfs', con=self.engine, if_exists='replace', index=False)
        self.crypto_df.to_sql('crypto', con=self.engine, if_exists='replace', index=False)
        self.positions_df.to_sql('positions', con=self.engine, if_exists='replace', index=False)

    

    
master = DatabaseManipulation()