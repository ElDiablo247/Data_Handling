
from sqlalchemy import create_engine, text
import pandas as pd
import random
import string
import re


class Portofolio:
    def __init__(self, username: str):
        # Initialize SQLAlchemy engine
        self.engine = create_engine('postgresql+psycopg2://postgres:6987129457@localhost/assets_project_db')
        self.username = ""
        self.set_username(username)
        self.create_empty()
        self.stocks_df = pd.DataFrame()
        self.etfs_df = pd.DataFrame()
        self.crypto_df = pd.DataFrame()
        self.positions_df = pd.DataFrame()
        self.unique_codes = set()
        self.load_data_from_db_to_memory()
        self.account_amount = 0

    def add_money(self, amount: int):
        self.account_amount += amount

    def get_account_amount(self) -> int:
        return self.account_amount

    def get_username(self) -> str:
        return self.username
    

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

    
        memory_table_df = getattr(self, f"{asset_table}_df", None) # table to be compared is set dynamically depending on user input
        if memory_table_df is None:
            print("Not a valid table name")
            return False
        
        # SQL table equivalent with the one in memory is fetched and converted to a dataframe
        query = f"SELECT * FROM {asset_table};"
        sql_table = self.execute_query(query, fetch=True)
        sql_table_df = pd.DataFrame(sql_table, columns=memory_table_df.columns)
        if memory_table_df.empty and sql_table_df.empty:
            print("Both in-memory and SQL database tables are empty.")
            return True
        # below we sort both memory and SQL tables to have the same order
        sorted_memory_df = memory_table_df.sort_values(by=memory_table_df.columns.tolist()).reset_index(drop=True)
        sorted_sql_df = sql_table_df.sort_values(by=sql_table_df.columns.tolist()).reset_index(drop=True)

        # here we compare to see if they are equal and contain the same entries
        if sorted_memory_df.equals(sorted_sql_df):
            print("Both tables match.")
            return True
        else:
            print("Tables have mismatches")
            return False
    
    def call_test_function(self, name: str):
        """Calls test_function(name) in PostgreSQL and returns the result."""
        query = """
        SELECT test_function(:name);
        """
        result = self.execute_query(query, {"name": name}, fetch=True)
        return result[0][0]  # Extract the message from the fetched result


master = Portofolio("Raul")




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