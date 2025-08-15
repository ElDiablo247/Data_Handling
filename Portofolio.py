
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


