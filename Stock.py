from datetime import datetime
import pandas as pd
import time


class Stock:
    """
    Description: Class that represents an asset in the stock market of type stock like nvidia or Apple etc.

    Methods:
        1.
        2.
        3.

    """

    def __init__(self, company_name: str, sector: str):
        """
        Description: Constructor

        Attributes:
            1. ???? - Type: ???? - ?????????????
            2. ???? - Type: ???? - ?????????????
            3. ???? - Type: ???? - ?????????????
            4. ???? - Type: ???? - ?????????????

        Function calls:
            1.
            2.

        """
        self.name = company_name
        self.type = "Stock"
        self.sector = sector
        self.total_amount = 0
        self.positions = dict()

    def get_name(self):
        return self.name

    def get_type(self):
        return self.type

    def get_sector(self):
        return self.sector

    def get_total_amount(self):
        return self.total_amount

    def get_positions(self):
        for key, value in self.positions.items():
            print(key, value)

    def open_position(self, amount: int):
        time.sleep(1)
        if amount >= 0:
            self.total_amount += amount
        self.store_position(amount)

    def store_position(self, amount: int):
        position_id = len(self.positions) + 1
        current_datetime = datetime.now()
        date_str = current_datetime.strftime("%d-%m-%Y")
        time_str = current_datetime.strftime("%H:%M:%S")
        self.positions[position_id] = [date_str, time_str, amount]
        print("Position opened and saved succesfully")

    def sell_all_positions(self):
        self.positions = dict()
        print("Positions is now empty!")

    def create_dataframe(self):
        # Convert dictionary to DataFrame
        df = pd.DataFrame.from_dict(self.positions, orient='index', columns=['Date', 'Time', 'Amount'])
        df.index.name = 'ID'  # Setting the DataFrame index name as 'ID'
        print("Stocks collection ")
        print(df)
        return df




