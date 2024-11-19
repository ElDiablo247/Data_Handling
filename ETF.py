from datetime import datetime
import pandas as pd


class ETF:
    def __init__(self, etf_name: str, provider: str):
        self.name = etf_name
        self.type = "ETF"
        self.provider = provider
        self.total_amount = 0
        self.positions = dict()

    def get_name(self):
        return self.name

    def get_type(self):
        return self.type

    def get_provider(self):
        return self.provider

    def get_total_amount(self):
        return self.total_amount

    def get_positions(self):
        for key, value in self.positions.items():
            print(key, value)

    def open_position(self, amount: int):
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
        print(df)
        return df


