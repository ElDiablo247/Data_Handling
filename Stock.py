from datetime import datetime
import random


class Stock:
    def __init__(self, company_name: str, sector: str):
        self.name = company_name
        self.type = "Stock"
        self.sector = sector
        self.amount = 0
        self.positions = dict()

    def get_name(self):
        return self.name
    
    def get_type(self):
        return self.type

    def get_sector(self):
        return self.sector

    def get_amount(self):
        return self.amount

    def get_positions(self):
        for key, value in self.positions.items():
            print(key, value)

    def open_position(self, amount: int):
        if amount >= 0:
            self.amount += amount
        self.store_position(amount)

    def store_position(self, amount: int):
        datetime_str = datetime.now().strftime("Date: %Y-%m-%d Time: %H:%M:%S")
        datetime_amount = [datetime_str, amount]
        position_id = self.create_random_id()
        self.positions[position_id] = datetime_amount
        print("Position opened and saved succesfully")

    def close_position(self, position_id: int):
        if position_id in self.positions:
            del self.positions[position_id]
            print(f"Position with number {position_id} was removed")
        else:
            raise KeyError("position_id not found in self.positions")

    def create_random_id(self):
        while True:
            random_id = random.randint(10000, 99999)
            if random_id not in self.positions:
                return random_id