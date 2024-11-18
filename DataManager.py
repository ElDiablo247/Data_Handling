import Stock, ETF, Crypto


class Assets:
    def __init__(self, holder_name: str):
        self.holder_name = holder_name
        self.total_amount = 0
        self.assets = dict()
        self.populate_assets()

    def get_holder_name(self):
        return self.holder_name
    
    def get_total_amount(self):
        return self.total_amount
    
    def add_asset(self, asset):
        if asset.get_type() == "Stock":
            self.assets["Stocks"].append(asset)
        elif asset.get_type() == "ETF":
            self.assets["ETFs"].append(asset)
        elif asset.get_type() == "Crypto":
            self.assets["Crypto"].append(asset)
        else:
            raise KeyError("Asset type entered is not allowed or does not exist.")
           
    def populate_assets(self):
        self.assets = {
            "Stocks": [],
            "ETFs": [],
            "Crypto": []
        }

    def show_assets(self):
        for key, value in self.assets.items():
            print(key, value)

nvidia = Stock.Stock("Nvidia", "Technology")
nvidia.open_position(200)
nvidia.open_position(300)
nvidia.open_position(400)
nvidia.open_position(500)
nvidia.get_positions()
voo = ETF.ETF("VOO", "Blackrock")
voo.open_position(250)
voo.open_position(34)
voo.get_positions()
asset_manager = Assets("Raul Birta")
asset_manager.add_asset(nvidia)
asset_manager.add_asset(voo)
asset_manager.show_assets()