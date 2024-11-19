import Stock
import ETF
import Crypto


class Assets:
    def __init__(self, holder_name: str):
        self.holder_name = holder_name
        self.total_amount = 0
        self.stocks = dict()
        self.etfs = set()
        self.crypto = set()

    def get_holder_name(self):
        return self.holder_name

    def get_total_amount(self):
        return self.total_amount

    def add_asset(self, asset):
        asset_type = asset.get_type()
        if asset_type == "Stock":
            sector = asset.get_sector()
            if sector in self.stocks:
                self.stocks[sector].add(asset)
            else:
                self.stocks[sector] = set()
                self.stocks[sector].add(asset)

        elif asset_type == "ETF":
            self.etfs.add(asset)
        elif asset_type == "Crypto":
            self.crypto.add(asset)
        else:
            raise KeyError("Asset type entered is not allowed. Type must be 'Stock', 'ETF' or 'Crypto'.")

    def show_assets(self):
        print("Here the assets")
        for key, values in self.stocks.items():
            print(key, "Sector", " has:")
            for value in values:
                print(value.get_name())
        print("These are the ETFs:")
        for etf in self.etfs:
            print(etf.get_name())
        print("These are the cryptocoins:")
        for crypto in self.crypto:
            print(crypto.get_name())


nvidia = Stock.Stock("Nvidia", "Technology")
nvidia.open_position(200)
nvidia.open_position(300)

amd = Stock.Stock("AMD", "Technology")
amd.open_position(200)
amd.open_position(300)

voo = ETF.ETF("VOO", "Blackrock")
voo.open_position(250)
voo.open_position(34)
vti = ETF.ETF("VTI", "Blackrock")
vti.open_position(250)
vti.open_position(34)

bitcoin = Crypto.Crypto("Bitcoin")
bitcoin.open_position(225)
bitcoin.open_position(125)

asset_manager = Assets("Raul Birta")
asset_manager.add_asset(nvidia)
asset_manager.add_asset(amd)
asset_manager.add_asset(voo)
asset_manager.add_asset(vti)
asset_manager.add_asset(bitcoin)

asset_manager.show_assets()
