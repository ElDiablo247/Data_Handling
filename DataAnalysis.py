from Portofolio import Portofolio
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


class DataAnalysis:
    def __init__(self, object: Portofolio):
        self.positions_df = object.positions_df
        self.data_analysis_df = self.create_assets_data_df()
        self.create_assets_data_df()

    def create_assets_data_df(self):
        """Function that calculates total amounts of money for each asset type(450$ for stocks, 505$ for etfs etc.), count of assets for 
        each asset type(5 stocks, 8 crypto etc.) and also percentages of money invested and asset counts in comparison to total amount and total counts
        (32% in stocks, 45% in crypto etc.)"""
        # assets dataframes
        stocks = self.positions_df[self.positions_df['asset_type'] == 'stock']
        etfs = self.positions_df[self.positions_df['asset_type'] == 'etf']
        crypto = self.positions_df[self.positions_df['asset_type'] == 'crypto']
        # variables that store the total amounts invested in each type of asset
        total_amount_stocks = stocks['position_amount'].sum()
        total_amount_etfs = etfs['position_amount'].sum()
        total_amount_crypto = crypto['position_amount'].sum()
        total_amount_all_assets = self.positions_df['position_amount'].sum()

        # amount percentages for each asset type in comparison to total invested
        stocks_amount_percentage = (total_amount_stocks / total_amount_all_assets) * 100 
        etfs_amount_percentage = (total_amount_etfs / total_amount_all_assets) * 100
        crypto_amount_percentage = (total_amount_crypto / total_amount_all_assets) * 100
        all_assets_amount_percentage = (total_amount_all_assets / total_amount_all_assets) * 100

        data = {
            'asset type': ['stocks', 'etfs', 'crypto', 'all assets' ],
            'total amount invested': [total_amount_stocks, total_amount_etfs, total_amount_crypto, total_amount_all_assets],
            'amount invested(percentage)': [round(stocks_amount_percentage, 2), round(etfs_amount_percentage, 2), round(crypto_amount_percentage, 2), round(all_assets_amount_percentage, 2)]
        }
        local_df = pd.DataFrame(data)
        return local_df
        
    def show_data_analysis_df(self):
        print(self.data_analysis_df)

    def show_data_chart(self):
        plt.figure(figsize=(8, 6))
        sns.barplot(
            x='asset type', 
            y='amount invested(percentage)', 
            data=self.data_analysis_df, 
            palette='Blues_r'
        )
        for index, row in self.data_analysis_df.iterrows():
            plt.text(index, row['amount invested(percentage)'], f"${row['total amount invested']}", ha='center')

        # Add titles and labels
        plt.title("Portfolio Allocation")
        plt.ylabel("Percentage of Total Investment")
        plt.xlabel("Asset Type")
        plt.show()





