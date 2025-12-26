import pandas as pd
from backend_manager import BackendManager
from user import User

class DataVisualiser:
    def __init__(self):
        """
        Initializes the DataVisualiser.
        The constructor is intentionally empty; dependencies are passed to methods.
        """
        self.backend_instance = BackendManager()

    def fetch_positions_dataframe(self, user: User):
        """
        Fetches and displays the user's positions in a Pandas DataFrame.
        """
        user_id = user.get_user_id()
        positions = self.backend_instance.retrieve_user_positions(user_id)

        # Convert SQLAlchemy Row objects to a list of dictionaries to preserve column names
        if not positions:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row._mapping) for row in positions])

        # Split the datetime column for better readability
        if 'open_datetime' in df.columns:
            df['open_datetime'] = pd.to_datetime(df['open_datetime'])
            df['open_date'] = df['open_datetime'].dt.strftime('%Y-%m-%d')
            df['open_time'] = df['open_datetime'].dt.strftime('%H:%M:%S')
            df = df.drop(columns=['open_datetime'])

        return df
    
    def fetch_trades_dataframe(self, user: User):
        """
        Fetches and displays the user's trade history in a Pandas DataFrame.
        """
        user_id = user.get_user_id()
        trades = self.backend_instance.retrieve_user_trades(user_id)

        if not trades:
            return pd.DataFrame()

        # Convert SQLAlchemy Row objects to a list of dictionaries to preserve column names
        df = pd.DataFrame([dict(row._mapping) for row in trades])

        # Ensure open_datetime is a datetime type for sorting before we format it
        if 'open_datetime' in df.columns:
            df['open_datetime'] = pd.to_datetime(df['open_datetime'])

        # Sort the DataFrame to group trades by position and order them chronologically.
        # - open_datetime: The primary sort key, ensuring chronological order of positions.
        # - position_id: A secondary key to group the BUY and SELL trades of the same position together.
        #                This is crucial for positions opened in the same second.
        # - state: A tertiary key to ensure that for any given position, the 'BUY' trade always appears before the 'SELL' trade.
        df = df.sort_values(by=['open_datetime', 'position_id', 'state'], ascending=[True, True, True])

        # Split datetime columns for better readability
        if 'open_datetime' in df.columns:
            df['open_date'] = df['open_datetime'].dt.strftime('%Y-%m-%d')
            df['open_time'] = df['open_datetime'].dt.strftime('%H:%M:%S')
            df = df.drop(columns=['open_datetime'])

        if 'close_datetime' in df.columns:
            df['close_datetime'] = pd.to_datetime(df['close_datetime'], errors='coerce')
            df['close_date'] = df['close_datetime'].dt.strftime('%Y-%m-%d').fillna('-')
            df['close_time'] = df['close_datetime'].dt.strftime('%H:%M:%S').fillna('-')
            df = df.drop(columns=['close_datetime'])

        return df
