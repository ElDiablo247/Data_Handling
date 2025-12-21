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
        df = pd.DataFrame([dict(row._mapping) for row in positions])
        return(df)
