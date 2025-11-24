class User:

    def __init__(self, user_id: str, user_name: str, funds: float):
        """
        Initializes a new User instance with the user's core data.

        Args:
            user_id (str): The user's unique identifier.
            user_name (str): The user's login username.
            funds (float): The user's current available funds.
        """
        self._user_id = user_id
        self._user_name = user_name
        self._user_funds = funds


    def get_user_id(self) -> str:
        """
        Retrieves the user's unique ID.

        Args:
            None

        Returns:
            str: The unique identifier for the user.
        """
        return self._user_id

    def get_user_name(self) -> str:
        """
        Retrieves the user's username used for logging in.

        Args:
            None

        Returns:
            str: The login username of the user.
        """
        return self._user_name
    
    def get_user_funds(self) -> float:
        """
        Retrieves the user's current funds.

        Args:
            None

        Returns:
            float: The amount of funds the user has.
        """
        return self._user_funds
