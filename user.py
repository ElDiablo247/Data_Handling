class User:
    """
    Represents a user entity within the application.

    This class acts as a simple data container for user-related information.
    It is typically created upon successful login and holds identity data
    like the user's name and unique ID for the duration of a session.
    """

    def __init__(self, name: str):
        """
        Initializes a new User instance.

        Args:
            name (str): The user's real name.
        """
        self._real_name = name
        self._user_id = None
        self._user_name = None 


    def get_real_name(self) -> str:
        """
        Retrieves the user's full name.

        Args:
            None

        Returns:
            str: The full name of the user.
        """
        return self._real_name

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

    def set_user_id(self, value: str):
        """
        Sets or updates the user's unique ID.

        Args:
            value (str): The new unique ID to assign to the user.
        """
        self._user_id = value

    def set_user_name(self, value: str):
        """
        Sets or updates the user's login username.

        Args:
            value (str): The new login username to assign to the user.
        """
        self._user_name = value
