from user_manager import UserManager
from user import User
from main_system import System

class UserInterface:

    def __init__(self):
        """Initializes the UserInterface."""
        self.user = None
        self.user_manager = UserManager()
        self.system = System()

    
    def sign_up(self, user_name: str, password: str):
        """
        Orchestrates the user sign-up process
        It first validates that no user is currently logged into the session.
        It then delegates the complex registration logic to the UserManager service.

        Args:
            user_name (str): The desired username for the new account.
            password (str): The plain-text password for the new account.

        Raises:
            PermissionError: If a user is already logged in during the current session.
            ValueError: Propagated from UserManager if the chosen username already exists.
        """      
        if self.user != None:
            raise PermissionError("You are already logged in. To register a new account, please log out first.")
        self.user_manager.register_user(user_name, password)

    def log_in_user(self, user_name: str, password: str):
        
        if self.user != None:
            raise PermissionError("You are already logged in. To log in with another account, please log out first.")
        logged_in_user = self.user_manager.log_in_user(user_name, password)
        self.user = logged_in_user

    @requires_login
    def log_out_user(self):
        """
        Logs out the currently logged-in user by resetting user-related attributes.
        This function clears the user ID and username from the instance and sets
        the login status to False.

        Args:
            None

        Raises:
            PermissionError: If no user is currently logged in.
        Returns:
            None: Resets the user's session data.
        """
        self.user_id = None
        self.user_name = None
        self.signed_in = False
        print("Logged out successfully.")