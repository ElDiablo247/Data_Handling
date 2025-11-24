from user_manager import UserManager
from user import User

class UserInterface:

    def __init__(self, user_manager_instance: UserManager):
        """
        Initializes the UserInterface with a dependency on a UserManager instance.

        Args:
            user_manager (UserManager): An instance of the UserManager for business logic.
        """
        self.user = None
        self.user_manager = user_manager_instance


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
        try:
            self.user_manager.register_user(user_name, password)
            print(f"User '{user_name}' registered successfully. You can now log in.")
        except ValueError as e:
            print(e) # The UI catches the error and is responsible for the FAILURE notification

    def log_in_user(self, user_name: str, password: str):
        """
        Orchestrates the user login process and session management.
        This function first validates that no user is currently logged into the session.
        It then delegates the authentication logic to the UserManager service. If the
        credentials are correct, it establishes a new session by storing the returned
        User object. If authentication fails, it catches the error and displays a
        user-friendly message.

        Args:
            user_name (str): The username of the account to log in.
            password (str): The plain-text password for verification.

        Raises:
            PermissionError: If a user is already logged in during the current session.
        """
        if self.user != None:
            raise PermissionError("You are already logged in. To log in with another account, please log out first.")
        try:
            logged_in_user = self.user_manager.log_in_user(user_name, password)
            self.user = logged_in_user
            print(f"Logged in successfully as {user_name}")
        except ValueError as e:
            print(e) # The UI catches the error and is responsible for the FAILURE notification

    def log_out_user(self):
        """
        Logs out the currently logged-in user by terminating the session.
        This function validates that a user is currently logged in. It then ends
        the session by clearing the stored User object.

        Raises:
            PermissionError: If no user is currently logged in to the session.
        """
        if self.user == None:
            raise PermissionError("No user is currently logged in.")
        self.user = None
        print("Logged out successfully.")