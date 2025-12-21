from user_manager import UserManager
from user import User
from main_system import System
from data_visualiser import DataVisualiser
import pandas as pd


class UserInterface:

    def __init__(self, user_manager: UserManager, system: System):
        """
        Initializes the UserInterface with dependencies on the required services.

        Args:
            user_manager (UserManager): The service for user-related logic.
            system (System): The service for portfolio and trading logic.
        """
        self.user = None
        self.user_manager = user_manager
        self.system = system
        self.data_visualiser = DataVisualiser()


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

    def top_up_account_balance(self, amount: float):
        """
        Handles a request to top up the logged-in user's account balance.

        This function delegates the top-up operation to the System service and
        prints a success or failure message to the user.

        Args:
            amount (float): The amount to add to the user's account balance.
        """
        try:
            self.system.top_up_account_balance(self.user, amount)
            print(f"Successfully topped up account balance by ${amount}.")
        except ValueError as e:
            print(e) # The UI catches the error and is responsible for the FAILURE notification

    def open_position(self, ticker_symbol: str, position_amount: float):
        """
        Opens a new trading position for the logged-in user by delegating
        the operation to the System service.

        Args:
            ticker_symbol (str): The stock ticker symbol for the position.
            position_amount (float): The monetary amount to invest in the position.
        """
        try:
            self.system.open_position(self.user, ticker_symbol, position_amount)
            print(f"Successfully opened position {ticker_symbol} with amount ${position_amount}.")
        except ValueError as e:
            print(e) # The UI catches the error and is responsible for the FAILURE notification

    def close_position(self, index_to_close: int = None):
        """
        Closes a position, either interactively or by a pre-selected index.

        If `index_to_close` is None, the user is prompted to enter an index.
        Otherwise, the function attempts to close the position at the given index.

        Args:
            index_to_close (int, optional): The DataFrame index of the position to close.
                                            Defaults to None for interactive mode.
        """
        if self.user is None:
            print("[!] You must be logged in to close a position.")
            return

        try:
            # 1. Fetch and display positions
            positions_table = self.data_visualiser.fetch_positions_dataframe(self.user)
            if positions_table.empty:
                print("\n[!] You have no open positions to close.")
                return
            print("Your open positions:")
            print(positions_table)

            # 2. Determine the index to use
            index: int
            if index_to_close is None:
                # Interactive mode: get input from user
                user_input = input(f"\nEnter the index number (0-{len(positions_table) - 1}) to close: ")
                index = int(user_input)
            else:
                # Scripting mode: use provided index
                index = index_to_close
                print(f"\nAttempting to non-interactively close position at index: {index}")

            # 3. Validate index and get position ID
            if not 0 <= index < len(positions_table):
                print(f"[!] Error: Index {index} is out of range.")
                return

            position_id = positions_table.iloc[index]['position_id']

            # 4. Execute the closing logic
            self.system.close_position(self.user, position_id)
            print(f"✅ Successfully closed position {position_id}.")

        except ValueError:
            # Catches errors from int(user_input) if it's not a number
            print("[!] Error: Input must be a valid integer.")
        except Exception as e:
            # Catches other potential errors (e.g., from system layer)
            print(f"[!] Operation Failed: {e}")

    
    def show_user_positions(self):
        """
        Displays the current positions for the logged-in user using the DataVisualiser.
        """
        if self.user is None:
            raise PermissionError("You must be logged in to view your positions.")
        try:
            positions_table = self.data_visualiser.fetch_positions_dataframe(self.user)
            print(positions_table)
        except ValueError as e:
            print(e) # The UI catches the error and is responsible for the FAILURE notification