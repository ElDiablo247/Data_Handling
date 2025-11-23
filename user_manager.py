from user import User
from backend_manager import BackendManager
import bcrypt
import random
import string

class UserManager:

    def __init__(self):
        """Initializes the UserManager with an empty user list."""
        self.data_manager = BackendManager()


    def register_user(self, username: str, password: str):
        """
        Orchestrates the registration of a new user in a single atomic transaction.

        This function manages the entire registration workflow:
        1. It begins a database transaction to ensure all steps succeed or fail together.
        2. It checks if the desired username is already taken.
        3. It generates a guaranteed unique user ID.
        4. It securely hashes the user's password.
        5. It inserts the new user record into the database.

        Args:
            username (str): The desired username for the new account.
            password (str): The plain-text password to hash and store securely.

        Raises:
            ValueError: If the username already exists in the database.
        """
        
        with self.data_manager.engine.begin() as connection:
            user_exists = self.data_manager.username_exists(username, connection=connection)
            if user_exists:
                raise ValueError(f"Username '{username}' already exists. Try another one.") 
            
            hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode() # Hash the password before storing
            new_user_id = self.generate_user_id(connection=connection)
            self.data_manager.insert_user_db(new_user_id, username, hashed_password, connection=connection)

    def log_in_user(self, user_name: str, password: str):
        """
        Authenticates a user by verifying their username and password.
        If the credentials are correct, the user's ID and username are loaded into memory.

        Args:
            user_name (str): The username of the account to log in.
            password (str): The plain-text password to verify against the stored hash.

        Raises:
            PermissionError: If a user is already logged in.
            ValueError: If the username is not found or the password is incorrect.

        Returns:
            None: Updates the object's user-related attributes and prints login confirmation.
        """
        user_row = self.data_manager.retrieve_user_by_username(user_name)
        if not user_row:
            raise ValueError(f"The username '{user_name}' was not found.") 
            
        stored_hash_password = user_row.hash_password

        if not bcrypt.checkpw(password.encode(), stored_hash_password.encode()):
            raise ValueError("Incorrect password.")
        return User(user_id=user_row.user_id, user_name=user_row.user_name, funds=user_row.funds)
        

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

    def generate_user_id(self, connection=None) -> str:
        """
        Generates a unique, random ID for a new user.

        This function repeatedly generates a 5-character ID (e.g., 'AB123') and
        checks for its uniqueness in the database until a free ID is found.
        This entire process is designed to run within a larger transaction.

        Args:
            connection (sqlalchemy.engine.Connection, optional): An existing database
                connection to use for the uniqueness check. Defaults to None.

        Returns:
            str: A guaranteed unique 5-character user ID.
        """
        while True:
            # Generate a new id strinig
            letter_1 = random.choice(string.ascii_uppercase)
            letter_2 = random.choice(string.ascii_uppercase)
            numbers = f"{random.randint(0, 999):03d}"
            combined_string = f"{letter_1}{letter_2}{numbers}"

            user_id_exists = self.data_manager.user_id_exists(combined_string, connection=connection)
            if not user_id_exists:
                return combined_string
            