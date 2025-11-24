from user_interface import UserInterface
from user_manager import UserManager
from backend_manager import BackendManager

# 1. Create a single, shared instance of the BackendManager.
#    This creates the database engine and tables only once.
backend_manager = BackendManager()

# 2. Create a single, shared instance of the UserManager.
#    Inject the backend_manager into it.
user_manager = UserManager(backend_manager)

# 3. Create a UI instance for a user session.
#    Inject the shared user_manager into it.
ui = UserInterface(user_manager)
print("System is running... You can now use the 'ui' object.")