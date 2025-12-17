from user_interface import UserInterface
from user_manager import UserManager
from backend_manager import BackendManager
from main_system import System

# 1. Create a single, shared instance of the BackendManager.
#    This creates the database engine and tables only once.
backend_manager = BackendManager()

# 2. Create service layer instances.
# Inject the shared backend_manager into them.
user_manager = UserManager(backend_manager)
system = System(backend_manager)

# 3. Create a UI instance for a user session.
#    Inject the shared services into it.
ui = UserInterface(user_manager=user_manager, system=system)

print("System is running... You can now use the 'ui' object.")

ui.log_in_user("eldiablo", "12345")
ui.open_position("AAPL", 320)
ui.open_position("MSFT", 150)
ui.log_out_user()