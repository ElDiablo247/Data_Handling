
import logging
import time

# --- A Simple, Practical Logging Example ---

# 1. Configure the logger. This is the setup for your 'logbook'.
# - level=logging.DEBUG: Record everything, even the little details.
# - format=...: Puts a timestamp, level name, and message in every entry.
# - filename='test_process.log': The name of our log file.
# - filemode='w': 'w' means 'write', which overwrites the file every time we run.
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='test_process.log',
    filemode='w' # Use 'w' to start with a fresh log file for each run
)

print("Logging has been set up. Check the 'test_process.log' file after this script runs.")

# This function simulates a multi-step process, like your database testing.
def simulate_long_process():
    """Simulates creating a user, adding data, and then processing it."""
    
    logging.info("--- Starting the process --- ")
    
    # Step 1: Create a user
    logging.info("Creating a new user named 'test_user'.")
    user = {'name': 'test_user', 'funds': 1000}
    logging.debug(f"User object created: {user}")
    time.sleep(0.5) # Pause to make the log easier to read
    
    # Step 2: Add some positions (simulated)
    logging.info("Adding two positions for 'test_user'.")
    positions = [
        {'asset': 'AAPL', 'amount': 300},
        {'asset': 'GOOG', 'amount': 0} # Uh oh, a potential problem
    ]
    logging.debug(f"Positions to be processed: {positions}")
    time.sleep(0.5)

    # Step 3: Process the positions (where the error will happen)
    logging.info("Now, processing the positions to calculate shares.")
    for pos in positions:
        try:
            asset_name = pos['asset']
            amount = pos['amount']
            price = 150 # Let's pretend we fetched this price
            
            logging.debug(f"Processing {asset_name} with amount {amount} at price {price}")
            
            # This is where the error will happen
            if amount <= 0:
                raise ValueError("Investment amount cannot be zero or less.")
            
            shares = amount / price
            logging.info(f"Successfully calculated shares for {asset_name}: {shares:.2f}")

        except ValueError as e:
            # Here, logging is much better than print!
            # It records the error with full context to the log file.
            logging.error(f"Failed to process position for '{asset_name}'. Reason: {e}")

    logging.info("--- Process finished --- ")

# Run the simulation
if __name__ == "__main__":
    simulate_long_process()
    print("Process finished. See 'test_process.log' for the detailed story of what happened.")
