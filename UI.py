import tkinter as tk
from test import Counter

class CounterApp:
    """
    The UI layer of the application.
    It handles creating the window and widgets, and displaying data.
    It calls methods from the Counter class to perform operations.
    """
    def __init__(self, root):
        self.counter_logic = Counter()

        root.title("Counter App")
        root.geometry("250x150")

        # The label that displays the data from the business logic
        self.count_label = tk.Label(root, text="Count: 0", font=("Helvetica", 24))
        self.count_label.pack(pady=10)

        # --- Buttons that call methods in the UI layer ---
        increment_button = tk.Button(root, text="Increment", command=self.increment_ui)
        increment_button.pack(side=tk.LEFT, padx=10)

        decrement_button = tk.Button(root, text="Decrement", command=self.decrement_ui)
        decrement_button.pack(side=tk.RIGHT, padx=10)

    def increment_ui(self):
        """Method in the UI layer that calls the business logic and updates the UI."""
        new_count = self.counter_logic.increment()
        self.count_label.config(text=f"Count: {new_count}")

    def decrement_ui(self):
        """Method in the UI layer that calls the business logic and updates the UI."""
        new_count = self.counter_logic.decrement()
        self.count_label.config(text=f"Count: {new_count}")

if __name__ == "__main__":
    # This block runs when you execute the UI.py file directly
    main_window = tk.Tk()
    app = CounterApp(main_window)
    main_window.mainloop() # Starts the tkinter event loop
