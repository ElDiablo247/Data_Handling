class Counter:
    """
    A simple class that holds a value and can be incremented or decremented.
    This class contains only business logic and does not print anything.
    """
    def __init__(self):
        self._count = 0

    def get_count(self) -> int:
        """Returns the current count."""
        return self._count

    def increment(self) -> int:
        """Increments the count by 1 and returns the new value."""
        self._count += 1
        return self._count

    def decrement(self) -> int:
        """Decrements the count by 1 and returns the new value."""
        self._count -= 1
        return self._count