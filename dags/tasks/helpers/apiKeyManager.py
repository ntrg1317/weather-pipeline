import time
import logging
from itertools import cycle

class ApiKeyManager:
    def __init__(self, api_keys, rate_limit):
        """
        Initializes the APIKeyManager.

        :param api_keys: List of API keys.
        :param rate_limit: Number of requests allowed per key per minute.
        """
        self.api_keys = cycle(api_keys)  # Create a cyclic iterator for API keys
        self.rate_limit = rate_limit
        self.usage = {key: 0 for key in api_keys}  # Track usage of each API key
        self.reset_time = time.time() + 60  # Track when to reset limits
        self.current_key = next(self.api_keys)

    def get_api_key(self):
        """
        Returns the current API key, switching to a new one if needed.
        """
        # Reset usage counts after one minute
        if time.time() > self.reset_time:
            logging.info("Resetting API key usage counts.")
            self.usage = {key: 0 for key in self.usage}
            self.reset_time = time.time() + 60

        # Check if current key is within limit
        if self.usage[self.current_key] < self.rate_limit:
            return self.current_key

        # Switch to the next key
        logging.warning(f"API key {self.current_key} reached its rate limit. Switching keys.")
        self.current_key = next(self.api_keys)
        return self.get_api_key()

    def increment_usage(self, api_key):
        """
        Increments the usage count for the given API key.
        """
        self.usage[api_key] += 1