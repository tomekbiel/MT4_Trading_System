"""
Trade Defaults - Default order parameters for MT4 trading operations.

This module provides default values for trade orders and utility functions
to manage these defaults. It's used to maintain consistent trading parameters
across the application.

Example:
    # Show current default order parameters
    from mt4_connector.trade_defaults import show_default_order, update_default_order
    
    show_default_order()
    
    # Update a default parameter
    update_default_order('volume', 0.02)
"""

# Default order parameters used when creating new trades
DEFAULT_ORDER = {
    "symbol": "US.100+",
    "type": 0,
    "volume": 0.01,
    "open_price": 0.0,
    "sl": 0.0,
    "tp": 0.0,
    "magic": 123456,
    "comment": "AutoTrade",
    "expiration": 0
}

def show_default_order():
    """
    Display the current default order parameters.
    
    Prints all default order parameters in a human-readable format.
    This is useful for debugging and verifying current settings.
    
    Example:
        >>> show_default_order()
        Default order parameters:
          symbol: US.100+
          type: 0
          volume: 0.01
          ...
    """
    print(" Default order parameters:")
    for key, value in DEFAULT_ORDER.items():
        print(f"  {key}: {value}")

def update_default_order(key, value):
    """
    Update a default order parameter.
    
    Args:
        key (str): The parameter name to update (must be a key in DEFAULT_ORDER)
        value: The new value for the parameter (type must match expected type)
        
    Returns:
        bool: True if the update was successful, False otherwise
        
    Example:
        >>> update_default_order('volume', 0.02)
        Changed: volume = 0.02
        True
        
        >>> update_default_order('invalid_key', 100)
        Key 'invalid_key' does not exist in the default order.
        False
    """
    if key in DEFAULT_ORDER:
        try:
            # Try to convert the value to the same type as the default
            current_type = type(DEFAULT_ORDER[key])
            if current_type is not type(None):  # Don't convert None types
                if current_type is float and isinstance(value, (int, float)):
                    value = float(value)
                elif current_type is int and isinstance(value, (int, float)):
                    value = int(value)
                elif current_type is str and not isinstance(value, str):
                    value = str(value)
                    
            DEFAULT_ORDER[key] = value
            print(f"Changed: {key} = {value}")
            return True
        except (ValueError, TypeError) as e:
            print(f"Error updating {key}: {e}")
            return False
    else:
        print(f"Key '{key}' does not exist in the default order.")
        return False