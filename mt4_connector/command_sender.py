"""
MT4 Command Sender - A high-level interface for sending commands to MetaTrader 4.

This module provides the MT4CommandSender class, which extends MT4BaseConnector to provide
convenience methods for sending various trading commands to a MetaTrader 4 terminal.

Example:
    # Create a command sender instance
    sender = MT4CommandSender()
    
    # Send a heartbeat
    sender.send_heartbeat()
    
    # Get account information
    sender.send_get_account_info()
    
    # Request historical data
    sender.send_historical_data('EURUSD', 'M15', '2023.01.01', '2023.01.31')"""

import time
from mt4_connector.base_connector import MT4BaseConnector

class MT4CommandSender(MT4BaseConnector):
    """
    A high-level interface for sending trading commands to MetaTrader 4.
    
    This class extends MT4BaseConnector to provide convenient methods for sending
    various trading commands to an MT4 terminal. It handles the command formatting
    and provides feedback through the logging system.
    
    The class implements the abstract methods from MT4BaseConnector and adds
    specific command methods for different trading operations.
    
    Note:
        This class is designed to be used with the corresponding MQL4 Expert Advisor
        that understands the command protocol used by these methods.
    """

    def _process_message(self, msg):
        """
        Process incoming messages from MT4.
        
        This method is called automatically when a message is received from MT4.
        It currently prints the message to the console.
        
        Args:
            msg (str): The message received from MT4
        """
        print("✅ Response from MT4:", msg)

    def _process_stream_message(self, msg):
        """
        Handle streaming data from MT4.
        
        This implementation ignores streaming data as this class is focused on
        command/response interactions. Override in a subclass to handle streaming data.
        
        Args:
            msg (str): The stream message from MT4
        """
        pass  # Ignore SUB stream data

    def send_heartbeat(self):
        """
        Send a heartbeat message to MT4.
        
        This is typically used to check if the connection to MT4 is alive.
        
        Returns:
            bool: True if the message was sent successfully, False otherwise.
        """
        print("📡 Sending: HEARTBEAT")
        return self.send("HEARTBEAT")

    def send_open_trade(self):
        """
        Send a command to open a trade.
        
        Returns:
            bool: True if the message was sent successfully, False otherwise.
        """
        print("📡 Sending: TRADE;OPEN")
        return self.send("TRADE;OPEN")

    def send_modify_trade(self):
        """
        Send a command to modify an existing trade.
        
        Returns:
            bool: True if the message was sent successfully, False otherwise.
        """
        print("📡 Sending: TRADE;MODIFY")
        return self.send("TRADE;MODIFY")

    def send_close_trade(self):
        """
        Send a command to close a trade.
        
        Returns:
            bool: True if the message was sent successfully, False otherwise.
        """
        print("📡 Sending: TRADE;CLOSE")
        return self.send("TRADE;CLOSE")

    def send_close_partial_trade(self):
        """
        Send a command to close a part of a trade.
        
        Returns:
            bool: True if the message was sent successfully, False otherwise.
        """
        print("📡 Sending: TRADE;CLOSE_PARTIAL")
        return self.send("TRADE;CLOSE_PARTIAL")

    def send_close_magic_trade(self):
        """
        Send a command to close a trade by magic number.
        
        Returns:
            bool: True if the message was sent successfully, False otherwise.
        """
        print("📡 Sending: TRADE;CLOSE_MAGIC")
        return self.send("TRADE;CLOSE_MAGIC")

    def send_close_all_trades(self):
        """
        Send a command to close all trades.
        
        Returns:
            bool: True if the message was sent successfully, False otherwise.
        """
        print("📡 Sending: TRADE;CLOSE_ALL")
        return self.send("TRADE;CLOSE_ALL")

    def send_get_open_trades(self):
        """
        Send a command to retrieve a list of open trades.
        
        Returns:
            bool: True if the message was sent successfully, False otherwise.
        """
        print("📡 Sending: TRADE;GET_OPEN_TRADES")
        return self.send("TRADE;GET_OPEN_TRADES")

    def send_get_account_info(self):
        """
        Send a command to retrieve account information.
        
        Returns:
            bool: True if the message was sent successfully, False otherwise.
        """
        print("📡 Sending: TRADE;GET_ACCOUNT_INFO")
        return self.send("TRADE;GET_ACCOUNT_INFO")

    def send_track_prices(self):
        """
        Send a command to track prices.
        
        Returns:
            bool: True if the message was sent successfully, False otherwise.
        """
        print("📡 Sending: TRACK_PRICES")
        return self.send("TRACK_PRICES")

    def send_track_rates(self):
        """
        Send a command to track rates.
        
        Returns:
            bool: True if the message was sent successfully, False otherwise.
        """
        print("📡 Sending: TRACK_RATES")
        return self.send("TRACK_RATES")

    def send_historical_data(self, symbol, timeframe, date_from, date_to):
        """
        Request historical price data from MT4.
        
        Args:
            symbol (str): The trading symbol (e.g., 'EURUSD')
            timeframe (str): The chart timeframe (e.g., 'M15', 'H1', 'D1')
            date_from (str): Start date in 'YYYY.MM.DD' format
            date_to (str): End date in 'YYYY.MM.DD' format
            
        Returns:
            bool: True if the request was sent successfully, False otherwise
            
        Note:
            The actual historical data will be received asynchronously through
            the _process_message method.
        """
        command = f"HIST;{symbol};{timeframe};{date_from};{date_to}"
        print(f"📡 Sending: {command}")
        return self.send(command)