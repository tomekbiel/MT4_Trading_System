# -*- coding: utf-8 -*-
import datetime

"""
MT4 Market Data Handler - Real-time market data processing and storage.

This module provides the MT4MarketDataHandler class which extends MT4BaseConnector
to handle real-time market data from MetaTrader 4. It supports subscribing to
multiple currency pairs, processing streaming data, and optionally saving to CSV.

Features:
- Real-time bid/ask price streaming
- Multiple symbol subscription
- CSV data storage with automatic file management
- Thread-safe data handling
- Verbose logging options

Example:
    # Initialize the market data handler
    handler = MT4MarketDataHandler(
        csv_output_dir='market_data',
        save_to_csv=True,
        verbose=True
    )
    
    # Subscribe to symbols
    handler.subscribe('EURUSD+')
    handler.subscribe('US.100+')
    
    # When done
    handler.shutdown()
"""

from .base_connector import MT4BaseConnector
import csv
import os
from pandas import Timestamp
import zmq
import time


class MT4MarketDataHandler(MT4BaseConnector):
    """
    Handler for real-time market data from MetaTrader 4.
    
    This class extends MT4BaseConnector to provide specialized handling of
    real-time market data, including subscription management and data storage.
    
    Args:
        csv_output_dir (str): Directory to save CSV files (default: 'market_data')
        save_to_csv (bool): Whether to save market data to CSV files (default: True)
        **kwargs: Additional arguments passed to MT4BaseConnector
        
    Attributes:
        market_data (dict): In-memory storage of market data by symbol and timestamp
        csv_writers (dict): CSV writers for each symbol
        csv_files (dict): Open file handles for each symbol's CSV file
        msg_delimiter (str): Delimiter used in incoming messages (default: ';')
        main_delimiter (str): Main delimiter for message parsing (default: ':|:')
    """
    def __init__(self, csv_output_dir='market_data', save_to_csv=True, **kwargs):
        super().__init__(**kwargs)
        self.csv_output_dir = csv_output_dir
        self.save_to_csv = save_to_csv
        self.market_data = {}
        self.csv_writers = {}
        self.csv_files = {}
        self.msg_delimiter = ';'
        self.main_delimiter = ':|:'
        # Initialize raw data callback as None
        self.raw_data_callback = None  # Add this line


        if self.save_to_csv:
            self._init_csv_directory()

    def _init_csv_directory(self):
        """
        Initialize the CSV output directory.
        
        Creates the output directory if it doesn't exist and logs the action
        if verbose mode is enabled.
        """
        if not os.path.exists(self.csv_output_dir):
            os.makedirs(self.csv_output_dir)
            if self.verbose:
                print(f"[CSV] Created output directory: {self.csv_output_dir}")

    def _init_csv_writer(self, symbol):
        """
        Initialize a CSV writer for a specific symbol.
        
        Args:
            symbol (str): The trading symbol to initialize writer for
            
        Creates a new CSV file if it doesn't exist, or appends to an existing one.
        The CSV will have columns: timestamp, bid, ask
        """
        if symbol in self.csv_writers:
            return  # Already initialized
            
        # Initialize csv_file variable in case we need to close it in the except block
        csv_file = None
        
        try:
            # Create the full file path
            filename = os.path.join(os.path.abspath(self.csv_output_dir), f"{symbol}.csv")
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            # Check if file exists to determine if we need to write headers
            file_exists = os.path.isfile(filename)
            
            # Open the file in append mode with newline='' to prevent extra newlines
            csv_file = open(filename, 'a', newline='', encoding='utf-8')
            writer = csv.writer(csv_file)
            
            # Write headers if this is a new file
            if not file_exists:
                writer.writerow(['timestamp', 'bid', 'ask'])
                csv_file.flush()
            
            # Store the file and writer objects
            self.csv_files[symbol] = csv_file
            self.csv_writers[symbol] = writer
            
        except Exception as e:
            import traceback
            error_msg = f"[ERROR] Failed to initialize CSV writer for {symbol}: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            
            # Clean up if there was an error
            if csv_file is not None:
                try:
                    csv_file.close()
                except Exception as close_error:
                    print(f"[WARNING] Failed to close CSV file for {symbol}: {close_error}")
                    
            # Remove from dictionaries to allow retry
            self.csv_writers.pop(symbol, None)
            self.csv_files.pop(symbol, None)



    def _save_to_csv(self, symbol, timestamp, bid, ask, max_retries=3, retry_delay=0.1):
        """
        Save market data to CSV file with error handling and retries.
        
        Args:
            symbol (str): Trading symbol
            timestamp (str): Timestamp of the data point
            bid (float): Bid price
            ask (float): Ask price
            max_retries (int): Maximum number of retry attempts (default: 3)
            retry_delay (float): Delay between retries in seconds (default: 0.1)
            
        Returns:
            bool: True if data was saved successfully, False otherwise
        """
        if not self.save_to_csv or not hasattr(self, 'csv_output_dir'):
            return False
            
        last_error = None
        
        for attempt in range(max_retries + 1):  # +1 for the initial attempt
            try:
                # Initialize CSV writer if it doesn't exist for this symbol
                if symbol not in self.csv_writers:
                    self._init_csv_writer(symbol)
                
                # Skip if writer initialization failed
                if symbol not in self.csv_writers or symbol not in self.csv_files:
                    print(f"[WARNING] Failed to initialize CSV writer for {symbol}")
                    return False
                
                # Get writer and file handle
                writer = self.csv_writers[symbol]
                csv_file = self.csv_files[symbol]
                
                # Write data
                writer.writerow([timestamp, bid, ask])
                csv_file.flush()
                os.fsync(csv_file.fileno())
                return True
                
            except (IOError, OSError) as e:
                last_error = e
                if attempt < max_retries:
                    print(f"[WARNING] File operation failed for {symbol} (attempt {attempt + 1}/{max_retries}): {str(e)}")
                    time.sleep(retry_delay)
                    
                    # Try to reinitialize the writer on error
                    try:
                        if symbol in self.csv_files:
                            try:
                                self.csv_files[symbol].close()
                            except:
                                pass
                        
                        self.csv_writers.pop(symbol, None)
                        self.csv_files.pop(symbol, None)
                        self._init_csv_writer(symbol)
                    except Exception as init_error:
                        print(f"[WARNING] Failed to reinitialize CSV writer for {symbol}: {str(init_error)}")
                        
            except Exception as e:
                last_error = e
                print(f"[ERROR] Unexpected error saving {symbol} data: {str(e)}")
                if attempt >= max_retries:
                    break
                time.sleep(retry_delay)
        
        # If we get here, all retries failed
        error_msg = f"[ERROR] Failed to save {symbol} data after {max_retries + 1} attempts"
        if last_error:
            error_msg += f": {str(last_error)}"
        print(error_msg)
        return False

    # ====== ADD THESE NEW METHODS NEW MODIFICATION ======
    def set_raw_data_callback(self, callback_func):
        """
        Set a callback function to receive raw MT4 data.
        The callback will receive the raw message string from MT4.
        """
        self.raw_data_callback = callback_func

    def _process_message(self, message):
        """Process incoming message from MT4 with raw data forwarding"""
        try:
            print(f"[DEBUG] _process_message called with message: {message[:100]}..." if len(str(message)) > 100 else f"[DEBUG] _process_message called with message: {message}")
            print(f"[DEBUG] raw_data_callback is set: {self.raw_data_callback is not None}")
            
            # Call raw data callback if set
            if self.raw_data_callback is not None:
                try:
                    print("[DEBUG] Calling raw_data_callback...")
                    self.raw_data_callback(message)
                    print("[DEBUG] raw_data_callback completed")
                except Exception as e:
                    print(f"[RAW DATA ERROR] In callback: {str(e)}")
                    import traceback
                    traceback.print_exc()
            else:
                print("[DEBUG] No raw_data_callback set")

            # Call parent class implementation with explicit class reference
            print("[DEBUG] Calling parent _process_message...")
            super(MT4MarketDataHandler, self)._process_message(message)
            print("[DEBUG] Parent _process_message completed")
        except Exception as e:
            print(f"[ERROR] Processing message: {str(e)}")
            if self.verbose:
                import traceback
                traceback.print_exc()

    # ====== END OF ADDED CODE - END OF MODIFICATION ======

    def subscribe(self, symbol='US.100+'):
        """
        Subscribe to market data updates for a specific symbol.
        
        Args:
            symbol (str): The trading symbol to subscribe to (e.g., 'EURUSD+', 'US.100+')
        """
        self.sub_sock.setsockopt_string(zmq.SUBSCRIBE, symbol)
        if self.verbose:
            pass  # Removed verbose subscription messages

    def subscribe_all(self):
        """
        Subscribe to market data updates for all available symbols.
        
        Uses the symbols list from the configuration to subscribe to all
        available trading instruments.
        """
        for symbol in self.symbols:
            self.subscribe(symbol)

    def unsubscribe(self, symbol):
        """
        Unsubscribe from market data updates for a specific symbol.
        
        Args:
            symbol (str): The trading symbol to unsubscribe from
        """
        self.sub_sock.setsockopt_string(zmq.UNSUBSCRIBE, symbol)
        if self.verbose:
            print(f"\n**\n[KERNEL] Unsubscribed from {symbol}\n**")

    def unsubscribe_all(self):
        """
        Unsubscribe from all currently subscribed symbols.
        
        This will stop all market data updates until new subscriptions are made.
        """
        for symbol in list(self.market_data.keys()):
            self.unsubscribe(symbol)

    def _process_stream_message(self, msg):
        """
        Process incoming market data messages from the stream.
        
        Expected message format: "SYMBOL:|:BID;ASK"
        """
        # Call raw data callback if set
        if hasattr(self, 'raw_data_callback') and self.raw_data_callback is not None:
            try:
                self.raw_data_callback(msg)
            except Exception as e:
                self.logger.error(f"Error in raw_data_callback: {str(e)}")
                
        try:
            if not msg:
                return

            # Split the message into parts
            try:
                symbol, data = msg.split(self.main_delimiter, 1)
                bid, ask = data.split(';')
                bid = float(bid)
                ask = float(ask)
            except (ValueError, AttributeError) as e:
                self.logger.error(f"Invalid message format: {msg} - {str(e)}")
                return

            # Get current timestamp
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

            # Store in memory
            if symbol not in self.market_data:
                self.market_data[symbol] = []
            self.market_data[symbol].append((timestamp, bid, ask))

            # Save to CSV if enabled
            if self.save_to_csv:
                self._save_to_csv(symbol, timestamp, bid, ask)

            # Print to console if verbose
            if self.verbose:
                print(f"[{symbol}] {timestamp} ({bid}/{ask}) BID/ASK")

        except Exception as e:
            self.logger.error(f"Error in _process_stream_message: {str(e)}")
            if self.verbose:
                import traceback
                traceback.print_exc()

    def shutdown(self):
        """
        Gracefully shut down the market data handler.
        
        Closes all open CSV files and cleans up resources before calling
        the parent class's shutdown method.
        """
        if hasattr(self, 'csv_files'):
            for symbol, csv_file in self.csv_files.items():
                try:
                    if self.verbose:
                        print(f"[SHUTDOWN] Closing CSV file for {symbol}")
                    csv_file.close()
                except Exception as e:
                    self.logger.error(f"Error closing CSV file for {symbol}: {e}")
        super().shutdown()

"""
# Inicjalizacja
handler = MT4MarketDataHandler(
    csv_output_dir='market_data',
    save_to_csv=True,
    client_id='tomasz_biel',
    verbose=True
)

# Subskrypcja wybranych symboli
handler.subscribe('EURUSD+')
handler.subscribe('US.100+')

# Lub wszystkich symboli
# handler.subscribe_all()

# Po zakończeniu
handler.shutdown()
"""