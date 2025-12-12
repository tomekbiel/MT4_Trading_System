# -*- coding: utf-8 -*-
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
            if 'csv_file' in locals():
                try:
                    csv_file.close()
                except:
                    pass
            # Remove from dictionaries to allow retry
            self.csv_writers.pop(symbol, None)
            self.csv_files.pop(symbol, None)

    def _save_to_csv(self, symbol, timestamp, bid, ask):
        """
        Save market data to CSV file.
        
        Args:
            symbol (str): Trading symbol
            timestamp (str): Timestamp of the data point
            bid (float): Bid price
            ask (float): Ask price
            
        Returns:
            bool: True if data was saved successfully, False otherwise
        """
        if not self.save_to_csv:
            return False
            
        try:
            # Get the full path where the file should be saved
            filename = os.path.join(os.path.abspath(self.csv_output_dir), f"{symbol}.csv")
            
            # Initialize writer if it doesn't exist
            if symbol not in self.csv_writers:
                self._init_csv_writer(symbol)
                if symbol not in self.csv_writers:  # Check if initialization failed
                    return False
            
            # Get the writer and file handle
            writer = self.csv_writers.get(symbol)
            csv_file = self.csv_files.get(symbol)
            
            if writer is None or csv_file is None:
                print(f"[ERROR] Writer or file handle is None for {symbol}")
                return False
                
            try:
                # Write the data
                writer.writerow([timestamp, bid, ask])
                csv_file.flush()
                os.fsync(csv_file.fileno())  # Force write to disk
                return True
                
            except (IOError, OSError) as e:
                print(f"[ERROR] File operation failed for {symbol}: {str(e)}")
                # Try to reinitialize the writer and retry once
                try:
                    if symbol in self.csv_files:
                        self.csv_files[symbol].close()
                    
                    # Reinitialize
                    self.csv_writers.pop(symbol, None)
                    self.csv_files.pop(symbol, None)
                    self._init_csv_writer(symbol)
                    
                    if symbol in self.csv_writers and symbol in self.csv_files:
                        writer = self.csv_writers[symbol]
                        csv_file = self.csv_files[symbol]
                        writer.writerow([timestamp, bid, ask])
                        csv_file.flush()
                        os.fsync(csv_file.fileno())
                        return True
                    
                except Exception as retry_e:
                    print(f"[ERROR] Retry failed for {symbol}: {str(retry_e)}")
                    
                return False
                
        except Exception as e:
            print(f"[ERROR] Unexpected error saving {symbol} data: {str(e)}")
            return False

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
        if not msg or self.main_delimiter not in msg:
            return

        try:
            # Parse the message
            symbol, data = msg.split(self.main_delimiter, 1)
            bid, ask = map(float, data.split(self.msg_delimiter))
            
            # Get current timestamp
            timestamp = Timestamp.now()
            
            # Store in memory
            if symbol not in self.market_data:
                self.market_data[symbol] = []
            
            self.market_data[symbol].append({
                'timestamp': timestamp,
                'bid': bid,
                'ask': ask
            })
            
            # Save to CSV if enabled
            if self.save_to_csv:
                success = self._save_to_csv(symbol, timestamp, bid, ask)
                if not success and self.verbose:
                    print(f"\n[WARNING] Failed to save data for {symbol}")
            
            # Display the update if verbose mode is on
            if self.verbose:
                # Truncate the bid/ask to 5 decimal places for display
                bid_str = f"{bid:.5f}".rstrip('0').rstrip('.')
                ask_str = f"{ask:.5f}".rstrip('0').rstrip('.')
                print(f"[{symbol}] {timestamp} ({bid_str}/{ask_str}) BID/ASK")
                
        except Exception as e:
            if self.verbose:
                print(f"\n[ERROR] Failed to process message '{msg}': {str(e)}")
            return

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