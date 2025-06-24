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
        if symbol not in self.csv_writers:
            filename = os.path.join(self.csv_output_dir, f"{symbol}.csv")
            file_exists = os.path.exists(filename)

            csv_file = open(filename, 'a', newline='')
            writer = csv.writer(csv_file)

            if not file_exists:
                writer.writerow(['timestamp', 'bid', 'ask'])
                csv_file.flush()

            self.csv_files[symbol] = csv_file
            self.csv_writers[symbol] = writer
            if self.verbose:
                print(f"[CSV] Initialized writer for {symbol}")

    def _save_to_csv(self, symbol, timestamp, bid, ask):
        """
        Save market data to CSV file.
        
        Args:
            symbol (str): Trading symbol
            timestamp (str): Timestamp of the data point
            bid (float): Bid price
            ask (float): Ask price
            
        Automatically initializes CSV writer if it doesn't exist for the symbol.
        """
        try:
            if symbol not in self.csv_writers:
                self._init_csv_writer(symbol)

            writer = self.csv_writers[symbol]
            writer.writerow([timestamp, bid, ask])
            self.csv_files[symbol].flush()
        except Exception as e:
            print(f"[CSV ERROR] Failed to save data for {symbol}: {str(e)}")

    def subscribe(self, symbol='US.100+'):
        """
        Subscribe to market data updates for a specific symbol.
        
        Args:
            symbol (str): The trading symbol to subscribe to (e.g., 'EURUSD+', 'US.100+')
        """
        self.sub_sock.setsockopt_string(zmq.SUBSCRIBE, symbol)
        if self.verbose:
            print(f"[KERNEL] Subscribed to {symbol} updates")

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
        
        This method is called automatically for each message received on the
        subscribed socket. It parses the message, updates the in-memory data
        structure, and optionally saves to CSV.
        
        Expected message format: "SYMBOL:|:BID;ASK"
        
        Args:
            msg (str): The raw message from the market data stream
        """
        try:
            timestamp = str(Timestamp.now('UTC'))[:-6]

            if self.main_delimiter in msg:
                symbol, data = msg.split(self.main_delimiter)
                if self.msg_delimiter in data:
                    parts = data.split(self.msg_delimiter)
                    if len(parts) == 2:
                        bid, ask = float(parts[0]), float(parts[1])

                        # Update in-memory data
                        if symbol not in self.market_data:
                            self.market_data[symbol] = {}
                        self.market_data[symbol][timestamp] = (bid, ask)

                        if self.verbose:
                            print(f"\n[{symbol}] {timestamp} ({bid}/{ask}) BID/ASK")

                        if self.save_to_csv:
                            self._save_to_csv(symbol, timestamp, bid, ask)
        except Exception as e:
            self.logger.error(f"[STREAM ERROR] Error processing stream message: {e}")

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