# -*- coding: utf-8 -*-
"""
Handler danych rynkowych w czasie rzeczywistym - wersja dostosowana do poprawionej klasy bazowej
"""

from .base_connector import MT4BaseConnector
import csv
import os
from pandas import Timestamp
import zmq
import time


class MT4MarketDataHandler(MT4BaseConnector):
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
        if not os.path.exists(self.csv_output_dir):
            os.makedirs(self.csv_output_dir)
            if self.verbose:
                print(f"[CSV] Created output directory: {self.csv_output_dir}")

    def _init_csv_writer(self, symbol):
        if symbol not in self.csv_writers:
            filename = os.path.join(self.csv_output_dir, f"{symbol.replace('+', '')}.csv")
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
        try:
            if symbol not in self.csv_writers:
                self._init_csv_writer(symbol)

            writer = self.csv_writers[symbol]
            writer.writerow([timestamp, bid, ask])
            self.csv_files[symbol].flush()
        except Exception as e:
            print(f"[CSV ERROR] Failed to save data for {symbol}: {str(e)}")

    def subscribe(self, symbol='US.100+'):
        self.sub_sock.setsockopt_string(zmq.SUBSCRIBE, symbol)
        if self.verbose:
            print(f"[KERNEL] Subscribed to {symbol} updates")

    def subscribe_all(self):
        for symbol in self.symbols:
            self.subscribe(symbol)

    def unsubscribe(self, symbol):
        self.sub_sock.setsockopt_string(zmq.UNSUBSCRIBE, symbol)
        if self.verbose:
            print(f"\n**\n[KERNEL] Unsubscribed from {symbol}\n**")

    def unsubscribe_all(self):
        for symbol in list(self.market_data.keys()):
            self.unsubscribe(symbol)

    def _process_stream_message(self, msg):
        """
        Nowa wymagana metoda do przetwarzania danych strumieniowych SUB
        """
        try:
            timestamp = str(Timestamp.now('UTC'))[:-6]

            if self.main_delimiter in msg:
                symbol, data = msg.split(self.main_delimiter)
                if self.msg_delimiter in data:
                    parts = data.split(self.msg_delimiter)
                    if len(parts) == 2:
                        bid, ask = float(parts[0]), float(parts[1])

                        # Zapis danych w pamięci
                        if symbol not in self.market_data:
                            self.market_data[symbol] = {}
                        self.market_data[symbol][timestamp] = (bid, ask)

                        if self.verbose:
                            print(f"\n[{symbol}] {timestamp} ({bid}/{ask}) BID/ASK")

                        if self.save_to_csv:
                            self._save_to_csv(symbol, timestamp, bid, ask)
        except Exception as e:
            self.logger.error(f"[STREAM ERROR] Błąd przetwarzania danych strumieniowych: {e}")

    def shutdown(self):
        if hasattr(self, 'csv_files'):
            for csv_file in self.csv_files.values():
                try:
                    csv_file.close()
                except:
                    pass
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