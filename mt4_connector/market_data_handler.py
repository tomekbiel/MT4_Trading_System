# -*- coding: utf-8 -*-
"""
Handler danych rynkowych w czasie rzeczywistym - wersja dostosowana do poprawionej klasy bazowej
"""

from .base_connector import MT4BaseConnector
import csv
import os
from pandas import Timestamp
import time
import zmq


class MT4MarketDataHandler(MT4BaseConnector):
    def __init__(self, csv_output_dir='market_data', save_to_csv=True, **kwargs):
        """
        Handler do obsługi danych rynkowych w czasie rzeczywistym.

        :param csv_output_dir: Katalog do zapisywania danych CSV
        :param save_to_csv: Czy zapisywać dane do plików CSV
        :param kwargs: Argumenty przekazywane do klasy bazowej
        """
        super().__init__(**kwargs)
        self.csv_output_dir = csv_output_dir
        self.save_to_csv = save_to_csv
        self.market_data = {}
        self.csv_writers = {}
        self.csv_files = {}
        self.msg_delimiter = ';'  # Zachowane oryginalne nazewnictwo
        self.main_delimiter = ':|:'  # Zachowane oryginalne nazewnictwo

        if self.save_to_csv:
            self._init_csv_directory()

    def _init_csv_directory(self):
        """Inicjalizacja katalogu na dane CSV"""
        if not os.path.exists(self.csv_output_dir):
            os.makedirs(self.csv_output_dir)
            if self.verbose:
                print(f"[CSV] Created output directory: {self.csv_output_dir}")

    def _init_csv_writer(self, symbol):
        """Inicjalizacja writera CSV dla symbolu"""
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
        """Zapis danych do pliku CSV"""
        try:
            if symbol not in self.csv_writers:
                self._init_csv_writer(symbol)

            writer = self.csv_writers[symbol]
            writer.writerow([timestamp, bid, ask])
            self.csv_files[symbol].flush()
        except Exception as e:
            print(f"[CSV ERROR] Failed to save data for {symbol}: {str(e)}")

    def subscribe(self, symbol='US.100+'):
        """Subskrypcja danych dla konkretnego symbolu (zachowane oryginalne nazewnictwo)"""
        self.sub_sock.setsockopt_string(zmq.SUBSCRIBE, symbol)
        if self.verbose:
            print(f"[KERNEL] Subscribed to {symbol} updates")

    def subscribe_all(self):
        """Subskrypcja wszystkich symboli z centralnej listy (zachowane oryginalne nazewnictwo)"""
        for symbol in self.symbols:
            self.subscribe(symbol)

    def unsubscribe(self, symbol):
        """Rezygnacja z subskrypcji symbolu (zachowane oryginalne nazewnictwo)"""
        self.sub_sock.setsockopt_string(zmq.UNSUBSCRIBE, symbol)
        if self.verbose:
            print(f"\n**\n[KERNEL] Unsubscribed from {symbol}\n**")

    def unsubscribe_all(self):
        """Rezygnacja ze wszystkich subskrypcji (zachowane oryginalne nazewnictwo)"""
        for symbol in list(self.market_data.keys()):
            self.unsubscribe(symbol)

    def _poll_data(self):
        """Główna pętla odbierająca dane rynkowe (zachowane oryginalne funkcjonalności)"""
        while self.active:
            time.sleep(self.sleep_delay)
            sockets = dict(self.poller.poll(self.poll_timeout))

            if self.sub_sock in sockets and sockets[self.sub_sock] == zmq.POLLIN:
                msg = self.sub_sock.recv_string(zmq.DONTWAIT)
                if msg:
                    timestamp = str(Timestamp.now('UTC'))[:-6]
                    symbol, data = msg.split(self.main_delimiter)

                    if self.verbose:
                        if self.msg_delimiter in data:
                            parts = data.split(self.msg_delimiter)
                            if len(parts) == 2:
                                print(f"\n[{symbol}] {timestamp} ({parts[0]}/{parts[1]}) BID/ASK")

                    if symbol not in self.market_data:
                        self.market_data[symbol] = {}

                    parts = data.split(self.msg_delimiter)
                    if len(parts) == 2:
                        bid, ask = float(parts[0]), float(parts[1])
                        self.market_data[symbol][timestamp] = (bid, ask)

                        if self.save_to_csv:
                            self._save_to_csv(symbol, timestamp, bid, ask)

    def shutdown(self):
        """Rozszerzona metoda shutdown do zamknięcia plików CSV"""
        # Zamknięcie plików CSV
        if hasattr(self, 'csv_files'):
            for csv_file in self.csv_files.values():
                try:
                    csv_file.close()
                except:
                    pass

        # Wywołanie oryginalnej metody shutdown z klasy bazowej
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
# handler.shutdown()
"""