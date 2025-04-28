# mt4_connector/historical_data_handler.py
from .base_connector import MT4BaseConnector
from datetime import datetime, timedelta
import pandas as pd
import os
import csv
import time
import zmq


class MT4HistoricalDataHandler(MT4BaseConnector):
    def __init__(self, csv_output_dir='../data/historical', save_to_csv=True, **kwargs):
        super().__init__(**kwargs)
        self.csv_output_dir = csv_output_dir
        self.save_to_csv = save_to_csv
        self.history_db = {}
        self.csv_writer = None
        self.csv_file = None
        self.current_filename = None

        if self.save_to_csv:
            self._init_csv_directory()

    def _init_csv_directory(self):
        if not os.path.exists(self.csv_output_dir):
            os.makedirs(self.csv_output_dir)
            if self.verbose:
                print(f"[CSV] Created output directory: {self.csv_output_dir}")

    def request_history(self, symbol='US.100+', timeframe=1440,
                        start=None, end=None, days_back=30):
        valid_timeframes = [1, 5, 15, 30, 60, 240, 1440, 10080, 43200]
        if timeframe not in valid_timeframes:
            raise ValueError(f"Invalid timeframe. Use one of: {valid_timeframes}")

        if end is None:
            end = datetime.now().strftime('%Y.%m.%d %H:%M:00')

        if start is None:
            start_date = datetime.now() - timedelta(days=days_back)
            start = start_date.strftime('%Y.%m.%d %H:%M:00')

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.current_filename = os.path.join(
            self.csv_output_dir,
            f"{symbol.replace('+', '')}_M{timeframe}_{timestamp}.csv"
        )

        if self.save_to_csv:
            self.csv_file = open(self.current_filename, 'w', newline='')
            self.csv_writer = csv.writer(self.csv_file)
            self.csv_writer.writerow(['timestamp', 'bid', 'ask'])

        msg = f"HIST;{symbol};{timeframe};{start};{end}"
        self.send(msg)

    def _process_message(self, msg):
        try:
            data = eval(msg)
            if data.get('_action') == 'HIST':
                symbol = data['_symbol']
                if '_data' in data and isinstance(data['_data'], list):
                    for item in data['_data']:
                        if isinstance(item, dict):
                            timestamp = item.get('time')
                            bid = item.get('close')
                            ask = item.get('close')
                            if timestamp and bid is not None and ask is not None:
                                self.history_db.setdefault(symbol, {})[timestamp] = (bid, ask)
                                if self.save_to_csv and self.csv_writer:
                                    self.csv_writer.writerow([timestamp, bid, ask])
                    if self.verbose:
                        print(f"[HIST] Zapisano dane historyczne dla {symbol} do {self.current_filename}")
        except Exception as e:
            self.logger.error(f"[HIST ERROR] Błąd przetwarzania danych: {e}")

    def shutdown(self):
        if self.save_to_csv and self.csv_file:
            try:
                self.csv_file.close()
                if self.verbose:
                    print(f"[CSV] Zamknięto plik: {self.current_filename}")
            except:
                pass
        super().shutdown()

    def get_history_as_dataframe(self, symbol):
        if symbol in self.history_db:
            return pd.DataFrame.from_dict(self.history_db[symbol], orient='index')
        return None
    def _process_stream_message(self, msg):
        """Zignoruj dane strumieniowe w handlerze danych historycznych."""
        pass
