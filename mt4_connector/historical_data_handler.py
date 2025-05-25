# mt4_connector/historical_data_handler.py
from .base_connector import MT4BaseConnector
from datetime import datetime, timedelta
import pandas as pd
import os
import csv
import time
import zmq
import json
from pathlib import Path


class MT4HistoricalDataHandler(MT4BaseConnector):
    def __init__(self, csv_output_dir='../data/historical', save_to_csv=True, **kwargs):
        super().__init__(**kwargs)
        self.csv_output_dir = csv_output_dir
        self.save_to_csv = save_to_csv
        self.history_db = {}
        self.csv_writer = None
        self.csv_file = None
        self.current_filename = None
        self.timeframes = self._load_timeframes()

        if self.save_to_csv:
            self._init_csv_directory()

    def _load_timeframes(self):
        """Wczytuje dostępne timeframy z pliku JSON"""
        try:
            config_path = Path(__file__).parent.parent / 'config' / 'timeframes.json'
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return config.get('timeframes', [])
        except Exception as e:
            self.logger.error(f"❌ Błąd ładowania pliku z timeframe'ami: {e}")
            return ["M1", "M5", "M15", "H1", "H4", "D1"]  # Domyślne wartości

    def _init_csv_directory(self):
        if not os.path.exists(self.csv_output_dir):
            os.makedirs(self.csv_output_dir)
            if self.verbose:
                print(f"[CSV] Created output directory: {self.csv_output_dir}")

    def send_historical_data(self, symbol, timeframe, start_date, end_date):
        """Wysyła żądanie danych historycznych do MT4"""
        try:
            msg = f"HIST;{symbol};{timeframe};{start_date};{end_date}"
            self.send(msg)
            return True
        except Exception as e:
            self.logger.error(f"❌ Błąd wysyłania żądania danych historycznych: {e}")
            return False

    def request_history(self, symbol='US.100+', timeframe=1440,
                      start=None, end=None, days_back=30):
        """Oryginalna metoda - pozostawiona dla kompatybilności"""
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

        return self.send_historical_data(symbol, timeframe, start, end)

    def fetch_and_save(self, symbol, timeframe=None):
        """Pobiera i zapisuje dane historyczne dla symbolu i timeframe'u"""
        if timeframe is None:
            # Pobierz dane dla wszystkich dostępnych timeframe'ów
            for tf in self.timeframes:
                self._fetch_single_timeframe(symbol, tf)
        else:
            if timeframe not in self.timeframes:
                self.logger.warning(f"⚠️ Nieobsługiwany timeframe: {timeframe}")
                return
            self._fetch_single_timeframe(symbol, timeframe)

    def _fetch_single_timeframe(self, symbol, timeframe):
        """Pomocnicza metoda do pobierania danych dla pojedynczego timeframe'u"""
        self.logger.info(f"Rozpoczynam pobieranie danych dla {symbol} {timeframe}")

        start_date = "2001.01.01"
        end_date = datetime.today().strftime("%Y.%m.%d")

        success = self.send_historical_data(symbol, timeframe, start_date, end_date)
        if not success:
            self.logger.error(f"❌ Nie udało się wysłać komendy dla {symbol} {timeframe}")
            return

        self.logger.info("⌛ Oczekiwanie na odpowiedź z MT4...")
        response = self.receive(timeout=10)

        if not response:
            self.logger.warning(f"⚠️ Brak odpowiedzi z MT4 dla {symbol} {timeframe}")
            return

        try:
            response_fixed = response.replace("'", '"').replace("False", "false").replace("True", "true")
            if "}{" in response_fixed:
                response_fixed = response_fixed.split("}{")[0] + "}"

            parsed = json.loads(response_fixed)

            if "_response" in parsed and parsed["_response"] == "NOT_AVAILABLE":
                self.logger.warning(f"⚠️ Symbol {symbol} {timeframe} niedostępny")
                return

            if "_data" not in parsed:
                self.logger.warning("⚠️ Brak danych _data w odpowiedzi.")
                return

            self.save_data(symbol, timeframe, parsed["_data"])
        except Exception as e:
            self.logger.error(f"❌ Błąd parsowania odpowiedzi: {e}")

    def save_data(self, symbol, timeframe, data):
        """Zapisuje dane do pliku CSV"""
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        directory = os.path.join(base_dir, 'data', 'historical', symbol, timeframe)
        os.makedirs(directory, exist_ok=True)
        filepath = os.path.join(directory, f"{symbol}_{timeframe}.csv")

        last_ts = self.get_last_timestamp(filepath)
        new_rows = []

        for row in data:
            if last_ts is None or row["time"] > last_ts:
                new_rows.append(
                    f"{row['time']},{row['open']},{row['high']},{row['low']}," +
                    f"{row['close']},{row['tick_volume']},{row['spread']},{row['real_volume']}\n"
                )

        if not new_rows:
            self.logger.info("ℹ️ Brak nowych danych do dopisania.")
            return

        write_mode = "a" if os.path.exists(filepath) else "w"
        with open(filepath, write_mode, encoding="utf-8") as f:
            if write_mode == "w":
                f.write("time,open,high,low,close,tick_volume,spread,real_volume\n")
            f.writelines(new_rows)

        self.logger.info(f"✅ Dopisano {len(new_rows)} świec do pliku: {filepath}")

    def get_last_timestamp(self, filepath):
        """Pobiera ostatni timestamp z pliku CSV"""
        if not os.path.exists(filepath):
            return None
        with open(filepath, "r", encoding="utf-8") as f:
            lines = f.readlines()
            if len(lines) < 2:
                return None
            last_line = lines[-1].strip()
            if not last_line:
                return None
            return last_line.split(',')[0]

    def _process_message(self, msg):
        """Przetwarza wiadomości z MT4"""
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
        """Zamyka połączenia i pliki"""
        if self.save_to_csv and self.csv_file:
            try:
                self.csv_file.close()
                if self.verbose:
                    print(f"[CSV] Zamknięto plik: {self.current_filename}")
            except:
                pass
        super().shutdown()

    def get_history_as_dataframe(self, symbol):
        """Zwraca dane historyczne jako DataFrame"""
        if symbol in self.history_db:
            return pd.DataFrame.from_dict(self.history_db[symbol], orient='index')
        return None

    def _process_stream_message(self, msg):
        """Zignoruj dane strumieniowe w handlerze danych historycznych"""
        pass