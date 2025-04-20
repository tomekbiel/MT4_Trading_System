# -*- coding: utf-8 -*-
"""
Handler danych historycznych - wersja zintegrowana z poprawioną klasą bazową
"""

from .base_connector import MT4BaseConnector
from datetime import datetime, timedelta
import time
import zmq


class MT4HistoricalDataHandler(MT4BaseConnector):
    def __init__(self, **kwargs):
        """
        Handler do obsługi danych historycznych.
        """
        super().__init__(**kwargs)
        self.history_db = {}
        self.last_request = {}  # Zachowane oryginalne śledzenie ostatniego żądania

    def request_history(self, symbol='US.100+', timeframe=1440,
                        start=None, end=None, days_back=30):
        """
        Żądanie danych historycznych (zachowane oryginalne nazewnictwo).

        :param symbol: Symbol instrumentu
        :param timeframe: Ramka czasowa w minutach
        :param start: Data początkowa (YYYY.MM.DD HH:MM:SS)
        :param end: Data końcowa
        :param days_back: Liczba dni wstecz jeśli start nie podany
        """
        # Walidacja timeframe (zachowane z oryginału)
        valid_timeframes = [1, 5, 15, 30, 60, 240, 1440, 10080, 43200]
        if timeframe not in valid_timeframes:
            raise ValueError(f"Invalid timeframe. Use one of: {valid_timeframes}")

        if end is None:
            end = datetime.now().strftime('%Y.%m.%d %H:%M:00')

        if start is None:
            start_date = datetime.now() - timedelta(days=days_back)
            start = start_date.strftime('%Y.%m.%d %H:%M:00')

        # Formatowanie wiadomości (zachowane oryginalne formatowanie)
        msg = f"HIST;{symbol};{timeframe};{start};{end}"
        self.send(msg)

        # Przechowywanie parametrów żądania (zachowane z oryginału)
        self.last_request = {
            'symbol': symbol,
            'timeframe': timeframe,
            'start': start,
            'end': end
        }

    def _poll_data(self):
        """Główna pętla odbierająca dane historyczne (zachowane oryginalne funkcjonalności)"""
        while self.active:
            time.sleep(self.sleep_delay)
            sockets = dict(self.poller.poll(self.poll_timeout))

            if self.pull_sock in sockets and sockets[self.pull_sock] == zmq.POLLIN:
                msg = self.receive(self.pull_sock)
                if msg:
                    try:
                        data = eval(msg)
                        if '_action' in data:
                            if data['_action'] == 'HIST':
                                symbol = data['_symbol']
                                if '_data' in data:
                                    # Przetwarzanie danych historycznych (zachowane oryginalne przetwarzanie)
                                    if isinstance(data['_data'], list):
                                        self.history_db[symbol] = {}
                                        for item in data['_data']:
                                            if isinstance(item, dict):
                                                timestamp = item.get('time')
                                                bid = item.get('close')  # Użycie close jako bid
                                                ask = item.get('close')  # Użycie close jako ask
                                                if timestamp and bid is not None and ask is not None:
                                                    self.history_db[symbol][timestamp] = (bid, ask)
                                    else:
                                        if self.verbose:
                                            print('No data found for symbol:', symbol)

                            elif data['_action'] == 'GET_ACCOUNT_INFORMATION':
                                # Zachowane oryginalne przetwarzanie informacji o koncie
                                acc_num = data['account_number']
                                if '_data' in data:
                                    self.account_info.setdefault(acc_num, []).extend(data['_data'])

                        if self.verbose:
                            print(data)

                    except Exception as e:
                        print(f"Exception: {type(e).__name__}, Args: {e.args}")

    def get_history_as_dataframe(self, symbol):
        """
        Dodatkowa metoda do konwersji danych historycznych na DataFrame
        (przykład rozszerzenia funkcjonalności)
        """
        import pandas as pd
        if symbol in self.history_db:
            return pd.DataFrame.from_dict(self.history_db[symbol], orient='index')
        return None

"""
# Inicjalizacja
hist_handler = MT4HistoricalDataHandler(
    client_id='tomasz_biel',
    verbose=True
)

# Żądanie danych historycznych
hist_handler.request_history(
    symbol='EURUSD+',
    timeframe=1440,
    start='2023.01.01 00:00:00',
    end='2023.12.31 23:59:59'
)

# Można też użyć domyślnych parametrów
# hist_handler.request_history(days_back=365)

# Po zakończeniu
# hist_handler.shutdown()
"""