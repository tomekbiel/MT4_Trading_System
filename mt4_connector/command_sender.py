# mt4_connector/command_sender.py

import time
from mt4_connector.base_connector import MT4BaseConnector

class MT4CommandSender(MT4BaseConnector):
    """
    Klasa pomocnicza do wysyłania komend do MT4
    """

    def _process_message(self, msg):
        print("✅ Odpowiedź z MT4:", msg)

    def _process_stream_message(self, msg):
        pass  # Ignorujemy dane strumieniowe SUB

    def send_heartbeat(self):
        """Wysyła HEARTBEAT"""
        print("📡 Wysyłam: HEARTBEAT")
        return self.send("HEARTBEAT")

    def send_open_trade(self):
        """Wysyła TRADE;OPEN"""
        print("📡 Wysyłam: TRADE;OPEN")
        return self.send("TRADE;OPEN")

    def send_modify_trade(self):
        """Wysyła TRADE;MODIFY"""
        print("📡 Wysyłam: TRADE;MODIFY")
        return self.send("TRADE;MODIFY")

    def send_close_trade(self):
        """Wysyła TRADE;CLOSE"""
        print("📡 Wysyłam: TRADE;CLOSE")
        return self.send("TRADE;CLOSE")

    def send_close_partial_trade(self):
        """Wysyła TRADE;CLOSE_PARTIAL"""
        print("📡 Wysyłam: TRADE;CLOSE_PARTIAL")
        return self.send("TRADE;CLOSE_PARTIAL")

    def send_close_magic_trade(self):
        """Wysyła TRADE;CLOSE_MAGIC"""
        print("📡 Wysyłam: TRADE;CLOSE_MAGIC")
        return self.send("TRADE;CLOSE_MAGIC")

    def send_close_all_trades(self):
        """Wysyła TRADE;CLOSE_ALL"""
        print("📡 Wysyłam: TRADE;CLOSE_ALL")
        return self.send("TRADE;CLOSE_ALL")

    def send_get_open_trades(self):
        """Wysyła TRADE;GET_OPEN_TRADES"""
        print("📡 Wysyłam: TRADE;GET_OPEN_TRADES")
        return self.send("TRADE;GET_OPEN_TRADES")

    def send_get_account_info(self):
        """Wysyła TRADE;GET_ACCOUNT_INFO"""
        print("📡 Wysyłam: TRADE;GET_ACCOUNT_INFO")
        return self.send("TRADE;GET_ACCOUNT_INFO")

    def send_track_prices(self):
        """Wysyła TRACK_PRICES"""
        print("📡 Wysyłam: TRACK_PRICES")
        return self.send("TRACK_PRICES")

    def send_track_rates(self):
        """Wysyła TRACK_RATES"""
        print("📡 Wysyłam: TRACK_RATES")
        return self.send("TRACK_RATES")

    def send_historical_data(self, symbol, timeframe, date_from, date_to):
        """
        Wysyła HIST;SYMBOL;TF;START;END
        """
        command = f"HIST;{symbol};{timeframe};{date_from};{date_to}"
        print(f"📡 Wysyłam: {command}")
        return self.send(command)
