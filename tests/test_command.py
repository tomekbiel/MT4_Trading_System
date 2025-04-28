import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from mt4_connector.base_connector import MT4BaseConnector

class TestConnector(MT4BaseConnector):
    def _process_message(self, msg):
        print("✅ [MT4] Odpowiedź z PULL socket:", msg)

    def _process_stream_message(self, msg):
        pass  # Ignorujemy dane z SUB

def main():
    command = "TRADE;GET_OPEN_TRADES" # 🔁 Tu wpisz dowolną komendę np. TRADE;GET_ACCOUNT_INFO
    print(f"📡 Test: Wysyłam komendę → {command}")

    connector = TestConnector(client_id="test_command", verbose=True)

    time.sleep(1)  # ⏳ chwilka na stabilizację socketów

    success = connector.send(command)

    if not success:
        print(f"❌ Nie udało się wysłać wiadomości: {command}")
        return

    time.sleep(2)  # ⏳ dajemy MT4 chwilę na odpowiedź

    connector.shutdown()

if __name__ == "__main__":
    main()


# Dostępne komendy dla DWX_ZeroMQ_Server:
# - HEARTBEAT
# - TRADE;OPEN
# - TRADE;MODIFY
# - TRADE;CLOSE
# - TRADE;CLOSE_PARTIAL
# - TRADE;CLOSE_MAGIC
# - TRADE;CLOSE_ALL
# - TRADE;GET_OPEN_TRADES
# - TRADE;GET_ACCOUNT_INFO
# - HIST;SYMBOL;TF;DATE_FROM;DATE_TO /// na przykład: "HIST;US.100+;M15;2024.01.01;2025.04.25"HIST;US.100+;M5;2022.01.01;2024.10.28
# - TRACK_PRICES
# - TRACK_RATES