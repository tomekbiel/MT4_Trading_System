import os
import sys
import time
import json
from datetime import datetime
from mt4_connector.command_sender import MT4CommandSender  # 🔥 zmiana tutaj

# Globalne zmienne (do użycia w funkcji main)
SYMBOL = None
TIMEFRAME = None
START_DATE = None
END_DATE = None

class HistoricalConnector(MT4CommandSender):  # 🔥 zmiana tutaj
    def _process_message(self, msg):
        print("\u2705 Odpowiedź z MT4:", msg)
        try:
            msg_fixed = msg.replace("'", '"')
            msg_fixed = msg_fixed.replace("False", "false").replace("True", "true")
            parsed = json.loads(msg_fixed)

            if "_data" in parsed:
                self.save_data(parsed["_data"])
            else:
                print("❌ Brak danych _data w odpowiedzi.")
        except Exception as e:
            print(f"❌ Błąd przy przetwarzaniu wiadomości: {e}")
            self.save_raw_message(msg)

    def _process_stream_message(self, msg):
        pass

    def save_raw_message(self, msg):
        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        directory = os.path.join(base_dir, "data", "historical", SYMBOL)
        os.makedirs(directory, exist_ok=True)
        filepath = os.path.join(directory, f"{SYMBOL}_{TIMEFRAME}_{now}_raw.txt")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(msg)
        print(f"💾 Zapisano surową odpowiedź do: {filepath}")

    def save_data(self, data):
        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        directory = os.path.join(base_dir, "data", "historical", SYMBOL)
        os.makedirs(directory, exist_ok=True)
        filename = f"{SYMBOL}_{TIMEFRAME}_{now}.csv"
        filepath = os.path.join(directory, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write("time,open,high,low,close,tick_volume,spread,real_volume\n")
            for row in data:
                f.write(
                    f"{row['time']},{row['open']},{row['high']},{row['low']},{row['close']}," +
                    f"{row['tick_volume']},{row['spread']},{row['real_volume']}\n"
                )

        print(f"💾 Dane zapisane do: {filepath}")

# 🚀 Funkcja główna - steruje komunikacją
def main():
    global SYMBOL, TIMEFRAME, START_DATE, END_DATE

    # 🔧 Parametry
    SYMBOL = "US.100+"
    TIMEFRAME = "M1"
    START_DATE = "2001.01.01"
    END_DATE = datetime.today().strftime("%Y.%m.%d")  # 🔥 Dynamiczna data końcowa

    print(f"\U0001F4E1 Wysyłam komendę do MT4: HIST;{SYMBOL};{TIMEFRAME};{START_DATE};{END_DATE}")

    connector = HistoricalConnector(client_id="historical_data", verbose=True)
    time.sleep(1)

    success = connector.send_historical_data(SYMBOL, TIMEFRAME, START_DATE, END_DATE)  # 🔥 używamy send_historical_data()

    if not success:
        print("\u274C Nie udało się wysłać wiadomości do MT4.")
    else:
        time.sleep(5)

    connector.shutdown()

# ➡️ Umożliwia zarówno uruchomienie przez import, jak i samodzielnie
if __name__ == "__main__":
    main()
