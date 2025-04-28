import os
import sys
import time
import json
from datetime import datetime
from mt4_connector.command_sender import MT4CommandSender

# 🔧 Dynamiczne ładowanie symbolu i timeframe

def load_symbol_and_timeframe():
    symbol = os.environ.get("SYMBOL", None)
    timeframe = os.environ.get("TIMEFRAME", None)

    if symbol and timeframe:
        return symbol, timeframe

    # fallback - ładowanie domyślnego symbolu i timeframe z plików json
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

    # ładuj symbole
    symbols_path = os.path.join(base_dir, 'config', 'symbols.json')
    with open(symbols_path, "r", encoding="utf-8") as f:
        symbols = json.load(f).get("symbols", [])

    # ładuj timeframe'y
    timeframes_path = os.path.join(base_dir, 'config', 'timeframes.json')
    with open(timeframes_path, "r", encoding="utf-8") as f:
        timeframes = json.load(f).get("timeframes", [])

    # fallback: pierwszy symbol i pierwszy timeframe
    return symbols[0], timeframes[0]

# 📂 Funkcja do tworzenia folderu docelowego

def ensure_directory(symbol, timeframe):
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    path = os.path.join(base_dir, 'data', 'historical', symbol, timeframe)
    os.makedirs(path, exist_ok=True)
    return path

# 📅 Zapis danych do pliku CSV

def save_data(symbol, timeframe, data, timestamp=None):
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    directory = ensure_directory(symbol, timeframe)
    filename = f"{symbol}_{timeframe}_{timestamp}.csv"
    filepath = os.path.join(directory, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("time,open,high,low,close,tick_volume,spread,real_volume\n")
        for row in data:
            f.write(
                f"{row['time']},{row['open']},{row['high']},{row['low']}," +
                f"{row['close']},{row['tick_volume']},{row['spread']},{row['real_volume']}\n"
            )

    print(f"✅ Zapisano dane do pliku: {filepath}")

# ⚖️ Główna funkcja pobierania jednego symbolu/timeframe

def fetch_single():
    symbol, timeframe = load_symbol_and_timeframe()

    print(f"🚀 Pobieram: {symbol} {timeframe}")

    connector = MT4CommandSender(client_id="historical_fetch_single", verbose=True)
    time.sleep(1)

    start_date = "2001.01.01"
    end_date = datetime.today().strftime("%Y.%m.%d")

    success = connector.send_historical_data(symbol, timeframe, start_date, end_date)

    if not success:
        print(f"❌ Nie udało się wysłać komendy dla {symbol} {timeframe}")
        connector.shutdown()
        sys.exit(0)

    response = connector.receive(timeout=10)

    if response:
        try:
            response_fixed = response.replace("'", '"')
            response_fixed = response_fixed.replace("False", "false").replace("True", "true")

            if "}{" in response_fixed:
                parts = response_fixed.split("}{")
                response_fixed = parts[0] + "}"

            parsed = json.loads(response_fixed)

            if "_response" in parsed and parsed["_response"] == "NOT_AVAILABLE":
                print(f"⚠️ Symbol {symbol} {timeframe} niedostępny (NOT_AVAILABLE)")
            elif "_data" in parsed:
                now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                save_data(symbol, timeframe, parsed["_data"], now)
            else:
                print(f"⚠️ Brak danych _data dla {symbol} {timeframe}")

        except Exception as e:
            print(f"❌ Błąd parsowania danych: {e}")

    else:
        print(f"⚠️ Brak odpowiedzi na {symbol} {timeframe}")

    connector.shutdown()
    time.sleep(2)
    sys.exit(0)

if __name__ == "__main__":
    fetch_single()