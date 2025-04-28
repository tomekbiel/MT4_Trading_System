# scripts/historical/fetch_historical_for_all_v5.py

import os
import time
import json
from datetime import datetime
from mt4_connector.command_sender import MT4CommandSender

# 🔧 Parametry startowe
START_DATE = "2022.01.01"  # 📕 Od kiedy pobieramy dane
END_DATE = "2024.10.28"     # 📖 Do kiedy pobieramy dane
END_TIME = "10:35"          # Opcjonalnie: konkretna godzina
TIMEFRAMES = ["M1", "M5", "H1", "D1"]

# 🛠️ Ladowanie symboli

def load_symbols():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    config_path = os.path.join(base_dir, 'config', 'symbols.json')
    with open(config_path, "r") as f:
        data = json.load(f)
    return data.get("symbols", [])

# 🛠️ Usuwanie znaków specjalnych

def sanitize_symbol_name(symbol):
    return symbol.replace("+", "").replace(".", "_")

# 🛠️ Tworzenie folderów

def ensure_directory(symbol, timeframe):
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    safe_symbol = sanitize_symbol_name(symbol)
    path = os.path.join(base_dir, 'data', 'historical', safe_symbol, timeframe)
    os.makedirs(path, exist_ok=True)
    return path

# 📥 Zapis surowych wiadomości

def save_raw_message(symbol, timeframe, msg, timestamp=None):
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    directory = ensure_directory(symbol, timeframe)
    filepath = os.path.join(directory, f"{sanitize_symbol_name(symbol)}_{timeframe}_{timestamp}_raw.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(msg)
    print(f"📃 RAW zapisany: {filepath}")

# 📥 Zapis poprawnych danych CSV

def save_data(symbol, timeframe, data, timestamp=None):
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    directory = ensure_directory(symbol, timeframe)
    filename = f"{sanitize_symbol_name(symbol)}_{timeframe}_{timestamp}.csv"
    filepath = os.path.join(directory, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("time,open,high,low,close,tick_volume,spread,real_volume\n")
        for row in data:
            f.write(
                f"{row['time']},{row['open']},{row['high']},{row['low']}," +
                f"{row['close']},{row['tick_volume']},{row['spread']},{row['real_volume']}\n"
            )
    print(f"✅ CSV zapisany: {filepath}")

# 🛂 Pobieranie danych dla symbolu i timeframe z retry

def fetch_data_for_symbol(connector, symbol, timeframe, retries=2):
    command = f"HIST;{symbol};{timeframe};{START_DATE};{END_DATE}"  # bez END_TIME bo MT4 tego nie obsluguje
    print(f"🛱 Wysyłam: {command}")

    success = connector.send(command)
    if not success:
        print(f"❌ Nie wyslano komendy: {symbol} {timeframe}")
        return

    for attempt in range(retries + 1):
        time.sleep(3)  # krótka pauza
        response = connector.receive(timeout=30)  # 🕒 Timeout 30 sekund
        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        if response:
            save_raw_message(symbol, timeframe, response, now)
            try:
                msg_fixed = response.replace("'", '"').replace("False", "false").replace("True", "true")
                if "}{" in msg_fixed:
                    parts = msg_fixed.split("}{")
                    msg_fixed = parts[0] + "}"

                parsed = json.loads(msg_fixed)

                if "_data" in parsed:
                    save_data(symbol, timeframe, parsed["_data"], now)
                    return  # sukces, wychodzimy
                else:
                    print(f"⚠️ Brak _data w odpowiedzi: {symbol} {timeframe}")

            except Exception as e:
                print(f"⚠️ Błąd parsowania: {e}")
                return  # nie powtarzamy dalej jak JSON byl zly

        else:
            print(f"⚠️ Brak odpowiedzi (próba {attempt + 1}/{retries}) dla {symbol} {timeframe}")
            if attempt < retries:
                print(f"🔄 Retry: {symbol} {timeframe}")
                time.sleep(5)  # dluzej czekaj przy retry

# 🚀 Funkcja glowna

def main():
    symbols = load_symbols()
    print(f"🔎 Ladowanie symboli: {len(symbols)}")

    connector = MT4CommandSender(client_id="historical_fetch_all_v5", verbose=True)
    time.sleep(1)

    for symbol in symbols:
        for timeframe in TIMEFRAMES:
            fetch_data_for_symbol(connector, symbol, timeframe)
            time.sleep(8)  # spokojny sleep po kazdym symbolu/timeframe

    connector.shutdown()
    print("🐟 Pobieranie zakonczone.")

# 🛠️ Uruchamianie
if __name__ == "__main__":
    main()
