import os
import sys
import time
import json
from datetime import datetime
from collections import Counter
from mt4_connector.command_sender import MT4CommandSender

# 📥 Ładowanie symbolu i interwału z plików JSON
def load_symbol_and_timeframe():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    symbols_path = os.path.join(base_dir, 'config', 'symbols.json')
    timeframes_path = os.path.join(base_dir, 'config', 'timeframes.json')

    with open(symbols_path, encoding="utf-8") as f:
        symbols = json.load(f).get("symbols", [])

    with open(timeframes_path, encoding="utf-8") as f:
        valid_timeframes = json.load(f).get("timeframes", [])

    print("\n📌 Dostępne symbole:")
    for i, s in enumerate(symbols):
        print(f"{i + 1}. {s}")

    default_symbol = "US.100+"
    default_timeframe = "M1"

    symbol_input = input(f"\nWybierz symbol (ENTER = {default_symbol}): ").strip()
    symbol = symbol_input if symbol_input else default_symbol

    if symbol not in symbols:
        print(f"⚠️ Symbol '{symbol}' nie znajduje się na liście – używam domyślnego: {default_symbol}")
        symbol = default_symbol

    timeframe_input = input(f"Wybierz interwał czasowy (ENTER = {default_timeframe}): ").strip()
    timeframe = timeframe_input if timeframe_input else default_timeframe

    if timeframe not in valid_timeframes:
        print(f"⚠️ Interwał '{timeframe}' nieobsługiwany – używam domyślnego: {default_timeframe}")
        timeframe = default_timeframe

    return symbol, timeframe, valid_timeframes

# 📂 Tworzy folder docelowy
def ensure_directory(symbol, timeframe):
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    path = os.path.join(base_dir, 'data', 'historical', symbol, timeframe)
    os.makedirs(path, exist_ok=True)
    return path

# 🕒 Pobiera ostatni znany znacznik czasu
def get_last_timestamp(filepath):
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

# 🧠 Wykrywa realny interwał na podstawie różnic między świecami
def detect_actual_timeframe(data):
    if len(data) < 2:
        return None

    try:
        # Próba parsowania z godzinami i minutami
        times = [datetime.strptime(row['time'], "%Y.%m.%d %H:%M") for row in data]
        diffs = [(times[i + 1] - times[i]).seconds for i in range(len(times) - 1)]

        # Jeśli wszystkie różce są 0, sprawdzamy różnicę w dniach
        if all(diff == 0 for diff in diffs):
            day_diffs = [(times[i + 1] - times[i]).days for i in range(len(times) - 1)]
            most_common_day_diff = Counter(day_diffs).most_common(1)[0][0] if day_diffs else None

            if most_common_day_diff == 1:
                return 'D1'
            elif most_common_day_diff == 7:
                return 'W1'
            elif most_common_day_diff == 30:
                return 'MN1'
            else:
                return f"UNKNOWN({most_common_day_diff} days)"

        most_common = Counter(diffs).most_common(1)[0][0] if diffs else None
        tf_map = {
            60: 'M1',
            300: 'M5',
            900: 'M15',
            1800: 'M30',
            3600: 'H1',
            14400: 'H4',
            86400: 'D1',
            604800: 'W1',
            2592000: 'MN1'
        }
        return tf_map.get(most_common, f"UNKNOWN({most_common}s)")

    except ValueError:
        # Jeśli parsowanie z godzinami i minutami się nie uda, próbujemy tylko datę
        try:
            times = [datetime.strptime(row['time'].split()[0], "%Y.%m.%d") for row in data]
            day_diffs = [(times[i + 1] - times[i]).days for i in range(len(times) - 1)]
            most_common_day_diff = Counter(day_diffs).most_common(1)[0][0] if day_diffs else None

            if most_common_day_diff == 1:
                return 'D1'
            elif most_common_day_diff == 7:
                return 'W1'
            elif most_common_day_diff == 30:
                return 'MN1'
            else:
                return f"UNKNOWN({most_common_day_diff} days)"
        except Exception as e:
            print(f"Błąd podczas wykrywania interwału: {e}")
            return None

# 💾 Zapisuje dane (jeśli interwał się zgadza)
def save_data(symbol, timeframe, data):
    directory = ensure_directory(symbol, timeframe)
    filepath = os.path.join(directory, f"{symbol}_{timeframe}.csv")
    last_ts = get_last_timestamp(filepath)
    new_rows = []

    for row in data:
        if last_ts is None or row["time"] > last_ts:
            new_rows.append(
                f"{row['time']},{row['open']},{row['high']},{row['low']}," +
                f"{row['close']},{row['tick_volume']},{row['spread']},{row['real_volume']}\n"
            )

    if not new_rows:
        print("ℹ️ Brak nowych danych do dopisania.")
        return

    mode = "a" if os.path.exists(filepath) else "w"
    with open(filepath, mode, encoding="utf-8") as f:
        if mode == "w":
            f.write("time,open,high,low,close,tick_volume,spread,real_volume\n")
        f.writelines(new_rows)

    print(f"✅ Dopisano {len(new_rows)} świec do: {filepath}")

# 🚀 Główna funkcja
def fetch_single(symbol, timeframe, valid_timeframes):
    print(f"\n🚀 Pobieram dane: {symbol} {timeframe}")
    connector = MT4CommandSender(client_id="historical_fetch_single", verbose=True)
    time.sleep(5)

    start_date = "2001.01.01"
    end_date = datetime.today().strftime("%Y.%m.%d")
    success = connector.send_historical_data(symbol, timeframe, start_date, end_date)

    if not success:
        print(f"❌ Nie udało się wysłać komendy dla {symbol} {timeframe}")
        connector.shutdown()
        sys.exit(1)

    response = connector.receive(timeout=10)

    if response:
        try:
            response_fixed = response.replace("'", '"').replace("False", "false").replace("True", "true")
            if "}{" in response_fixed:
                response_fixed = response_fixed.split("}{")[0] + "}"

            parsed = json.loads(response_fixed)

            if "_response" in parsed and parsed["_response"] == "NOT_AVAILABLE":
                print(f"⚠️ Symbol {symbol} {timeframe} niedostępny")
            elif "_data" in parsed:
                actual_tf = detect_actual_timeframe(parsed["_data"])
                if actual_tf != timeframe:
                    print(f"⚠️ Rzeczywisty interwał danych to {actual_tf}, a nie {timeframe}. Dane NIE zostaną zapisane.")
                elif actual_tf not in valid_timeframes:
                    print(f"⚠️ Rzeczywisty interwał {actual_tf} nie znajduje się na liście dozwolonych interwałów.")
                else:
                    save_data(symbol, timeframe, parsed["_data"])
            else:
                print("⚠️ Brak danych _data w odpowiedzi.")
        except Exception as e:
            print(f"❌ Błąd parsowania odpowiedzi: {e}")
    else:
        print(f"⚠️ Brak odpowiedzi z MT4 dla {symbol} {timeframe}")

    connector.shutdown()
    time.sleep(5)

# ▶️ Start
if __name__ == "__main__":
    symbol, timeframe, valid_timeframes = load_symbol_and_timeframe()
    fetch_single(symbol, timeframe, valid_timeframes)
