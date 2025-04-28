import os
import subprocess
import time
import json

# 🔧 Opcje filtrowania i parametry
FILTER_SYMBOLS = []  # ["US.", "DE.", "VIX+", "GOLDs+"] - jeśli pusto, bierze wszystkie
FILTER_TIMEFRAMES = ["M1", "M5", "M15", "H1"]  # - jeśli pusto, bierze wszystkie

MAX_RETRIES = 3  # Ile razy ponowić próbę pobrania w razie błędu
SLEEP_BETWEEN_TASKS = 3  # sekundy przerwy między fetchami

# 📂 Ładowanie symboli
def load_symbols():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config'))
    config_path = os.path.join(base_dir, 'symbols.json')
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("symbols", [])

# 📂 Ładowanie timeframe'ów
def load_timeframes():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config'))
    config_path = os.path.join(base_dir, 'timeframes.json')
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("timeframes", [])

# 💻 Funkcja do odpalania fetch_single
def run_fetch(symbol, timeframe):
    env = os.environ.copy()
    env["SYMBOL"] = symbol
    env["TIMEFRAME"] = timeframe

    print(f"🚀 Pobieram: {symbol} {timeframe}")
    result = subprocess.run(["python", "fetch_single.py"], cwd=os.path.join(os.path.dirname(__file__)), env=env)
    return result.returncode == 0

# 🚀 Główna pętla
def main():
    symbols = load_symbols()
    timeframes = load_timeframes()

    # ✨ Filtrowanie symboli
    if FILTER_SYMBOLS:
        symbols = [s for s in symbols if any(f in s for f in FILTER_SYMBOLS)]

    # ✨ Filtrowanie timeframe'ów
    if FILTER_TIMEFRAMES:
        timeframes = [t for t in timeframes if t in FILTER_TIMEFRAMES]

    print(f"🔎 Symbole do pobrania: {symbols}")
    print(f"🔎 Timeframe'y do pobrania: {timeframes}")

    for symbol in symbols:
        for timeframe in timeframes:
            attempts = 0
            success = False

            while attempts <= MAX_RETRIES and not success:
                success = run_fetch(symbol, timeframe)

                if success:
                    print(f"✅ Sukces: {symbol} {timeframe}")
                else:
                    attempts += 1
                    if attempts <= MAX_RETRIES:
                        print(f"⚠️ Błąd przy pobieraniu {symbol} {timeframe}, ponawiam ({attempts}/{MAX_RETRIES})...")
                        time.sleep(SLEEP_BETWEEN_TASKS)
                    else:
                        print(f"❌ Nieudane pobranie: {symbol} {timeframe} po {MAX_RETRIES} próbach.")

            time.sleep(SLEEP_BETWEEN_TASKS)

if __name__ == "__main__":
    main()
