import os
import json
from mt4_connector.historical_data_handler import MT4HistoricalDataHandler

# 📥 Dynamiczne ładowanie symbolu i interwału z ENV lub fallback do plików JSON
def load_symbol_and_timeframe():
    symbol = os.environ.get("SYMBOL", None)
    timeframe = os.environ.get("TIMEFRAME", None)

    if symbol and timeframe:
        return symbol, timeframe

    # Jeśli nie ustawiono ENV, wczytaj z configów domyślne wartości
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    with open(os.path.join(base_dir, 'config', 'symbols.json'), encoding="utf-8") as f:
        symbols = json.load(f).get("symbols", [])
    with open(os.path.join(base_dir, 'config', 'timeframes.json'), encoding="utf-8") as f:
        timeframes = json.load(f).get("timeframes", [])
    return symbols[0], timeframes[0]

# 🧠 Główna funkcja
def main():
    symbol, timeframe = load_symbol_and_timeframe()
    print(f"📊 Start pobierania: {symbol} {timeframe}")

    handler = MT4HistoricalDataHandler()
    handler.fetch_and_save(symbol, timeframe)

if __name__ == "__main__":
    main()
