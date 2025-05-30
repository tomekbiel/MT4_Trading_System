import os
import csv
from datetime import datetime

# Bazowy katalog z danymi historycznymi
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'historical'))

def get_last_timestamp(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # Pomijamy nagłówek
            timestamps = []

            for row in reader:
                if row and row[0].strip():
                    try:
                        dt = datetime.strptime(row[0], "%Y.%m.%d %H:%M")
                        timestamps.append(dt)
                    except:
                        try:
                            dt = datetime.strptime(row[0], "%Y.%m.%d")
                            timestamps.append(dt)
                        except:
                            continue

            return max(timestamps) if timestamps else None
    except Exception as e:
        print(f"❌ Error reading {filepath}: {e}")
        return None

def main():
    print("📂 Scanning historical data files...\n")
    for symbol in os.listdir(BASE_DIR):
        symbol_path = os.path.join(BASE_DIR, symbol)
        if not os.path.isdir(symbol_path):
            continue

        for tf in os.listdir(symbol_path):
            tf_path = os.path.join(symbol_path, tf)
            if not os.path.isdir(tf_path):
                continue

            for file in os.listdir(tf_path):
                if file.endswith(".csv"):
                    full_path = os.path.join(tf_path, file)
                    last_ts = get_last_timestamp(full_path)
                    print(f"{symbol:<10} | {tf:<4} | {last_ts} | {file}")

if __name__ == "__main__":
    main()
