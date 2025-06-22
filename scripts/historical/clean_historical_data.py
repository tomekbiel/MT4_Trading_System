import os
import csv
from datetime import datetime
from collections import defaultdict


def load_symbols():
    """Load symbols from a configuration file"""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    path = os.path.join(base_dir, 'config', 'symbols.json')

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f).get("symbols", [])
    except Exception as e:
        print(f"❌ Error loading symbols: {e}")
        return []


def load_timeframes():
    """Load available timeframes"""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    path = os.path.join(base_dir, 'config', 'timeframes.json')

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f).get("timeframes", [])
    except Exception as e:
        print(f"❌ Error loading timeframes: {e}")
        return ['M1', 'M5', 'M15', 'H1', 'H4', 'D1', 'W1', 'MN1']


def get_data_path(symbol, timeframe):
    """Get path to data file"""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    return os.path.join(base_dir, 'data', 'historical', symbol, timeframe, f"{symbol}_{timeframe}.csv")


def parse_timestamp(ts):
    """Parse timestamp from string with flexible format
    Supported formats:
    - YYYY.MM.DD HH:MM
    - YYYY/MM/DD HH:MM
    - YYYY-MM-DD HH:MM:SS
    - YYYY.MM.DD
    - YYYY/MM/DD
    - YYYY-MM-DD
    """
    if not isinstance(ts, str):
        return None

    # List of formats to check
    formats = [
        "%Y-%m-%d %H:%M:%S",  # YYYY-MM-DD HH:MM:SS
        "%Y.%m.%d %H:%M",     # YYYY.MM.DD HH:MM
        "%Y/%m/%d %H:%M",     # YYYY/MM/DD HH:MM
        "%Y-%m-%d",           # YYYY-MM-DD
        "%Y.%m.%d",           # YYYY.MM.DD
        "%Y/%m/%d"            # YYYY/MM/DD
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(ts.strip(), fmt)
        except ValueError:
            continue

    # If no format matches, try to normalize separators
    try:
        # Try to convert all separators to dots and check again
        normalized_ts = ts.replace('-', '.').replace('/', '.')
        return datetime.strptime(normalized_ts, "%Y.%m.%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(normalized_ts, "%Y.%m.%d %H:%M")
        except ValueError:
            try:
                return datetime.strptime(normalized_ts, "%Y.%m.%d")
            except ValueError:
                pass

    # If still unable to parse, display the message and return None
    print(f"⚠️ Could not parse timestamp: {ts}")
    return None


def clean_and_sort_csv(filepath):
    """
    Clean and sort CSV file:
    1. Remove duplicates by timestamp
    2. Sort chronologically
    3. Preserve the original structure
    """
    if not os.path.exists(filepath):
        print(f"⚠️ File not found: {filepath}")
        return False

    # Read existing data
    data = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)

        if not header:
            print(f"⚠️ Empty file: {filepath}")
            return False

        for row in reader:
            if len(row) >= 5:  # Minimal required columns
                data.append(row)

    if not data:
        print(f"ℹ️ No data rows in: {filepath}")
        return True

    # Remove duplicates and sort
    unique_data = {}
    for row in data:
        ts = parse_timestamp(row[0])
        if ts:
            unique_data[ts] = row

    if not unique_data:
        print(f"⚠️ No valid timestamps in: {filepath}")
        return False

    # Sort by timestamp
    sorted_timestamps = sorted(unique_data.keys())

    # Prepare data for writing, formatting timestamp to YYYY.MM.DD HH:MM
    sorted_data = []
    for ts in sorted_timestamps:
        row = unique_data[ts].copy()
        # Formatuj timestamp do pożądanego formatu
        formatted_ts = ts.strftime("%Y.%m.%d %H:%M")
        row[0] = formatted_ts  # Pierwsza kolumna to timestamp
        sorted_data.append(row)

    # Write back to the file
    with open(filepath, "w", encoding="utf-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(sorted_data)

    print(f"✅ Cleaned {filepath}: {len(data)} -> {len(sorted_data)} rows")
    return True


def main():
    """Main processing function"""
    symbols = load_symbols()
    timeframes = load_timeframes()

    if not symbols:
        print("❌ No symbols found")
        return

    print(f"\n🔍 Starting cleanup for {len(symbols)} symbols and {len(timeframes)} timeframes")

    processed_files = 0
    for symbol in symbols:
        for timeframe in timeframes:
            filepath = get_data_path(symbol, timeframe)
            if clean_and_sort_csv(filepath):
                processed_files += 1

    print(f"\n✅ Finished processing:")
    print(f"   • Symbols processed: {len(symbols)}")
    print(f"   • Timeframes checked: {len(timeframes)}")
    print(f"   • Files cleaned: {processed_files}")


if __name__ == "__main__":
    import json  # Import moved here to avoid shadowing

    main()