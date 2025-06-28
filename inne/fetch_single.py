import os
import sys
import time
import json
from datetime import datetime
from collections import Counter
from mt4_connector.command_sender import MT4CommandSender

# 📥 Loading symbol and timeframe from JSON files
def load_symbol_and_timeframe():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    symbols_path = os.path.join(base_dir, 'config', 'symbols.json')
    timeframes_path = os.path.join(base_dir, 'config', 'timeframes.json')

    with open(symbols_path, encoding="utf-8") as f:
        symbols = json.load(f).get("symbols", [])

    with open(timeframes_path, encoding="utf-8") as f:
        valid_timeframes = json.load(f).get("timeframes", [])

    print("\n📌 Available symbols:")
    for i, s in enumerate(symbols):
        print(f"{i + 1}. {s}")

    default_symbol = "US.100+"
    default_timeframe = "M1"

    symbol_input = input(f"\nSelect symbol (ENTER = {default_symbol}): ").strip()
    symbol = symbol_input if symbol_input else default_symbol

    if symbol not in symbols:
        print(f"⚠️ Symbol '{symbol}' not found in the list – using default: {default_symbol}")
        symbol = default_symbol

    timeframe_input = input(f"Select timeframe (ENTER = {default_timeframe}): ").strip()
    timeframe = timeframe_input if timeframe_input else default_timeframe

    if timeframe not in valid_timeframes:
        print(f"⚠️ Timeframe '{timeframe}' not supported – using default: {default_timeframe}")
        timeframe = default_timeframe

    return symbol, timeframe, valid_timeframes

# 📂 Creates target directory
def ensure_directory(symbol, timeframe):
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    path = os.path.join(base_dir, 'data', 'historical', symbol, timeframe)
    os.makedirs(path, exist_ok=True)
    return path

# 🕒 Gets the last known timestamp
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

# 🧠 Detects actual timeframe based on differences between candles
def detect_actual_timeframe(data):
    if len(data) < 2:
        return None

    try:
        # Attempt to parse with hours and minutes
        times = [datetime.strptime(row['time'], "%Y.%m.%d %H:%M") for row in data]
        diffs = [(times[i + 1] - times[i]).seconds for i in range(len(times) - 1)]

        # If all differences are 0, check day differences
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
            print(f"Error detecting interval: {e}")
            return None

# 💾 Saves data (if the interval matches)
def save_data(symbol, timeframe, data):
    directory = ensure_directory(symbol, timeframe)
    filepath = os.path.join(directory, f"{symbol}_{timeframe}.csv")
    last_ts = get_last_timestamp(filepath)
    new_rows = []
    skipped_count = 0
    
    print(f"\n💾 Saving data to: {filepath}")
    print(f"📅 Last timestamp in file: {last_ts or 'No existing file'}")
    print(f"📊 Processing {len(data)} rows of data...")

    for row in data:
        row_time = row["time"]
        if last_ts is None or row_time > last_ts:
            new_rows.append(
                f"{row_time},{row['open']},{row['high']},{row['low']}," +
                f"{row['close']},{row['tick_volume']},{row['spread']},{row['real_volume']}\n"
            )
        else:
            skipped_count += 1
            if skipped_count <= 5:  # Log first 5 skipped rows
                print(f"  ⏩ Skipping row (not newer than last_ts): {row_time}")
    
    if skipped_count > 5:
        print(f"  ... and {skipped_count - 5} more rows skipped")

    print(f"📝 Found {len(new_rows)} new rows to append")
    print(f"⏭️  Skipped {skipped_count} existing rows")

    if not new_rows:
        print("ℹ️ No new data to append.")
        return

    mode = "a" if os.path.exists(filepath) else "w"
    with open(filepath, mode, encoding="utf-8") as f:
        if mode == "w":
            f.write("time,open,high,low,close,tick_volume,spread,real_volume\n")
        f.writelines(new_rows)

    print(f"✅ Appended {len(new_rows)} candles to: {filepath}")

# 🚀 Main function
def fetch_single(symbol, timeframe, valid_timeframes):
    print(f"\n🚀 Fetching data: {symbol} {timeframe}")
    connector = MT4CommandSender(client_id="historical_fetch_single", verbose=True)
    time.sleep(5)

    start_date = "2001.01.01"
    end_date = datetime.today().strftime("%Y.%m.%d")
    success = connector.send_historical_data(symbol, timeframe, start_date, end_date)

    if not success:
        print(f"❌ Failed to send command for {symbol} {timeframe}")
        connector.shutdown()
        sys.exit(3)

    response = connector.receive(timeout=10)

    if response:
        try:
            response_fixed = response.replace("'", '"').replace("False", "false").replace("True", "true")
            if "}{" in response_fixed:
                response_fixed = response_fixed.split("}{")[0] + "}"

            parsed = json.loads(response_fixed)

            if "_response" in parsed and parsed["_response"] == "NOT_AVAILABLE":
                print(f"⚠️ Symbol {symbol} {timeframe} not available")
            elif "_data" in parsed:
                actual_tf = detect_actual_timeframe(parsed["_data"])
                if actual_tf != timeframe:
                    print(f"⚠️ Actual data interval is {actual_tf}, not {timeframe}. Data will NOT be saved.")
                elif actual_tf not in valid_timeframes:
                    print(f"⚠️ Actual interval {actual_tf} is not in the list of allowed timeframes.")
                else:
                    save_data(symbol, timeframe, parsed["_data"])
            else:
                print("⚠️ No _data in the response.")
        except Exception as e:
            print(f"❌ Error parsing response: {e}")
    else:
        print(f"⚠️ No response from MT4 for {symbol} {timeframe}")

    connector.shutdown()
    time.sleep(3)

# ▶️ Start
if __name__ == "__main__":
    symbol, timeframe, valid_timeframes = load_symbol_and_timeframe()
    fetch_single(symbol, timeframe, valid_timeframes)
