import os
import time
import json
import csv
from datetime import datetime, timedelta
from collections import Counter
from mt4_connector.command_sender import MT4CommandSender

# Configuration constants
START_DATE = "2001.01.01"  # Default start date for historical data
MAX_RETRIES = 1  # Single attempt like in fetch_single.py
RETRY_DELAY = 5  # Delay between retries in seconds


def load_config(file_name, key):
    """
    Load configuration from JSON file
    Args:
        file_name: Name of config file (e.g. 'symbols.json')
        key: Key to extract from config (e.g. 'symbols')
    Returns:
        List of configured values or empty list on error
    """
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    path = os.path.join(base_dir, 'config', file_name)

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f).get(key, [])
    except Exception as e:
        print(f"❌ Error loading {file_name}: {e}")
        return []


def select_timeframe():
    """
    Display available timeframes and let user select one
    Returns:
        Selected timeframe string (e.g. 'M1', 'D1')
    """
    timeframes = load_config("timeframes.json", "timeframes")

    print("\n📊 Available timeframes:")
    for i, tf in enumerate(timeframes):
        print(f"{i + 1}. {tf}")

    while True:
        try:
            choice = input("\nSelect timeframe (1-" + str(len(timeframes)) + "): ")
            selected = timeframes[int(choice) - 1]
            print(f"Selected timeframe: {selected}")
            return selected
        except (ValueError, IndexError):
            print("⚠️ Invalid selection. Please try again.")


def get_data_path(symbol, timeframe):
    """
    Create proper path for data file
    Args:
        symbol: Trading symbol (e.g. 'EURUSD')
        timeframe: Timeframe string (e.g. 'M1')
    Returns:
        Full path to CSV file
    """
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    folder = os.path.join(base_dir, 'data', 'historical', symbol, timeframe)
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, f"{symbol}_{timeframe}.csv")


def get_last_timestamp(filepath):
    """
    Get last timestamp from CSV file with proper error handling
    Args:
        filepath: Path to CSV file
    Returns:
        Last timestamp string or None if error
    """
    if not os.path.exists(filepath):
        return None

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header

            # Read all valid timestamps
            timestamps = []
            for row in reader:
                if row and row[0].strip():
                    try:
                        # Try parsing to validate format
                        datetime.strptime(row[0], "%Y.%m.%d %H:%M")
                        timestamps.append(row[0])
                    except ValueError:
                        try:
                            datetime.strptime(row[0], "%Y.%m.%d")
                            timestamps.append(row[0])
                        except:
                            continue

            if not timestamps:
                return None

            # Return newest timestamp
            return max(timestamps)

    except Exception as e:
        print(f"⚠️ Error reading {filepath}: {e}")
        return None


def is_trading_day(date):
    """
    Check if date is a trading day (Mon-Fri)
    Args:
        date: datetime.date object
    Returns:
        True if trading day, False otherwise
    """
    return date.weekday() < 5  # 0=Monday, 4=Friday


def get_last_trading_day():
    """
    Get last trading day (returns Friday for weekends)
    Returns:
        datetime.date object of last trading day
    """
    today = datetime.now().date()
    if is_trading_day(today):
        return today
    # For weekends return Friday
    return today - timedelta(days=today.weekday() - 4)


def needs_update(filepath, timeframe):
    """
    Check if file needs update according to MT4's behavior:
    - On weekdays: single 00:00 entry for current day
    - On weekend: treat Friday's data as current until Sunday midnight
    Args:
        filepath: Path to data file
        timeframe: Selected timeframe (e.g. 'M1', 'D1')
    Returns:
        True if update needed, False otherwise
    """
    if not os.path.exists(filepath):
        return True

    last_ts = get_last_timestamp(filepath)
    if not last_ts:
        return True

    try:
        # Parse last timestamp
        last_dt = datetime.strptime(last_ts, "%Y.%m.%d %H:%M" if ' ' in last_ts else "%Y.%m.%d")
        current_dt = datetime.now()

        # Adjust for timezone difference (Ireland is 1h behind Poland)
        adjusted_now = current_dt + timedelta(hours=1)
        adjusted_now_date = adjusted_now.date()
        adjusted_now_weekday = adjusted_now.weekday()  # 0=Monday, 6=Sunday

        if timeframe == 'D1':
            # For daily data, compare dates only
            return last_dt.date() < adjusted_now_date

        elif timeframe in ['M1', 'M5', 'M15', 'M30', 'H1', 'H4']:
            # For intraday data
            if adjusted_now_weekday < 5:  # Monday to Friday
                # Check if we have today's 00:00 entry
                return last_dt.date() < adjusted_now_date
            else:  # Weekend (Saturday/Sunday)
                last_friday = adjusted_now - timedelta(days=adjusted_now_weekday - 4)
                # Accept any Friday's data during weekend
                return last_dt.date() < last_friday.date()

        else:  # W1, MN1
            return last_dt.date() < adjusted_now_date

    except Exception as e:
        print(f"⚠️ Error checking update status: {e}")
        return True


def verify_timeframe(data, requested_tf):
    """
    Verify if received data matches requested timeframe
    Args:
        data: List of candle data
        requested_tf: Timeframe we requested (e.g. 'M1')
    Returns:
        Tuple: (bool result, str detected_timeframe)
    """
    if len(data) < 2:
        return False, "INSUFFICIENT_DATA"

    try:
        # Parse timestamps
        times = [datetime.strptime(row['time'], "%Y.%m.%d %H:%M") for row in data]
        diffs = [(times[i + 1] - times[i]).seconds for i in range(len(times) - 1)]

        # Find most common interval
        if not diffs:
            return False, "NO_INTERVALS"

        most_common = Counter(diffs).most_common(1)[0][0]

        # Map seconds to timeframe
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

        detected_tf = tf_map.get(most_common, f"UNKNOWN({most_common}s)")
        return detected_tf == requested_tf, detected_tf

    except Exception as e:
        print(f"❌ Timeframe verification error: {e}")
        return False, "VERIFICATION_ERROR"


def save_data(symbol, timeframe, data):
    """
    Save data to CSV with duplicate prevention and sorting
    Args:
        symbol: Trading symbol
        timeframe: Timeframe string
        data: List of candle data
    Returns:
        True if saved successfully, False otherwise
    """
    if not data:
        print(f"⚠️ No data to save for {symbol} {timeframe}")
        return False

    filepath = get_data_path(symbol, timeframe)

    # Read existing data
    existing = []
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing = [row for row in reader if row.get("time")]

    # Combine and remove duplicates
    all_data = {row["time"]: row for row in existing}
    all_data.update({row["time"]: row for row in data})

    # Sort by timestamp
    sorted_data = sorted(all_data.values(),
                         key=lambda x: datetime.strptime(x["time"], "%Y.%m.%d %H:%M"))

    # Write to file
    with open(filepath, "w", encoding="utf-8", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["time", "open", "high", "low", "close",
                                               "tick_volume", "spread", "real_volume"])
        writer.writeheader()
        writer.writerows(sorted_data)

    print(f"✅ {symbol} {timeframe}: saved {len(data)} new candles (total: {len(sorted_data)})")
    return True


def fetch_symbol_data(connector, symbol, timeframe):
    """
    Fetch and validate data for single symbol
    Args:
        connector: MT4 connection object
        symbol: Trading symbol
        timeframe: Requested timeframe
    Returns:
        True if successful, False otherwise
    """
    print(f"\n📡 Fetching {symbol} {timeframe}...")

    filepath = get_data_path(symbol, timeframe)
    if not needs_update(filepath, timeframe):
        print(f"ℹ️ Data for {symbol} {timeframe} is up to date - skipping")
        return True

    # Send request to MT4
    if not connector.send_historical_data(symbol, timeframe, START_DATE, datetime.now().strftime("%Y.%m.%d")):
        print(f"❌ Failed to send command for {symbol} {timeframe}")
        return False

    # Get response
    response = connector.receive(timeout=15)
    if not response:
        print(f"⚠️ No response for {symbol} {timeframe}")
        return False

    try:
        # Clean and parse response
        response = response.replace("'", '"').replace("False", "false").replace("True", "true")
        if "}{" in response:  # Fix malformed JSON
            response = response.split("}{")[0] + "}"

        data = json.loads(response)

        # Check for errors
        if data.get("_response") == "NOT_AVAILABLE":
            print(f"⚠️ Symbol {symbol} {timeframe} not available")
            return False

        received_data = data.get("_data", [])
        if not received_data:
            print(f"⚠️ Empty data for {symbol} {timeframe}")
            return False

        # Verify timeframe matches
        is_valid, detected_tf = verify_timeframe(received_data, timeframe)
        if not is_valid:
            print(f"❌ Timeframe mismatch: requested {timeframe}, got {detected_tf}")
            return False

        return save_data(symbol, timeframe, received_data)

    except Exception as e:
        print(f"❌ Processing error for {symbol} {timeframe}: {str(e)}")
        return False


def main():
    """Main execution function with progress tracking"""
    # User selects timeframe
    timeframe = select_timeframe()
    symbols = load_config("symbols.json", "symbols")

    if not symbols:
        print("❌ No symbols configured")
        return

    # Initialize tracking
    stats = {
        'total': len(symbols),
        'up_to_date': 0,
        'needs_update': 0,
        'updated': 0,
        'failed': 0,
        'success': []
    }

    print(f"\n🔍 Analyzing {stats['total']} symbols for {timeframe} timeframe...")
    print(f"ℹ️ Last trading day: {get_last_trading_day()}")

    # First pass: check which symbols need updates
    to_update = []
    for symbol in symbols:
        filepath = get_data_path(symbol, timeframe)
        requires_update = needs_update(filepath, timeframe)

        if requires_update:
            to_update.append(symbol)
            stats['needs_update'] += 1
            print(f"   ↻ {symbol} needs update")
        else:
            stats['up_to_date'] += 1
            print(f"   ✓ {symbol} is current")

    print(f"\n📊 Update analysis:")
    print(f"   • Total symbols: {stats['total']}")
    print(f"   • Current: {stats['up_to_date']}")
    print(f"   • Needing update: {stats['needs_update']}")

    if not to_update:
        print("\n✅ All symbols are up to date!")
        return

    # Connect to MT4
    print("\n⏳ Connecting to MT4...")
    connector = MT4CommandSender(client_id="historical_fetcher", verbose=True)
    time.sleep(5)  # Connection stabilization

    # Process symbols needing updates
    print(f"\n🔄 Updating {len(to_update)} symbols:")
    for i, symbol in enumerate(to_update, 1):
        print(f"\n[{i}/{len(to_update)}] Processing {symbol}")
        success = fetch_symbol_data(connector, symbol, timeframe)

        if success:
            stats['updated'] += 1
            stats['success'].append(symbol)
            print("   ✅ Update successful")
        else:
            stats['failed'] += 1
            print("   ❌ Update failed")

        # Show progress
        print(f"\n   📊 Progress:")
        print(f"      • Updated: {stats['updated']}/{stats['needs_update']}")
        print(f"      • Failed: {stats['failed']}")
        print(f"      • Remaining: {len(to_update) - i}")

        time.sleep(1)  # Rate limiting

    # Cleanup
    connector.shutdown()

    # Final report
    print(f"\n{'=' * 50}")
    print("✅ Update complete")
    print(f"   • Total: {stats['total']}")
    print(f"   • Current: {stats['up_to_date']}")
    print(f"   • Updated: {stats['updated']}")
    print(f"   • Failed: {stats['failed']}")

    if stats['updated']:
        print(f"\n✔ Successfully updated:")
        for i, s in enumerate(stats['success'], 1):
            print(f"   {i}. {s}")

    if stats['failed']:
        print(f"\n❌ Failed to update:")
        failed = [s for s in to_update if s not in stats['success']]
        for i, s in enumerate(failed, 1):
            print(f"   {i}. {s}")

    print("=" * 50)


if __name__ == "__main__":
    main()