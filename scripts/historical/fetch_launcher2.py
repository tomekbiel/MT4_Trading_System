import os
import time
import json
import csv
import pandas as pd
from datetime import datetime, timedelta
from collections import Counter
from mt4_connector.command_sender import MT4CommandSender

# Configuration constants
START_DATE = "2001.01.01"  # Default start date for historical data
MAX_RETRIES = 1  # Single attempt like in fetch_single.py
RETRY_DELAY = 5  # Delay between retries in seconds
MAX_PRICE_CHANGE_PCT = 50  # Maksymalna akceptowalna zmiana ceny w %
LAST_HOURS_TO_KEEP = 72  # Ostatnie godziny danych, które uznajemy za wiarygodne

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
                
            # Return the most recent timestamp
            return max(timestamps)
            
    except Exception as e:
        print(f"⚠️ Error reading timestamps from {filepath}: {e}")
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
    if today.weekday() == 5:  # Saturday
        return today - timedelta(days=1)
    elif today.weekday() == 6:  # Sunday
        return today - timedelta(days=2)
    return today

def needs_update(filepath, timeframe):
    """
    Check if file needs update according to MT4's behavior:
    - For D1: Only update on trading days after market close (21:55)
    - On weekend: treat Friday's data as current until Sunday midnight
    - For intraday: update more frequently during market hours
    Args:
        filepath: Path to data file
        timeframe: Selected timeframe (e.g. 'M1', 'D1')
    Returns:
        Tuple: (bool needs_update, str reason)
    """
    if not os.path.exists(filepath):
        return True, "File does not exist"

    last_timestamp = get_last_timestamp(filepath)
    if not last_timestamp:
        return True, "No valid timestamps in file"

    try:
        # Parse last timestamp from file
        try:
            last_dt = datetime.strptime(last_timestamp, "%Y.%m.%d %H:%M")
        except ValueError:
            last_dt = datetime.strptime(last_timestamp + " 23:59", "%Y.%m.%d %H:%M")
            
        last_date = last_dt.date()
        now = datetime.now()
        today = now.date()
        
        # If data is from the future (shouldn't happen)
        if last_date > today:
            return False, f"Future date detected: {last_date}"
        
        # Get last trading day (Friday if weekend, previous day if before 21:55)
        if today.weekday() >= 5:  # Weekend
            last_trading_day = today - timedelta(days=today.weekday() - 4)  # Last Friday
        elif now.hour < 21 or (now.hour == 21 and now.minute < 55):
            # Before 21:55 on trading day, previous day is last trading day
            last_trading_day = today - timedelta(days=1 if today.weekday() > 0 else 3)
        else:
            # After 21:55 on trading day, today is last trading day
            last_trading_day = today
        
        # For daily data (D1)
        if timeframe == 'D1':
            # If we already have data from the last trading day
            if last_date >= last_trading_day:
                return False, f"Already has data from {last_date}"
                
            # If we're on weekend and have Friday's data
            if today.weekday() >= 5 and last_date.weekday() == 4:  # Friday
                return False, "Weekend - using Friday's data"
                
            return True, f"Missing data from {last_trading_day}"
        
        # For intraday timeframes (M1, M5, etc.)
        else:
            # If data is from today or last trading day
            if last_date >= last_trading_day:
                # Check if we need an intraday update
                minutes = int(timeframe[1:]) if timeframe[0] == 'M' else 5
                time_since_last = (now - last_dt).total_seconds() / 60  # in minutes
                
                # During market hours (Monday-Friday 00:00-21:55)
                if today.weekday() < 5 and (now.hour < 21 or (now.hour == 21 and now.minute < 55)):
                    if time_since_last > minutes * 3:  # Allow some delay
                        return True, f"Intraday update needed ({time_since_last:.0f} min old)"
                    return False, f"Recently updated ({time_since_last:.0f} min ago)"
                else:  # Outside market hours or weekend
                    return False, "Outside market hours - no update needed"
            
            # If data is from before last trading day, update is needed
            return True, f"Old data from {last_date}, last trading day was {last_trading_day}"
            
    except Exception as e:
        return True, f"Error checking update status: {e}"

def verify_timeframe(data, requested_tf):
    """
    Verify if received data matches requested timeframe
    Args:
        data: List of candle data
        requested_tf: Timeframe we requested (e.g. 'M1')
    Returns:
        Tuple: (bool result, str detected_timeframe)
    """
    if not data or len(data) < 2:
        return True, requested_tf  # Not enough data to verify

    # Extract timestamps
    timestamps = [candle["time"] for candle in data]
    
    try:
        # Parse timestamps to datetime objects
        times = [datetime.strptime(ts, "%Y.%m.%d %H:%M") for ts in timestamps]
        
        # Sort to ensure chronological order
        times.sort()
        
        # Calculate time differences between consecutive candles
        diffs = [(times[i+1] - times[i]).total_seconds() // 60 for i in range(len(times)-1)]
        
        # Find most common interval (in minutes)
        if not diffs:
            return True, requested_tf  # Only one data point
            
        most_common = Counter(diffs).most_common(1)[0][0]
        
        # Map minutes to timeframe string
        tf_map = {
            1: 'M1',
            5: 'M5',
            15: 'M15',
            30: 'M30',
            60: 'H1',
            240: 'H4',
            1440: 'D1',
            10080: 'W1',
            43200: 'MN1'
        }
        
        detected_tf = tf_map.get(most_common, f"M{int(most_common)}")
        
        # Special case for D1 data which might have gaps on weekends
        if detected_tf == 'D1' and most_common in [1440, 1441, 1439]:
            return True, 'D1'
            
        # Allow small deviations (e.g., 4-6 minutes for M5)
        expected_minutes = int(requested_tf[1:]) if requested_tf[0] == 'M' else {
            'H1': 60, 'H4': 240, 'D1': 1440, 'W1': 10080, 'MN1': 43200
        }.get(requested_tf, 1)
        
        if abs(most_common - expected_minutes) <= 1:
            return True, requested_tf
            
        print(f"⚠️ Timeframe mismatch: expected ~{expected_minutes}min, got {most_common}min")
        return False, detected_tf
        
    except Exception as e:
        print(f"⚠️ Error verifying timeframe: {e}")
        return True, requested_tf  # On error, assume it's correct

def check_data_consistency(df):
    """
    Check if data is consistent by comparing with recent data
    Args:
        df: DataFrame with 'time' as index and 'close' column
    Returns:
        Tuple: (bool is_consistent, DataFrame clean_df)
    """
    if df is None or df.empty:
        return True, df
        
    # Make a copy to avoid modifying original
    clean_df = df.copy()
    
    # Convert index to datetime if it's not already
    if not isinstance(clean_df.index, pd.DatetimeIndex):
        clean_df.index = pd.to_datetime(clean_df.index, format='%Y.%m.%d %H:%M')
    
    # Sort by time
    clean_df = clean_df.sort_index()
    
    # Get last 72 hours of data as reference
    last_valid_time = clean_df.index.max()
    if pd.isna(last_valid_time):
        return False, clean_df
        
    threshold_time = last_valid_time - pd.Timedelta(hours=LAST_HOURS_TO_KEEP)
    recent_data = clean_df[clean_df.index >= threshold_time]
    
    if recent_data.empty:
        return True, clean_df
    
    # Calculate reference values from recent data
    recent_mean = recent_data['close'].mean()
    recent_std = recent_data['close'].std()
    
    # If we don't have enough data, skip consistency check
    if pd.isna(recent_mean) or pd.isna(recent_std) or recent_std == 0:
        return True, clean_df
    
    # Calculate z-scores for all data points
    clean_df['z_score'] = (clean_df['close'] - recent_mean) / recent_std
    
    # Identify outliers (beyond 3 standard deviations)
    outliers_mask = (clean_df['z_score'].abs() > 3)
    
    # Also check for percentage change from recent mean
    clean_df['pct_change'] = ((clean_df['close'] - recent_mean) / recent_mean * 100).abs()
    pct_outliers = clean_df['pct_change'] > MAX_PRICE_CHANGE_PCT
    
    # Combine both conditions
    all_outliers = outliers_mask | pct_outliers
    
    if not all_outliers.any():
        return True, clean_df.drop(columns=['z_score', 'pct_change'], errors='ignore')
    
    print(f"⚠️ Found {all_outliers.sum()} potential anomalies in the data")
    
    # Remove outliers
    clean_df = clean_df[~all_outliers].drop(columns=['z_score', 'pct_change'], errors='ignore')
    return False, clean_df

def save_data(symbol, timeframe, data):
    """
    Save data to CSV with consistency checks and duplicate prevention
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
    
    try:
        # Convert input data to DataFrame
        new_data = pd.DataFrame(data)
        
        # Ensure 'time' is datetime and set as index
        new_data['time'] = pd.to_datetime(new_data['time'], format='%Y.%m.%d %H:%M')
        new_data.set_index('time', inplace=True)
        
        # Convert numeric columns
        for col in ['open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']:
            if col in new_data.columns:
                new_data[col] = pd.to_numeric(new_data[col], errors='coerce')
        
        # Load existing data if file exists
        if os.path.exists(filepath):
            try:
                # Try reading with header first
                existing_data = pd.read_csv(
                    filepath, 
                    parse_dates=['time'], 
                    index_col='time',
                    dtype={'tick_volume': float, 'spread': int, 'real_volume': float}
                )
                
                # If no header, try without
                if existing_data.empty or existing_data.isna().all().all():
                    existing_data = pd.read_csv(
                        filepath, 
                        header=None,
                        names=['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume'],
                        parse_dates=['time'],
                        index_col='time',
                        dtype={'tick_volume': float, 'spread': int, 'real_volume': float}
                    )
                
                # Combine old and new data, keeping the newest version of each timestamp
                combined = pd.concat([existing_data, new_data[~new_data.index.isin(existing_data.index)]])
                
                # Sort by index
                combined = combined.sort_index()
                
                # Check for duplicates and keep last occurrence
                combined = combined[~combined.index.duplicated(keep='last')]
                
            except Exception as e:
                print(f"⚠️ Error reading existing data: {e}")
                combined = new_data
        else:
            combined = new_data
        
        # Format index to desired string format
        combined.index = combined.index.strftime('%Y.%m.%d %H:%M')
        
        # Ensure all required columns exist
        required_columns = ['open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']
        for col in required_columns:
            if col not in combined.columns:
                combined[col] = 0
        
        # Reorder columns
        combined = combined[required_columns]
        
        # Save to file
        combined.to_csv(filepath, index=True, header=True, date_format='%Y.%m.%d %H:%M')
        
        print(f"✅ {symbol} {timeframe}: saved {len(combined)} candles")
        return True
        
    except Exception as e:
        print(f"❌ Error saving {symbol} {timeframe}: {e}")
        import traceback
        traceback.print_exc()
        return False

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
        import traceback
        traceback.print_exc()
        return False

# Global variable to track inconsistencies
inconsistencies = []

def verify_new_data(symbol, timeframe, new_data, existing_data):
    """
    Verify if new data is consistent with existing data
    Returns True if data is consistent, False otherwise
    """
    global inconsistencies
    
    if new_data is None or new_data.empty:
        inconsistencies.append(f"{symbol} {timeframe}: No new data to verify")
        return False
        
    if existing_data is None or existing_data.empty:
        return True  # No existing data to compare with
    
    try:
        # Get the last known good close price from existing data
        last_good_close = existing_data['close'].iloc[-1]
        last_good_time = existing_data.index[-1]
        
        # Get the first new close price and time
        first_new_close = new_data['close'].iloc[0] if not new_data.empty else None
        first_new_time = new_data.index[0] if not new_data.empty else None
        
        # If we can't get these values, assume data is OK
        if pd.isna(last_good_close) or pd.isna(first_new_close):
            return True
            
        # Calculate percentage change
        price_diff = first_new_close - last_good_close
        pct_change = abs(price_diff / last_good_close * 100)
        
        # Check for data gaps (missing periods)
        time_gap = (first_new_time - last_good_time).total_seconds() / 60  # in minutes
        expected_interval = int(timeframe[1:]) if timeframe.startswith('M') else 1
        
        # Prepare inconsistency message (if any)
        inconsistency_msg = None
        
        # If change is too big, data might be invalid
        if pct_change > 50:  # 50% change threshold
            inconsistency_msg = (
                f"{symbol} {timeframe}: Large price change detected - "
                f"{pct_change:.2f}% ({last_good_close:.5f} -> {first_new_close:.5f}) "
                f"at {first_new_time.strftime('%Y.%m.%d %H:%M')}"
            )
        # Check for suspiciously small changes (possible flatline)
        elif pct_change < 0.001 and abs(price_diff) > 0:
            inconsistency_msg = (
                f"{symbol} {timeframe}: Suspiciously small price change - "
                f"{pct_change:.6f}% ({last_good_close:.5f} -> {first_new_close:.5f})"
            )
        # Check for data gaps
        elif time_gap > expected_interval * 3:  # More than 3x expected interval
            inconsistency_msg = (
                f"{symbol} {timeframe}: Large time gap detected - "
                f"{time_gap:.0f} minutes between {last_good_time.strftime('%Y.%m.%d %H:%M')} "
                f"and {first_new_time.strftime('%Y.%m.%d %H:%M')}"
            )
        
        if inconsistency_msg:
            inconsistencies.append(inconsistency_msg)
            print(f"⚠️ {inconsistency_msg}")
            return False
            
        return True
        
    except Exception as e:
        error_msg = f"{symbol} {timeframe}: Verification error - {str(e)}"
        print(f"⚠️ {error_msg}")
        inconsistencies.append(error_msg)
        return True  # On error, assume data is OK

def save_data(symbol, timeframe, data):
    """
    Save data to CSV with basic validation
    Returns True if saved successfully, False otherwise
    """
    if not data:
        print(f"⚠️ No data to save for {symbol} {timeframe}")
        return False

    filepath = get_data_path(symbol, timeframe)
    
    try:
        # Convert input data to DataFrame
        new_data = pd.DataFrame(data)
        
        # Basic column handling - ensure required columns exist
        required_columns = ['time', 'open', 'high', 'low', 'close']
        for col in required_columns:
            if col not in new_data.columns:
                print(f"⚠️ Missing required column: {col}")
                return False
        
        # Ensure 'time' is datetime and set as index
        new_data['time'] = pd.to_datetime(new_data['time'], format='%Y.%m.%d %H:%M', errors='coerce')
        new_data = new_data.dropna(subset=['time'])  # Drop rows with invalid dates
        
        if new_data.empty:
            print("⚠️ No valid data after date parsing")
            return False
            
        new_data.set_index('time', inplace=True)
        
        # Convert numeric columns, coerce errors to NaN
        numeric_cols = ['open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']
        for col in numeric_cols:
            if col in new_data.columns:
                new_data[col] = pd.to_numeric(new_data[col], errors='coerce')
        
        # Load existing data if file exists
        existing_data = None
        if os.path.exists(filepath):
            try:
                # Try reading with header first
                existing_data = pd.read_csv(
                    filepath, 
                    parse_dates=['time'], 
                    index_col='time',
                    dtype={'tick_volume': float, 'spread': int, 'real_volume': float},
                    encoding='utf-8'
                )
                
                # If no header or empty, try without header
                if existing_data.empty or existing_data.isna().all().all():
                    existing_data = pd.read_csv(
                        filepath, 
                        header=None,
                        names=['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume'],
                        parse_dates=['time'],
                        index_col='time',
                        dtype={'tick_volume': float, 'spread': int, 'real_volume': float},
                        encoding='utf-8'
                    )
                
                # Sort existing data
                existing_data = existing_data.sort_index()
                
                # Verify new data against existing data
                if not verify_new_data(symbol, timeframe, new_data, existing_data):
                    print(f"❌ Data verification failed for {symbol} {timeframe}")
                    return False
                
                # Combine data, keeping the newest version of each timestamp
                combined = pd.concat([existing_data, new_data])
                
            except Exception as e:
                print(f"⚠️ Error reading existing data: {e}")
                combined = new_data
        else:
            combined = new_data
        
        # Remove duplicates, keeping the last occurrence
        combined = combined[~combined.index.duplicated(keep='last')]
        
        # Sort by index
        combined = combined.sort_index()
        
        # Ensure all required columns exist with default values
        for col in numeric_cols:
            if col not in combined.columns:
                combined[col] = 0.0
        
        # Format index to desired string format
        combined.index = combined.index.strftime('%Y.%m.%d %H:%M')
        
        # Reorder columns for consistency
        final_columns = ['open', 'high', 'low', 'close']
        if 'tick_volume' in combined.columns:
            final_columns.append('tick_volume')
        if 'spread' in combined.columns:
            final_columns.append('spread')
        if 'real_volume' in combined.columns:
            final_columns.append('real_volume')
            
        combined = combined[final_columns]
        
        # Save to file
        combined.to_csv(filepath, index=True, header=True, date_format='%Y.%m.%d %H:%M', encoding='utf-8')
        
        return True
        
    except Exception as e:
        print(f"❌ Error saving {symbol} {timeframe}: {e}")
        import traceback
        traceback.print_exc()
        return False

def print_inconsistencies():
    """Print all detected inconsistencies at the end"""
    if not inconsistencies:
        print("✅ No data inconsistencies detected")
        return
    
    print("\n" + "=" * 80)
    print("⚠️  DETECTED DATA INCONSISTENCIES:")
    print("=" * 80)
    for i, msg in enumerate(inconsistencies, 1):
        print(f"{i}. {msg}")
    print("=" * 80)
    print(f"Total inconsistencies detected: {len(inconsistencies)}")

def check_files_status(symbols, timeframe):
    """Check status of all files before updating"""
    print("\n" + "=" * 80)
    print("🔍 CHECKING FILES STATUS:")
    print("=" * 80)
    
    status = {
        'need_update': [],
        'up_to_date': [],
        'missing': []
    }
    
    for symbol in sorted(symbols):
        filepath = get_data_path(symbol, timeframe)
        
        if not os.path.exists(filepath):
            status['missing'].append((symbol, "File does not exist"))
            continue
            
        needs_update_flag, reason = needs_update(filepath, timeframe)
        if needs_update_flag:
            status['need_update'].append((symbol, reason))
        else:
            status['up_to_date'].append((symbol, reason))
    
    # Print summary
    print(f"\n📋 FOUND {len(symbols)} SYMBOLS:")
    print(f"   • Need update: {len(status['need_update'])}")
    print(f"   • Up to date:  {len(status['up_to_date'])}")
    print(f"   • Missing:     {len(status['missing'])}")
    
    # Print details if needed
    if status['need_update']:
        print("\n🔄 SYMBOLS NEEDING UPDATE:")
        for symbol, reason in status['need_update']:
            print(f"   • {symbol}: {reason}")
    
    if status['missing']:
        print("\n❌ MISSING FILES:")
        for symbol, reason in status['missing']:
            print(f"   • {symbol}: {reason}")
    
    print("\n" + "=" * 80)
    
    # Ask for confirmation
    if status['need_update'] or status['missing']:
        print("\nDo you want to proceed with updates? (y/n): ", end='')
        if input().strip().lower() != 'y':
            print("\nUpdate cancelled by user.")
            return False
    
    return True

def main():
    """Main function to fetch historical data for all symbols"""
    global inconsistencies
    inconsistencies = []  # Reset inconsistencies at start
    
    print("=" * 80)
    print("📊 MT4 Historical Data Fetcher v2 - With Data Validation")
    print("=" * 80)
    
    # Load symbols from config
    symbols = load_config("symbols.json", "symbols")
    if not symbols:
        print("❌ No symbols found in configuration")
        return
    
    # Select timeframe
    timeframe = select_timeframe()
    
    # First check status of all files
    if not check_files_status(symbols, timeframe):
        return
    
    # Initialize MT4 connector
    connector = MT4CommandSender()
    
    # Initialize tracking
    total_symbols = len(symbols)
    updated = []
    up_to_date = []
    failed = []
    
    print(f"\n🔍 Processing {total_symbols} symbols...")
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Process each symbol
    for i, symbol in enumerate(symbols, 1):
        print(f"\n[{i}/{total_symbols}] Processing {symbol} {timeframe}...")
        
        filepath = get_data_path(symbol, timeframe)
        
        # Check if update is needed
        needs_update_flag, reason = needs_update(filepath, timeframe)
        
        if not needs_update_flag:
            print(f"   ✓ Already up to date: {reason}")
            up_to_date.append((symbol, reason))
            continue
        
        print(f"   ! Update needed: {reason}")
        
        # Fetch new data
        if fetch_symbol_data(connector, symbol, timeframe):
            print(f"   ✓ Successfully updated")
            updated.append((symbol, reason))
        else:
            print(f"   ✗ Update failed")
            failed.append((symbol, reason))
    
    # Print detailed summary
    print("\n" + "=" * 80)
    print("📊 DETAILED UPDATE SUMMARY:")
    print("=" * 80)
    
    # Updated symbols
    if updated:
        print("\n🔄 UPDATED SYMBOLS:")
        for symbol, reason in sorted(updated):
            print(f"   • {symbol}: {reason}")
    
    # Up-to-date symbols
    if up_to_date:
        print(f"\n✅ UP-TO-DATE SYMBOLS ({len(up_to_date)}):")
        # Show only first 5 up-to-date symbols to save space
        for symbol, reason in sorted(up_to_date)[:5]:
            print(f"   • {symbol}: {reason}")
        if len(up_to_date) > 5:
            print(f"   ... and {len(up_to_date) - 5} more symbols are up to date")
    
    # Failed updates
    if failed:
        print("\n❌ FAILED UPDATES:")
        for symbol, reason in sorted(failed):
            print(f"   • {symbol}: {reason}")
    
    # Missing files
    missing_files = [(s, "File not found") for s in symbols 
                    if not os.path.exists(get_data_path(s, timeframe))]
    if missing_files:
        print("\n❌ MISSING FILES:")
        for symbol, reason in sorted(missing_files):
            print(f"   • {symbol}: {reason}")
    
    # Summary counts
    print("\n" + "=" * 80)
    print("📊 SUMMARY COUNTS:")
    print("-" * 80)
    print(f"Total symbols:      {total_symbols}")
    print(f"Successfully updated: {len(updated)}")
    print(f"Already up-to-date:  {len(up_to_date)}")
    print(f"Failed updates:      {len(failed)}")
    print(f"Missing files:       {len(missing_files)}")
    
    # Show any inconsistencies found
    print_inconsistencies()
    
    print("=" * 80)
    print("✅ Data fetch completed")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Operation cancelled by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
