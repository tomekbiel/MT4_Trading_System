import os
import pandas as pd
import re
from pathlib import Path
from datetime import datetime


def is_historical_file_valid(df):
    """Check if the DataFrame has the expected structure of a historical file"""
    required_columns = ['date', 'time_col', 'open', 'high', 'low', 'close', 'tick_volume']
    return all(col in df.columns for col in required_columns)


def convert_historical_file(hist_file):
    """
    Convert a historical data file from MT4 History Center format to standard format.
    Returns a DataFrame with the converted data or None if conversion fails.
    """
    try:
        # Try to read the file with different encodings
        try:
            df = pd.read_csv(hist_file, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(hist_file, encoding='latin1')

        # Check if the file has headers or not
        if not is_historical_file_valid(df):
            # Try reading without headers
            df = pd.read_csv(hist_file, header=None,
                             names=['date', 'time_col', 'open', 'high', 'low', 'close', 'tick_volume'])

        # Check if we have the required columns
        if not is_historical_file_valid(df):
            print(f"   ⚠️  Skipping {hist_file.name} - unexpected file format")
            return None

        # Convert columns to appropriate types
        for col in ['open', 'high', 'low', 'close', 'tick_volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Drop rows with invalid data
        df = df.dropna(subset=['open', 'high', 'low', 'close'])

        # Combine date and time columns
        df['time'] = df['date'].astype(str) + ' ' + df['time_col'].astype(str)

        # Convert to datetime, trying multiple formats
        for fmt in ['%Y.%m.%d %H:%M', '%Y.%m.%d %H:%M:%S', '%Y/%m/%d %H:%M', '%Y-%m-%d %H:%M']:
            try:
                df['time'] = pd.to_datetime(df['time'], format=fmt, errors='raise')
                break
            except:
                continue

        # If still not converted, try without format (as last resort)
        if not pd.api.types.is_datetime64_any_dtype(df['time']):
            df['time'] = pd.to_datetime(df['time'], errors='coerce')

        # Drop rows with invalid timestamps
        df = df.dropna(subset=['time'])

        if df.empty:
            print(f"   ⚠️  No valid timestamps found in {hist_file.name}")
            return None

        # Add missing columns with default values
        df['spread'] = 0
        df['real_volume'] = 0

        # Select and reorder columns
        df = df[['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']]

        # Sort by time and remove duplicates
        df = df.sort_values('time')
        df = df.drop_duplicates(subset='time', keep='last')

        print(f"   ✓ Converted {len(df)} rows from {hist_file.name}")
        return df

    except Exception as e:
        # Only show error message if it's not a known format issue
        if 'Unknown datetime string format' not in str(e):
            print(f"   ❌ Error processing {hist_file.name}: {str(e)[:100]}...")
        return None


def find_auto_file(hist_file):
    """
    Find the corresponding auto file for a historical file.
    Returns the Path to the auto file or None if not found.
    """
    # Extract base name and time frame
    name = hist_file.stem
    time_frame_match = re.search(r'(\d+)$', name)

    if not time_frame_match:
        print(f"   ⚠️  Could not determine time frame for {name}")
        return None

    time_frame = time_frame_match.group(1)
    base_name = name[:-len(time_frame)]

    # Map time frame to standard format
    time_frame_map = {
        '1': 'M1', '5': 'M5', '15': 'M15', '30': 'M30',
        '60': 'H1', '240': 'H4', '1440': 'D1'
    }

    if time_frame not in time_frame_map:
        print(f"   ⚠️  Unknown time frame: {time_frame}")
        return None

    time_frame_suffix = time_frame_map[time_frame]

    # Look for matching auto files in different locations
    search_patterns = [
        f"{base_name}_{time_frame_suffix}.csv",
        f"{base_name}{time_frame_suffix}.csv",
        f"{base_name}_{time_frame}.csv"
    ]

    # Search in the historical file's directory and its parent
    search_dirs = [hist_file.parent, hist_file.parent.parent]

    for pattern in search_patterns:
        for search_dir in search_dirs:
            for auto_file in search_dir.glob(pattern):
                if auto_file != hist_file:  # Make sure we don't match the historical file itself
                    return auto_file

    print(f"   ⚠️  Could not find auto file for {name}")
    return None


def merge_with_auto(hist_df, auto_file):
    """
    Merge historical data with auto data.
    Returns the merged DataFrame and number of new rows added.
    """
    try:
        # Convert hist_df time to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(hist_df['time']):
            hist_df['time'] = pd.to_datetime(hist_df['time'])

        # Read the auto file if it exists
        if auto_file.exists():
            # Read auto file with proper date parsing
            auto_df = pd.read_csv(auto_file)

            # Convert time column to datetime, handling different formats
            if not pd.api.types.is_datetime64_any_dtype(auto_df['time']):
                auto_df['time'] = pd.to_datetime(auto_df['time'])

            # Convert both time columns to the same timezone-naive format for comparison
            hist_df['time'] = pd.to_datetime(hist_df['time']).dt.tz_localize(None)
            auto_df['time'] = pd.to_datetime(auto_df['time']).dt.tz_localize(None)

            # Find the latest timestamp in auto_df
            latest_auto_time = auto_df['time'].max()
            print(f"   📅 Latest timestamp in auto file: {latest_auto_time}")

            # Find new rows in hist_df that are newer than the latest in auto_df
            new_rows = hist_df[hist_df['time'] > latest_auto_time]

            if not new_rows.empty:
                print(f"   ➕ Found {len(new_rows)} new rows to add")

                # Combine old and new data
                final_df = pd.concat([auto_df, new_rows], ignore_index=True)

                # Sort by time and remove any duplicates (keeping the last occurrence)
                final_df = final_df.sort_values('time')
                final_df = final_df.drop_duplicates(subset='time', keep='last')

                # Format time column consistently
                final_df['time'] = final_df['time'].dt.strftime('%Y.%m.%d %H:%M')

                return final_df, len(new_rows)
            else:
                print("   ℹ️  No new data to add")
                # Return the original data with consistent time formatting
                auto_df['time'] = auto_df['time'].dt.strftime('%Y.%m.%d %H:%M')
                return auto_df, 0
        else:
            print("   🆕 Auto file doesn't exist, creating new one")
            # If auto file doesn't exist, use the historical data
            hist_df = hist_df.sort_values('time')
            hist_df['time'] = hist_df['time'].dt.strftime('%Y.%m.%d %H:%M')
            return hist_df, len(hist_df)

    except Exception as e:
        print(f"   ❌ Error merging with {auto_file.name}: {str(e)}")
        return None, 0


def is_historical_file(file_path):
    """Check if the file is a historical file (from MT4 History Center)"""
    try:
        # Read first line to check the format
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            first_line = f.readline().strip()

        # Historical files typically don't have headers or have specific format
        # We'll assume that if the first line has 7 comma-separated values, it's a historical file
        parts = first_line.split(',')
        if len(parts) == 7:
            # Check if the first two parts look like date and time
            date_part = parts[0].strip()
            time_part = parts[1].strip()

            # Check if date is in YYYY.MM.DD format (or similar)
            if ('.' in date_part or '-' in date_part or '/' in date_part) and ':' in time_part:
                return True

        return False
    except:
        return False


def process_historical_files(directory):
    """
    Process all historical data files in the directory and its subdirectories.
    """
    directory = Path(directory)
    if not directory.is_dir():
        print(f"❌ Error: {directory} is not a valid directory")
        return

    # Find all CSV files
    all_csv_files = list(directory.rglob('*.csv'))

    if not all_csv_files:
        print("❌ No CSV files found")
        return

    print(f"🔍 Found {len(all_csv_files)} CSV files")

    # Filter only historical files
    historical_files = [f for f in all_csv_files if is_historical_file(f)]

    if not historical_files:
        print("❌ No historical files found (expected format: date,time,open,high,low,close,volume)")
        return

    print(f"🔍 Found {len(historical_files)} historical files to process")

    processed_count = 0
    updated_count = 0

    for i, hist_file in enumerate(historical_files, 1):
        print(f"\n[{i}/{len(historical_files)}] Processing {hist_file.name}...")

        try:
            # Convert the historical file
            hist_df = convert_historical_file(hist_file)
            if hist_df is None or hist_df.empty:
                print(f"   ⚠️  Could not convert {hist_file.name}, skipping...")
                continue

            # Find the corresponding auto file
            auto_file = find_auto_file(hist_file)
            if auto_file is None:
                print(f"   ⚠️  No matching auto file found for {hist_file.name}")
                continue

            print(f"   🔄 Merging with {auto_file.name}")

            # Merge with auto file
            merged_df, new_rows = merge_with_auto(hist_df, auto_file)

            if merged_df is not None and new_rows > 0:
                # Save the result
                merged_df.to_csv(auto_file, index=False)

                print(f"   ✅ Updated {auto_file.name}")
                print(f"   📊 Total rows: {len(merged_df)}")
                print(f"   ➕ Added {new_rows} new rows")
                print(f"   📅 Date range: {merged_df['time'].iloc[0]} to {merged_df['time'].iloc[-1]}")

                updated_count += 1
            else:
                print(f"   ℹ️  No new data to add to {auto_file.name}")

            processed_count += 1

        except Exception as e:
            print(f"   ❌ Error processing {hist_file.name}: {str(e)[:200]}")
            continue

    print(f"\n✅ Processed {processed_count} files, updated {updated_count} auto files")


if __name__ == "__main__":
    # Default directory
    BASE_DIR = r"C:\python\MT4_Trading_System\data\historical"

    print("🔄 Processing historical data files...")
    print(f"🔍 Searching in: {BASE_DIR}")

    process_historical_files(BASE_DIR)

    print("\nOperation completed. Press Enter to exit...")
    input()