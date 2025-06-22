import os
import pandas as pd
from datetime import datetime

# Mapping of manual file extensions to target file extensions
file_mapping = {
    '1.csv': '_M1.csv',
    '5.csv': '_M5.csv',
    '15.csv': '_M15.csv',
    '60.csv': '_H1.csv',
    '240.csv': '_H4.csv',
    '1440.csv': '_D1.csv'
}

# Base path to data directory
BASE_DIR = r"C:\python\MT4_Trading_System\data\historical"


def convert_to_timestamp(date_str, time_str):
    """
    Converts various date and time formats to required timestamp format.
    
    Args:
        date_str (str): Date string in various possible formats
        time_str (str): Time string
        
    Returns:
        str: Formatted timestamp in 'YYYY.MM.DD HH:MM' format
        
    Raises:
        ValueError: If conversion fails
    """
    try:
        # Try different input formats
        for fmt in ('%Y.%m.%d %H:%M', '%d/%m/%Y %H:%M', '%d.%m.%Y %H:%M', '%Y-%m-%d %H:%M'):
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", fmt)
                return dt.strftime("%Y.%m.%d %H:%M")
            except ValueError:
                continue
        # If no format matches, use automatic parsing
        dt = pd.to_datetime(f"{date_str} {time_str}", dayfirst=True)
        return dt.strftime("%Y.%m.%d %H:%M")
    except Exception as e:
        raise ValueError(f"Cannot convert '{date_str} {time_str}' to timestamp: {str(e)}")


def merge_and_replace_data():
    """
    Processes manual files while maintaining exact timestamp format (2025.05.13 12:10)
    and handles various input date formats.
    
    The function:
    1. Walks through the directory tree
    2. Identifies manual files
    3. Converts data to required format
    4. Merges with existing automatic files if present
    5. Removes duplicates and sorts by timestamp
    6. Saves processed data and removes manual files
    """
    for root, dirs, files in os.walk(BASE_DIR):
        for file in files:
            # Check if it's a manual file
            for manual_suffix, auto_suffix in file_mapping.items():
                if file.endswith(manual_suffix):
                    manual_path = os.path.join(root, file)
                    auto_path = os.path.join(root, file.replace(manual_suffix, auto_suffix))

                    try:
                        # Read manual file - check if first row is header
                        with open(manual_path, 'r') as f:
                            first_line = f.readline().strip()

                        # Check if first line contains text (header)
                        has_header = any(c.isalpha() for c in first_line)

                        # Load data, skipping header if it exists
                        df_manual = pd.read_csv(manual_path, header=None if has_header else None)

                        # Check if file contains data
                        if df_manual.empty:
                            print(f"⚠️ File {file} is empty - skipping")
                            continue

                        # Verify if file has at least 7 columns
                        if df_manual.shape[1] < 7:
                            raise ValueError(f"Invalid number of columns ({df_manual.shape[1]}) in file {file}")

                        # Combine first two columns into timestamp in required format
                        df_manual['time'] = df_manual.apply(
                            lambda x: convert_to_timestamp(str(x[0]), str(x[1])),
                            axis=1
                        )

                        # Assign remaining columns
                        df_manual['open'] = df_manual[2]
                        df_manual['high'] = df_manual[3]
                        df_manual['low'] = df_manual[4]
                        df_manual['close'] = df_manual[5]
                        df_manual['volume'] = df_manual[6]

                        # Add missing columns with default values
                        df_manual['spread'] = 0
                        df_manual['real_volume'] = 0

                        # Select only required columns
                        final_columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'spread', 'real_volume']
                        df_manual = df_manual[final_columns]

                        # If automatic file exists, load it
                        if os.path.exists(auto_path):
                            df_auto = pd.read_csv(auto_path)

                            # Find the latest date in automatic file
                            last_auto_time = df_auto['time'].max()

                            # Filter only newer data from manual file
                            df_new_data = df_manual[df_manual['time'] > last_auto_time]

                            if df_new_data.empty:
                                print(f"ℹ️ No newer data in {file} than in {os.path.basename(auto_path)}")
                                os.remove(manual_path)
                                print(f"🗑️ Removed manual file: {file}")
                                continue

                            # Merge data
                            df_combined = pd.concat([df_auto, df_new_data], ignore_index=True)
                        else:
                            # If no automatic file exists, use all manual data
                            df_combined = df_manual

                        # Sort and remove duplicates
                        df_combined.sort_values('time', inplace=True)
                        df_combined.drop_duplicates(subset='time', keep='last', inplace=True)

                        # Save results while preserving timestamp format
                        df_combined.to_csv(auto_path, index=False)
                        print(f"✅ Updated {os.path.basename(auto_path)} with data from {file}")

                        # Remove manual file
                        os.remove(manual_path)
                        print(f"🗑️ Removed manual file: {file}")

                    except Exception as e:
                        print(f"❌ Error processing {file}: {str(e)}")


if __name__ == "__main__":
    merge_and_replace_data()