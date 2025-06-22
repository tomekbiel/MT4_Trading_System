import pandas as pd
import os

# Path to the data folder
BASE_DIR = r"C:\python\MT4_Trading_System\data\historical\US.100+\M5"

# File names to merge (change to your files)
FILE1 = "US.100+_M1.csv"
FILE2 = "US.100+_1.csv"
OUTPUT_FILE = "US.100+_M1.csv"


def merge_csv_by_time():
    """
    Merges two CSV files based on the time column, preserving all data.
    - Loads both files
    - Merges them keeping all rows
    - Sorts by date
    - Saves to a new file
    """
    try:
        # Load both files
        df1 = pd.read_csv(os.path.join(BASE_DIR, FILE1))
        df2 = pd.read_csv(os.path.join(BASE_DIR, FILE2))

        # Check if both have the time column
        if 'time' not in df1.columns or 'time' not in df2.columns:
            print("❌ Both files must have a 'time' column")
            return

        # Merge the files
        merged = pd.concat([df1, df2], ignore_index=True)

        # Convert time to datetime for proper sorting
        merged['time'] = pd.to_datetime(merged['time'])

        # Sort by time
        merged.sort_values('time', inplace=True)

        # Remove duplicates (keeps the last occurrence)
        merged.drop_duplicates(subset='time', keep='last', inplace=True)
        
        # Convert dates back to the required format
        merged['time'] = merged['time'].dt.strftime('%Y.%m.%d %H:%M')

        # Save the result
        output_path = os.path.join(BASE_DIR, OUTPUT_FILE)
        merged.to_csv(output_path, index=False, date_format='%Y.%m.%d %H:%M')

        print(f"✅ Files merged successfully. Result saved to: {output_path}")
        print(f"Number of rows: {len(merged)}")

    except Exception as e:
        print(f"❌ Error while merging files: {str(e)}")


if __name__ == "__main__":
    print("Merging CSV files...")
    merge_csv_by_time()