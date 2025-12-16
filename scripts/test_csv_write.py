import os
import csv
from datetime import datetime

def test_csv_write():
    # Create test directory if it doesn't exist
    test_dir = r"C:\python\MT4_Trading_System\data\live\2025-12-11"
    os.makedirs(test_dir, exist_ok=True)
    
    # Test file path
    test_file = os.path.join(test_dir, "test_write.csv")
    
    try:
        # Test writing to the file
        with open(test_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if os.path.getsize(test_file) == 0:  # Write header if file is new
                writer.writerow(['timestamp', 'value'])
            
            # Write test data
            timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            writer.writerow([timestamp, "TEST_VALUE"])
            f.flush()
            os.fsync(f.fileno())
        
        print(f"✅ Successfully wrote test data to: {os.path.abspath(test_file)}")
        return True
        
    except Exception as e:
        print(f"❌ Error writing test file: {e}")
        return False

if __name__ == "__main__":
    test_csv_write()
