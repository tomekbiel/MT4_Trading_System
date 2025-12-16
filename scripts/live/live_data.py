# scripts/live_data.py
from mt4_connector import MT4MarketDataHandler
import time
import keyboard  # requires installation: pip install keyboard
from datetime import datetime
import os

def main():
    print("🟢 MT4 Connector LIVE DATA - Press ESC to stop")
    
    # Use absolute path for the output directory

    # Go up two levels from the current script's directory to reach MT4_Trading_System
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Go up one more level to get to the project root, then into data/live
    project_root = os.path.dirname(script_dir)
    output_dir = os.path.join(project_root, 'data', 'live')
    
    # Create the directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"[INFO] Saving data to: {output_dir}")

    # ====== RAW DATA FORWARDING SETUP ======
    # To disable raw data logging, comment out this entire section
    academic_project_dir = r"C:\python\project_programming_for_ai\data\hfd"
    os.makedirs(academic_project_dir, exist_ok=True)

    # Initialize log file variables
    current_log_file = None
    current_log_date = None

    def get_log_file_path():
        """Get the current log file path based on the current date"""
        date_str = datetime.now().strftime('%Y%m%d')
        return os.path.join(academic_project_dir, f"mt4_raw_{date_str}.log")

    def forward_raw_data(message):
        """Forward raw MT4 data to academic project directory with MT4 timestamp"""
        nonlocal current_log_file, current_log_date
        try:
5            import re
            from datetime import datetime
            
            # Try to parse as formatted message first: [SYMBOL] YYYY-MM-DD HH:MM:SS.mmm (BID/ASK) BID/ASK
            formatted_match = re.match(r'^\[([^\]]+)\]\s+([\d\s:-]+)\.\d+\s+\(([\d.]+)\/([\d.]+)\)', message)
            
            # Try to parse as raw message: SYMBOL:|:BID;ASK
            raw_match = re.match(r'^([^:]+):\|:([\d.]+);([\d.]+)', message)
            
            if formatted_match:
                # Handle formatted message
                symbol, timestamp_str, bid_str, ask_str = formatted_match.groups()
                mt4_timestamp = timestamp_str.strip()
            elif raw_match:
                # Handle raw message - use current time as timestamp
                symbol, bid_str, ask_str = raw_match.groups()
                mt4_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            else:
                print(f"[WARNING] Could not parse MT4 message: {message}")
                return
            
            # Format: MT4_TIMESTAMP|SYMBOL|BID|ASK
            formatted_message = f"{mt4_timestamp}|{symbol}|{bid_str}|{ask_str}"
            
            current_date = datetime.now().strftime('%Y%m%d')
            current_log_path = get_log_file_path()

            # Check if directory exists, create if not
            os.makedirs(os.path.dirname(current_log_path), exist_ok=True)

            # Check if we need to rotate the log file
            if current_log_date != current_date or current_log_file is None:
                # Close previous file if it exists
                if current_log_file:
                    current_log_file.close()

                # Open new log file
                current_log_file = open(current_log_path, 'a', buffering=1)
                current_log_date = current_date
                print(f"[INFO] Writing raw data to: {current_log_path}")

            # Write the formatted message
            current_log_file.write(f"{formatted_message}\n")
            current_log_file.flush()

        except Exception as e:
            print(f"[ERROR] Failed to process message: {str(e)}")
            if current_log_file:
                try:
                    current_log_file.close()
                except:
                    pass
                current_log_file = None

    # ====== END OF RAW DATA FORWARDING SETUP ======
    handler = MT4MarketDataHandler(
        csv_output_dir=output_dir,
        save_to_csv=True,
        verbose=True
    )
    
    # Ustawiamy callback dla surowych danych
    print(f"[DEBUG] Setting raw data callback to: {forward_raw_data}")
    handler.set_raw_data_callback(forward_raw_data)
    print(f"[DEBUG] Raw data callback set. Handler's callback is now: {getattr(handler, 'raw_data_callback', 'Not set')}")
    
    # Subscribe to instruments
    symbols = [
        'US.100+', 'OIL.WTI+', 'VIX+', 'EURUSD+', 'MEXComp+', 'SILVERs+',
        'BRAComp+', 'CZKCASH+', 'DE.30+', 'EU.50+', 'FRA.40+', 'ITA.40+', 'NED25+',
        'SPA.35+', 'SUI20+', 'UK.100+', 'US.30+', 'COTTONs+', 'US2000+', 'W.20+',
        'WHEAT+', 'COCOA+', 'COFFEE+', 'COPPER+', 'CORN+', 'EMISS+', 'NICKEL+',
        'OILs+', 'NATGAS+', 'PLATINUM+', 'SOYBEAN+', 'SUGARs+', 'ZINC+', 'BUND10Y+',
        'SCHATZ2Y+', 'USDIDX+', 'AUT20+', 'PALLADIUM+', 'DE40+', 'AUDUSD+', 'EURCHF+',
        'EURGBP+', 'EURJPY+', 'GBPUSD+', 'NZDUSD+', 'USDCAD+', 'USDCHF+', 'USDJPY+',
        'US.500+'
    ]
    for symbol in symbols:
        handler.subscribe(symbol)

    try:
        # The _data_loop runs in a separate thread, so we just need to keep the main thread alive
        while not keyboard.is_pressed('esc'):
            time.sleep(0.1)  # Small delay to prevent high CPU usage
    except KeyboardInterrupt:
        pass
    finally:
        print("\n🔄 Shutting down...")
        handler.shutdown()
        print("🔴 Data collection stopped")


if __name__ == "__main__":
    main()