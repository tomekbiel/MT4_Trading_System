# scripts/live_data.py
from mt4_connector import MT4MarketDataHandler
import time
import keyboard  # requires installation: pip install keyboard


def main():
    print("🟢 MT4 Connector LIVE DATA - Press ESC to stop")
    
    # Use absolute path for the output directory
    import os
    # Go up two levels from the current script's directory to reach MT4_Trading_System
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Go up one more level to get to the project root, then into data/live
    project_root = os.path.dirname(script_dir)
    output_dir = os.path.join(project_root, 'data', 'live')
    
    # Create the directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"[INFO] Saving data to: {output_dir}")
    
    handler = MT4MarketDataHandler(
        csv_output_dir=output_dir,
        save_to_csv=True,
        verbose=True
    )
    
    # Subscribe to instruments
    symbols = [
        'US.100+', 'OIL.WTI+', 'VIX+', 'EURUSD+', 'MEXComp+', 'GOLDs+', 'SILVERs+',
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