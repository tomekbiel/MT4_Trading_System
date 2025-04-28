# scripts/live_data.py
from mt4_connector import MT4MarketDataHandler
import time
import keyboard  # wymaga instalacji: pip install keyboard


def main():
    print("🟢 MT4 Connector LIVE DATA - Naciśnij ESC aby zatrzymać")
    handler = MT4MarketDataHandler(
        csv_output_dir='../../data/live',
        save_to_csv=True,
        verbose=True
    )
    handler.subscribe('US.100+')

    try:
        while not keyboard.is_pressed('esc'):
            time.sleep(0.1)
    finally:
        handler.shutdown()
        print("🔴 Zatrzymano pobieranie danych")


if __name__ == "__main__":
    main()