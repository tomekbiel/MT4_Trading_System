import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
from mt4_connector.base_connector import MT4BaseConnector

def main():
    print("📡 Test: Wysyłam komendę get_account_info do MT4...")

    connector = MT4BaseConnector(client_id="test_ping", verbose=True)

    # Wysyłamy prostą komendę
    time.sleep(1)
    sent = connector.send("GET ACCOUNT INFORMATION")

    if not sent:
        print("❌ Nie udało się wysłać wiadomości.")
        return

    # Czekamy na odpowiedź do 5 sekund
    response = connector.receive(timeout=5)

    if response:
        print(f"✅ Odebrano odpowiedź z MT4:\n{response}")
    else:
        print("⚠️ Brak odpowiedzi od MT4 (timeout).")

    connector.shutdown()

if __name__ == "__main__":
    main()
