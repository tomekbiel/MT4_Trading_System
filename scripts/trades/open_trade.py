# scripts/trades/open_trade.py

import time
from mt4_connector.command_sender import MT4CommandSender
from mt4_connector.trade_defaults import DEFAULT_ORDER, show_default_order, update_default_order

def open_default_trade():
    # Inicjalizacja połączenia
    conn = MT4CommandSender(client_id="open_trade_script", verbose=True)
    time.sleep(1)

    # Wyświetl domyślne parametry
    print("\n=== Domyślne parametry nowego zlecenia ===")
    show_default_order()

    # Wysłanie komendy TRADE;OPEN
    print("\n🚀 Wysyłam komendę: TRADE;OPEN")
    success = conn.send("TRADE;OPEN")

    if not success:
        print("\n❌ Nie udało się wysłać komendy TRADE;OPEN")
    else:
        print("\n✅ Komenda TRADE;OPEN wysłana poprawnie!")

    # Zakończenie połączenia
    conn.shutdown()


if __name__ == "__main__":
#    update_default_order('type', 1)    # zmiana typu
#    update_default_order('volume', 0.2)  # zmiana volume
    open_default_trade()


"""
from mt4_connector.command_sender import MT4CommandSender
from mt4_connector.trade_defaults import show_default_order, update_default_order

# 1. Pokaż domyślne parametry
show_default_order()

# 2. Zmieniam np. wolumen
update_default_order('volume', 0.01)

# 3. Tworzę połączenie
conn = MT4CommandSender(client_id="trade_test", verbose=True)

# 4. Wysyłam komendę do MT4
conn.send("TRADE;OPEN")

# 5. Czekam na odpowiedź
response = conn.receive(timeout=3)
if response:
    print("✅ Odpowiedź:", response)
else:
    print("❌ Brak odpowiedzi lub MT4 nie potwierdził (może zamknięty rynek)")

# 6. Zamykam połączenie
conn.shutdown()

"""