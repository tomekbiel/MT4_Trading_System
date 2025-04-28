# mt4_connector/trade_defaults.py

DEFAULT_ORDER = {
    "symbol": "US.100+",
    "type": "buy",
    "volume": 0.01,
    "open_price": 0.0,
    "sl": 0.0,
    "tp": 0.0,
    "magic": 123456,
    "comment": "AutoTrade",
    "expiration": 0
}

def show_default_order():
    print("📋 Domyślne parametry nowego zlecenia:")
    for key, value in DEFAULT_ORDER.items():
        print(f"  {key}: {value}")

def update_default_order(key, value):
    if key in DEFAULT_ORDER:
        DEFAULT_ORDER[key] = value
        print(f"✅ Zmieniono: {key} = {value}")
    else:
        print(f"❌ Klucz '{key}' nie istnieje w domyślnym zleceniu.")
