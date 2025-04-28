import json
import os
from mt4_connector import MT4TradeHandler

def test_symbols_loaded():
    handler = MT4TradeHandler(verbose=True)
    assert len(handler.symbols) > 0, "Brak symboli – nie wczytano z JSON"
    assert isinstance(handler.timeframes, dict), "Brak poprawnego słownika timeframes"
    print("🔍 Dostępne symbole:", handler.symbols)
    print("⏱️ Timeframes:", handler.timeframes)
    handler.shutdown()

def test_timeframe_limits():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(BASE_DIR, "config", "symbols.json")
    assert os.path.exists(path), "Nie znaleziono pliku config/symbols.json"

    with open(path, "r", encoding="utf-8") as f:
        config = json.load(f)

    assert "timeframes" in config, "Brak klucza 'timeframes' w pliku JSON"
    timeframes = config["timeframes"]

    for tf, limits in timeframes.items():
        has_limit = any(key in limits for key in ("max_days", "max_months", "max_years"))
        assert has_limit, f"⛔ Timeframe '{tf}' nie zawiera żadnych limitów czasowych (max_days / max_months / max_years)"
        print(f"✅ {tf}: {limits}")
