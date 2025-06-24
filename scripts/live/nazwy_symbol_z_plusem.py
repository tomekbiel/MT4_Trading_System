import os
import glob

# Katalog główny
base_dir = r"C:\python\MT4_Trading_System\data\live"

# Lista symboli do zmiany
symbols = [
    "US.30", "US.500", "DE.30", "JAP225", "OIL.WTI",
    "GOLDs", "EURUSD", "USDJPY", "EURPLN", "VIX", "W.20"
]

# Przeszukaj wszystkie pliki CSV rekursywnie
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(root, file)
            file_name = os.path.splitext(file)[0]

            # Sprawdź czy plik pasuje do któregoś z symboli i nie ma już plusa
            if any(file_name == s for s in symbols) and not file_name.endswith('+'):
                new_name = f"{file_name}+.csv"
                new_path = os.path.join(root, new_name)

                print(f"Zmieniam: {file} -> {new_name}")
                try:
                    os.rename(file_path, new_path)
                    print(f"  ✓ ZMIENIONO: {file} -> {new_name}")
                except Exception as e:
                    print(f"  ✗ BŁĄD przy zmianie {file}: {e}")

print("\nZakończono przetwarzanie plików.")