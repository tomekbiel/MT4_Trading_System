# scripts/run_mt4.py

import argparse
import sys
import os
import time
from rich.console import Console
from rich.prompt import Prompt

# 🔧 Ustawienie ścieżki projektu
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

console = Console()

def menu():
    console.print("\n[bold cyan]🌟 Wybierz tryb uruchomienia systemu MT4:[/bold cyan]")
    console.print("[yellow]1[/yellow] - Heartbeat (test komunikacji)")
    console.print("[yellow]2[/yellow] - Live data (dane na żywo)")
    console.print("[yellow]3[/yellow] - History "
                  "data (dane historyczne)")
    console.print("[yellow]4[/yellow] - Trade operations (konto i zlecenia)")
    console.print("[red]5[/red] - Wyjście")

    choice = Prompt.ask("\n👉 [green]Podaj numer[/green]", choices=["1", "2", "3", "4", "5"])
    return choice

def run_mode(choice):
    if choice == "1":
        from scripts.heartbeat.send_heartbeat import main as heartbeat_main
        heartbeat_main()

    elif choice == "2":
        from scripts.live.live_data import main as live_main
        live_main()

    elif choice == "3":
        from scripts.historical.fetch_single_2 import main as hist_main
        hist_main()

    elif choice == "4":
        from scripts.trades.get_account_info import main as account_info_main
        from scripts.trades.get_open_trades import main as open_trades_main

        console.print("\n[bold magenta]🔹 Pobieram dane konta...[/bold magenta]")
        account_info_main()

        time.sleep(5)

        console.print("\n[bold magenta]🔹 Pobieram otwarte zlecenia...[/bold magenta]")
        open_trades_main()

    elif choice == "5":
        console.print("\n👋 [bold red]Wyjście z programu.[/bold red]")
        sys.exit(0)

    else:
        console.print(f"❌ Nieznany wybór: {choice}")

def main():
    while True:
        choice = menu()
        run_mode(choice)

if __name__ == "__main__":
    main()
