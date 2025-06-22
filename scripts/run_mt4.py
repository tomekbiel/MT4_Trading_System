"""
MT4 Trading System - Main Entry Point

This script provides a command-line interface to interact with the MT4 trading system.
It allows starting different components like heartbeat monitoring, live data streaming,
historical data processing, and trade operations.
"""

import argparse
import sys
import os
import time
from rich.console import Console
from rich.prompt import Prompt

# Add project root to Python path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Initialize console for rich text output
console = Console()

def menu() -> str:
    """
    Display the main menu and get user's choice.
    
    Returns:
        str: User's menu choice as a string ('1' to '5')
    """
    console.print("\n[bold cyan]🌟 MT4 Trading System - Main Menu:[/bold cyan]")
    console.print("[yellow]1[/yellow] - Heartbeat (connection test)")
    console.print("[yellow]2[/yellow] - Live data (real-time market data)")
    console.print("[yellow]3[/yellow] - Historical data (historical price data)")
    console.print("[yellow]4[/yellow] - Trade operations (account and orders)")
    console.print("[red]5[/red] - Exit")

    choice = Prompt.ask("\n👉 [green]Enter your choice[/green]", choices=["1", "2", "3", "4", "5"])
    return choice

def run_mode(choice: str) -> None:
    """
    Execute the selected mode based on user's choice.
    
    Args:
        choice (str): The selected menu option ('1' to '4')
    """
    if choice == "1":
        # Run heartbeat test to verify connection
        from scripts.heartbeat.send_heartbeat import main as heartbeat_main
        heartbeat_main()

    elif choice == "2":
        # Start live market data streaming
        from scripts.live.live_data import main as live_main
        live_main()

    elif choice == "3":
        # Process historical market data
        from scripts.historical.fetch_single_2 import main as hist_main
        hist_main()

    elif choice == "4":
        # Execute trade-related operations
        from scripts.trades.get_account_info import main as account_info_main
        from scripts.trades.get_open_trades import main as open_trades_main

        console.print("\n[bold magenta]🔹 Fetching account information...[/bold magenta]")
        account_info_main()

        # Add a small delay for better user experience
        time.sleep(2)


        console.print("\n[bold magenta]🔹 Retrieving open orders...[/bold magenta]")
        open_trades_main()

    elif choice == "5":
        console.print("\n👋 [bold red]Exiting the program.[/bold red]")
        sys.exit(0)

    else:
        console.print(f"❌ Unknown choice: {choice}")

def main():
    while True:
        choice = menu()
        run_mode(choice)

if __name__ == "__main__":
    main()
