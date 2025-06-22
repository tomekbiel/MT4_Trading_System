#!/usr/bin/env python3
"""
Heartbeat Sender - A utility script for sending heartbeat signals to MT4.

This script establishes a connection to a running MT4 instance and sends a heartbeat
signal to verify the connection is alive. It's useful for monitoring the connection
status between the Python trading system and MetaTrader 4.

Usage:
    python send_heartbeat.py

The script will:
1. Connect to the MT4 Command Sender
2. Send a single heartbeat
3. Wait briefly to ensure delivery
4. Shut down cleanly
"""

import time
import sys
from pathlib import Path

# Add parent directory to path to allow importing mt4_connector
sys.path.append(str(Path(__file__).parent.parent.parent))

from mt4_connector import MT4CommandSender

def main():
    """
    Main function to send a heartbeat to MT4.
    
    Initializes the MT4 connection, sends a heartbeat signal, and ensures
    proper cleanup of resources.
    """
    try:
        # Initialize the MT4 command sender with a descriptive client ID
        connector = MT4CommandSender(
            client_id="heartbeat_sender",
            verbose=True
        )
        
        # Brief pause to ensure connection is established
        time.sleep(1)
        
        # Send the heartbeat signal
        print("💓 Sending heartbeat to MT4...")
        success = connector.send_heartbeat()
        
        if success:
            print("✅ Heartbeat sent successfully")
        else:
            print("❌ Failed to send heartbeat")
        
        # Brief pause before shutdown to ensure message delivery
        time.sleep(2)
        
    except Exception as e:
        print(f"❌ Error in heartbeat script: {e}")
        return 1
    finally:
        # Ensure proper cleanup
        if 'connector' in locals():
            connector.shutdown()
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
