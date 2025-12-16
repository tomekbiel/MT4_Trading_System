#!/usr/bin/env python3
"""
Simple script to run live data collection.
"""
import os
import sys

# Add current directory to Python path
sys.path.insert(0, os.path.abspath('.'))

# Import and run the live data script
from scripts.live.live_data import main

if __name__ == "__main__":
    print("🔄 Starting live data collection...")
    main()
