#!/usr/bin/env python3
"""
Orchestrate the Twitch ETL: extract → transform → load.
Run from project root:  python scripts/run_etl.py
"""
from pathlib import Path
import sys
here = Path(__file__).resolve().parent
sys.path.append(str(here))

from extract_twitch import run_extract
from transform import run_transform
from load_db import run_load

if __name__ == "__main__":
    print("🚀 Starting Twitch ETL")
    run_extract()
    run_transform()
    run_load()
    print("🏁 ETL complete")