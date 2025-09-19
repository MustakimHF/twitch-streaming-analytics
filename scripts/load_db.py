#!/usr/bin/env python3
"""
Load:
- Write processed streams into SQLite (or DB in DB_URL)
- Create helpful indexes
"""
from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

DB_URL = os.getenv("DB_URL", "sqlite:///db/twitch.db")
PROC = ROOT / "data" / "processed"
DB_DIR = ROOT / "db"
DB_DIR.mkdir(parents=True, exist_ok=True)

def run_load(proc_csv: Path | None = None):
    proc_csv = proc_csv or (PROC / "streams_processed.csv")
    df = pd.read_csv(proc_csv)

    engine = create_engine(DB_URL, future=True)
    with engine.begin() as conn:
        df.to_sql("streams", conn, if_exists="replace", index=False)
        try:
            conn.execute(text("CREATE INDEX idx_streams_started ON streams(started_at);"))
            conn.execute(text("CREATE INDEX idx_streams_game ON streams(game_id);"))
            conn.execute(text("CREATE INDEX idx_streams_lang ON streams(language);"))
            conn.execute(text("CREATE INDEX idx_streams_hour ON streams(hour_of_day);"))
        except Exception:
            pass

    print(f"✅ Loaded {len(df)} rows into DB → {DB_URL}")

if __name__ == "__main__":
    run_load()