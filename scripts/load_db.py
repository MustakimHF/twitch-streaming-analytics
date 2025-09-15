#!/usr/bin/env python3
"""
Load:
- Write processed streams into SQLite (or DB in DB_URL) with historical data preservation
- Append new data while avoiding duplicates
- Create helpful indexes
"""
from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

DB_URL = os.getenv("DB_URL", "sqlite:///db/twitch.db")
PROC = ROOT / "data" / "processed"
DB_DIR = ROOT / "db"
DB_DIR.mkdir(parents=True, exist_ok=True)

def run_load(proc_csv: Path | None = None, preserve_history: bool = True):
    """
    Load processed streams into database.
    
    Args:
        proc_csv: Path to processed CSV file
        preserve_history: If True, append new data and avoid duplicates. If False, replace all data.
    """
    proc_csv = proc_csv or (PROC / "streams_processed.csv")
    df = pd.read_csv(proc_csv)
    
    # Add ingestion timestamp for tracking when data was added
    df['ingested_at'] = datetime.now().isoformat()

    engine = create_engine(DB_URL, future=True)
    with engine.begin() as conn:
        if preserve_history:
            # Check if table exists
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='streams'"))
            table_exists = result.fetchone() is not None
            
            if table_exists:
                # Get existing stream IDs to avoid duplicates
                existing_ids = pd.read_sql_query("SELECT DISTINCT id FROM streams", conn)['id'].tolist()
                
                # Filter out streams that already exist
                new_streams = df[~df['id'].isin(existing_ids)]
                
                if len(new_streams) > 0:
                    new_streams.to_sql("streams", conn, if_exists="append", index=False)
                    print(f"✅ Added {len(new_streams)} new streams to existing data")
                else:
                    print("ℹ️ No new streams to add (all streams already exist)")
                
                # Show total count
                total_count = pd.read_sql_query("SELECT COUNT(*) as count FROM streams", conn)['count'].iloc[0]
                print(f"📊 Total streams in database: {total_count:,}")
            else:
                # First time - create table with all data
                df.to_sql("streams", conn, if_exists="replace", index=False)
                print(f"✅ Created new table with {len(df)} streams")
        else:
            # Replace mode (original behavior)
            df.to_sql("streams", conn, if_exists="replace", index=False)
            print(f"✅ Replaced all data with {len(df)} streams")
        
        # Create indexes (ignore if they already exist)
        try:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_streams_started ON streams(started_at);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_streams_game ON streams(game_id);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_streams_lang ON streams(language);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_streams_hour ON streams(hour_of_day);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_streams_id ON streams(id);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_streams_ingested ON streams(ingested_at);"))
        except Exception as e:
            print(f"⚠️ Index creation warning: {e}")

    print(f"✅ Database updated → {DB_URL}")

def get_historical_summary():
    """Get summary of historical data in the database."""
    engine = create_engine(DB_URL, future=True)
    with engine.begin() as conn:
        try:
            # Check if table exists
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='streams'"))
            if result.fetchone() is None:
                print("📊 No historical data found - database is empty")
                return
            
            # Check if ingested_at column exists
            columns_result = conn.execute(text("PRAGMA table_info(streams)"))
            columns = [row[1] for row in columns_result.fetchall()]
            has_ingested_at = 'ingested_at' in columns
            
            # Build query based on available columns
            if has_ingested_at:
                query = """
                    SELECT 
                        COUNT(*) as total_streams,
                        COUNT(DISTINCT DATE(started_at)) as unique_days,
                        MIN(DATE(started_at)) as earliest_date,
                        MAX(DATE(started_at)) as latest_date,
                        COUNT(DISTINCT user_id) as unique_streamers,
                        COUNT(DISTINCT game_id) as unique_games,
                        MIN(ingested_at) as first_ingestion,
                        MAX(ingested_at) as last_ingestion
                    FROM streams
                """
            else:
                query = """
                    SELECT 
                        COUNT(*) as total_streams,
                        COUNT(DISTINCT DATE(started_at)) as unique_days,
                        MIN(DATE(started_at)) as earliest_date,
                        MAX(DATE(started_at)) as latest_date,
                        COUNT(DISTINCT user_id) as unique_streamers,
                        COUNT(DISTINCT game_id) as unique_games,
                        'N/A' as first_ingestion,
                        'N/A' as last_ingestion
                    FROM streams
                """
            
            summary = pd.read_sql_query(query, conn)
            
            if not summary.empty:
                s = summary.iloc[0]
                print("📊 Historical Data Summary:")
                print(f"   📅 Date range: {s['earliest_date']} to {s['latest_date']}")
                print(f"   📈 Total streams: {s['total_streams']:,}")
                print(f"   📆 Unique days: {s['unique_days']}")
                print(f"   👤 Unique streamers: {s['unique_streamers']:,}")
                print(f"   🎮 Unique games: {s['unique_games']:,}")
                if has_ingested_at:
                    print(f"   ⏰ First ingestion: {s['first_ingestion']}")
                    print(f"   ⏰ Last ingestion: {s['last_ingestion']}")
                else:
                    print("   ⏰ Ingestion tracking: Not available (legacy data)")
                
        except Exception as e:
            print(f"⚠️ Error getting historical summary: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--replace":
        print("🔄 Running in REPLACE mode (will lose historical data)")
        run_load(preserve_history=False)
    else:
        print("📚 Running in HISTORICAL mode (preserves existing data)")
        run_load(preserve_history=True)
        get_historical_summary()
