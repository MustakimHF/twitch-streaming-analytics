#!/usr/bin/env python3
"""
Transform:
- Parse timestamps
- Derive hour_of_day, weekday, is_weekend
- Join game names
- Keep useful columns
- Save data/processed/streams_processed.csv
"""
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROC = ROOT / "data" / "processed"
PROC.mkdir(parents=True, exist_ok=True)

def run_transform(streams_csv: Path | None = None, games_csv: Path | None = None) -> "pd.DataFrame":
    streams_csv = streams_csv or (RAW / "twitch_streams.csv")
    games_csv = games_csv or (RAW / "twitch_games.csv")

    s = pd.read_csv(streams_csv)
    g = pd.read_csv(games_csv) if games_csv.exists() else pd.DataFrame(columns=["id","name"])

    # Parse timestamps (Helix field: started_at is ISO 8601, UTC)
    if "started_at" in s.columns:
        s["started_at"] = pd.to_datetime(s["started_at"], errors="coerce", utc=True)
        s["hour_of_day"] = s["started_at"].dt.hour
        s["weekday"] = s["started_at"].dt.day_name()
        s["is_weekend"] = s["weekday"].isin(["Saturday", "Sunday"])
    else:
        s["hour_of_day"] = np.nan
        s["weekday"] = np.nan
        s["is_weekend"] = False

   # --- ensure game_name exists: read games CSV, and fetch any missing IDs ---
    from auth import auth_headers
    import requests, time

    g = g.rename(columns={"id": "game_id", "name": "game_name"})  # expected columns

    # Ensure keys are strings
    if "game_id" in s.columns: s["game_id"] = s["game_id"].astype(str)

    # If games CSV is empty or lacks names, rebuild it from stream IDs
    have = set(g["game_id"].astype(str)) if not g.empty and "game_id" in g.columns else set()
    need = sorted({gid for gid in s["game_id"].dropna().astype(str).unique() if gid and gid not in have})

    def fetch_games_chunk(ids):
        if not ids: 
          return []
        out = []
        for i in range(0, len(ids), 100):
            chunk = ids[i:i+100]
            params = [("id", gid) for gid in chunk]
            r = requests.get("https://api.twitch.tv/helix/games", headers=auth_headers(), params=params, timeout=30)
            r.raise_for_status()
            out.extend(r.json().get("data", []))
            time.sleep(0.2)
        return out

    if need:
        fetched = fetch_games_chunk(need)
        g_new = pd.DataFrame(fetched)
        if not g_new.empty:
            g_new = g_new.rename(columns={"id":"game_id","name":"game_name"})
            g = pd.concat([g if not g.empty else pd.DataFrame(columns=["game_id","game_name"]), g_new], ignore_index=True)
            g = g.drop_duplicates(subset=["game_id"])

    # Final join (guarantee columns exist)
    if "game_id" in g.columns:
        g["game_id"] = g["game_id"].astype(str)
    else:
        g = pd.DataFrame(columns=["game_id","game_name"])

    s = s.merge(g[["game_id","game_name"]], how="left", on="game_id")
    if "game_name" not in s.columns:
        s["game_name"] = pd.Series(dtype="object")
    s["game_name"] = s["game_name"].fillna("Unknown")


    # Keep useful columns
    keep = [
        "id",  # stream id
        "user_id", "user_login", "user_name",
        "game_id", "game_name",
        "type", "title", "viewer_count",
        "language", "broadcaster_language",
        "started_at", "hour_of_day", "weekday", "is_weekend",
        "tags", "is_mature",
    ]
    present = [c for c in keep if c in s.columns]
    out = s[present].copy()

    out_csv = PROC / "streams_processed.csv"
    out.to_csv(out_csv, index=False)
    print(f"✅ Transformed {len(out)} rows → {out_csv}")
    return out

if __name__ == "__main__":
    run_transform()
