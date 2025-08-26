#!/usr/bin/env python3
from pathlib import Path
import os, time, requests, pandas as pd
from dotenv import load_dotenv
from auth import auth_headers

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

RAW_DIR = ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

HELIX = "https://api.twitch.tv/helix"
MAX_PAGES = int(os.getenv("TWITCH_MAX_PAGES", "5"))
LANG_FILTER = os.getenv("TWITCH_LANG_FILTER", "").strip()
PER_PAGE = 100

def fetch_streams(max_pages=MAX_PAGES, per_page=PER_PAGE, langs=None) -> pd.DataFrame:
    url = f"{HELIX}/streams"
    all_rows = []

    def fetch_page(extra_params):
        r = requests.get(url, headers=auth_headers(), params={"first": per_page, **extra_params}, timeout=30)
        r.raise_for_status()
        return r.json()

    lang_list = [s.strip() for s in langs.split(",") if s.strip()] if langs else [None]
    for lang in lang_list:
        after = None
        for _ in range(max_pages):
            extra = {}
            if lang: extra["language"] = lang
            if after: extra["after"] = after
            js = fetch_page(extra)
            data = js.get("data", [])
            all_rows.extend(data)
            after = js.get("pagination", {}).get("cursor")
            if not after or not data:
                break
            time.sleep(0.25)
    return pd.DataFrame(all_rows)

def fetch_games(game_ids: list[str]) -> pd.DataFrame:
    if not game_ids:
        return pd.DataFrame(columns=["id", "name", "box_art_url"])
    out = []
    for i in range(0, len(game_ids), 100):
        chunk = game_ids[i:i+100]
        params = [("id", gid) for gid in chunk]
        r = requests.get(f"{HELIX}/games", headers=auth_headers(), params=params, timeout=30)
        r.raise_for_status()
        out.extend(r.json().get("data", []))
        time.sleep(0.2)
    return pd.DataFrame(out)

def ensure_games_cover_streams(streams_df: pd.DataFrame, games_df: pd.DataFrame) -> pd.DataFrame:
    # make sure every stream game_id has a row in games
    have = set(map(str, games_df.get("id", [])))
    need = sorted({str(g) for g in streams_df.get("game_id", []) if pd.notna(g) and str(g) and str(g) not in have})
    if not need:
        return games_df
    more = fetch_games(need)
    return pd.concat([games_df, more], ignore_index=True).drop_duplicates(subset=["id"])

def run_extract():
    df = fetch_streams(langs=LANG_FILTER if LANG_FILTER else None)
    if df.empty:
        print("⚠️ No streams returned. Increase TWITCH_MAX_PAGES or remove TWITCH_LANG_FILTER.")
    # normalise types
    if "game_id" in df.columns:
        df["game_id"] = df["game_id"].astype(str)

    # initial games fetch from unique ids
    gids = sorted({str(g) for g in df.get("game_id", []) if pd.notna(g) and str(g)})
    games = fetch_games(gids)
    games = ensure_games_cover_streams(df, games)  # <- strong fallback

    streams_path = RAW_DIR / "twitch_streams.csv"
    games_path   = RAW_DIR / "twitch_games.csv"

    df.to_csv(streams_path, index=False, encoding="utf-8")
    games.to_csv(games_path,   index=False, encoding="utf-8")

    print(f"✅ Saved {len(df)} streams → {streams_path}")
    print(f"✅ Saved {len(games)} games → {games_path}")

    return df, games

if __name__ == "__main__":
    run_extract()
