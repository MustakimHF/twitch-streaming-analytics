#!/usr/bin/env python3
"""
Twitch OAuth (Client Credentials) – fetch an app access token.
"""
import os
import time
import requests
from dotenv import load_dotenv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TOKEN_URL = "https://id.twitch.tv/oauth2/token"

_token_cache = {"access_token": None, "expires_at": 0}

def get_app_token() -> str:
    """Return a valid app access token; refresh if expired."""
    now = time.time()
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - 30:
        return _token_cache["access_token"]

    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit("❌ Set TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET in .env")

    resp = requests.post(
        TOKEN_URL,
        params={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    resp.raise_for_status()
    js = resp.json()
    _token_cache["access_token"] = js["access_token"]
    _token_cache["expires_at"] = now + int(js.get("expires_in", 3600))
    return _token_cache["access_token"]

def auth_headers() -> dict:
    """Headers required by Helix endpoints."""
    return {
        "Authorization": f"Bearer {get_app_token()}",
        "Client-Id": CLIENT_ID,
    }
