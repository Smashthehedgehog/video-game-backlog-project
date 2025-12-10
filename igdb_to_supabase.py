# igdb_to_supabase.py
import os
from dotenv import load_dotenv
import time
import math
import requests
import sqlite3
from datetime import datetime, timezone
from typing import List, Dict

load_dotenv()

# ---------- CONFIG ----------
CLIENT_ID = os.getenv("IGDB_CLIENT_ID")
CLIENT_SECRET = os.getenv("IGDB_CLIENT_SECRET")
SUPABASE_URL = os.getenv("SUPABASE_URL")           # https://yourproject.supabase.co
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE")  # use service role key, store securely
SQLITE_DB = os.getenv("IGDB_SQLITE", "igdb_staging.db")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "800"))   # Supabase insert batch size
FETCH_LIMIT = 500  # IGDB max per request
RATE_SLEEP = 0.25  # seconds between requests, keep <=4 reqs/sec
# ----------------------------

if not (CLIENT_ID and CLIENT_SECRET and SUPABASE_URL and SUPABASE_KEY):
    raise SystemExit("Set IGDB_CLIENT_ID, IGDB_CLIENT_SECRET, SUPABASE_URL, SUPABASE_SERVICE_ROLE env vars.")

# ---------- Helpers ----------

def get_access_token():
    """
    Authenticates with the IGDB API to obtain an access token for subsequent API requests.

    Input:
        None (uses global CLIENT_ID and CLIENT_SECRET from environment variables)

    Output:
        str: The OAuth2 access token for authenticating subsequent IGDB API calls
    """

    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    r = requests.post(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def igdb_post(endpoint: str, query: str, token: str):
    """
    Makes a POST request to the IGDB API for a specified endpoint.

    Input:
        endpoint (str): The IGDB API endpoint to call (e.g., "games", "platforms")
        query (str): The Apicalypse query string to send
        token (str): The OAuth2 access token for authentication

    Output:
        list/dict: The JSON response from the IGDB API, or raises an Exception on error
    """
    
    url = f"https://api.igdb.com/v4/{endpoint}"
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {token}"
    }
    r = requests.post(url, headers=headers, data=query, timeout=60)
    if r.status_code == 200:
        return r.json()
    else:
        raise Exception(f"IGDB error {r.status_code}: {r.text}")