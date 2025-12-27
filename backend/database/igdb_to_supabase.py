# igdb_to_supabase.py
import os
from dotenv import load_dotenv
import time
import math
import requests
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import List, Dict

load_dotenv(dotenv_path="../.env")

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

# ---------- SQLite staging ----------
def init_sqlite(conn):
    """
    Initializes the SQLite database with all required tables for staging IGDB data.

    Input:
        conn (sqlite3.Connection): An active SQLite database connection

    Output:
        None (creates tables in-place and commits the transaction)
    """
    cur = conn.cursor()
    cur.execute("""
    create table if not exists sync_state (
      key text primary key,
      value text
    );
    """)
    # basic tables for staging (mirrors supabase)
    cur.executescript("""
    create table if not exists igdb_games (
      id integer primary key,
      name text,
      summary text,
      first_release_date integer,
      rating real,
      total_rating_count integer,
      updated_at integer,
      cover_id integer
    );
    create table if not exists igdb_genres (id integer primary key, name text);
    create table if not exists igdb_platforms (id integer primary key, name text);
    create table if not exists igdb_companies (id integer primary key, name text);
    create table if not exists igdb_covers (id integer primary key, url text, width integer, height integer);
    create table if not exists game_genres (game_id integer, genre_id integer, primary key (game_id, genre_id));
    create table if not exists game_platforms (game_id integer, platform_id integer, primary key (game_id, platform_id));
    create table if not exists game_companies (game_id integer, company_id integer, primary key (game_id, company_id));
    """)
    conn.commit()

def get_state(conn, key, default=None):
    """
    Retrieves a value from the sync_state table used for tracking sync progress.

    Input:
        conn (sqlite3.Connection): An active SQLite database connection
        key (str): The state key to look up (e.g., "last_sync_timestamp")
        default: The value to return if the key is not found (default: None)

    Output:
        str or default: The stored value for the given key, or the default if not found
    """
    cur = conn.cursor()
    cur.execute("select value from sync_state where key = ?", (key,))
    row = cur.fetchone()
    return row[0] if row else default

def set_state(conn, key, value):
    """
    Stores or updates a value in the sync_state table for tracking sync progress.

    Input:
        conn (sqlite3.Connection): An active SQLite database connection
        key (str): The state key to store (e.g., "last_sync_timestamp")
        value: The value to store (will be converted to string)

    Output:
        None (inserts or updates the record and commits the transaction)
    """
    cur = conn.cursor()
    cur.execute("insert into sync_state(key, value) values(?, ?) on conflict(key) do update set value=excluded.value", (key, str(value)))
    conn.commit()

# ---------- Fetching logic ----------
def fetch_games(token, since_updated_at=None):
    """
    Fetches all games from IGDB, optionally filtered by last update timestamp.

    Input:
        token (str): The OAuth2 access token for IGDB API authentication
        since_updated_at (int, optional): Unix timestamp to filter games updated after this time

    Output:
        list[dict]: A list of game dictionaries containing core fields and related entity IDs
    """
    all_games = []
    offset = 0
    while True:
        # request core fields plus references to related entities
        where_clause = ""
        if since_updated_at:
            # IGDB updated_at is seconds since epoch
            where_clause = f"where updated_at >= {int(since_updated_at)};"
        query = f"""
            fields id, name, summary, first_release_date, rating, total_rating_count, genres, platforms, involved_companies, cover, updated_at;
            {where_clause}
            limit {FETCH_LIMIT};
            offset {offset};
        """
        batch = igdb_post("games", query, token)
        if not batch:
            break
        all_games.extend(batch)
        offset += FETCH_LIMIT
        print(f"Fetched {len(all_games)} games (offset {offset}).")
        time.sleep(RATE_SLEEP)
    return all_games

def fetch_lookup(token, endpoint, ids: List[int]):
    """
    Fetches records by ID from a specified IGDB endpoint in chunked batches.

    Input:
        token (str): The OAuth2 access token for IGDB API authentication
        endpoint (str): The IGDB API endpoint to query (e.g., "genres", "platforms", "covers")
        ids (List[int]): A list of entity IDs to fetch

    Output:
        list[dict]: A list of entity dictionaries with all available fields
    """
    if not ids:
        return []
    ids = list(set(ids))
    # IGDB supports "where id = (1,2,3);"
    CHUNK = 500
    results = []
    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i+CHUNK]
        q = f"fields *; where id = ({','.join(map(str, chunk))}); limit {len(chunk)};"
        batch = igdb_post(endpoint, q, token)
        results.extend(batch)
        time.sleep(RATE_SLEEP)
    return results

# ---------- Insert into SQLite staging ----------
def save_to_sqlite(conn, games, genres, platforms, companies, covers):
    """
    Upserts fetched IGDB data into the SQLite staging database.

    Input:
        conn (sqlite3.Connection): An active SQLite database connection
        games (list[dict]): List of game dictionaries from IGDB
        genres (list[dict]): List of genre dictionaries from IGDB
        platforms (list[dict]): List of platform dictionaries from IGDB
        companies (list[dict]): List of involved_companies dictionaries from IGDB
        covers (list[dict]): List of cover image dictionaries from IGDB

    Output:
        None (inserts/updates records in staging tables and commits the transaction)
    """
    cur = conn.cursor()
    # upsert helpers
    for g in genres:
        cur.execute("insert into igdb_genres(id, name) values (?,?) on conflict(id) do update set name=excluded.name", (g.get("id"), g.get("name")))
    for p in platforms:
        cur.execute("insert into igdb_platforms(id, name) values (?,?) on conflict(id) do update set name=excluded.name", (p.get("id"), p.get("name")))
    for c in companies:
        cur.execute("insert into igdb_companies(id, name) values (?,?) on conflict(id) do update set name=excluded.name", (c.get("id"), c.get("company", {}).get("name") if isinstance(c.get("company"), dict) else c.get("name")))
    for cov in covers:
        cur.execute("insert into igdb_covers(id, url, width, height) values (?,?,?,?) on conflict(id) do update set url=excluded.url, width=excluded.width, height=excluded.height", (cov.get("id"), cov.get("url"), cov.get("width"), cov.get("height")))
    # games and relationships
    for g in games:
        cur.execute("""
            insert into igdb_games(id, name, summary, first_release_date, rating, total_rating_count, updated_at, cover_id)
            values (?,?,?,?,?,?,?,?)
            on conflict(id) do update set name=excluded.name, summary=excluded.summary, first_release_date=excluded.first_release_date, rating=excluded.rating, total_rating_count=excluded.total_rating_count, updated_at=excluded.updated_at, cover_id=excluded.cover_id
        """, (
            g.get("id"),
            g.get("name"),
            g.get("summary"),
            g.get("first_release_date"),
            g.get("rating"),
            g.get("total_rating_count"),
            g.get("updated_at"),
            g.get("cover")
        ))
        # relationships
        gid = g.get("id")
        for genre_id in g.get("genres", []) or []:
            cur.execute("insert into game_genres(game_id, genre_id) values (?,?) on conflict(game_id, genre_id) do nothing", (gid, genre_id))
        for plat_id in g.get("platforms", []) or []:
            cur.execute("insert into game_platforms(game_id, platform_id) values (?,?) on conflict(game_id, platform_id) do nothing", (gid, plat_id))
        for ic in g.get("involved_companies", []) or []:
            # involved_companies entries are ids; we map to company ids by fetching involved_companies later.
            cur.execute("insert into game_companies(game_id, company_id) values (?,?) on conflict(game_id, company_id) do nothing", (gid, ic))
    conn.commit()

# ---------- Supabase upsert via PostgREST ----------
def supabase_upsert(table: str, records: List[Dict]):
    if not records:
        return
    # Supabase PostgREST endpoint
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"  # upsert
    }
    # chunked upload
    for i in range(0, len(records), BATCH_SIZE):
        chunk = records[i:i+BATCH_SIZE]
        r = requests.post(url, headers=headers, json=chunk, timeout=60)
        if not (200 <= r.status_code < 300):
            raise Exception(f"Supabase upsert error {r.status_code}: {r.text}")
        print(f"Upserted {min(i+BATCH_SIZE, len(records))} / {len(records)} into {table}")
        time.sleep(0.1)

def export_from_sqlite_and_sync(conn):
    cur = conn.cursor()
    # export genres
    cur.execute("select id, name from igdb_genres")
    genres = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
    supabase_upsert("igdb_genres", genres)

    cur.execute("select id, name from igdb_platforms")
    platforms = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
    supabase_upsert("igdb_platforms", platforms)

    cur.execute("select id, name from igdb_companies")
    companies = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
    supabase_upsert("igdb_companies", companies)

    cur.execute("select id, url, width, height from igdb_covers")
    covers = [{"id": r[0], "url": r[1], "width": r[2], "height": r[3]} for r in cur.fetchall()]
    supabase_upsert("igdb_covers", covers)

    # games
    cur.execute("select id, name, summary, first_release_date, rating, total_rating_count, updated_at, cover_id from igdb_games")
    games = []
    for r in cur.fetchall():
        # convert first_release_date and updated_at from epoch to ISO strings if present
        fdate = (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=r[3])).isoformat() if r[3] else None
        updated = (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=r[6])).isoformat() if r[6] else None
        games.append({
            "id": r[0],
            "name": r[1],
            "summary": r[2],
            "first_release_date": fdate,
            "rating": r[4],
            "total_rating_count": r[5],
            "updated_at": updated,
            "cover_id": r[7]
        })
    supabase_upsert("igdb_games", games)

    # relationships: game_genres, game_platforms, game_companies, game_screenshots
    cur.execute("select game_id, genre_id from game_genres")
    game_genres = [{"game_id": r[0], "genre_id": r[1]} for r in cur.fetchall()]
    supabase_upsert("game_genres", game_genres)

    cur.execute("select game_id, platform_id from game_platforms")
    game_platforms = [{"game_id": r[0], "platform_id": r[1]} for r in cur.fetchall()]
    supabase_upsert("game_platforms", game_platforms)

    cur.execute("select game_id, company_id from game_companies")
    game_companies = [{"game_id": r[0], "company_id": r[1]} for r in cur.fetchall()]
    supabase_upsert("game_companies", game_companies)

# def test_function(conn):
#     cur = conn.cursor()
#     cur.execute("select id, name, summary, first_release_date, rating, total_rating_count, updated_at from igdb_games")
#     games = []
#     for r in cur.fetchall():
#         # convert first_release_date and updated_at from epoch to ISO strings if present
#         fdate = (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=r[3])).isoformat() if r[3] else None
#         updated = (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=r[6])).isoformat() if r[6] else None
#         games.append({
#             "id": r[0],
#             "name": r[1],
#             "summary": r[2],
#             "first_release_date": fdate,
#             "rating": r[4],
#             "total_rating_count": r[5],
#             "updated_at": updated
#         })

def main():
    token = get_access_token()
    conn = sqlite3.connect(SQLITE_DB)
    init_sqlite(conn)

    # get last sync timestamp from sqlite state (seconds epoch)
    last_sync = get_state(conn, "last_sync_epoch")
    since = int(last_sync) if last_sync else None

    print("Starting fetch. since:", since)
    games = fetch_games(token, since_updated_at=since)


    if not games:
        print("No updates found. Exiting.")
        conn.close()
        return

    # collect referenced ids
    genre_ids = []
    platform_ids = []
    involved_company_ids = []
    cover_ids = []

    for g in games:
        genre_ids += (g.get("genres") or [])
        platform_ids += (g.get("platforms") or [])
        involved_company_ids += (g.get("involved_companies") or [])
        if g.get("cover"):
            cover_ids.append(g.get("cover"))

    # fetch related objects
    genres = fetch_lookup(token, "genres", genre_ids)
    platforms = fetch_lookup(token, "platforms", platform_ids)
    # involved_companies endpoint returns objects with .company which can be mapped to companies
    involved_companies = fetch_lookup(token, "involved_companies", involved_company_ids)
    # map involved_companies -> company ids and then fetch companies
    company_ids = []
    for ic in involved_companies:
        if "company" in ic and isinstance(ic["company"], int):
            company_ids.append(ic["company"])
    companies = fetch_lookup(token, "companies", company_ids)
    covers = fetch_lookup(token, "covers", cover_ids)

    # persist to sqlite staging
    save_to_sqlite(conn, games, genres, platforms, companies, covers)

    # update last sync to max updated_at in fetched games
    max_updated = max([g.get("updated_at") or 0 for g in games])
    set_state(conn, "last_sync_epoch", str(int(max_updated)))

    # export to supabase
    export_from_sqlite_and_sync(conn)

    conn.close()
    print("Sync complete.")

if __name__ == "__main__":
    time_start = time.time()
    main()
    time_end = time.time()
    print(f"Time taken: {time_end - time_start} seconds")