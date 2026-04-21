"""
ingest_players.py
Pulls the full NFL player registry from Sleeper and loads it into `players`.
This endpoint returns ~3MB of JSON — run once, then only re-run to refresh rosters.
"""

import json
import os
from db_setup import get_connection
from sleeper_api import get_all_players

CACHE_PATH = os.path.join(os.path.dirname(__file__), "players_cache.json")

RELEVANT_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF"}


def fetch_players(use_cache: bool = True) -> dict:
    if use_cache and os.path.exists(CACHE_PATH):
        print("  Using cached players file...")
        with open(CACHE_PATH, "r") as f:
            return json.load(f)

    print("  Fetching from Sleeper API (this may take a moment)...")
    players = get_all_players()

    with open(CACHE_PATH, "w") as f:
        json.dump(players, f)
    print(f"  Cached {len(players)} players to {CACHE_PATH}")

    return players


def ingest_players(conn, players: dict):
    cursor = conn.cursor()
    count = 0
    skipped = 0

    for player_id, p in players.items():
        position = p.get("position", "")

        # Only load fantasy-relevant positions
        if position not in RELEVANT_POSITIONS:
            skipped += 1
            continue

        cursor.execute("""
            INSERT OR REPLACE INTO players
                (player_id, full_name, position, team, status, age, years_exp, college)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            player_id,
            p.get("full_name") or f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
            position,
            p.get("team", ""),
            p.get("status", ""),
            p.get("age"),
            p.get("years_exp"),
            p.get("college", "")
        ))
        count += 1

    conn.commit()
    print(f"✓ Loaded {count} players (skipped {skipped} non-fantasy positions)")


if __name__ == "__main__":
    print("── Ingesting NFL players ──")
    players = fetch_players(use_cache=True)

    conn = get_connection()
    ingest_players(conn, players)
    conn.close()

    print("✓ Done — player registry loaded")
    print("\nTip: Delete players_cache.json to force a fresh pull from the API.")
