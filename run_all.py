"""
run_all.py
Runs the full ingestion pipeline in the correct order.
Safe to re-run — all inserts use INSERT OR REPLACE.

Order:
  1. db_setup       — create tables if not exist
  2. ingest_players — NFL player registry (uses cache if available)
  3. ingest_leagues — leagues + teams for cplunk479
  4. ingest_stats   — weekly stats for all seasons
  5. ingest_matchups — matchups, roster slots, transactions

Usage:
    python run_all.py             # full refresh
    python run_all.py --quick     # skip stats (fast re-run for league/matchup updates)
"""

import argparse
import time

from db_setup import get_connection, create_tables
from ingest_players import fetch_players, ingest_players
from ingest_leagues import ingest_leagues, ingest_teams
from ingest_stats import ingest_week, SEASONS, weeks_for_season, PLAYOFF_WEEKS
from ingest_matchups import get_all_leagues, ingest_matchups_for_league, ingest_transactions_for_league, get_playoff_start
from sleeper_api import get_user

USERNAME = "cplunk479"


def run(quick: bool = False):
    start = time.time()
    conn = get_connection()

    # ── Step 1: Schema ─────────────────────────────────────────────────────
    print("\n═══ Step 1/5: Database Setup ═══")
    create_tables(conn)

    # ── Step 2: Players ────────────────────────────────────────────────────
    print("\n═══ Step 2/5: NFL Players ═══")
    players = fetch_players(use_cache=True)
    ingest_players(conn, players)

    # ── Step 3: Leagues ────────────────────────────────────────────────────
    print("\n═══ Step 3/5: Leagues & Teams ═══")
    user = get_user(USERNAME)
    user_id = user["user_id"]
    print(f"  User: {user.get('display_name')} ({user_id})")
    league_ids = ingest_leagues(conn, user_id)
    ingest_teams(conn, league_ids)

    # ── Step 4: Stats ──────────────────────────────────────────────────────
    if quick:
        print("\n═══ Step 4/5: Stats — SKIPPED (--quick mode) ═══")
    else:
        print("\n═══ Step 4/5: Weekly Stats ═══")
        for season in SEASONS:
            print(f"\n  Season {season}:")
            for week in weeks_for_season(season):
                n = ingest_week(conn, season, week, "regular")
                print(f"    W{week:>2}: {n} rows")
            for week in PLAYOFF_WEEKS:
                n = ingest_week(conn, season, week, "post")
                if n > 0:
                    print(f"    Playoff W{week}: {n} rows")

    # ── Step 5: Matchups ───────────────────────────────────────────────────
    print("\n═══ Step 5/5: Matchups & Transactions ═══")
    leagues = get_all_leagues(conn)
    for row in leagues:
        league_id     = row["league_id"]
        season        = row["season"]
        playoff_start = get_playoff_start(row["playoff_weeks"])
        m, s = ingest_matchups_for_league(conn, league_id, season, playoff_start)
        t    = ingest_transactions_for_league(conn, league_id, season)
        print(f"  League {league_id} ({season}): {m} matchups | {s} slots | {t} txns")

    conn.close()
    elapsed = round(time.time() - start, 1)
    print(f"\n✓ Full pipeline complete in {elapsed}s")
    print("  Database: fantasy.db")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true",
                        help="Skip stats ingestion for faster re-runs")
    args = parser.parse_args()
    run(quick=args.quick)
