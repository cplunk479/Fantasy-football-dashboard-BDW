"""
ingest_matchups.py
Pulls weekly matchups and transactions for all leagues in the DB
and loads them into `matchups`, `roster_slots`, and `transactions`.
"""

import json
from db_setup import get_connection
from sleeper_api import get_matchups, get_transactions, get_league


def get_all_leagues(conn) -> list:
    return conn.execute("SELECT league_id, season, playoff_weeks, num_teams FROM leagues").fetchall()


def get_playoff_start(playoff_weeks_json: str) -> int:
    """Return the first playoff week number, default 15."""
    try:
        weeks = json.loads(playoff_weeks_json or "[]")
        if isinstance(weeks, list) and weeks:
            return min(int(w) for w in weeks)
    except Exception:
        pass
    return 15


def ingest_matchups_for_league(conn, league_id: str, season: int, playoff_start: int):
    cursor = conn.cursor()
    total_matchups = 0
    total_slots = 0

    weeks = range(1, 19) if season >= 2021 else range(1, 18)

    for week in weeks:
        is_playoff = 1 if week >= playoff_start else 0

        try:
            matchup_data = get_matchups(league_id, week)
        except Exception as e:
            print(f"    W{week} matchups error: {e}")
            continue

        if not matchup_data:
            continue

        for entry in matchup_data:
            roster_id  = entry.get("roster_id")
            matchup_id = entry.get("matchup_id")
            points     = entry.get("points", 0) or 0
            starters   = entry.get("starters", [])
            players    = entry.get("players", [])

            if not roster_id:
                continue

            cursor.execute("""
                INSERT OR REPLACE INTO matchups
                    (league_id, season, week, matchup_id, roster_id, points_for, is_playoff)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (league_id, season, week, matchup_id, roster_id, points, is_playoff))
            total_matchups += 1

            # Roster slots: starters get positional slot labels, bench gets 'BN'
            starter_set = set(starters)
            for i, player_id in enumerate(starters):
                slot = f"STARTER_{i+1}"  # positional labels need roster config; use index for now
                cursor.execute("""
                    INSERT OR REPLACE INTO roster_slots
                        (league_id, roster_id, player_id, season, week, slot)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (league_id, roster_id, player_id, season, week, slot))
                total_slots += 1

            for player_id in players:
                if player_id not in starter_set:
                    cursor.execute("""
                        INSERT OR REPLACE INTO roster_slots
                            (league_id, roster_id, player_id, season, week, slot)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (league_id, roster_id, player_id, season, week, "BN"))
                    total_slots += 1

    conn.commit()
    return total_matchups, total_slots


def ingest_transactions_for_league(conn, league_id: str, season: int):
    cursor = conn.cursor()
    count = 0
    weeks = range(1, 19) if season >= 2021 else range(1, 18)

    for week in weeks:
        try:
            txns = get_transactions(league_id, week)
        except Exception as e:
            print(f"    W{week} transactions error: {e}")
            continue

        for txn in (txns or []):
            player_ids = json.dumps(list((txn.get("adds") or {}).keys()) +
                                    list((txn.get("drops") or {}).keys()))
            roster_ids = json.dumps(txn.get("roster_ids", []))

            cursor.execute("""
                INSERT OR IGNORE INTO transactions
                    (transaction_id, league_id, type, status, created, player_ids, roster_ids)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                txn.get("transaction_id"),
                league_id,
                txn.get("type", ""),
                txn.get("status", ""),
                txn.get("created", 0),
                player_ids,
                roster_ids
            ))
            count += 1

    conn.commit()
    return count


if __name__ == "__main__":
    conn = get_connection()
    leagues = get_all_leagues(conn)

    if not leagues:
        print("No leagues found. Run ingest_leagues.py first.")
        conn.close()
        exit(1)

    print(f"Found {len(leagues)} leagues to process\n")
    total_m = total_s = total_t = 0

    for row in leagues:
        league_id     = row["league_id"]
        season        = row["season"]
        playoff_start = get_playoff_start(row["playoff_weeks"])

        print(f"── {league_id} (season {season}) ──")

        m, s = ingest_matchups_for_league(conn, league_id, season, playoff_start)
        t    = ingest_transactions_for_league(conn, league_id, season)

        print(f"  ✓ {m} matchup rows | {s} roster slots | {t} transactions")
        total_m += m
        total_s += s
        total_t += t

    conn.close()
    print(f"\n✓ Done")
    print(f"  Matchups:      {total_m}")
    print(f"  Roster slots:  {total_s}")
    print(f"  Transactions:  {total_t}")
