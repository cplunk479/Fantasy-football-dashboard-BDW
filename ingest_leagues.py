"""
ingest_leagues.py
Fetches all leagues for cplunk479 across seasons and loads:
  - leagues
  - teams (rosters + owners)
"""

import json
from db_setup import get_connection
from sleeper_api import get_user, get_user_leagues, get_league, get_league_rosters, get_league_users

USERNAME = "cplunk479"
SEASONS = list(range(2019, 2026))  # adjust start year if needed


def infer_scoring_format(league: dict) -> str:
    settings = league.get("scoring_settings", {})
    rec = settings.get("rec", 0)
    if rec == 1.0:
        return "ppr"
    elif rec == 0.5:
        return "half_ppr"
    return "standard"


def ingest_leagues(conn, user_id: str):
    cursor = conn.cursor()
    all_league_ids = set()

    for season in SEASONS:
        print(f"  Fetching leagues for season {season}...")
        try:
            leagues = get_user_leagues(user_id, season)
        except Exception as e:
            print(f"    Skipping {season}: {e}")
            continue

        for lg in leagues:
            league_id = lg["league_id"]
            if league_id in all_league_ids:
                continue
            all_league_ids.add(league_id)

            # Get full league detail
            try:
                detail = get_league(league_id)
            except Exception as e:
                print(f"    Could not fetch detail for {league_id}: {e}")
                continue

            scoring = infer_scoring_format(detail)
            playoff_weeks = json.dumps(detail.get("settings", {}).get("playoff_week_start", []))

            cursor.execute("""
                INSERT OR REPLACE INTO leagues
                    (league_id, name, season, scoring_format, num_teams, playoff_weeks)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                league_id,
                detail.get("name", "Unknown"),
                detail.get("season", season),
                scoring,
                detail.get("total_rosters", 0),
                playoff_weeks
            ))
            print(f"    ✓ League: {detail.get('name')} ({season})")

    conn.commit()
    print(f"\n✓ Ingested {len(all_league_ids)} leagues")
    return all_league_ids


def ingest_teams(conn, league_ids: set):
    cursor = conn.cursor()
    count = 0

    for league_id in league_ids:
        try:
            rosters = get_league_rosters(league_id)
            users = get_league_users(league_id)
        except Exception as e:
            print(f"  Skipping teams for {league_id}: {e}")
            continue

        # Build owner lookup: user_id → display_name + team_name
        owner_map = {}
        for user in users:
            owner_map[user["user_id"]] = {
                "display_name": user.get("display_name", "Unknown"),
                "team_name": user.get("metadata", {}).get("team_name", "")
            }

        for roster in rosters:
            owner_id = roster.get("owner_id", "")
            owner_info = owner_map.get(owner_id, {})

            cursor.execute("""
                INSERT OR REPLACE INTO teams
                    (roster_id, league_id, owner_id, owner_name, team_name)
                VALUES (?, ?, ?, ?, ?)
            """, (
                roster["roster_id"],
                league_id,
                owner_id,
                owner_info.get("display_name", "Unknown"),
                owner_info.get("team_name", "")
            ))
            count += 1

    conn.commit()
    print(f"✓ Ingested {count} team roster entries")


if __name__ == "__main__":
    print("── Fetching Sleeper user ──")
    user = get_user(USERNAME)
    user_id = user["user_id"]
    print(f"  Found user: {user.get('display_name')} (ID: {user_id})")

    conn = get_connection()

    print("\n── Ingesting leagues ──")
    league_ids = ingest_leagues(conn, user_id)

    print("\n── Ingesting teams ──")
    ingest_teams(conn, league_ids)

    conn.close()
    print("\n✓ Done — leagues and teams loaded")
