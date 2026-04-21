"""
ingest_stats.py
Pulls weekly player stats from Sleeper for each season/week combo
and loads them into `player_stats`.

Usage:
    python ingest_stats.py                    # all seasons, regular season
    python ingest_stats.py --season 2024      # single season
    python ingest_stats.py --season 2024 --week 1  # single week
"""

import argparse
from db_setup import get_connection
from sleeper_api import get_stats

SEASONS = list(range(2019, 2026))
REGULAR_SEASON_WEEKS = range(1, 19)   # weeks 1-18 (2021+ is 18 weeks; pre-2021 is 17)
PLAYOFF_WEEKS = range(15, 19)          # weeks 15-18


def weeks_for_season(season: int) -> range:
    # NFL expanded to 18 weeks in 2021
    return range(1, 19) if season >= 2021 else range(1, 18)


def ingest_week(conn, season: int, week: int, season_type: str = "regular"):
    cursor = conn.cursor()

    try:
        stats = get_stats(season, week, season_type)
    except Exception as e:
        print(f"    Skipping {season} W{week} ({season_type}): {e}")
        return 0

    count = 0
    for player_id, s in stats.items():
        if not isinstance(s, dict):
            continue

        # Only insert if player exists in our players table
        exists = cursor.execute(
            "SELECT 1 FROM players WHERE player_id = ?", (player_id,)
        ).fetchone()
        if not exists:
            continue

        pts_ppr      = s.get("pts_ppr", 0) or 0
        pts_std      = s.get("pts_std", 0) or 0
        pts_half_ppr = s.get("pts_half_ppr", 0) or 0

        cursor.execute("""
            INSERT OR REPLACE INTO player_stats
                (player_id, season, week,
                 pts_ppr, pts_std, pts_half_ppr,
                 pass_yd, pass_td,
                 rush_yd, rush_td,
                 rec, rec_yd, rec_td, targets)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            player_id, season, week,
            pts_ppr, pts_std, pts_half_ppr,
            s.get("pass_yd", 0) or 0,
            s.get("pass_td", 0) or 0,
            s.get("rush_yd", 0) or 0,
            s.get("rush_td", 0) or 0,
            s.get("rec", 0) or 0,
            s.get("rec_yd", 0) or 0,
            s.get("rec_td", 0) or 0,
            s.get("rec_tgt", 0) or 0,  # Sleeper uses rec_tgt for targets
        ))
        count += 1

    conn.commit()
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=None)
    parser.add_argument("--week",   type=int, default=None)
    args = parser.parse_args()

    conn = get_connection()
    seasons = [args.season] if args.season else SEASONS
    total = 0

    for season in seasons:
        weeks = [args.week] if args.week else list(weeks_for_season(season))
        print(f"\n── Season {season} ──")

        for week in weeks:
            n = ingest_week(conn, season, week, "regular")
            print(f"  Week {week:>2}: {n} players loaded")
            total += n

        # Also pull playoff weeks
        if not args.week:
            for week in PLAYOFF_WEEKS:
                n = ingest_week(conn, season, week, "post")
                if n > 0:
                    print(f"  Playoff W{week}: {n} players loaded")
                    total += n

    conn.close()
    print(f"\n✓ Done — {total} stat rows loaded")
