# Database Schema

## Overview
SQLite database (`fantasy.db`) with three layers: Fantasy, NFL Player, and NFL Team. A bridge table (`roster_slots`) connects all three for cross-league analytics.

---

## Fantasy Layer

### `leagues`
Stores one row per league-season.

| Column | Type | Notes |
|---|---|---|
| league_id | TEXT PK | Sleeper league ID |
| name | TEXT | League display name |
| season | INTEGER | e.g. 2023 |
| scoring_format | TEXT | 'ppr', 'half_ppr', 'standard' |
| num_teams | INTEGER | |
| playoff_weeks | TEXT | JSON array of week numbers |

### `teams`
One row per roster within a league.

| Column | Type | Notes |
|---|---|---|
| roster_id | INTEGER PK | Sleeper roster ID |
| league_id | TEXT FK | → leagues |
| owner_id | TEXT | Sleeper user_id |
| owner_name | TEXT | Display name |
| team_name | TEXT | Custom team name |

### `matchups`
One row per team per week. Two rows share a `matchup_id` = one game.

| Column | Type | Notes |
|---|---|---|
| league_id | TEXT FK | → leagues |
| season | INTEGER | |
| week | INTEGER | |
| matchup_id | INTEGER | Same ID = same game |
| roster_id | INTEGER | |
| points_for | REAL | |
| is_playoff | BOOLEAN | |

### `transactions`
Trades, waiver claims, and free agent adds/drops.

| Column | Type | Notes |
|---|---|---|
| transaction_id | TEXT PK | |
| league_id | TEXT FK | → leagues |
| type | TEXT | 'trade', 'waiver', 'free_agent' |
| status | TEXT | 'complete', 'failed', etc. |
| created | INTEGER | Epoch timestamp |
| player_ids | TEXT | JSON array |
| roster_ids | TEXT | JSON array |

---

## NFL Player Layer

### `players`
Master registry of all NFL players from Sleeper.

| Column | Type | Notes |
|---|---|---|
| player_id | TEXT PK | Sleeper player ID |
| full_name | TEXT | |
| position | TEXT | QB, RB, WR, TE, K, DEF |
| team | TEXT | Current NFL team abbr |
| status | TEXT | Active, Injured, etc. |
| age | INTEGER | |
| years_exp | INTEGER | |
| college | TEXT | |

### `player_stats`
Weekly stats per player per season.

| Column | Type | Notes |
|---|---|---|
| player_id | TEXT FK | → players |
| season | INTEGER | |
| week | INTEGER | |
| pts_ppr | REAL | |
| pts_std | REAL | |
| pts_half_ppr | REAL | |
| pass_yd | REAL | |
| pass_td | REAL | |
| rush_yd | REAL | |
| rush_td | REAL | |
| rec | REAL | Receptions |
| rec_yd | REAL | |
| rec_td | REAL | |
| targets | REAL | |

---

## NFL Team Layer

### `nfl_teams`

| Column | Type | Notes |
|---|---|---|
| team_abbr | TEXT PK | e.g. 'KC', 'PHI' |
| full_name | TEXT | |
| conference | TEXT | AFC / NFC |
| division | TEXT | e.g. 'AFC West' |

### `nfl_games`

| Column | Type | Notes |
|---|---|---|
| game_id | TEXT PK | |
| season | INTEGER | |
| week | INTEGER | |
| home_team | TEXT FK | → nfl_teams |
| away_team | TEXT FK | → nfl_teams |
| home_score | INTEGER | |
| away_score | INTEGER | |
| game_date | DATE | |

---

## Bridge Table

### `roster_slots`
Links a fantasy roster spot to an NFL player in a given week. Enables cross-league player tracking.

| Column | Type | Notes |
|---|---|---|
| league_id | TEXT FK | → leagues |
| roster_id | INTEGER | |
| player_id | TEXT FK | → players |
| season | INTEGER | |
| week | INTEGER | |
| slot | TEXT | 'QB', 'RB1', 'FLEX', 'BN', etc. |

---

## Design Decisions

- **SQLite first** — simple, zero-config, file-based. Migrate to PostgreSQL if multi-user or large scale is needed.
- `matchup_id` pairs rows for head-to-head records — query `WHERE matchup_id = X` to get both teams in a game.
- `roster_slots.slot` captures starting vs bench, enabling "points left on bench" analysis.
- `player_stats` stores raw stat columns alongside fantasy points for all three scoring formats.

---

## Key Queries

```sql
-- Head-to-head record between two owners in a league
SELECT 
    winner.owner_name,
    COUNT(*) as wins
FROM matchups a
JOIN matchups b ON a.matchup_id = b.matchup_id AND a.roster_id != b.roster_id
JOIN teams winner ON a.roster_id = winner.roster_id AND a.league_id = winner.league_id
WHERE a.league_id = '?'
  AND a.points_for > b.points_for
GROUP BY winner.owner_name;

-- Top waiver adds by points scored after acquisition
SELECT p.full_name, SUM(ps.pts_ppr) as pts_after_add
FROM transactions t
JOIN players p ON p.player_id IN (SELECT value FROM json_each(t.player_ids))
JOIN player_stats ps ON ps.player_id = p.player_id
WHERE t.type = 'waiver'
GROUP BY p.full_name
ORDER BY pts_after_add DESC;
```
