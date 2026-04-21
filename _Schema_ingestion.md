# Ingestion Scripts

## Overview
Five Python scripts that pull data from the Sleeper API and load it into `fantasy.db`.

---

## Scripts & Run Order

| Order | Script | What It Does |
|---|---|---|
| 1 | `db_setup.py` | Creates all tables and indexes |
| 2 | `ingest_players.py` | Loads NFL player registry (cached) |
| 3 | `ingest_leagues.py` | Loads leagues + team rosters for cplunk479 |
| 4 | `ingest_stats.py` | Loads weekly stats per player per season |
| 5 | `ingest_matchups.py` | Loads matchups, roster slots, transactions |

**Run everything at once:**
```bash
python run_all.py
```

**Quick refresh (skip stats):**
```bash
python run_all.py --quick
```

---

## Setup

```bash
# Install dependencies
pip install requests

# Run from the scripts/ folder
cd scripts/
python run_all.py
```

No API key required. Sleeper's API is fully public.

---

## Individual Script Usage

```bash
# Single season stats only
python ingest_stats.py --season 2024

# Single week
python ingest_stats.py --season 2024 --week 14

# Refresh player registry (force re-pull, ignores cache)
# Delete players_cache.json, then:
python ingest_players.py
```

---

## Sleeper API Endpoints Used

| Endpoint | Used By |
|---|---|
| `GET /user/{username}` | ingest_leagues |
| `GET /user/{user_id}/leagues/nfl/{season}` | ingest_leagues |
| `GET /league/{league_id}` | ingest_leagues |
| `GET /league/{league_id}/rosters` | ingest_leagues |
| `GET /league/{league_id}/users` | ingest_leagues |
| `GET /league/{league_id}/matchups/{week}` | ingest_matchups |
| `GET /league/{league_id}/transactions/{week}` | ingest_matchups |
| `GET /players/nfl` | ingest_players |
| `GET /stats/nfl/{type}/{season}/{week}` | ingest_stats |

---

## Caching

- `players_cache.json` — cached copy of the full player registry (~3MB)
  - Delete this file to force a fresh pull
  - Re-pull when rosters change significantly (preseason, trade deadline)

---

## Re-run Safety

All inserts use `INSERT OR REPLACE` — safe to re-run without duplicates.

---

## Seasons Tracked

2019 → 2025 (edit `SEASONS` in `run_all.py` to adjust)

---

## Known Limitations

- Sleeper doesn't expose exact positional slot labels (QB1, RB1, FLEX) in matchup data — `roster_slots.slot` uses `STARTER_N` for starters, `BN` for bench. This is sufficient for "started vs benched" analysis.
- Stats API returns `None` for players who didn't play — these are coerced to `0`.
- Playoff week detection uses league settings; defaults to week 15 if not set.
