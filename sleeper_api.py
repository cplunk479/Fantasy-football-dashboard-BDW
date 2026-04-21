"""
sleeper_api.py
Thin wrapper around the Sleeper REST API.
All functions return parsed JSON or raise on non-200 responses.
"""

import time
import requests

BASE = "https://api.sleeper.app/v1"
RATE_DELAY = 0.5  # seconds between requests — be polite to the API


def _get(path: str) -> dict | list:
    url = f"{BASE}{path}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    time.sleep(RATE_DELAY)
    return resp.json()


# ── Users ──────────────────────────────────────────────────────────────────

def get_user(username: str) -> dict:
    return _get(f"/user/{username}")


def get_user_leagues(user_id: str, season: int) -> list:
    return _get(f"/user/{user_id}/leagues/nfl/{season}")


# ── League ─────────────────────────────────────────────────────────────────

def get_league(league_id: str) -> dict:
    return _get(f"/league/{league_id}")


def get_league_rosters(league_id: str) -> list:
    return _get(f"/league/{league_id}/rosters")


def get_league_users(league_id: str) -> list:
    return _get(f"/league/{league_id}/users")


def get_matchups(league_id: str, week: int) -> list:
    return _get(f"/league/{league_id}/matchups/{week}")


def get_transactions(league_id: str, week: int) -> list:
    return _get(f"/league/{league_id}/transactions/{week}")


# ── Players & Stats ────────────────────────────────────────────────────────

def get_all_players() -> dict:
    """Returns full NFL player registry (~3MB). Cache this — don't call every run."""
    return _get("/players/nfl")


def get_stats(season: int, week: int, season_type: str = "regular") -> dict:
    """Weekly stats for all players. season_type: 'regular' or 'post'."""
    return _get(f"/stats/nfl/{season_type}/{season}/{week}")
