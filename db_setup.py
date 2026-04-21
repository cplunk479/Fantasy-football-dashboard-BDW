"""
db_setup.py
Creates the fantasy.db SQLite database and all tables.
Run this once before any ingestion scripts.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "fantasy.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def create_tables(conn):
    conn.executescript("""
    -- ─────────────────────────────────────────
    -- FANTASY LAYER
    -- ─────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS leagues (
        league_id       TEXT PRIMARY KEY,
        name            TEXT,
        season          INTEGER,
        scoring_format  TEXT,
        num_teams       INTEGER,
        playoff_weeks   TEXT
    );

    CREATE TABLE IF NOT EXISTS teams (
        roster_id       INTEGER,
        league_id       TEXT REFERENCES leagues(league_id),
        owner_id        TEXT,
        owner_name      TEXT,
        team_name       TEXT,
        PRIMARY KEY (roster_id, league_id)
    );

    CREATE TABLE IF NOT EXISTS matchups (
        league_id       TEXT REFERENCES leagues(league_id),
        season          INTEGER,
        week            INTEGER,
        matchup_id      INTEGER,
        roster_id       INTEGER,
        points_for      REAL,
        is_playoff      BOOLEAN DEFAULT 0,
        PRIMARY KEY (league_id, season, week, roster_id)
    );

    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id  TEXT PRIMARY KEY,
        league_id       TEXT REFERENCES leagues(league_id),
        type            TEXT,
        status          TEXT,
        created         INTEGER,
        player_ids      TEXT,
        roster_ids      TEXT
    );

    -- ─────────────────────────────────────────
    -- NFL PLAYER LAYER
    -- ─────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS players (
        player_id       TEXT PRIMARY KEY,
        full_name       TEXT,
        position        TEXT,
        team            TEXT,
        status          TEXT,
        age             INTEGER,
        years_exp       INTEGER,
        college         TEXT
    );

    CREATE TABLE IF NOT EXISTS player_stats (
        player_id       TEXT REFERENCES players(player_id),
        season          INTEGER,
        week            INTEGER,
        pts_ppr         REAL,
        pts_std         REAL,
        pts_half_ppr    REAL,
        pass_yd         REAL,
        pass_td         REAL,
        rush_yd         REAL,
        rush_td         REAL,
        rec             REAL,
        rec_yd          REAL,
        rec_td          REAL,
        targets         REAL,
        PRIMARY KEY (player_id, season, week)
    );

    -- ─────────────────────────────────────────
    -- NFL TEAM LAYER
    -- ─────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS nfl_teams (
        team_abbr       TEXT PRIMARY KEY,
        full_name       TEXT,
        conference      TEXT,
        division        TEXT
    );

    CREATE TABLE IF NOT EXISTS nfl_games (
        game_id         TEXT PRIMARY KEY,
        season          INTEGER,
        week            INTEGER,
        home_team       TEXT REFERENCES nfl_teams(team_abbr),
        away_team       TEXT REFERENCES nfl_teams(team_abbr),
        home_score      INTEGER,
        away_score      INTEGER,
        game_date       DATE
    );

    -- ─────────────────────────────────────────
    -- BRIDGE TABLE
    -- ─────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS roster_slots (
        league_id       TEXT REFERENCES leagues(league_id),
        roster_id       INTEGER,
        player_id       TEXT,
        season          INTEGER,
        week            INTEGER,
        slot            TEXT,
        PRIMARY KEY (league_id, roster_id, player_id, season, week)
    );

    -- ─────────────────────────────────────────
    -- INDEXES
    -- ─────────────────────────────────────────

    CREATE INDEX IF NOT EXISTS idx_matchups_league   ON matchups(league_id, season);
    CREATE INDEX IF NOT EXISTS idx_player_stats_week ON player_stats(season, week);
    CREATE INDEX IF NOT EXISTS idx_roster_slots_player ON roster_slots(player_id, season);
    CREATE INDEX IF NOT EXISTS idx_transactions_league ON transactions(league_id);
    """)
    conn.commit()
    print("✓ All tables created in fantasy.db")


if __name__ == "__main__":
    conn = get_connection()
    create_tables(conn)
    conn.close()
