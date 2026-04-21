"""
dashboard.py — Fantasy Football Analytics Dashboard
=====================================================
Run with:
    pip install streamlit plotly pandas
    streamlit run dashboard.py

Views:
  1. League Standings — W/L records, scoring trends, all-time win %
  2. Head-to-Head    — matchup history between any two owners
  3. Waiver Adds     — best pickups by owner/season with season totals
  4. Cross-League    — roster comparison + player overlap across leagues
"""

import json
import os
import sqlite3

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─── Config ──────────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "fantasy.db")

st.set_page_config(
    page_title="Fantasy Football Dashboard",
    page_icon="🏈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── DB Connection ────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

conn = get_conn()

# ─── Data Loaders ─────────────────────────────────────────────────────────────
@st.cache_data
def load_leagues(_conn):
    return pd.read_sql("SELECT * FROM leagues ORDER BY season", _conn)

@st.cache_data
def load_teams(_conn):
    return pd.read_sql("SELECT * FROM teams", _conn)

@st.cache_data
def load_matchups(_conn):
    return pd.read_sql("SELECT * FROM matchups", _conn)

@st.cache_data
def load_transactions(_conn):
    return pd.read_sql("SELECT * FROM transactions", _conn)

@st.cache_data
def load_players(_conn):
    return pd.read_sql("SELECT player_id, full_name, position, team FROM players", _conn)

@st.cache_data
def load_player_stats(_conn):
    return pd.read_sql("SELECT * FROM player_stats", _conn)

@st.cache_data
def load_roster_slots(_conn, season):
    return pd.read_sql(
        f"SELECT league_id, roster_id, player_id, week, slot FROM roster_slots WHERE season = {season}",
        _conn
    )

leagues_df     = load_leagues(conn)
teams_df       = load_teams(conn)
matchups_df    = load_matchups(conn)
transactions_df = load_transactions(conn)
players_df     = load_players(conn)

# ─── Guard: empty DB ──────────────────────────────────────────────────────────
if leagues_df.empty:
    st.error("No data found in fantasy.db. Run `python run_all.py` first.")
    st.stop()

# ─── W/L Helper ───────────────────────────────────────────────────────────────
@st.cache_data
def compute_records(_matchups_df, _teams_df, _leagues_df):
    """Compute regular-season W/L records for every team in every league/season."""
    reg = _matchups_df[_matchups_df["is_playoff"] == 0].copy()

    # Self-join to pair opponents sharing the same matchup_id
    paired = reg.merge(
        reg[["league_id", "season", "week", "matchup_id", "roster_id", "points_for"]],
        on=["league_id", "season", "week", "matchup_id"],
        suffixes=("", "_opp"),
    )
    paired = paired[paired["roster_id"] != paired["roster_id_opp"]]

    paired["win"]  = (paired["points_for"] > paired["points_for_opp"]).astype(int)
    paired["loss"] = (paired["points_for"] < paired["points_for_opp"]).astype(int)
    paired["tie"]  = (paired["points_for"] == paired["points_for_opp"]).astype(int)

    records = (
        paired.groupby(["league_id", "season", "roster_id"])
        .agg(
            wins=("win", "sum"),
            losses=("loss", "sum"),
            ties=("tie", "sum"),
            points_for=("points_for", "sum"),
            points_against=("points_for_opp", "sum"),
            avg_pts=("points_for", "mean"),
        )
        .reset_index()
    )

    records = records.merge(
        _teams_df[["roster_id", "league_id", "owner_name", "team_name"]],
        on=["roster_id", "league_id"],
        how="left",
    )
    records = records.merge(
        _leagues_df[["league_id", "name"]].rename(columns={"name": "league_name"}),
        on="league_id",
        how="left",
    )
    records["record"]  = records.apply(lambda r: f"{int(r.wins)}-{int(r.losses)}", axis=1)
    records["win_pct"] = records["wins"] / (records["wins"] + records["losses"]).clip(lower=1)
    return records

records_df = compute_records(matchups_df, teams_df, leagues_df)

# ─── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.title("🏈 Fantasy Football")

all_owners = sorted(teams_df["owner_name"].dropna().unique())
my_name = st.sidebar.selectbox("My Team (owner name)", all_owners)

view = st.sidebar.radio(
    "View",
    ["📊 League Standings", "⚔️ Head-to-Head", "📈 Waiver Adds", "🔀 Cross-League Comparison"],
)

st.sidebar.markdown("---")
st.sidebar.caption(f"DB: {os.path.basename(DB_PATH)}")

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 1 — LEAGUE STANDINGS
# ══════════════════════════════════════════════════════════════════════════════
if view == "📊 League Standings":
    st.title("League Standings History")

    season_options = sorted(leagues_df["season"].unique(), reverse=True)
    col1, col2 = st.columns([1, 2])
    with col1:
        sel_season = st.selectbox("Season", season_options)
    with col2:
        season_leagues = leagues_df[leagues_df["season"] == sel_season]
        league_name_map = dict(zip(season_leagues["name"], season_leagues["league_id"]))
        sel_league_name = st.selectbox("League", list(league_name_map.keys()))
    sel_league_id = league_name_map[sel_league_name]

    df = (
        records_df[
            (records_df["season"] == sel_season)
            & (records_df["league_id"] == sel_league_id)
        ]
        .copy()
        .sort_values("wins", ascending=False)
        .reset_index(drop=True)
    )

    if df.empty:
        st.warning("No matchup data for this league/season.")
    else:
        # ── Standings table ──
        display_df = df[["owner_name", "team_name", "record", "wins", "losses", "points_for", "points_against", "avg_pts"]].copy()
        display_df.columns = ["Owner", "Team", "Record", "W", "L", "PF", "PA", "Avg Pts"]
        display_df["PF"]      = display_df["PF"].round(1)
        display_df["PA"]      = display_df["PA"].round(1)
        display_df["Avg Pts"] = display_df["Avg Pts"].round(1)
        display_df.insert(0, "#", range(1, len(display_df) + 1))

        def highlight_me(row):
            style = [""] * len(row)
            if row["Owner"] == my_name:
                style = ["background-color: #1a3a5c; color: white"] * len(row)
            return style

        st.dataframe(
            display_df.style.apply(highlight_me, axis=1),
            use_container_width=True,
            hide_index=True,
        )

        # ── Week-by-week scoring chart ──
        st.subheader(f"Week-by-Week Scoring — {sel_league_name} {sel_season}")

        my_roster_ids = teams_df[
            (teams_df["league_id"] == sel_league_id) & (teams_df["owner_name"] == my_name)
        ]["roster_id"].tolist()

        reg_matchups = matchups_df[
            (matchups_df["league_id"] == sel_league_id)
            & (matchups_df["season"] == sel_season)
            & (matchups_df["is_playoff"] == 0)
        ]

        if my_roster_ids and not reg_matchups.empty:
            my_weekly = reg_matchups[reg_matchups["roster_id"].isin(my_roster_ids)].sort_values("week")
            avg_weekly = reg_matchups.groupby("week")["points_for"].mean().reset_index(name="league_avg")
            chart_df = my_weekly.merge(avg_weekly, on="week", how="left")

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=chart_df["week"], y=chart_df["points_for"],
                mode="lines+markers", name="My Score",
                line=dict(color="#4e8df5", width=2.5),
                marker=dict(size=7),
            ))
            fig.add_trace(go.Scatter(
                x=chart_df["week"], y=chart_df["league_avg"],
                mode="lines", name="League Avg",
                line=dict(color="#aaa", width=1.5, dash="dash"),
            ))
            fig.update_layout(
                xaxis_title="Week", yaxis_title="Points",
                legend=dict(orientation="h", y=1.1),
                height=350, margin=dict(t=10, b=40),
            )
            st.plotly_chart(fig, use_container_width=True)

        # ── All-time win % bar chart ──
        st.subheader("All-Time Win % by Season")
        my_all = records_df[records_df["owner_name"] == my_name].sort_values("season")
        if not my_all.empty:
            fig2 = px.bar(
                my_all,
                x="season", y="win_pct",
                color="league_name",
                barmode="group",
                text=my_all["record"],
                labels={"win_pct": "Win %", "season": "Season", "league_name": "League"},
                color_discrete_sequence=px.colors.qualitative.Plotly,
            )
            fig2.update_traces(textposition="outside")
            fig2.update_layout(yaxis_tickformat=".0%", height=350, margin=dict(t=10, b=40))
            st.plotly_chart(fig2, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 2 — HEAD-TO-HEAD
# ══════════════════════════════════════════════════════════════════════════════
elif view == "⚔️ Head-to-Head":
    st.title("Head-to-Head Records")

    col1, col2 = st.columns(2)
    with col1:
        default_a = all_owners.index(my_name) if my_name in all_owners else 0
        owner_a = st.selectbox("Team A", all_owners, index=default_a)
    with col2:
        others = [o for o in all_owners if o != owner_a]
        owner_b = st.selectbox("Team B", others)

    # Find shared leagues
    ta = teams_df[teams_df["owner_name"] == owner_a][["league_id", "roster_id"]].rename(columns={"roster_id": "rid_a"})
    tb = teams_df[teams_df["owner_name"] == owner_b][["league_id", "roster_id"]].rename(columns={"roster_id": "rid_b"})
    shared = ta.merge(tb, on="league_id")

    reg = matchups_df[matchups_df["is_playoff"] == 0]

    h2h_rows = []
    for _, row in shared.iterrows():
        lg, rid_a, rid_b = row["league_id"], row["rid_a"], row["rid_b"]
        ma = reg[(reg["league_id"] == lg) & (reg["roster_id"] == rid_a)][
            ["season", "week", "matchup_id", "points_for"]
        ].rename(columns={"points_for": "pts_a"})
        mb = reg[(reg["league_id"] == lg) & (reg["roster_id"] == rid_b)][
            ["season", "week", "matchup_id", "points_for"]
        ].rename(columns={"points_for": "pts_b"})
        both = ma.merge(mb, on=["season", "week", "matchup_id"])
        if both.empty:
            continue
        both["league_id"] = lg
        both["result"] = both.apply(
            lambda r: "W" if r.pts_a > r.pts_b else ("L" if r.pts_a < r.pts_b else "T"), axis=1
        )
        h2h_rows.append(both)

    if not h2h_rows:
        st.info(f"**{owner_a}** and **{owner_b}** have never faced each other.")
    else:
        h2h = pd.concat(h2h_rows)
        h2h = h2h.merge(
            leagues_df[["league_id", "name"]].rename(columns={"name": "League"}), on="league_id"
        )

        wins_a = (h2h["result"] == "W").sum()
        wins_b = (h2h["result"] == "L").sum()
        ties   = (h2h["result"] == "T").sum()
        avg_a  = h2h["pts_a"].mean()
        avg_b  = h2h["pts_b"].mean()

        # ── Summary metrics ──
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric(f"{owner_a} Wins",  wins_a)
        m2.metric(f"{owner_b} Wins",  wins_b)
        m3.metric("Ties",             ties)
        m4.metric("Total Games",      len(h2h))
        m5.metric(f"{owner_a} Avg",   f"{avg_a:.1f}")
        m6.metric(f"{owner_b} Avg",   f"{avg_b:.1f}")

        st.markdown("---")

        # ── Scatter plot ──
        max_val = max(h2h["pts_a"].max(), h2h["pts_b"].max()) + 15
        fig = px.scatter(
            h2h, x="pts_a", y="pts_b",
            color="result",
            hover_data=["season", "week", "League"],
            labels={
                "pts_a": f"{owner_a} Score",
                "pts_b": f"{owner_b} Score",
                "result": "Result",
            },
            color_discrete_map={"W": "#4e8df5", "L": "#e05252", "T": "#aaa"},
            title=f"{owner_a} vs {owner_b} — All Matchups",
        )
        fig.add_shape(
            type="line", x0=0, y0=0, x1=max_val, y1=max_val,
            line=dict(dash="dash", color="#555", width=1),
        )
        fig.update_layout(height=420, margin=dict(t=40))
        st.plotly_chart(fig, use_container_width=True)

        # ── History table ──
        h2h_table = h2h[["season", "week", "League", "pts_a", "pts_b", "result"]].rename(columns={
            "pts_a": f"{owner_a}", "pts_b": f"{owner_b}",
            "result": "Result", "season": "Season", "week": "Week",
        }).sort_values(["Season", "Week"])
        st.dataframe(h2h_table, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 3 — WAIVER ADDS
# ══════════════════════════════════════════════════════════════════════════════
elif view == "📈 Waiver Adds":
    st.title("Best Waiver & Free Agent Adds")

    col1, col2, col3 = st.columns(3)
    with col1:
        default_idx = all_owners.index(my_name) if my_name in all_owners else 0
        sel_owner = st.selectbox("Owner", all_owners, index=default_idx)
    with col2:
        seasons = sorted(leagues_df["season"].unique(), reverse=True)
        sel_season = st.selectbox("Season", seasons)
    with col3:
        scoring_label = st.selectbox("Scoring", ["PPR", "Half-PPR", "Standard"])
        scoring_col = {"PPR": "pts_ppr", "Half-PPR": "pts_half_ppr", "Standard": "pts_std"}[scoring_label]

    owner_league_rows = teams_df.merge(leagues_df[["league_id", "season"]], on="league_id")
    owner_league_rows = owner_league_rows[
        (owner_league_rows["owner_name"] == sel_owner)
        & (owner_league_rows["season"] == sel_season)
    ]

    if owner_league_rows.empty:
        st.warning("No leagues found for this owner/season.")
    else:
        league_ids = owner_league_rows["league_id"].tolist()
        roster_ids = owner_league_rows["roster_id"].tolist()

        txn = transactions_df[
            transactions_df["league_id"].isin(league_ids)
            & transactions_df["type"].isin(["waiver", "free_agent"])
            & (transactions_df["status"] == "complete")
        ].copy()

        def parse_json(val):
            try:
                return json.loads(val)
            except Exception:
                return []

        txn["roster_ids_list"] = txn["roster_ids"].apply(parse_json)
        txn["player_ids_list"] = txn["player_ids"].apply(parse_json)
        txn = txn[txn["roster_ids_list"].apply(lambda x: any(r in x for r in roster_ids))]

        if txn.empty:
            st.warning("No waiver/FA transactions found for this owner/season.")
        else:
            # Explode to one row per player per transaction
            txn_exp = (
                txn.explode("player_ids_list")
                .rename(columns={"player_ids_list": "player_id"})
            )
            txn_exp = txn_exp[txn_exp["player_id"].notna() & (txn_exp["player_id"] != "")]

            # Season totals
            stats_df = load_player_stats(conn)
            season_stats = (
                stats_df[stats_df["season"] == sel_season]
                .groupby("player_id")
                .agg(
                    total_pts=(scoring_col, "sum"),
                    weeks_played=(scoring_col, lambda x: (x > 0).sum()),
                    avg_pts=(scoring_col, "mean"),
                )
                .reset_index()
            )

            # Dedupe: keep one row per player (highest-scoring league if in multiple)
            result = (
                txn_exp[["player_id", "league_id", "type"]]
                .drop_duplicates("player_id", keep="first")
                .merge(players_df[["player_id", "full_name", "position", "team"]], on="player_id", how="left")
                .merge(season_stats, on="player_id", how="left")
                .merge(leagues_df[["league_id", "name"]].rename(columns={"name": "League"}), on="league_id", how="left")
                .dropna(subset=["full_name"])
                .sort_values("total_pts", ascending=False)
            )

            if result.empty:
                st.warning("No player stats found for these transactions.")
            else:
                # ── Top adds bar chart ──
                top15 = result.head(15)
                fig = px.bar(
                    top15,
                    x="total_pts", y="full_name",
                    orientation="h",
                    color="position",
                    labels={
                        "total_pts": f"Season Total ({scoring_label})",
                        "full_name": "Player",
                        "position": "Position",
                    },
                    title=f"Top Waiver/FA Adds — {sel_owner} {sel_season}",
                    color_discrete_sequence=px.colors.qualitative.Plotly,
                )
                fig.update_layout(
                    yaxis={"categoryorder": "total ascending"},
                    height=460,
                    margin=dict(t=40, b=20),
                )
                st.plotly_chart(fig, use_container_width=True)

                # ── Full table ──
                table = result[["full_name", "position", "team", "League", "type", "total_pts", "weeks_played", "avg_pts"]].rename(columns={
                    "full_name": "Player", "position": "Pos", "team": "NFL Team",
                    "type": "Add Type", "total_pts": "Season Pts",
                    "weeks_played": "Weeks Active", "avg_pts": "Avg/Wk",
                })
                table["Season Pts"] = table["Season Pts"].round(1)
                table["Avg/Wk"]     = table["Avg/Wk"].round(1)
                st.dataframe(table, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 4 — CROSS-LEAGUE COMPARISON
# ══════════════════════════════════════════════════════════════════════════════
elif view == "🔀 Cross-League Comparison":
    st.title("Cross-League Roster Comparison")

    col1, col2, col3 = st.columns(3)
    with col1:
        default_idx = all_owners.index(my_name) if my_name in all_owners else 0
        sel_owner = st.selectbox("Owner", all_owners, index=default_idx)
    with col2:
        seasons = sorted(leagues_df["season"].unique(), reverse=True)
        sel_season = st.selectbox("Season", seasons)
    with col3:
        sel_week = st.number_input("Roster Week", min_value=1, max_value=18, value=1)

    owner_leagues = (
        teams_df.merge(leagues_df, on="league_id")
        .query("owner_name == @sel_owner and season == @sel_season")
    )

    if owner_leagues.empty:
        st.warning("No leagues found for this owner/season.")
    else:
        slots_df = load_roster_slots(conn, sel_season)

        if slots_df.empty:
            st.warning("No roster slot data for this season.")
        else:
            stats_df = load_player_stats(conn)
            season_stats = (
                stats_df[stats_df["season"] == sel_season]
                .groupby("player_id")
                .agg(
                    season_pts=("pts_ppr", "sum"),
                    avg_pts=("pts_ppr", "mean"),
                )
                .reset_index()
            )

            # ── Per-league rosters ──
            all_rosters = []
            for _, row in owner_leagues.iterrows():
                week_slots = slots_df[
                    (slots_df["league_id"] == row["league_id"])
                    & (slots_df["roster_id"] == row["roster_id"])
                    & (slots_df["week"] == sel_week)
                ].copy()
                if week_slots.empty:
                    continue
                week_slots["league_name"] = row["name"]
                all_rosters.append(week_slots)

            if not all_rosters:
                st.info(f"No roster data for week {sel_week}. Try a different week.")
            else:
                combined = (
                    pd.concat(all_rosters)
                    .merge(players_df, on="player_id", how="left")
                    .merge(season_stats, on="player_id", how="left")
                )

                # ── Side-by-side roster tables ──
                league_names = combined["league_name"].unique()
                cols = st.columns(max(1, len(league_names)))

                for col, lg_name in zip(cols, league_names):
                    with col:
                        st.markdown(f"**{lg_name}**")
                        lg_data = (
                            combined[combined["league_name"] == lg_name]
                            [["slot", "full_name", "position", "season_pts", "avg_pts"]]
                            .rename(columns={
                                "slot": "Slot", "full_name": "Player",
                                "position": "Pos", "season_pts": "Szn Pts", "avg_pts": "Avg/Wk",
                            })
                            .sort_values("Slot")
                        )
                        lg_data["Szn Pts"] = lg_data["Szn Pts"].round(1)
                        lg_data["Avg/Wk"]  = lg_data["Avg/Wk"].round(1)
                        st.dataframe(lg_data, use_container_width=True, hide_index=True)

                st.markdown("---")

                # ── Player overlap ──
                st.subheader("Players Rostered in Multiple Leagues")
                overlap = (
                    combined.groupby(["player_id", "full_name", "position"])["league_name"]
                    .apply(list)
                    .reset_index()
                )
                overlap["# Leagues"] = overlap["league_name"].apply(len)
                overlap = (
                    overlap[overlap["# Leagues"] > 1]
                    .merge(season_stats, on="player_id", how="left")
                    .sort_values("season_pts", ascending=False)
                )

                if overlap.empty:
                    st.info("No players rostered in more than one of your leagues this week.")
                else:
                    overlap_table = overlap[["full_name", "position", "# Leagues", "season_pts"]].rename(columns={
                        "full_name": "Player", "position": "Pos", "season_pts": "Season Pts (PPR)",
                    })
                    overlap_table["Season Pts (PPR)"] = overlap_table["Season Pts (PPR)"].round(1)
                    st.dataframe(overlap_table, use_container_width=True, hide_index=True)

                # ── Scoring comparison bar chart ──
                st.subheader("Roster Quality by League (Season PPR Pts, Starters Only)")
                starters_only = combined[combined["slot"].str.startswith("STARTER")]
                if not starters_only.empty:
                    league_totals = (
                        starters_only.groupby("league_name")["season_pts"]
                        .sum()
                        .reset_index()
                        .rename(columns={"season_name": "League", "season_pts": "Total Starter Pts"})
                    )
                    fig = px.bar(
                        starters_only.groupby(["league_name", "player_id", "full_name"])["season_pts"]
                        .sum()
                        .reset_index()
                        .sort_values("season_pts", ascending=False),
                        x="season_pts", y="full_name",
                        color="league_name",
                        orientation="h",
                        labels={
                            "season_pts": "Season Pts (PPR)",
                            "full_name": "Player",
                            "league_name": "League",
                        },
                        title=f"Starter Pts Across Leagues — Week {sel_week} Roster",
                        color_discrete_sequence=px.colors.qualitative.Plotly,
                    )
                    fig.update_layout(
                        yaxis={"categoryorder": "total ascending"},
                        height=max(400, len(starters_only["full_name"].unique()) * 22),
                        margin=dict(t=40, b=20),
                    )
                    st.plotly_chart(fig, use_container_width=True)
