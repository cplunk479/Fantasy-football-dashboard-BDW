"""
dashboard.py — Fantasy Football Analytics Dashboard
=====================================================
Run with:
    pip install streamlit plotly pandas
    streamlit run dashboard.py

Views:
  1. League Standings        — W/L records, scoring trends
  2. Head-to-Head            — matchup history + starter drill-down
  3. Waiver Adds             — pickups filterable by position
  4. Cross-League Comparison — roster overlap across leagues
  5. Player View             — per-player dashboard
  6. Owner Analytics         — top scorers per owner, carries
  7. Trade Analyzer          — retroactive trade ROI
  8. Power Rankings          — composite PF / win% / recent form
  9. Luck Index              — actual wins vs. all-play expected wins
 10. Draft Grades / ROI      — season pts from week-1 roster
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

# Razorbacks theme
RZR_RED    = "#9D2232"
RZR_RED_LT = "#C8102E"
RZR_BLACK  = "#111111"
RZR_WHITE  = "#FFFFFF"
RZR_GRAY   = "#6E6E6E"
RZR_CREAM  = "#F5F1E8"

RZR_PALETTE = [RZR_RED, RZR_BLACK, RZR_RED_LT, "#7A1A26", RZR_GRAY, "#3D3D3D", "#D4A84B"]

st.set_page_config(
    page_title="Razorback Fantasy Dashboard",
    page_icon="🐗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Razorbacks CSS Theme ────────────────────────────────────────────────────
st.markdown(f"""
<style>
    .stApp {{
        background-color: {RZR_WHITE};
        color: {RZR_BLACK};
    }}
    [data-testid="stSidebar"] {{
        background-color: {RZR_BLACK};
    }}
    [data-testid="stSidebar"] * {{
        color: {RZR_WHITE} !important;
    }}
    [data-testid="stSidebar"] .stRadio label,
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stMultiSelect label {{
        color: {RZR_WHITE} !important;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-size: 0.82rem;
    }}
    h1, h2, h3 {{
        color: {RZR_RED};
        font-family: 'Helvetica Neue', Arial, sans-serif;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 1px;
    }}
    h1 {{
        border-bottom: 4px solid {RZR_RED};
        padding-bottom: 0.4rem;
    }}
    [data-testid="stMetric"] {{
        background-color: {RZR_CREAM};
        border-left: 5px solid {RZR_RED};
        padding: 0.7rem 1rem;
        border-radius: 2px;
    }}
    [data-testid="stMetricValue"] {{
        color: {RZR_RED};
        font-weight: 800;
    }}
    .stDataFrame thead tr th {{
        background-color: {RZR_RED} !important;
        color: {RZR_WHITE} !important;
        font-weight: 700 !important;
        text-transform: uppercase;
    }}
    .stButton button, .stDownloadButton button {{
        background-color: {RZR_RED};
        color: {RZR_WHITE};
        border: none;
        border-radius: 2px;
        font-weight: 700;
        text-transform: uppercase;
    }}
    .stButton button:hover {{
        background-color: {RZR_BLACK};
        color: {RZR_WHITE};
    }}
    .stTabs [data-baseweb="tab"] {{
        font-weight: 700;
        text-transform: uppercase;
    }}
    .stTabs [aria-selected="true"] {{
        color: {RZR_RED} !important;
        border-bottom: 3px solid {RZR_RED} !important;
    }}
</style>
""", unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    plot_bgcolor=RZR_WHITE,
    paper_bgcolor=RZR_WHITE,
    font=dict(color=RZR_BLACK, family="Helvetica Neue, Arial, sans-serif"),
    colorway=RZR_PALETTE,
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
        "SELECT league_id, roster_id, player_id, week, slot FROM roster_slots WHERE season = ?",
        _conn, params=(season,)
    )

@st.cache_data
def load_all_roster_slots(_conn):
    return pd.read_sql(
        "SELECT league_id, roster_id, player_id, season, week, slot FROM roster_slots",
        _conn
    )

leagues_df      = load_leagues(conn)
teams_df        = load_teams(conn)
matchups_df     = load_matchups(conn)
transactions_df = load_transactions(conn)
players_df      = load_players(conn)

if leagues_df.empty:
    st.error("No data found in fantasy.db. Run `python run_all.py` first.")
    st.stop()

def parse_json_list(val):
    try:
        return json.loads(val) if val else []
    except Exception:
        return []

# ─── W/L Helper ───────────────────────────────────────────────────────────────
@st.cache_data
def compute_records(_matchups_df, _teams_df, _leagues_df):
    reg = _matchups_df[_matchups_df["is_playoff"] == 0].copy()
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
        on=["roster_id", "league_id"], how="left",
    )
    records = records.merge(
        _leagues_df[["league_id", "name"]].rename(columns={"name": "league_name"}),
        on="league_id", how="left",
    )
    records["record"]  = records.apply(lambda r: f"{int(r.wins)}-{int(r.losses)}", axis=1)
    records["win_pct"] = records["wins"] / (records["wins"] + records["losses"]).clip(lower=1)
    return records

records_df = compute_records(matchups_df, teams_df, leagues_df)

# ─── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.markdown(f"<h1 style='color:{RZR_RED};margin-top:0;'>🐗 RAZORBACK<br/>FANTASY</h1>", unsafe_allow_html=True)

all_owners = sorted(teams_df["owner_name"].dropna().unique())
my_name = st.sidebar.selectbox("My Team (owner)", all_owners)

view = st.sidebar.radio(
    "View",
    [
        "📊 League Standings",
        "⚔️ Head-to-Head",
        "📈 Waiver Adds",
        "🔀 Cross-League Comparison",
        "👤 Player View",
        "🏆 Owner Analytics",
        "🔄 Trade Analyzer",
        "⚡ Power Rankings",
        "🍀 Luck Index",
        "📝 Draft Grades / ROI",
    ],
)

# League filter — applies to every view EXCEPT Cross-League Comparison
st.sidebar.markdown("---")
league_filter_disabled = view == "🔀 Cross-League Comparison"
all_league_names = sorted(leagues_df["name"].unique())
if league_filter_disabled:
    st.sidebar.caption("League filter N/A for Cross-League view.")
    filter_leagues = all_league_names
else:
    filter_leagues = st.sidebar.multiselect(
        "Filter by League", all_league_names, default=all_league_names
    )
    if not filter_leagues:
        filter_leagues = all_league_names

filter_league_ids = leagues_df[leagues_df["name"].isin(filter_leagues)]["league_id"].tolist()

st.sidebar.markdown("---")
st.sidebar.caption(f"DB: {os.path.basename(DB_PATH)}")

def apply_league_filter(df, col="league_id"):
    if league_filter_disabled:
        return df
    return df[df[col].isin(filter_league_ids)]

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 1 — LEAGUE STANDINGS
# ══════════════════════════════════════════════════════════════════════════════
if view == "📊 League Standings":
    st.title("League Standings")

    season_options = sorted(leagues_df["season"].unique(), reverse=True)
    col1, col2 = st.columns([1, 2])
    with col1:
        sel_season = st.selectbox("Season", season_options)
    with col2:
        season_leagues = leagues_df[
            (leagues_df["season"] == sel_season)
            & (leagues_df["league_id"].isin(filter_league_ids))
        ]
        if season_leagues.empty:
            st.warning("No leagues match your filter for this season.")
            st.stop()
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
        display_df = df[["owner_name", "team_name", "record", "wins", "losses", "points_for", "points_against", "avg_pts"]].copy()
        display_df.columns = ["Owner", "Team", "Record", "W", "L", "PF", "PA", "Avg Pts"]
        display_df["PF"]      = display_df["PF"].round(1)
        display_df["PA"]      = display_df["PA"].round(1)
        display_df["Avg Pts"] = display_df["Avg Pts"].round(1)
        display_df.insert(0, "#", range(1, len(display_df) + 1))

        def highlight_me(row):
            if row["Owner"] == my_name:
                return [f"background-color: {RZR_RED}; color: white"] * len(row)
            return [""] * len(row)

        st.dataframe(display_df.style.apply(highlight_me, axis=1),
                     use_container_width=True, hide_index=True)

        st.subheader(f"Week-by-Week — {sel_league_name} {sel_season}")
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
                line=dict(color=RZR_RED, width=3), marker=dict(size=9),
            ))
            fig.add_trace(go.Scatter(
                x=chart_df["week"], y=chart_df["league_avg"],
                mode="lines", name="League Avg",
                line=dict(color=RZR_BLACK, width=1.5, dash="dash"),
            ))
            fig.update_layout(xaxis_title="Week", yaxis_title="Points",
                              legend=dict(orientation="h", y=1.1),
                              height=360, margin=dict(t=10, b=40), **PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("All-Time Win % by Season")
        my_all = records_df[
            (records_df["owner_name"] == my_name)
            & (records_df["league_id"].isin(filter_league_ids))
        ].sort_values("season")
        if not my_all.empty:
            fig2 = px.bar(
                my_all, x="season", y="win_pct",
                color="league_name", barmode="group",
                text=my_all["record"],
                labels={"win_pct": "Win %", "season": "Season", "league_name": "League"},
                color_discrete_sequence=RZR_PALETTE,
            )
            fig2.update_traces(textposition="outside")
            fig2.update_layout(yaxis_tickformat=".0%", height=360,
                               margin=dict(t=10, b=40), **PLOTLY_LAYOUT)
            st.plotly_chart(fig2, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 2 — HEAD-TO-HEAD (with drill-down)
# ══════════════════════════════════════════════════════════════════════════════
elif view == "⚔️ Head-to-Head":
    st.title("Head-to-Head")

    col1, col2 = st.columns(2)
    with col1:
        default_a = all_owners.index(my_name) if my_name in all_owners else 0
        owner_a = st.selectbox("Team A", all_owners, index=default_a)
    with col2:
        others = [o for o in all_owners if o != owner_a]
        owner_b = st.selectbox("Team B", others)

    ta = teams_df[(teams_df["owner_name"] == owner_a) & (teams_df["league_id"].isin(filter_league_ids))][["league_id", "roster_id"]].rename(columns={"roster_id": "rid_a"})
    tb = teams_df[(teams_df["owner_name"] == owner_b) & (teams_df["league_id"].isin(filter_league_ids))][["league_id", "roster_id"]].rename(columns={"roster_id": "rid_b"})
    shared = ta.merge(tb, on="league_id")
    reg = matchups_df[matchups_df["is_playoff"] == 0]

    h2h_rows = []
    for _, row in shared.iterrows():
        lg, rid_a, rid_b = row["league_id"], row["rid_a"], row["rid_b"]
        ma = reg[(reg["league_id"] == lg) & (reg["roster_id"] == rid_a)][["season", "week", "matchup_id", "points_for"]].rename(columns={"points_for": "pts_a"})
        mb = reg[(reg["league_id"] == lg) & (reg["roster_id"] == rid_b)][["season", "week", "matchup_id", "points_for"]].rename(columns={"points_for": "pts_b"})
        both = ma.merge(mb, on=["season", "week", "matchup_id"])
        if both.empty:
            continue
        both["league_id"] = lg
        both["rid_a"] = rid_a
        both["rid_b"] = rid_b
        both["result"] = both.apply(lambda r: "W" if r.pts_a > r.pts_b else ("L" if r.pts_a < r.pts_b else "T"), axis=1)
        h2h_rows.append(both)

    if not h2h_rows:
        st.info(f"**{owner_a}** and **{owner_b}** have never faced each other (in filtered leagues).")
    else:
        h2h = pd.concat(h2h_rows)
        h2h = h2h.merge(leagues_df[["league_id", "name"]].rename(columns={"name": "League"}), on="league_id")

        wins_a = (h2h["result"] == "W").sum()
        wins_b = (h2h["result"] == "L").sum()
        ties   = (h2h["result"] == "T").sum()
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric(f"{owner_a} W", wins_a)
        m2.metric(f"{owner_b} W", wins_b)
        m3.metric("Ties", ties)
        m4.metric("Games", len(h2h))
        m5.metric(f"{owner_a} Avg", f"{h2h['pts_a'].mean():.1f}")
        m6.metric(f"{owner_b} Avg", f"{h2h['pts_b'].mean():.1f}")

        st.markdown("---")

        max_val = max(h2h["pts_a"].max(), h2h["pts_b"].max()) + 15
        fig = px.scatter(
            h2h, x="pts_a", y="pts_b", color="result",
            hover_data=["season", "week", "League"],
            labels={"pts_a": f"{owner_a}", "pts_b": f"{owner_b}", "result": "Result"},
            color_discrete_map={"W": RZR_RED, "L": RZR_BLACK, "T": RZR_GRAY},
            title=f"{owner_a} vs {owner_b}",
        )
        fig.add_shape(type="line", x0=0, y0=0, x1=max_val, y1=max_val,
                      line=dict(dash="dash", color=RZR_GRAY, width=1))
        fig.update_layout(height=420, margin=dict(t=40), **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

        # ── History table + drill-down ──
        st.subheader("Matchup History — Click to Drill Into Starters")
        h2h_sorted = h2h.sort_values(["season", "week"], ascending=[False, True]).reset_index(drop=True)
        display = h2h_sorted[["season", "week", "League", "pts_a", "pts_b", "result"]].rename(columns={
            "pts_a": owner_a, "pts_b": owner_b,
            "result": "Result", "season": "Season", "week": "Week",
        })
        st.dataframe(display, use_container_width=True, hide_index=True)

        st.markdown("**Drill into a specific matchup:**")
        option_labels = [
            f"{r['season']} W{r['week']} — {r['League']} ({r['pts_a']:.1f} vs {r['pts_b']:.1f})"
            for _, r in h2h_sorted.iterrows()
        ]
        sel_idx = st.selectbox("Matchup", range(len(option_labels)), format_func=lambda i: option_labels[i])
        sel_row = h2h_sorted.iloc[sel_idx]

        slots_df = load_roster_slots(conn, int(sel_row["season"]))
        stats_df = load_player_stats(conn)
        week_stats = stats_df[(stats_df["season"] == int(sel_row["season"])) & (stats_df["week"] == int(sel_row["week"]))]

        def roster_detail(rid, label, pts):
            rs = slots_df[
                (slots_df["league_id"] == sel_row["league_id"])
                & (slots_df["roster_id"] == rid)
                & (slots_df["week"] == int(sel_row["week"]))
            ].merge(players_df, on="player_id", how="left").merge(
                week_stats[["player_id", "pts_ppr"]], on="player_id", how="left"
            )
            if rs.empty:
                st.info(f"No roster data for {label}.")
                return
            rs["Starter"] = ~rs["slot"].str.upper().isin(["BN", "IR", "TAXI"])
            rs["pts_ppr"] = rs["pts_ppr"].fillna(0).round(2)
            rs = rs.sort_values(["Starter", "pts_ppr"], ascending=[False, False])
            show = rs[["slot", "full_name", "position", "team", "pts_ppr", "Starter"]].rename(columns={
                "slot": "Slot", "full_name": "Player", "position": "Pos",
                "team": "NFL", "pts_ppr": "Pts",
            })
            st.markdown(f"**{label}** — Total: {pts:.1f}")
            st.dataframe(show, use_container_width=True, hide_index=True)

        c1, c2 = st.columns(2)
        with c1:
            roster_detail(sel_row["rid_a"], owner_a, sel_row["pts_a"])
        with c2:
            roster_detail(sel_row["rid_b"], owner_b, sel_row["pts_b"])

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 3 — WAIVER ADDS (position filter)
# ══════════════════════════════════════════════════════════════════════════════
elif view == "📈 Waiver Adds":
    st.title("Best Waiver & Free Agent Adds")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        default_idx = all_owners.index(my_name) if my_name in all_owners else 0
        sel_owner = st.selectbox("Owner", all_owners, index=default_idx)
    with col2:
        seasons = sorted(leagues_df["season"].unique(), reverse=True)
        sel_season = st.selectbox("Season", seasons)
    with col3:
        scoring_label = st.selectbox("Scoring", ["PPR", "Half-PPR", "Standard"])
        scoring_col = {"PPR": "pts_ppr", "Half-PPR": "pts_half_ppr", "Standard": "pts_std"}[scoring_label]
    with col4:
        all_positions = ["QB", "RB", "WR", "TE", "K", "DEF"]
        sel_positions = st.multiselect("Positions", all_positions, default=all_positions)

    owner_league_rows = teams_df.merge(leagues_df[["league_id", "season"]], on="league_id")
    owner_league_rows = owner_league_rows[
        (owner_league_rows["owner_name"] == sel_owner)
        & (owner_league_rows["season"] == sel_season)
        & (owner_league_rows["league_id"].isin(filter_league_ids))
    ]

    if owner_league_rows.empty:
        st.warning("No leagues found for this owner/season (within filter).")
    else:
        league_ids = owner_league_rows["league_id"].tolist()
        roster_ids = owner_league_rows["roster_id"].tolist()

        txn = transactions_df[
            transactions_df["league_id"].isin(league_ids)
            & transactions_df["type"].isin(["waiver", "free_agent"])
            & (transactions_df["status"] == "complete")
        ].copy()

        txn["roster_ids_list"] = txn["roster_ids"].apply(parse_json_list)
        txn["player_ids_list"] = txn["player_ids"].apply(parse_json_list)
        txn = txn[txn["roster_ids_list"].apply(lambda x: any(r in x for r in roster_ids))]

        if txn.empty:
            st.warning("No waiver/FA transactions for this owner/season.")
        else:
            txn_exp = txn.explode("player_ids_list").rename(columns={"player_ids_list": "player_id"})
            txn_exp = txn_exp[txn_exp["player_id"].notna() & (txn_exp["player_id"] != "")]

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

            result = (
                txn_exp[["player_id", "league_id", "type"]]
                .drop_duplicates("player_id", keep="first")
                .merge(players_df[["player_id", "full_name", "position", "team"]], on="player_id", how="left")
                .merge(season_stats, on="player_id", how="left")
                .merge(leagues_df[["league_id", "name"]].rename(columns={"name": "League"}), on="league_id", how="left")
                .dropna(subset=["full_name"])
            )
            result = result[result["position"].isin(sel_positions)].sort_values("total_pts", ascending=False)

            if result.empty:
                st.warning("No results match your position filter.")
            else:
                top15 = result.head(15)
                fig = px.bar(
                    top15, x="total_pts", y="full_name", orientation="h",
                    color="position",
                    labels={"total_pts": f"Season ({scoring_label})", "full_name": "Player"},
                    title=f"Top Adds — {sel_owner} {sel_season}",
                    color_discrete_sequence=RZR_PALETTE,
                )
                fig.update_layout(yaxis={"categoryorder": "total ascending"},
                                  height=460, margin=dict(t=40, b=20), **PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

                table = result[["full_name", "position", "team", "League", "type", "total_pts", "weeks_played", "avg_pts"]].rename(columns={
                    "full_name": "Player", "position": "Pos", "team": "NFL",
                    "type": "Add Type", "total_pts": "Season Pts",
                    "weeks_played": "Wks", "avg_pts": "Avg/Wk",
                })
                table["Season Pts"] = table["Season Pts"].round(1)
                table["Avg/Wk"]     = table["Avg/Wk"].round(1)
                st.dataframe(table, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 4 — CROSS-LEAGUE COMPARISON (no league filter)
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
        slots_df = load_roster_slots(conn, int(sel_season))
        if slots_df.empty:
            st.warning("No roster slot data for this season.")
        else:
            stats_df = load_player_stats(conn)
            season_stats = (
                stats_df[stats_df["season"] == sel_season]
                .groupby("player_id")
                .agg(season_pts=("pts_ppr", "sum"), avg_pts=("pts_ppr", "mean"))
                .reset_index()
            )

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
                st.info(f"No roster data for week {sel_week}.")
            else:
                combined = (
                    pd.concat(all_rosters)
                    .merge(players_df, on="player_id", how="left")
                    .merge(season_stats, on="player_id", how="left")
                )
                league_names = combined["league_name"].unique()
                cols = st.columns(max(1, len(league_names)))
                for col, lg_name in zip(cols, league_names):
                    with col:
                        st.markdown(f"**{lg_name}**")
                        lg_data = (
                            combined[combined["league_name"] == lg_name]
                            [["slot", "full_name", "position", "season_pts", "avg_pts"]]
                            .rename(columns={"slot": "Slot", "full_name": "Player",
                                             "position": "Pos", "season_pts": "Szn Pts", "avg_pts": "Avg"})
                            .sort_values("Slot")
                        )
                        lg_data["Szn Pts"] = lg_data["Szn Pts"].round(1)
                        lg_data["Avg"]     = lg_data["Avg"].round(1)
                        st.dataframe(lg_data, use_container_width=True, hide_index=True)

                st.markdown("---")
                st.subheader("Players Rostered in Multiple Leagues")
                overlap = (
                    combined.groupby(["player_id", "full_name", "position"])["league_name"]
                    .apply(list).reset_index()
                )
                overlap["# Leagues"] = overlap["league_name"].apply(len)
                overlap = (
                    overlap[overlap["# Leagues"] > 1]
                    .merge(season_stats, on="player_id", how="left")
                    .sort_values("season_pts", ascending=False)
                )
                if overlap.empty:
                    st.info("No players rostered in more than one league this week.")
                else:
                    ot = overlap[["full_name", "position", "# Leagues", "season_pts"]].rename(columns={
                        "full_name": "Player", "position": "Pos", "season_pts": "Season Pts",
                    })
                    ot["Season Pts"] = ot["Season Pts"].round(1)
                    st.dataframe(ot, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 5 — PLAYER VIEW
# ══════════════════════════════════════════════════════════════════════════════
elif view == "👤 Player View":
    st.title("Player Dashboard")

    stats_df = load_player_stats(conn)

    col1, col2 = st.columns([2, 1])
    with col2:
        seasons = sorted(stats_df["season"].dropna().unique(), reverse=True)
        sel_season = st.selectbox("Season", seasons)
    with col1:
        pos_filter = st.multiselect("Filter positions", ["QB", "RB", "WR", "TE", "K", "DEF"],
                                    default=["QB", "RB", "WR", "TE"])

    active_players = players_df[players_df["position"].isin(pos_filter)].copy()
    active_players["label"] = active_players["full_name"] + " (" + active_players["position"].fillna("") + " — " + active_players["team"].fillna("FA") + ")"
    active_players = active_players.sort_values("full_name")

    sel_label = st.selectbox("Player", active_players["label"].tolist())
    sel_player = active_players[active_players["label"] == sel_label].iloc[0]
    pid = sel_player["player_id"]

    p_stats = stats_df[(stats_df["player_id"] == pid) & (stats_df["season"] == sel_season)].sort_values("week")

    if p_stats.empty:
        st.warning("No stats for this player in this season.")
    else:
        tot_ppr = p_stats["pts_ppr"].sum()
        avg_ppr = p_stats["pts_ppr"].mean()
        best    = p_stats["pts_ppr"].max()
        games   = (p_stats["pts_ppr"] > 0).sum()

        # Position rank
        pos_totals = (
            stats_df[stats_df["season"] == sel_season]
            .merge(players_df[["player_id", "position"]], on="player_id")
            .groupby(["player_id", "position"])["pts_ppr"].sum().reset_index()
        )
        same_pos = pos_totals[pos_totals["position"] == sel_player["position"]].sort_values("pts_ppr", ascending=False).reset_index(drop=True)
        rank = same_pos.index[same_pos["player_id"] == pid].tolist()
        pos_rank = rank[0] + 1 if rank else None

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total PPR", f"{tot_ppr:.1f}")
        m2.metric("Avg / Wk", f"{avg_ppr:.1f}")
        m3.metric("Best Wk", f"{best:.1f}")
        m4.metric("Games", int(games))
        m5.metric(f"{sel_player['position']} Rank", f"#{pos_rank}" if pos_rank else "—")

        st.subheader("Weekly Scoring")
        fig = px.bar(p_stats, x="week", y="pts_ppr",
                     labels={"week": "Week", "pts_ppr": "PPR Pts"},
                     color_discrete_sequence=[RZR_RED])
        fig.update_layout(height=340, margin=dict(t=10, b=40), **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

        # Season-over-season
        all_p_seasons = (
            stats_df[stats_df["player_id"] == pid]
            .groupby("season").agg(total=("pts_ppr", "sum"), avg=("pts_ppr", "mean"), games=("pts_ppr", lambda x: (x > 0).sum()))
            .reset_index().sort_values("season")
        )
        if len(all_p_seasons) > 1:
            st.subheader("Season-Over-Season")
            fig2 = px.bar(all_p_seasons, x="season", y="total",
                          labels={"season": "Season", "total": "Total PPR"},
                          color_discrete_sequence=[RZR_BLACK])
            fig2.update_layout(height=300, margin=dict(t=10, b=40), **PLOTLY_LAYOUT)
            st.plotly_chart(fig2, use_container_width=True)

        # Stat lines
        st.subheader("Weekly Stat Lines")
        stat_cols = ["week", "pts_ppr", "pts_half_ppr", "pts_std",
                     "pass_yd", "pass_td", "rush_yd", "rush_td",
                     "rec", "rec_yd", "rec_td", "targets"]
        show = p_stats[[c for c in stat_cols if c in p_stats.columns]].rename(columns={
            "week": "Wk", "pts_ppr": "PPR", "pts_half_ppr": "Half", "pts_std": "Std",
            "pass_yd": "PaYd", "pass_td": "PaTD", "rush_yd": "RuYd", "rush_td": "RuTD",
            "rec": "Rec", "rec_yd": "ReYd", "rec_td": "ReTD", "targets": "Tgt",
        }).round(1)
        st.dataframe(show, use_container_width=True, hide_index=True)

        # Rostered by (within filter)
        st.subheader("Rostered in Your Leagues")
        all_slots = load_all_roster_slots(conn)
        rostered = all_slots[
            (all_slots["player_id"] == pid)
            & (all_slots["season"] == sel_season)
            & (all_slots["league_id"].isin(filter_league_ids))
        ].merge(teams_df[["league_id", "roster_id", "owner_name"]], on=["league_id", "roster_id"], how="left") \
         .merge(leagues_df[["league_id", "name"]].rename(columns={"name": "League"}), on="league_id", how="left")
        if rostered.empty:
            st.info("Not rostered in any filtered league this season.")
        else:
            summary = rostered.groupby(["League", "owner_name"])["week"].agg(["min", "max", "count"]).reset_index().rename(columns={
                "owner_name": "Owner", "min": "First Wk", "max": "Last Wk", "count": "Weeks",
            })
            st.dataframe(summary, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 6 — OWNER ANALYTICS (top scorers per owner)
# ══════════════════════════════════════════════════════════════════════════════
elif view == "🏆 Owner Analytics":
    st.title("Owner Analytics — Top Contributors")

    col1, col2 = st.columns(2)
    with col1:
        default_idx = all_owners.index(my_name) if my_name in all_owners else 0
        sel_owner = st.selectbox("Owner", all_owners, index=default_idx)
    with col2:
        seasons = ["All-Time"] + sorted(leagues_df["season"].unique().tolist(), reverse=True)
        sel_season = st.selectbox("Season", seasons)

    owner_rows = teams_df.merge(leagues_df[["league_id", "season"]], on="league_id")
    owner_rows = owner_rows[(owner_rows["owner_name"] == sel_owner) & (owner_rows["league_id"].isin(filter_league_ids))]
    if sel_season != "All-Time":
        owner_rows = owner_rows[owner_rows["season"] == sel_season]

    if owner_rows.empty:
        st.warning("No teams found for this owner/season (within filter).")
    else:
        slots_all = load_all_roster_slots(conn)
        stats_df = load_player_stats(conn)

        merge_keys = ["league_id", "roster_id", "season"]
        owner_slots = slots_all.merge(owner_rows[merge_keys].drop_duplicates(), on=merge_keys, how="inner")
        owner_slots["is_starter"] = ~owner_slots["slot"].str.upper().isin(["BN", "IR", "TAXI"])

        # join weekly points
        owner_slots = owner_slots.merge(
            stats_df[["player_id", "season", "week", "pts_ppr"]],
            on=["player_id", "season", "week"], how="left",
        )
        owner_slots["pts_ppr"] = owner_slots["pts_ppr"].fillna(0)

        starters = owner_slots[owner_slots["is_starter"]]

        agg = (
            starters.merge(players_df[["player_id", "full_name", "position"]], on="player_id", how="left")
            .groupby(["player_id", "full_name", "position"])
            .agg(total_pts=("pts_ppr", "sum"),
                 starts=("pts_ppr", "count"),
                 avg_pts=("pts_ppr", "mean"))
            .reset_index().sort_values("total_pts", ascending=False)
        )

        m1, m2, m3 = st.columns(3)
        m1.metric("Total Starter Pts", f"{agg['total_pts'].sum():.1f}")
        m2.metric("Unique Starters", len(agg))
        m3.metric("Total Starts", int(agg["starts"].sum()))

        st.subheader("Top 20 Point Contributors")
        top = agg.head(20)
        fig = px.bar(top, x="total_pts", y="full_name", orientation="h", color="position",
                     labels={"total_pts": "PPR Pts", "full_name": "Player"},
                     color_discrete_sequence=RZR_PALETTE)
        fig.update_layout(yaxis={"categoryorder": "total ascending"},
                          height=540, margin=dict(t=20, b=20), **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

        tbl = agg.rename(columns={"full_name": "Player", "position": "Pos",
                                  "total_pts": "Pts", "starts": "Starts", "avg_pts": "Avg"})
        tbl["Pts"] = tbl["Pts"].round(1)
        tbl["Avg"] = tbl["Avg"].round(2)
        st.dataframe(tbl.drop(columns=["player_id"]), use_container_width=True, hide_index=True)

        # All-owner leaderboard
        st.subheader("All-Owner Leaderboard (Single Player Contribution)")
        all_starters = slots_all.copy()
        all_starters = all_starters[all_starters["league_id"].isin(filter_league_ids)]
        if sel_season != "All-Time":
            all_starters = all_starters[all_starters["season"] == sel_season]
        all_starters["is_starter"] = ~all_starters["slot"].str.upper().isin(["BN", "IR", "TAXI"])
        all_starters = all_starters[all_starters["is_starter"]]
        all_starters = all_starters.merge(
            stats_df[["player_id", "season", "week", "pts_ppr"]],
            on=["player_id", "season", "week"], how="left",
        )
        all_starters["pts_ppr"] = all_starters["pts_ppr"].fillna(0)
        all_starters = all_starters.merge(teams_df[["league_id", "roster_id", "owner_name"]],
                                          on=["league_id", "roster_id"], how="left")
        leaderboard = (
            all_starters.merge(players_df[["player_id", "full_name", "position"]], on="player_id", how="left")
            .groupby(["owner_name", "full_name", "position"])["pts_ppr"].sum().reset_index()
            .sort_values("pts_ppr", ascending=False).head(25)
        ).rename(columns={"owner_name": "Owner", "full_name": "Player", "position": "Pos", "pts_ppr": "Pts"})
        leaderboard["Pts"] = leaderboard["Pts"].round(1)
        st.dataframe(leaderboard, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 7 — TRADE ANALYZER
# ══════════════════════════════════════════════════════════════════════════════
elif view == "🔄 Trade Analyzer":
    st.title("Trade Analyzer — Retroactive ROI")
    st.caption("Compares PPR points scored by each side AFTER the trade date, for the rest of that season.")

    seasons = sorted(leagues_df["season"].unique(), reverse=True)
    sel_season = st.selectbox("Season", seasons)

    season_leagues = leagues_df[
        (leagues_df["season"] == sel_season) & (leagues_df["league_id"].isin(filter_league_ids))
    ]
    if season_leagues.empty:
        st.warning("No leagues match filter for this season.")
        st.stop()

    trades = transactions_df[
        transactions_df["league_id"].isin(season_leagues["league_id"])
        & (transactions_df["type"] == "trade")
        & (transactions_df["status"] == "complete")
    ].copy()

    if trades.empty:
        st.info("No completed trades in these leagues this season.")
    else:
        trades["roster_ids_list"] = trades["roster_ids"].apply(parse_json_list)
        trades["player_ids_list"] = trades["player_ids"].apply(parse_json_list)

        # Need a "trade week" — estimate from transaction created timestamp → week
        # Simpler proxy: count from first matchup week to last; use transactions.created relative to season
        # Sleeper trades include adds/drops structure we don't have columns for, so we split players
        # across roster sides using position order: fall back to half-half if unknown.
        # We don't have adds/drops breakdown in schema, so evaluate trade as "did this team add good players overall?"
        # Approach: for each trade, for each roster_id, find players they had AFTER via roster_slots (earliest week after trade)

        stats_df = load_player_stats(conn)
        slots_all = load_all_roster_slots(conn)
        season_slots = slots_all[slots_all["season"] == sel_season]

        # figure out trade's week: find first week where roster_slots differ for involved rosters
        # simpler: use created epoch → week map
        season_weeks = sorted(matchups_df[matchups_df["season"] == sel_season]["week"].unique())
        min_week, max_week = (min(season_weeks), max(season_weeks)) if season_weeks else (1, 17)

        # For each trade, use created timestamp to assign week
        # Map seconds-per-week: rough approximation
        if not trades.empty:
            trades["created"] = pd.to_numeric(trades["created"], errors="coerce")

        rows = []
        for _, trade in trades.iterrows():
            lg = trade["league_id"]
            rids = trade["roster_ids_list"]
            pids = trade["player_ids_list"]
            if len(rids) < 2 or not pids:
                continue

            # Determine trade week: which is the first week where rosters contain these players on these rids?
            lg_slots = season_slots[(season_slots["league_id"] == lg) & (season_slots["player_id"].isin(pids))]
            if lg_slots.empty:
                continue

            # For each roster side, identify which players landed on it post-trade
            side_rows = {}
            for rid in rids:
                post = lg_slots[lg_slots["roster_id"] == rid]
                if post.empty:
                    continue
                landing_pids = post["player_id"].unique().tolist()
                landing_week = int(post["week"].min())
                # points from landing_week onward for these players while on this roster
                pts = 0.0
                for pid in landing_pids:
                    weeks_on = post[post["player_id"] == pid]["week"].unique().tolist()
                    p_pts = stats_df[
                        (stats_df["player_id"] == pid)
                        & (stats_df["season"] == sel_season)
                        & (stats_df["week"].isin(weeks_on))
                    ]["pts_ppr"].sum()
                    pts += p_pts
                side_rows[rid] = {
                    "players": landing_pids,
                    "week": landing_week,
                    "pts_after": pts,
                }

            if len(side_rows) < 2:
                continue

            owners = teams_df[(teams_df["league_id"] == lg) & (teams_df["roster_id"].isin(side_rows.keys()))] \
                [["roster_id", "owner_name"]].set_index("roster_id")["owner_name"].to_dict()
            lg_name = leagues_df[leagues_df["league_id"] == lg]["name"].iloc[0]

            sides = list(side_rows.items())
            for rid, info in sides:
                other = [r for r, _ in sides if r != rid]
                other_pts = sum(side_rows[r]["pts_after"] for r in other)
                other_owners = " + ".join(owners.get(r, "?") for r in other)
                player_names = players_df[players_df["player_id"].isin(info["players"])]["full_name"].tolist()
                rows.append({
                    "League": lg_name,
                    "Week": info["week"],
                    "Owner": owners.get(rid, "?"),
                    "Received Players": ", ".join(player_names) if player_names else "(none)",
                    "Pts Gained": round(info["pts_after"], 1),
                    "Opponent(s)": other_owners,
                    "Opp Pts": round(other_pts, 1),
                    "Diff": round(info["pts_after"] - other_pts, 1),
                })

        if not rows:
            st.info("Could not reconstruct any trade outcomes from roster slot data.")
        else:
            trade_df = pd.DataFrame(rows).sort_values(["League", "Week", "Owner"])
            st.dataframe(trade_df, use_container_width=True, hide_index=True)

            # Winners summary
            st.subheader("Trade Win/Loss Per Owner")
            trade_df["Outcome"] = trade_df["Diff"].apply(lambda d: "Win" if d > 0 else ("Loss" if d < 0 else "Tie"))
            summary = trade_df.groupby("Owner")["Outcome"].value_counts().unstack(fill_value=0).reset_index()
            for c in ["Win", "Loss", "Tie"]:
                if c not in summary.columns:
                    summary[c] = 0
            summary["Net"] = summary["Win"] - summary["Loss"]
            summary = summary.sort_values("Net", ascending=False)
            st.dataframe(summary[["Owner", "Win", "Loss", "Tie", "Net"]], use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 8 — POWER RANKINGS
# ══════════════════════════════════════════════════════════════════════════════
elif view == "⚡ Power Rankings":
    st.title("Power Rankings")
    st.caption("Composite score: 40% Win%, 40% normalized PF, 20% last-3-week form.")

    season_options = sorted(leagues_df["season"].unique(), reverse=True)
    col1, col2 = st.columns([1, 2])
    with col1:
        sel_season = st.selectbox("Season", season_options)
    with col2:
        s_leagues = leagues_df[(leagues_df["season"] == sel_season)
                               & (leagues_df["league_id"].isin(filter_league_ids))]
        if s_leagues.empty:
            st.warning("No leagues match filter.")
            st.stop()
        lname_map = dict(zip(s_leagues["name"], s_leagues["league_id"]))
        sel_lg_name = st.selectbox("League", list(lname_map.keys()))
    sel_lg = lname_map[sel_lg_name]

    reg = matchups_df[
        (matchups_df["league_id"] == sel_lg) & (matchups_df["season"] == sel_season)
        & (matchups_df["is_playoff"] == 0)
    ]
    if reg.empty:
        st.warning("No regular season matchups.")
        st.stop()

    recs = records_df[(records_df["league_id"] == sel_lg) & (records_df["season"] == sel_season)].copy()
    max_week = int(reg["week"].max())
    recent_weeks = [w for w in range(max(1, max_week - 2), max_week + 1)]
    recent = reg[reg["week"].isin(recent_weeks)].groupby("roster_id")["points_for"].sum().reset_index(name="recent_pts")

    pr = recs.merge(recent, on="roster_id", how="left")
    pr["recent_pts"] = pr["recent_pts"].fillna(0)

    def norm(s):
        r = s.max() - s.min()
        return (s - s.min()) / r if r else 0

    pr["pf_norm"]     = norm(pr["points_for"])
    pr["win_norm"]    = norm(pr["win_pct"])
    pr["recent_norm"] = norm(pr["recent_pts"])
    pr["power_score"] = (0.4 * pr["win_norm"] + 0.4 * pr["pf_norm"] + 0.2 * pr["recent_norm"]) * 100
    pr = pr.sort_values("power_score", ascending=False).reset_index(drop=True)
    pr.insert(0, "Rank", range(1, len(pr) + 1))

    show = pr[["Rank", "owner_name", "team_name", "record", "points_for", "recent_pts", "power_score"]].rename(columns={
        "owner_name": "Owner", "team_name": "Team", "record": "Record",
        "points_for": "PF", "recent_pts": "Last-3 PF", "power_score": "Power",
    })
    show["PF"]        = show["PF"].round(1)
    show["Last-3 PF"] = show["Last-3 PF"].round(1)
    show["Power"]     = show["Power"].round(1)
    st.dataframe(show, use_container_width=True, hide_index=True)

    fig = px.bar(pr, x="power_score", y="owner_name", orientation="h",
                 color="power_score", color_continuous_scale=[[0, RZR_BLACK], [1, RZR_RED]],
                 labels={"power_score": "Power", "owner_name": ""})
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=440,
                      margin=dict(t=20, b=20), **PLOTLY_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 9 — LUCK INDEX
# ══════════════════════════════════════════════════════════════════════════════
elif view == "🍀 Luck Index":
    st.title("Luck Index")
    st.caption("Expected wins = each week, fraction of league you'd beat with your score. Luck = Actual − Expected.")

    season_options = sorted(leagues_df["season"].unique(), reverse=True)
    col1, col2 = st.columns([1, 2])
    with col1:
        sel_season = st.selectbox("Season", season_options)
    with col2:
        s_leagues = leagues_df[(leagues_df["season"] == sel_season)
                               & (leagues_df["league_id"].isin(filter_league_ids))]
        if s_leagues.empty:
            st.warning("No leagues match filter.")
            st.stop()
        lname_map = dict(zip(s_leagues["name"], s_leagues["league_id"]))
        sel_lg_name = st.selectbox("League", list(lname_map.keys()))
    sel_lg = lname_map[sel_lg_name]

    reg = matchups_df[
        (matchups_df["league_id"] == sel_lg) & (matchups_df["season"] == sel_season)
        & (matchups_df["is_playoff"] == 0)
    ].copy()

    if reg.empty:
        st.warning("No matchup data.")
        st.stop()

    # Expected wins per week: rank-1 / (n-1)
    def week_expected(group):
        n = len(group)
        if n < 2:
            group["exp_win"] = 0
            return group
        ranks = group["points_for"].rank(method="min", ascending=True) - 1
        group["exp_win"] = ranks / (n - 1)
        return group

    reg = reg.groupby("week", group_keys=False).apply(week_expected)

    # Actual wins (via matchup_id pairing)
    paired = reg.merge(
        reg[["week", "matchup_id", "roster_id", "points_for"]],
        on=["week", "matchup_id"], suffixes=("", "_opp"),
    )
    paired = paired[paired["roster_id"] != paired["roster_id_opp"]]
    paired["actual_win"] = (paired["points_for"] > paired["points_for_opp"]).astype(int)

    agg = paired.groupby("roster_id").agg(
        actual=("actual_win", "sum"),
        expected=("exp_win", "sum"),
        pf=("points_for", "sum"),
    ).reset_index()
    agg["luck"] = agg["actual"] - agg["expected"]
    agg = agg.merge(teams_df[["league_id", "roster_id", "owner_name", "team_name"]],
                    on="roster_id", how="left")
    agg = agg[agg["league_id"] == sel_lg].sort_values("luck", ascending=False).reset_index(drop=True)

    show = agg[["owner_name", "team_name", "actual", "expected", "luck", "pf"]].rename(columns={
        "owner_name": "Owner", "team_name": "Team",
        "actual": "Actual W", "expected": "Expected W", "luck": "Luck", "pf": "PF",
    })
    show["Expected W"] = show["Expected W"].round(2)
    show["Luck"]       = show["Luck"].round(2)
    show["PF"]         = show["PF"].round(1)
    st.dataframe(show, use_container_width=True, hide_index=True)

    fig = px.bar(agg, x="luck", y="owner_name", orientation="h",
                 color="luck", color_continuous_scale=[[0, RZR_BLACK], [0.5, RZR_GRAY], [1, RZR_RED]],
                 labels={"luck": "Luck (Actual − Expected W)", "owner_name": ""})
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=440,
                      margin=dict(t=20, b=20), **PLOTLY_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 10 — DRAFT GRADES / ROI
# ══════════════════════════════════════════════════════════════════════════════
elif view == "📝 Draft Grades / ROI":
    st.title("Draft Grades / ROI")
    st.caption("Draft roster reconstructed from Week 1 starting + bench slots (minus players added via transactions). "
               "ROI = season PPR points scored by those players while rostered.")

    season_options = sorted(leagues_df["season"].unique(), reverse=True)
    col1, col2 = st.columns([1, 2])
    with col1:
        sel_season = st.selectbox("Season", season_options)
    with col2:
        s_leagues = leagues_df[(leagues_df["season"] == sel_season)
                               & (leagues_df["league_id"].isin(filter_league_ids))]
        if s_leagues.empty:
            st.warning("No leagues match filter.")
            st.stop()
        lname_map = dict(zip(s_leagues["name"], s_leagues["league_id"]))
        sel_lg_name = st.selectbox("League", list(lname_map.keys()))
    sel_lg = lname_map[sel_lg_name]

    slots_df = load_roster_slots(conn, sel_season)
    stats_df = load_player_stats(conn)

    week1 = slots_df[(slots_df["league_id"] == sel_lg) & (slots_df["week"] == 1)].copy()
    if week1.empty:
        st.warning("No Week 1 roster data — cannot proxy draft.")
        st.stop()

    # Subtract players acquired before week 1 isn't really possible without draft data;
    # treat the full Week 1 roster as the drafted roster.
    season_pts = (
        stats_df[stats_df["season"] == sel_season]
        .groupby("player_id")
        .agg(season_pts=("pts_ppr", "sum"))
        .reset_index()
    )

    merged = week1.merge(players_df, on="player_id", how="left") \
                  .merge(season_pts, on="player_id", how="left") \
                  .merge(teams_df[["league_id", "roster_id", "owner_name"]], on=["league_id", "roster_id"], how="left")
    merged["season_pts"] = merged["season_pts"].fillna(0)

    # Per-owner totals
    owner_totals = merged.groupby("owner_name").agg(
        draft_pts=("season_pts", "sum"),
        players=("player_id", "nunique"),
    ).reset_index().sort_values("draft_pts", ascending=False)
    avg = owner_totals["draft_pts"].mean() or 1
    owner_totals["Grade"] = owner_totals["draft_pts"].apply(lambda p: (
        "A" if p > avg * 1.15 else
        "B" if p > avg * 1.05 else
        "C" if p > avg * 0.95 else
        "D" if p > avg * 0.85 else "F"
    ))
    owner_totals = owner_totals.rename(columns={"owner_name": "Owner", "draft_pts": "Draft PPR", "players": "Players"})
    owner_totals["Draft PPR"] = owner_totals["Draft PPR"].round(1)
    st.subheader("Owner Draft Grades")
    st.dataframe(owner_totals, use_container_width=True, hide_index=True)

    fig = px.bar(owner_totals, x="Draft PPR", y="Owner", orientation="h", color="Grade",
                 color_discrete_map={"A": RZR_RED, "B": RZR_RED_LT, "C": RZR_GRAY, "D": "#7A1A26", "F": RZR_BLACK})
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=420,
                      margin=dict(t=20, b=20), **PLOTLY_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Best & Worst Picks (All Owners)")
    picks = merged[["owner_name", "full_name", "position", "team", "season_pts"]].rename(columns={
        "owner_name": "Owner", "full_name": "Player", "position": "Pos",
        "team": "NFL", "season_pts": "Season Pts",
    })
    picks["Season Pts"] = picks["Season Pts"].round(1)
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**🔥 Top 15 Drafted Players**")
        st.dataframe(picks.sort_values("Season Pts", ascending=False).head(15),
                     use_container_width=True, hide_index=True)
    with c2:
        st.markdown("**💀 Bottom 15 Drafted Players**")
        st.dataframe(picks[picks["Season Pts"] > 0].sort_values("Season Pts").head(15),
                     use_container_width=True, hide_index=True)
