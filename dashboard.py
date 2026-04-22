"""
dashboard.py — BDW Fantasy Football Dashboard
==============================================
Single-league (Bring Dat Wood) analytics, optimized for clarity.

Run with:
    pip install streamlit plotly pandas
    streamlit run dashboard.py

Views:
  1. League Standings   — W/L records, weekly trends
  2. Head-to-Head       — click an opponent to compare vs. selected owner
  3. Waiver Adds        — pickups filterable by position
  4. Owner Analytics    — top scorers per owner
  5. Trade Analyzer     — retroactive trade ROI
  6. Power Rankings     — composite PF / win% / recent form
  7. Luck Index         — actual wins vs. all-play expected wins
  8. Draft Grades / ROI — season pts from earliest-week roster
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
LEAGUE_NAME = "Bring Dat Wood"

st.set_page_config(
    page_title="BDW Dashboard",
    page_icon="🪵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Theme: Light / Dark toggle ──────────────────────────────────────────────
if "theme" not in st.session_state:
    st.session_state.theme = "Light"

theme_choice = st.sidebar.radio("Theme", ["Light", "Dark"], horizontal=True,
                                index=0 if st.session_state.theme == "Light" else 1)
st.session_state.theme = theme_choice
DARK = theme_choice == "Dark"

# High-contrast palette, readable typography
if DARK:
    BG        = "#0F1116"
    SURFACE   = "#1A1D25"
    TEXT      = "#F5F5F5"
    MUTED     = "#A0A6B1"
    ACCENT    = "#4FC3F7"   # sky blue, easy on the eyes
    ACCENT_2  = "#FFB74D"   # amber
    POSITIVE  = "#66BB6A"
    NEGATIVE  = "#EF5350"
    BORDER    = "#2A2F3A"
else:
    BG        = "#FFFFFF"
    SURFACE   = "#F7F8FA"
    TEXT      = "#1A1A1A"
    MUTED     = "#5F6B7A"
    ACCENT    = "#1565C0"
    ACCENT_2  = "#E65100"
    POSITIVE  = "#2E7D32"
    NEGATIVE  = "#C62828"
    BORDER    = "#DDE2EA"

PALETTE = [ACCENT, ACCENT_2, POSITIVE, NEGATIVE, "#7E57C2", "#26A69A", "#EC407A"]

st.markdown(f"""
<style>
    html, body, .stApp {{
        background-color: {BG};
        color: {TEXT};
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }}
    [data-testid="stSidebar"] {{
        background-color: {SURFACE};
        border-right: 1px solid {BORDER};
    }}
    [data-testid="stSidebar"] * {{
        color: {TEXT} !important;
    }}
    h1, h2, h3, h4 {{
        color: {TEXT};
        font-weight: 700;
        letter-spacing: -0.01em;
    }}
    h1 {{ font-size: 1.9rem; margin-bottom: 0.3rem; }}
    h2 {{ font-size: 1.35rem; margin-top: 1.2rem; }}
    h3 {{ font-size: 1.1rem; }}
    [data-testid="stMetric"] {{
        background-color: {SURFACE};
        border: 1px solid {BORDER};
        border-radius: 6px;
        padding: 0.75rem 1rem;
    }}
    [data-testid="stMetricValue"] {{
        color: {ACCENT};
        font-weight: 700;
        font-size: 1.5rem;
    }}
    [data-testid="stMetricLabel"] {{ color: {MUTED}; font-size: 0.85rem; }}
    .stDataFrame {{ font-size: 0.95rem; }}
    .stDataFrame thead tr th {{
        background-color: {SURFACE} !important;
        color: {TEXT} !important;
        font-weight: 600 !important;
        border-bottom: 2px solid {ACCENT} !important;
    }}
    .stButton button {{
        background-color: {SURFACE};
        color: {TEXT};
        border: 1px solid {BORDER};
        border-radius: 6px;
        font-weight: 600;
        padding: 0.4rem 0.8rem;
        width: 100%;
    }}
    .stButton button:hover {{
        background-color: {ACCENT};
        color: {BG};
        border-color: {ACCENT};
    }}
    .stTabs [data-baseweb="tab"] {{ font-weight: 600; }}
    .stTabs [aria-selected="true"] {{
        color: {ACCENT} !important;
        border-bottom: 3px solid {ACCENT} !important;
    }}
    .bdw-opp-active button {{
        background-color: {ACCENT} !important;
        color: {BG} !important;
        border-color: {ACCENT} !important;
    }}
</style>
""", unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    plot_bgcolor=BG,
    paper_bgcolor=BG,
    font=dict(color=TEXT, family="-apple-system, Segoe UI, Roboto, sans-serif", size=13),
    colorway=PALETTE,
)

def style_axes(fig):
    fig.update_xaxes(gridcolor=BORDER, linecolor=BORDER)
    fig.update_yaxes(gridcolor=BORDER, linecolor=BORDER)
    return fig

# ─── DB Connection ────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

conn = get_conn()

# ─── Data Loaders (filtered to BDW) ───────────────────────────────────────────
@st.cache_data
def load_bdw_leagues(_conn):
    return pd.read_sql(
        "SELECT * FROM leagues WHERE name = ? ORDER BY season",
        _conn, params=(LEAGUE_NAME,)
    )

leagues_df = load_bdw_leagues(conn)
if leagues_df.empty:
    st.error(f"No '{LEAGUE_NAME}' data found in fantasy.db.")
    st.stop()

BDW_IDS = tuple(leagues_df["league_id"].tolist())
ID_MARKS = ",".join("?" * len(BDW_IDS))

@st.cache_data
def load_teams(_conn):
    return pd.read_sql(f"SELECT * FROM teams WHERE league_id IN ({ID_MARKS})", _conn, params=BDW_IDS)

@st.cache_data
def load_matchups(_conn):
    return pd.read_sql(f"SELECT * FROM matchups WHERE league_id IN ({ID_MARKS})", _conn, params=BDW_IDS)

@st.cache_data
def load_transactions(_conn):
    return pd.read_sql(f"SELECT * FROM transactions WHERE league_id IN ({ID_MARKS})", _conn, params=BDW_IDS)

@st.cache_data
def load_players(_conn):
    return pd.read_sql("SELECT player_id, full_name, position, team FROM players", _conn)

@st.cache_data
def load_player_stats(_conn):
    return pd.read_sql("SELECT * FROM player_stats", _conn)

@st.cache_data
def load_roster_slots_season(_conn, season):
    return pd.read_sql(
        f"SELECT league_id, roster_id, player_id, week, slot FROM roster_slots "
        f"WHERE season = ? AND league_id IN ({ID_MARKS})",
        _conn, params=(int(season), *BDW_IDS)
    )

@st.cache_data
def load_all_roster_slots(_conn):
    return pd.read_sql(
        f"SELECT league_id, roster_id, player_id, season, week, slot FROM roster_slots "
        f"WHERE league_id IN ({ID_MARKS})",
        _conn, params=BDW_IDS
    )

teams_df        = load_teams(conn)
matchups_df     = load_matchups(conn)
transactions_df = load_transactions(conn)
players_df      = load_players(conn)

def parse_json_list(val):
    try:
        return json.loads(val) if val else []
    except Exception:
        return []

# ─── Records ─────────────────────────────────────────────────────────────────
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
        .agg(wins=("win", "sum"), losses=("loss", "sum"), ties=("tie", "sum"),
             points_for=("points_for", "sum"),
             points_against=("points_for_opp", "sum"),
             avg_pts=("points_for", "mean"))
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
st.sidebar.markdown(f"<h1 style='margin-top:0;color:{ACCENT};font-size:1.5rem;'>🪵 BDW DASHBOARD</h1>",
                    unsafe_allow_html=True)
st.sidebar.caption(f"League: **{LEAGUE_NAME}**")
st.sidebar.markdown("---")

all_owners = sorted(teams_df["owner_name"].dropna().unique())
my_name = st.sidebar.selectbox("My Team (owner)", all_owners)

view = st.sidebar.radio(
    "View",
    [
        "📊 League Standings",
        "⚔️ Head-to-Head",
        "📈 Waiver Adds",
        "🏆 Owner Analytics",
        "🔄 Trade Analyzer",
        "⚡ Power Rankings",
        "🍀 Luck Index",
        "📝 Draft Grades / ROI",
    ],
)

st.sidebar.markdown("---")
st.sidebar.caption(f"DB: {os.path.basename(DB_PATH)}")

SEASONS = sorted(leagues_df["season"].unique().tolist(), reverse=True)

def season_to_league_id(season):
    row = leagues_df[leagues_df["season"] == season]
    return row["league_id"].iloc[0] if not row.empty else None

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 1 — LEAGUE STANDINGS
# ══════════════════════════════════════════════════════════════════════════════
if view == "📊 League Standings":
    st.title("League Standings")
    sel_season = st.selectbox("Season", SEASONS)
    sel_league_id = season_to_league_id(sel_season)

    df = (
        records_df[(records_df["season"] == sel_season) & (records_df["league_id"] == sel_league_id)]
        .copy().sort_values("wins", ascending=False).reset_index(drop=True)
    )

    if df.empty:
        st.warning("No matchup data for this season.")
    else:
        display_df = df[["owner_name", "team_name", "record", "wins", "losses",
                         "points_for", "points_against", "avg_pts"]].copy()
        display_df.columns = ["Owner", "Team", "Record", "W", "L", "PF", "PA", "Avg Pts"]
        for c in ["PF", "PA", "Avg Pts"]:
            display_df[c] = display_df[c].round(1)
        display_df.insert(0, "#", range(1, len(display_df) + 1))

        def highlight_me(row):
            if row["Owner"] == my_name:
                return [f"background-color: {ACCENT}; color: {BG}; font-weight: 600"] * len(row)
            return [""] * len(row)

        st.dataframe(display_df.style.apply(highlight_me, axis=1),
                     use_container_width=True, hide_index=True)

        st.subheader(f"Week-by-Week — {sel_season}")
        my_roster_ids = teams_df[
            (teams_df["league_id"] == sel_league_id) & (teams_df["owner_name"] == my_name)
        ]["roster_id"].tolist()

        reg = matchups_df[(matchups_df["league_id"] == sel_league_id)
                          & (matchups_df["season"] == sel_season)
                          & (matchups_df["is_playoff"] == 0)]

        if my_roster_ids and not reg.empty:
            my_weekly = reg[reg["roster_id"].isin(my_roster_ids)].sort_values("week")
            avg_weekly = reg.groupby("week")["points_for"].mean().reset_index(name="league_avg")
            chart_df = my_weekly.merge(avg_weekly, on="week", how="left")
            chart_df["points_for"] = chart_df["points_for"].round(1)
            chart_df["league_avg"] = chart_df["league_avg"].round(1)

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=chart_df["week"], y=chart_df["points_for"],
                mode="lines+markers", name=f"{my_name}",
                line=dict(color=ACCENT, width=3), marker=dict(size=9)))
            fig.add_trace(go.Scatter(x=chart_df["week"], y=chart_df["league_avg"],
                mode="lines", name="League Avg",
                line=dict(color=MUTED, width=1.5, dash="dash")))
            fig.update_layout(xaxis_title="Week", yaxis_title="Points",
                              legend=dict(orientation="h", y=1.1),
                              height=360, margin=dict(t=10, b=40), **PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 2 — HEAD-TO-HEAD (button-based opponent selection)
# ══════════════════════════════════════════════════════════════════════════════
elif view == "⚔️ Head-to-Head":
    st.title("Head-to-Head")

    owner_a = my_name
    st.markdown(f"**Comparing:** `{owner_a}` vs. &mdash; click an opponent:")

    # Opponents grid of buttons
    opponents = [o for o in all_owners if o != owner_a]
    if "h2h_opp" not in st.session_state or st.session_state.get("h2h_owner_a") != owner_a:
        st.session_state.h2h_opp = opponents[0] if opponents else None
        st.session_state.h2h_owner_a = owner_a

    cols_per_row = 6
    for i in range(0, len(opponents), cols_per_row):
        cols = st.columns(cols_per_row)
        for j, opp in enumerate(opponents[i:i+cols_per_row]):
            with cols[j]:
                active = opp == st.session_state.h2h_opp
                label = f"▶ {opp}" if active else opp
                if st.button(label, key=f"opp_{opp}"):
                    st.session_state.h2h_opp = opp
                    st.rerun()

    owner_b = st.session_state.h2h_opp
    if not owner_b:
        st.stop()

    st.markdown(f"### {owner_a}  vs.  {owner_b}")

    ta = teams_df[teams_df["owner_name"] == owner_a][["league_id", "roster_id"]].rename(columns={"roster_id": "rid_a"})
    tb = teams_df[teams_df["owner_name"] == owner_b][["league_id", "roster_id"]].rename(columns={"roster_id": "rid_b"})
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
        st.info(f"**{owner_a}** and **{owner_b}** have never faced each other.")
    else:
        h2h = pd.concat(h2h_rows)
        h2h["pts_a"] = h2h["pts_a"].round(1)
        h2h["pts_b"] = h2h["pts_b"].round(1)

        wins_a = (h2h["result"] == "W").sum()
        wins_b = (h2h["result"] == "L").sum()
        ties   = (h2h["result"] == "T").sum()
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric(f"{owner_a} W", int(wins_a))
        m2.metric(f"{owner_b} W", int(wins_b))
        m3.metric("Ties", int(ties))
        m4.metric("Games", len(h2h))
        m5.metric(f"{owner_a} Avg", f"{h2h['pts_a'].mean():.1f}")
        m6.metric(f"{owner_b} Avg", f"{h2h['pts_b'].mean():.1f}")

        st.markdown("---")

        max_val = max(h2h["pts_a"].max(), h2h["pts_b"].max()) + 15
        fig = px.scatter(h2h, x="pts_a", y="pts_b", color="result",
            hover_data=["season", "week"],
            labels={"pts_a": owner_a, "pts_b": owner_b, "result": "Result"},
            color_discrete_map={"W": POSITIVE, "L": NEGATIVE, "T": MUTED})
        fig.add_shape(type="line", x0=0, y0=0, x1=max_val, y1=max_val,
                      line=dict(dash="dash", color=MUTED, width=1))
        fig.update_layout(height=420, margin=dict(t=30), **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

        # History + drill-down
        st.subheader("Matchup History")
        h2h_sorted = h2h.sort_values(["season", "week"], ascending=[False, True]).reset_index(drop=True)
        display = h2h_sorted[["season", "week", "pts_a", "pts_b", "result"]].rename(columns={
            "pts_a": owner_a, "pts_b": owner_b, "result": "Result",
            "season": "Season", "week": "Week",
        })
        st.dataframe(display, use_container_width=True, hide_index=True)

        st.subheader("Drill Into a Matchup")
        option_labels = [
            f"{r['season']} W{r['week']} — {r['pts_a']:.1f} vs {r['pts_b']:.1f}"
            for _, r in h2h_sorted.iterrows()
        ]
        sel_idx = st.selectbox("Matchup", range(len(option_labels)), format_func=lambda i: option_labels[i])
        sel_row = h2h_sorted.iloc[sel_idx]

        slots_df = load_roster_slots_season(conn, int(sel_row["season"]))
        stats_df = load_player_stats(conn)
        week_stats = stats_df[(stats_df["season"] == int(sel_row["season"]))
                              & (stats_df["week"] == int(sel_row["week"]))]

        def roster_detail(rid, label, pts):
            rs = slots_df[(slots_df["league_id"] == sel_row["league_id"])
                          & (slots_df["roster_id"] == rid)
                          & (slots_df["week"] == int(sel_row["week"]))] \
                .merge(players_df, on="player_id", how="left") \
                .merge(week_stats[["player_id", "pts_ppr"]], on="player_id", how="left")
            if rs.empty:
                st.info(f"No roster data for {label}.")
                return
            rs["Starter"] = ~rs["slot"].str.upper().isin(["BN", "IR", "TAXI"])
            rs["pts_ppr"] = rs["pts_ppr"].fillna(0).round(1)
            rs = rs.sort_values(["Starter", "pts_ppr"], ascending=[False, False])
            show = rs[["slot", "full_name", "position", "team", "pts_ppr", "Starter"]].rename(columns={
                "slot": "Slot", "full_name": "Player", "position": "Pos",
                "team": "NFL", "pts_ppr": "Pts",
            })
            st.markdown(f"**{label}** — Total: {pts:.1f}")
            st.dataframe(show, use_container_width=True, hide_index=True)

        c1, c2 = st.columns(2)
        with c1: roster_detail(sel_row["rid_a"], owner_a, sel_row["pts_a"])
        with c2: roster_detail(sel_row["rid_b"], owner_b, sel_row["pts_b"])

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 3 — WAIVER ADDS
# ══════════════════════════════════════════════════════════════════════════════
elif view == "📈 Waiver Adds":
    st.title("Best Waiver & Free Agent Adds")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        default_idx = all_owners.index(my_name) if my_name in all_owners else 0
        sel_owner = st.selectbox("Owner", all_owners, index=default_idx)
    with col2:
        sel_season = st.selectbox("Season", SEASONS)
    with col3:
        scoring_label = st.selectbox("Scoring", ["PPR", "Half-PPR", "Standard"])
        scoring_col = {"PPR": "pts_ppr", "Half-PPR": "pts_half_ppr", "Standard": "pts_std"}[scoring_label]
    with col4:
        all_positions = ["QB", "RB", "WR", "TE", "K", "DEF"]
        sel_positions = st.multiselect("Positions", all_positions, default=all_positions)

    sel_league_id = season_to_league_id(sel_season)
    owner_teams = teams_df[(teams_df["owner_name"] == sel_owner) & (teams_df["league_id"] == sel_league_id)]

    if owner_teams.empty:
        st.warning("No team found for this owner/season.")
    else:
        roster_ids = owner_teams["roster_id"].tolist()
        txn = transactions_df[
            (transactions_df["league_id"] == sel_league_id)
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
                .agg(total_pts=(scoring_col, "sum"),
                     weeks_played=(scoring_col, lambda x: (x > 0).sum()),
                     avg_pts=(scoring_col, "mean"))
                .reset_index()
            )

            result = (
                txn_exp[["player_id", "type"]].drop_duplicates("player_id", keep="first")
                .merge(players_df[["player_id", "full_name", "position", "team"]], on="player_id", how="left")
                .merge(season_stats, on="player_id", how="left")
                .dropna(subset=["full_name"])
            )
            result = result[result["position"].isin(sel_positions)].sort_values("total_pts", ascending=False)

            if result.empty:
                st.warning("No results match your position filter.")
            else:
                top15 = result.head(15)
                fig = px.bar(top15, x="total_pts", y="full_name", orientation="h",
                    color="position",
                    labels={"total_pts": f"Season ({scoring_label})", "full_name": "Player"},
                    title=f"Top Adds — {sel_owner} {sel_season}",
                    color_discrete_sequence=PALETTE)
                fig.update_layout(yaxis={"categoryorder": "total ascending"},
                                  height=460, margin=dict(t=40, b=20), **PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

                table = result[["full_name", "position", "team", "type", "total_pts", "weeks_played", "avg_pts"]].rename(columns={
                    "full_name": "Player", "position": "Pos", "team": "NFL",
                    "type": "Add Type", "total_pts": "Season Pts",
                    "weeks_played": "Wks", "avg_pts": "Avg/Wk",
                })
                table["Season Pts"] = table["Season Pts"].round(1)
                table["Avg/Wk"]     = table["Avg/Wk"].round(1)
                st.dataframe(table, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 4 — OWNER ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════
elif view == "🏆 Owner Analytics":
    st.title("Owner Analytics — Top Contributors")

    col1, col2 = st.columns(2)
    with col1:
        default_idx = all_owners.index(my_name) if my_name in all_owners else 0
        sel_owner = st.selectbox("Owner", all_owners, index=default_idx)
    with col2:
        sel_season = st.selectbox("Season", ["All-Time"] + SEASONS)

    owner_rows = teams_df.merge(leagues_df[["league_id", "season"]], on="league_id")
    owner_rows = owner_rows[owner_rows["owner_name"] == sel_owner]
    if sel_season != "All-Time":
        owner_rows = owner_rows[owner_rows["season"] == sel_season]

    if owner_rows.empty:
        st.warning("No teams found for this owner/season.")
    else:
        slots_all = load_all_roster_slots(conn)
        stats_df = load_player_stats(conn)

        merge_keys = ["league_id", "roster_id", "season"]
        owner_slots = slots_all.merge(owner_rows[merge_keys].drop_duplicates(), on=merge_keys, how="inner")
        owner_slots["is_starter"] = ~owner_slots["slot"].str.upper().isin(["BN", "IR", "TAXI"])

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
                     color_discrete_sequence=PALETTE)
        fig.update_layout(yaxis={"categoryorder": "total ascending"},
                          height=540, margin=dict(t=20, b=20), **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

        tbl = agg.rename(columns={"full_name": "Player", "position": "Pos",
                                  "total_pts": "Pts", "starts": "Starts", "avg_pts": "Avg"})
        tbl["Pts"] = tbl["Pts"].round(1)
        tbl["Avg"] = tbl["Avg"].round(1)
        st.dataframe(tbl.drop(columns=["player_id"]), use_container_width=True, hide_index=True)

        # All-owner leaderboard
        st.subheader("League Leaderboard — Top Single-Player Contributions")
        all_starters = slots_all.copy()
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
# VIEW 5 — TRADE ANALYZER
# ══════════════════════════════════════════════════════════════════════════════
elif view == "🔄 Trade Analyzer":
    st.title("Trade Analyzer — Retroactive ROI")
    st.caption("PPR points each side scored from the players landing on their roster after the trade, for the rest of that season.")

    sel_season = st.selectbox("Season", SEASONS)
    sel_league_id = season_to_league_id(sel_season)

    trades = transactions_df[
        (transactions_df["league_id"] == sel_league_id)
        & (transactions_df["type"] == "trade")
        & (transactions_df["status"] == "complete")
    ].copy()

    if trades.empty:
        st.info("No completed trades this season.")
    else:
        trades["roster_ids_list"] = trades["roster_ids"].apply(parse_json_list)
        trades["player_ids_list"] = trades["player_ids"].apply(parse_json_list)

        stats_df = load_player_stats(conn)
        slots_all = load_all_roster_slots(conn)
        season_slots = slots_all[slots_all["season"] == sel_season]

        rows = []
        for _, trade in trades.iterrows():
            lg = trade["league_id"]
            rids = trade["roster_ids_list"]
            pids = trade["player_ids_list"]
            if len(rids) < 2 or not pids:
                continue

            lg_slots = season_slots[(season_slots["league_id"] == lg) & (season_slots["player_id"].isin(pids))]
            if lg_slots.empty:
                continue

            side_rows = {}
            for rid in rids:
                post = lg_slots[lg_slots["roster_id"] == rid]
                if post.empty:
                    continue
                landing_pids = post["player_id"].unique().tolist()
                landing_week = int(post["week"].min())
                pts = 0.0
                for pid in landing_pids:
                    weeks_on = post[post["player_id"] == pid]["week"].unique().tolist()
                    p_pts = stats_df[(stats_df["player_id"] == pid)
                                     & (stats_df["season"] == sel_season)
                                     & (stats_df["week"].isin(weeks_on))]["pts_ppr"].sum()
                    pts += p_pts
                side_rows[rid] = {"players": landing_pids, "week": landing_week, "pts_after": pts}

            if len(side_rows) < 2:
                continue

            owners = teams_df[(teams_df["league_id"] == lg) & (teams_df["roster_id"].isin(side_rows.keys()))] \
                [["roster_id", "owner_name"]].set_index("roster_id")["owner_name"].to_dict()

            sides = list(side_rows.items())
            for rid, info in sides:
                other = [r for r, _ in sides if r != rid]
                other_pts = sum(side_rows[r]["pts_after"] for r in other)
                other_owners = " + ".join(owners.get(r, "?") for r in other)
                player_names = players_df[players_df["player_id"].isin(info["players"])]["full_name"].tolist()
                rows.append({
                    "Week": info["week"],
                    "Owner": owners.get(rid, "?"),
                    "Received": ", ".join(player_names) if player_names else "(none)",
                    "Pts Gained": round(info["pts_after"], 1),
                    "Opponent(s)": other_owners,
                    "Opp Pts": round(other_pts, 1),
                    "Diff": round(info["pts_after"] - other_pts, 1),
                })

        if not rows:
            st.info("Could not reconstruct trade outcomes.")
        else:
            trade_df = pd.DataFrame(rows).sort_values(["Week", "Owner"])
            st.dataframe(trade_df, use_container_width=True, hide_index=True)

            st.subheader("Trade Win/Loss Per Owner")
            trade_df["Outcome"] = trade_df["Diff"].apply(lambda d: "Win" if d > 0 else ("Loss" if d < 0 else "Tie"))
            summary = trade_df.groupby("Owner")["Outcome"].value_counts().unstack(fill_value=0).reset_index()
            for c in ["Win", "Loss", "Tie"]:
                if c not in summary.columns:
                    summary[c] = 0
            summary["Net"] = summary["Win"] - summary["Loss"]
            summary = summary.sort_values("Net", ascending=False)
            st.dataframe(summary[["Owner", "Win", "Loss", "Tie", "Net"]],
                         use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 6 — POWER RANKINGS
# ══════════════════════════════════════════════════════════════════════════════
elif view == "⚡ Power Rankings":
    st.title("Power Rankings")
    st.caption("Composite: 40% Win%, 40% normalized PF, 20% last-3-week form.")

    sel_season = st.selectbox("Season", SEASONS)
    sel_lg = season_to_league_id(sel_season)

    reg = matchups_df[(matchups_df["league_id"] == sel_lg)
                      & (matchups_df["season"] == sel_season)
                      & (matchups_df["is_playoff"] == 0)]
    if reg.empty:
        st.warning("No regular season matchups.")
        st.stop()

    recs = records_df[(records_df["league_id"] == sel_lg) & (records_df["season"] == sel_season)].copy()
    max_week = int(reg["week"].max())
    recent_weeks = list(range(max(1, max_week - 2), max_week + 1))
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
    for c in ["PF", "Last-3 PF", "Power"]:
        show[c] = show[c].round(1)
    st.dataframe(show, use_container_width=True, hide_index=True)

    fig = px.bar(pr, x="power_score", y="owner_name", orientation="h",
                 color="power_score", color_continuous_scale=[[0, MUTED], [1, ACCENT]],
                 labels={"power_score": "Power", "owner_name": ""})
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=440,
                      margin=dict(t=20, b=20), **PLOTLY_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 7 — LUCK INDEX
# ══════════════════════════════════════════════════════════════════════════════
elif view == "🍀 Luck Index":
    st.title("Luck Index")
    st.caption("Expected wins = each week, the fraction of the league you'd beat with your score. Luck = Actual − Expected.")

    sel_season = st.selectbox("Season", SEASONS)
    sel_lg = season_to_league_id(sel_season)

    reg = matchups_df[(matchups_df["league_id"] == sel_lg)
                      & (matchups_df["season"] == sel_season)
                      & (matchups_df["is_playoff"] == 0)].copy()

    if reg.empty:
        st.warning("No matchup data for this season.")
        st.stop()

    # Expected wins per week via all-play rank
    reg["exp_win"] = 0.0
    for wk, grp in reg.groupby("week"):
        n = len(grp)
        if n < 2:
            continue
        ranks = grp["points_for"].rank(method="min", ascending=True) - 1
        reg.loc[grp.index, "exp_win"] = ranks / (n - 1)

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
    agg = agg.merge(
        teams_df[teams_df["league_id"] == sel_lg][["roster_id", "owner_name", "team_name"]],
        on="roster_id", how="left",
    ).sort_values("luck", ascending=False).reset_index(drop=True)

    if agg.empty:
        st.warning("Could not compute luck for this season.")
        st.stop()

    show = agg[["owner_name", "team_name", "actual", "expected", "luck", "pf"]].rename(columns={
        "owner_name": "Owner", "team_name": "Team",
        "actual": "Actual W", "expected": "Expected W", "luck": "Luck", "pf": "PF",
    })
    show["Expected W"] = show["Expected W"].round(1)
    show["Luck"]       = show["Luck"].round(1)
    show["PF"]         = show["PF"].round(1)
    st.dataframe(show, use_container_width=True, hide_index=True)

    fig = px.bar(agg, x="luck", y="owner_name", orientation="h",
                 color="luck", color_continuous_scale=[[0, NEGATIVE], [0.5, MUTED], [1, POSITIVE]],
                 labels={"luck": "Luck (Actual − Expected W)", "owner_name": ""})
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=440,
                      margin=dict(t=20, b=20), **PLOTLY_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# VIEW 8 — DRAFT GRADES / ROI
# ══════════════════════════════════════════════════════════════════════════════
elif view == "📝 Draft Grades / ROI":
    st.title("Draft Grades / ROI")
    st.caption("Draft roster proxied from earliest available week's roster. ROI = season PPR points for those players.")

    sel_season = st.selectbox("Season", SEASONS)
    sel_lg = season_to_league_id(sel_season)

    slots_df = load_roster_slots_season(conn, sel_season)
    slots_df = slots_df[slots_df["league_id"] == sel_lg]
    stats_df = load_player_stats(conn)

    if slots_df.empty:
        st.warning("No roster slot data for this season.")
        st.stop()

    earliest_week = int(slots_df["week"].min())
    st.caption(f"Using Week {earliest_week} rosters as draft proxy.")
    week1 = slots_df[slots_df["week"] == earliest_week].copy()

    season_pts = (
        stats_df[stats_df["season"] == sel_season]
        .groupby("player_id").agg(season_pts=("pts_ppr", "sum")).reset_index()
    )

    merged = (week1
              .merge(players_df, on="player_id", how="left")
              .merge(season_pts, on="player_id", how="left")
              .merge(teams_df[["league_id", "roster_id", "owner_name"]],
                     on=["league_id", "roster_id"], how="left"))
    merged["season_pts"] = merged["season_pts"].fillna(0)

    if merged["owner_name"].isna().all():
        st.warning("Could not join rosters to owners.")
        st.stop()

    owner_totals = merged.groupby("owner_name").agg(
        draft_pts=("season_pts", "sum"),
        players=("player_id", "nunique"),
    ).reset_index().sort_values("draft_pts", ascending=False)

    avg = owner_totals["draft_pts"].mean() or 1
    def grade(p):
        if p > avg * 1.15: return "A"
        if p > avg * 1.05: return "B"
        if p > avg * 0.95: return "C"
        if p > avg * 0.85: return "D"
        return "F"
    owner_totals["Grade"] = owner_totals["draft_pts"].apply(grade)
    owner_totals = owner_totals.rename(columns={"owner_name": "Owner", "draft_pts": "Draft PPR", "players": "Players"})
    owner_totals["Draft PPR"] = owner_totals["Draft PPR"].round(1)

    st.subheader("Owner Draft Grades")
    st.dataframe(owner_totals, use_container_width=True, hide_index=True)

    fig = px.bar(owner_totals, x="Draft PPR", y="Owner", orientation="h", color="Grade",
                 color_discrete_map={"A": POSITIVE, "B": ACCENT, "C": MUTED, "D": ACCENT_2, "F": NEGATIVE})
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
