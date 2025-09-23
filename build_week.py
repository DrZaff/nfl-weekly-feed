# build_week.py — weekly + season totals (Polars-only; JSON via json.dump)
import datetime as dt
import json
import polars as pl
import nflreadpy as nfl  # loader for nflverse data

# ---------- helpers ----------
def current_season(today=None):
    today = today or dt.date.today()
    # NFL season label = year it starts (Sept–Dec into next year)
    return today.year if today.month >= 9 else today.year - 1

def colexpr(df: pl.DataFrame, names: list[str], default: float = 0.0) -> pl.Expr:
    """Return the first matching numeric column as Float; otherwise 0 literal."""
    for n in names:
        if n in df.columns:
            return pl.col(n).cast(pl.Float64, strict=False)
    return pl.lit(default)

def strexpr(df: pl.DataFrame, names: list[str], default: str = "") -> pl.Expr:
    """Return the first matching string column; otherwise empty-string literal."""
    for n in names:
        if n in df.columns:
            return pl.col(n).cast(pl.Utf8, strict=False)
    return pl.lit(default)

# ---------- main ----------
def main():
    season = current_season()
    df = nfl.load_player_stats([season], summary_level="week")  # Polars DataFrame

    # Prefer REG season rows when present
    if "season_type" in df.columns:
        reg = df.filter(
            pl.col("season_type")
              .cast(pl.Utf8, strict=False)
              .str.to_uppercase()
              .str.starts_with("REG")
        )
        if reg.height > 0:
            df = reg

    # Latest completed week
    if "week" not in df.columns:
        raise SystemExit("No 'week' column in data.")
    latest_week = int(df.select(pl.col("week").max()).item())

    # ---- WEEKLY OUTPUT ----
    week_df = df.filter(pl.col("week") == latest_week)
    with open("latest_week.json", "w") as f:
        json.dump(week_df.to_dicts(), f)
    with open("meta.json", "w") as f:
        json.dump({"season": int(season), "week": latest_week, "rows": int(week_df.height)}, f)
    with open(f"week_{season}_{latest_week}.json", "w") as f:
        json.dump(week_df.to_dicts(), f)

    # ---- Half-PPR fantasy scoring expression added to every weekly row ----
    pass_yds = colexpr(df, ["pass_yds", "passing_yards", "pass_yards"])
    pass_td  = colexpr(df, ["pass_td", "passing_tds"])
    interceptions = colexpr(df, ["int", "interceptions", "interceptions_thrown"])

    rush_yds = colexpr(df, ["rush_yds", "rushing_yards"])
    rush_td  = colexpr(df, ["rush_td", "rushing_tds"])

    rec = colexpr(df, ["receptions", "rec"])
    rec_yds = colexpr(df, ["rec_yds", "receiving_yards"])
    rec_td  = colexpr(df, ["rec_td", "receiving_tds"])

    fumbles_lost = colexpr(df, ["fumbles_lost", "fum_lost"])

    # 2-pt conversions (sum whichever columns exist)
    two_pt = (
        colexpr(df, ["two_pt", "two_point_conversions"])
        + colexpr(df, ["two_pt_pass", "two_point_pass"])
        + colexpr(df, ["two_pt_rush", "two_point_rush"])
        + colexpr(df, ["two_pt_rec", "two_point_rec"])
    )

    ff_expr = (
        0.04 * pass_yds + 4 * pass_td - 2 * interceptions
        + 0.1 * rush_yds + 6 * rush_td
        + 0.1 * rec_yds + 6 * rec_td + 0.5 * rec
        + 2 * two_pt
        - 2 * fumbles_lost
    )
    df = df.with_columns(ff_expr.alias("ffpts_half_ppr"))

    # ---- Normalize ID/name/team/position for grouping ----
    df = df.with_columns([
        strexpr(df, ["player_id", "gsis_id", "pfr_player_id", "nflverse_id"]).alias("player_id"),
        strexpr(df, ["player", "player_name", "name"]).alias("player"),
        strexpr(df, ["recent_team", "team", "team_abbr", "player_team"]).alias("team"),
        strexpr(df, ["position", "pos"]).alias("position"),
    ])

    # Keep only rows up to latest_week
    df_to_date = df.filter(pl.col("week") <= latest_week)

    # ---- STANDARDIZE NUMERIC COLUMNS (turn literals into real columns) ----
    df_std = df_to_date.with_columns([
        pass_yds.alias("_pass_yds"),
        pass_td.alias("_pass_td"),
        interceptions.alias("_int"),
        rush_yds.alias("_rush_yds"),
        rush_td.alias("_rush_td"),
        rec.alias("_receptions"),
        rec_yds.alias("_rec_yds"),
        rec_td.alias("_rec_td"),
        fumbles_lost.alias("_fumbles_lost"),
        two_pt.alias("_two_pt"),
    ])

    # ---- CUMULATIVE SEASON TOTALS ----
    season_totals = (
        df_std
        .group_by(["player_id", "player", "team", "position"])
        .agg([
            pl.len().alias("games"),
            pl.col("_pass_yds").sum().alias("pass_yds"),
            pl.col("_pass_td").sum().alias("pass_td"),
            pl.col("_int").sum().alias("int"),
            pl.col("_rush_yds").sum().alias("rush_yds"),
            pl.col("_rush_td").sum().alias("rush_td"),
            pl.col("_receptions").sum().alias("receptions"),
            pl.col("_rec_yds").sum().alias("rec_yds"),
            pl.col("_rec_td").sum().alias("rec_td"),
            pl.col("_fumbles_lost").sum().alias("fumbles_lost"),
            pl.col("_two_pt").sum().alias("two_pt"),
            pl.col("ffpts_half_ppr").sum().alias("ffpts_half_ppr"),
        ])
        .with_columns((pl.col("ffpts_half_ppr") / pl.col("games")).alias("ffpts_per_game"))
        .sort("ffpts_half_ppr", descending=True)
    )
    with open("season_totals.json", "w") as f:
        json.dump(season_totals.to_dicts(), f)

    # ---- Top-10 by position (by FFPTS per game) ----
    top10 = {}
    for pos in ["QB", "RB", "WR", "TE"]:
        top10[pos] = (
            season_totals
            .filter(pl.col("position") == pos)
            .sort("ffpts_per_game", descending=True)
            .head(10)
            .to_dicts()
        )
    with open("season_top10.json", "w") as f:
        json.dump(top10, f)

    print(
        f"Wrote: latest_week.json, meta.json, week_{season}_{latest_week}.json, "
        f"season_totals.json, season_top10.json (S{season}, through W{latest_week})"
    )

if __name__ == "__main__":
    main()
