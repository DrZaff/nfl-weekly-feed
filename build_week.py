# build_week.py - use nflreadpy to grab weekly player stats reliably
import datetime as dt
import json
import pandas as pd
import nflreadpy as nfl  # official python loader for nflverse data

def current_season(today=None):
    today = today or dt.date.today()
    # NFL season labeled by the year it starts (Sept–Dec into next year)
    return today.year if today.month >= 9 else today.year - 1

def main():
    season = current_season()

    # nflreadpy mirrors nflreadr::load_player_stats(); default summary_level is "week".
    # It handles the correct release names for us.
    df_pl = nfl.load_player_stats([season], summary_level="week")  # returns a Polars DataFrame
    df = df_pl.to_pandas()  # convert to pandas for simple filtering / JSON export

    # Prefer regular season rows when available
    src = df
    if "season_type" in df.columns:
        reg = df[df["season_type"].astype(str).str.upper().str.startswith("REG")]
        if not reg.empty:
            src = reg

    if "week" not in src.columns:
        raise SystemExit("No 'week' column in data.")

    latest_week = int(src["week"].max())
    week_df = src[src["week"] == latest_week].copy()

    # Write outputs for your GPT
    week_df.to_json("latest_week.json", orient="records")
    with open("meta.json", "w") as f:
        json.dump({"season": int(season), "week": int(latest_week), "rows": int(len(week_df))}, f)
    week_df.to_json(f"week_{season}_{latest_week}.json", orient="records")

    print(f"Wrote latest_week.json/meta.json for S{season} W{latest_week} ({len(week_df)} rows).")

if __name__ == "__main__":
    main()
