# build_week.py
# Fetch weekly NFL player stats from nflverse and publish "latest_week.json" and "meta.json".
# Beginner-safe: no env vars; works on free GitHub Actions.

import datetime as dt
import io
import json
import sys
import requests
import pandas as pd

def current_season(today=None):
    """NFL season is labeled by the year it starts (e.g., 2025 season runs into 2026)."""
    today = today or dt.date.today()
    return today.year if today.month >= 9 else today.year - 1

def fetch_weekly_df(season: int) -> pd.DataFrame:
    # nflverse weekly player stats (CSV) hosted on GitHub releases.
    # Example path pattern is documented/linked across nflverse resources.
    url = f"https://github.com/nflverse/nflverse-data/releases/download/player_stats/stats_player_week_{season}.csv"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

def main():
    season = current_season()
    df = fetch_weekly_df(season)

    # Prefer regular season when available; fall back gracefully (preseason/postseason).
    if "season_type" in df.columns:
        reg_df = df[df["season_type"].astype(str).str.upper().str.startswith("REG")]
        week_source = reg_df if len(reg_df) else df
    else:
        week_source = df

    if "week" not in week_source.columns:
        print("No 'week' column found in data. Exiting.", file=sys.stderr)
        sys.exit(1)

    latest_week = int(week_source["week"].max())
    week_df = week_source[week_source["week"] == latest_week].copy()

    # Keep everything (simple for beginners). Your GPT can pick the columns it wants.
    # If you later want a slimmer file, you can select a subset of columns here.

    # Write outputs
    week_df.to_json("latest_week.json", orient="records")
    meta = {"season": int(season), "week": int(latest_week), "rows": int(len(week_df))}
    with open("meta.json", "w") as f:
        json.dump(meta, f)

    # Also keep a historical copy (optional but handy)
    week_df.to_json(f"week_{season}_{latest_week}.json", orient="records")

    print(f"Wrote latest_week.json and meta.json for season {season}, week {latest_week} with {len(week_df)} rows.")

if __name__ == "__main__":
    main()
