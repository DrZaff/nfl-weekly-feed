# build_week.py (robust fetcher)
import datetime as dt, io, json, sys, gzip, requests, pandas as pd

def current_season(today=None):
    today = today or dt.date.today()
    # NFL season is labeled by the year it *starts* (Sept–Dec into next year)
    return today.year if today.month >= 9 else today.year - 1

def read_csv_maybe_gz(content: bytes, url: str) -> pd.DataFrame:
    """Read CSV; if the file is gzipped, decompress and read."""
    try:
        return pd.read_csv(io.StringIO(content.decode("utf-8")))
    except UnicodeDecodeError:
        try:
            text = gzip.decompress(content).decode("utf-8")
            return pd.read_csv(io.StringIO(text))
        except Exception as e:
            raise RuntimeError(f"Could not parse CSV from {url}: {e}")

def fetch_weekly_df(season: int) -> pd.DataFrame:
    # Try the known nflverse release name variants for weekly player stats
    candidates = [
        f"https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_{season}.csv",
        f"https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_{season}.csv.gz",
        f"https://github.com/nflverse/nflverse-data/releases/download/player_stats/stats_player_week_{season}.csv",
        f"https://github.com/nflverse/nflverse-data/releases/download/player_stats/stats_player_week_{season}.csv.gz",
    ]
    last_err = None
    for url in candidates:
        try:
            print(f"Trying {url}")
            r = requests.get(url, timeout=60)
            if r.status_code != 200:
                print(f"...status {r.status_code}")
                continue
            df = read_csv_maybe_gz(r.content, url)
            print(f"Fetched from {url} with shape {df.shape}")
            return df
        except Exception as e:
            last_err = e
            print(f"Failed {url}: {e}", file=sys.stderr)
    raise SystemExit(f"Could not fetch weekly stats for season {season}. Last error: {last_err}")

def main():
    season = current_season()
    df = fetch_weekly_df(season)

    # Prefer regular season rows if present
    src = df
    if "season_type" in df.columns:
        reg = df[df["season_type"].astype(str).str.upper().str.startswith("REG")]
        if len(reg):
            src = reg

    if "week" not in src.columns:
        raise SystemExit("No 'week' column in data.")

    latest_week = int(src["week"].max())
    week_df = src[src["week"] == latest_week].copy()

    week_df.to_json("latest_week.json", orient="records")
    with open("meta.json", "w") as f:
        json.dump({"season": int(season), "week": int(latest_week), "rows": int(len(week_df))}, f)
    week_df.to_json(f"week_{season}_{latest_week}.json", orient="records")
    print(f"Wrote latest_week.json/meta.json for S{season} W{latest_week} ({len(week_df)} rows).")

if __name__ == "__main__":
    main()
