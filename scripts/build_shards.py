# scripts/build_shards.py
import os, json, gzip, pathlib

def load_any(path):
    with open(path, "rb") as f: b = f.read()
    if b[:2] == b"\x1f\x8b": b = gzip.decompress(b)
    t = b.decode("utf-8", errors="replace").strip()
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        return [json.loads(ln) for ln in t.splitlines() if ln.strip()]

SEASON = int(os.environ.get("SEASON", "2025"))
WEEK   = int(os.environ.get("WEEK", "1"))
RAW_SEASON = os.environ.get("RAW_SEASON", f"raw/season_totals_{SEASON}.json")
RAW_WEEKLY = os.environ.get("RAW_WEEKLY", f"raw/weekly_usage_{SEASON}_w{WEEK:02}.json")

season_rows = load_any(RAW_SEASON)
weekly_rows = load_any(RAW_WEEKLY)
if not isinstance(season_rows, list): season_rows = [season_rows]
if not isinstance(weekly_rows, list): weekly_rows = [weekly_rows]

keep_season = ["player_id","player","team","position","games","ffpts","ffpts_per_game"]
keep_weekly = ["player_id","player","team","position","week",
               "targets","receptions","rec_yds","rec_td",
               "rush_att","rush_yds","rush_td",
               "pass_att","pass_yds","pass_td","int",
               "routes","snaps","two_pt","fumbles_lost"]

def prune(row, fields): return {k: row.get(k) for k in fields if k in row}
season_min = [prune(r, keep_season) for r in season_rows]
weekly_min = [prune(r, keep_weekly) for r in weekly_rows]

def split_by_pos(rows):
    out = {"QB":[], "RB":[], "WR":[], "TE":[]}
    for r in rows:
        pos = str(r.get("position","")).upper()
        if pos in out: out[pos].append(r)
    return out

pub  = pathlib.Path("public")
sdir = pub / f"s{SEASON}"
wdir = sdir / f"w{WEEK:02}"
wdir.mkdir(parents=True, exist_ok=True)

# season shards
for pos, rows in split_by_pos(season_min).items():
    (sdir / f"season_baselines_min_{pos}.json").write_text(
        json.dumps(rows, ensure_ascii=False))

# weekly shards
for pos, rows in split_by_pos(weekly_min).items():
    (wdir / f"weekly_usage_min_{pos}.json").write_text(
        json.dumps(rows, ensure_ascii=False))

# meta + manifest
(pub / "meta.json").write_text(json.dumps({"season": SEASON, "week": WEEK}))
manifest = {
  "season": SEASON, "week": WEEK,
  "paths": {
    "season": {p: f"s{SEASON}/season_baselines_min_{p}.json" for p in ["QB","RB","WR","TE"]},
    "weekly": {p: f"s{SEASON}/w{WEEK:02}/weekly_usage_min_{p}.json" for p in ["QB","RB","WR","TE"]}
  }
}
(pub / "manifest.json").write_text(json.dumps(manifest))
print("Shards written → public/")
