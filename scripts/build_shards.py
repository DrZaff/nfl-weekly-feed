#!/usr/bin/env python3
import json, os, pathlib, sys
from collections import defaultdict

# INPUTS: change these to match your raw files
RAW_SEASON = os.environ.get("RAW_SEASON", "raw/season_totals_2025.json")
RAW_WEEKLY = os.environ.get("RAW_WEEKLY", "raw/weekly_usage_2025_w03.json")
SEASON = int(os.environ.get("SEASON", "2025"))
WEEK = int(os.environ.get("WEEK", "3"))

OUT = pathlib.Path("public")
OUT_SEASON = OUT / f"s{SEASON}"
OUT_WEEK = OUT_SEASON / f"w{WEEK:02d}"
OUT_PLAYERS = OUT / "players"

for p in [OUT, OUT_SEASON, OUT_WEEK, OUT_PLAYERS]:
    p.mkdir(parents=True, exist_ok=True)

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def keep_min(row):
    # only keep fields you actually use downstream (keeps files small)
    fields = [
        "player_id","player","team","position",
        "ffpts_half_ppr","ffpts_per_game",
        "targets","receptions","rec_yds","rec_td",
        "rush_att","rush_yds","rush_td",
        "pass_att","pass_yds","pass_td","int",
        "routes","snaps","snap_pct","two_pt","fumbles_lost","week"
    ]
    out = {}
    for k in fields:
        if k in row and row[k] not in (None, ""):
            out[k] = row[k]
    return out

# Load raw data
season_rows = load_json(RAW_SEASON)
weekly_rows = load_json(RAW_WEEKLY)

# Split weekly by position
pos_order = ["QB","RB","WR","TE"]
pos_groups_week = {p: [] for p in pos_order}
for r in weekly_rows:
    p = r.get("position")
    if p in pos_groups_week:
        pos_groups_week[p].append(keep_min(r))

# Write weekly position shards
for pos, rows in pos_groups_week.items():
    (OUT_WEEK / f"weekly_usage_min_{pos}.json").write_text(
        json.dumps(rows, separators=(",",":")), encoding="utf-8"
    )

# Split season baselines by position
pos_groups_season = {p: [] for p in pos_order}
for r in season_rows:
    p = r.get("position")
    if p in pos_groups_season:
        pos_groups_season[p].append(keep_min(r))

for pos, rows in pos_groups_season.items():
    (OUT_SEASON / f"season_baselines_min_{pos}.json").write_text(
        json.dumps(rows, separators=(",",":")), encoding="utf-8"
    )

# Build per-player shards + name index
def norm(name):
    return name.lower().replace(".","").replace("'","").strip()

players_index = {}
season_by_id = {}
weekly_by_id = defaultdict(list)

for r in season_rows:
    pid = r.get("player_id")
    if pid:
        season_by_id[pid] = keep_min(r)
    name = r.get("player")
    if pid and name:
        players_index[norm(name)] = pid

for r in weekly_rows:
    pid = r.get("player_id")
    if pid:
        weekly_by_id[pid].append(keep_min(r))

for pid, srow in season_by_id.items():
    d = OUT_PLAYERS / pid
    d.mkdir(parents=True, exist_ok=True)
    (d / "season_min.json").write_text(json.dumps(srow, separators=(",",":")), encoding="utf-8")
    for wrow in weekly_by_id.get(pid, []):
        wk = wrow.get("week")
        if wk:
            (d / f"w{wk:02d}_min.json").write_text(json.dumps(wrow, separators=(",",":")), encoding="utf-8")

# Write players index
(OUT_PLAYERS / "index_min.json").write_text(
    json.dumps(players_index, separators=(",",":")), encoding="utf-8"
)

# meta.json
(OUT / "meta.json").write_text(
    json.dumps({"season": SEASON, "week": WEEK}, separators=(",",":")), encoding="utf-8"
)

# manifest.json with sizes
def fsize(p): return os.path.getsize(p)
manifest = {
  "season": SEASON,
  "week": WEEK,
  "shards": {
    "season_min": {pos: {
        "url": f"/s{SEASON}/season_baselines_min_{pos}.json",
        "bytes": fsize(OUT_SEASON / f"season_baselines_min_{pos}.json")
      } for pos in pos_order},
    "weekly_min": {f"w{WEEK:02d}": {pos: {
        "url": f"/s{SEASON}/w{WEEK:02d}/weekly_usage_min_{pos}.json",
        "bytes": fsize(OUT_WEEK / f"weekly_usage_min_{pos}.json")
      } for pos in pos_order}}
  }
}
(OUT / "manifest.json").write_text(json.dumps(manifest, separators=(",",":")), encoding="utf-8")

print("Shards built under /public/")
