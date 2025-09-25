#!/usr/bin/env python3
"""
Build a compact players index (Top N) from season baselines shards.

- Reads public/meta.json for current season and week.
- Loads season shards: public/s{season}/season_baselines_min_{QB|RB|WR|TE}.json
- Computes a robust per-game score (half-PPR when fields exist; otherwise falls back
  to common "ffpts_per_game" style fields if present).
- Ranks across all positions and writes Top N to public/players/index.json:
  [{ id, name, pos, team, aliases: [] }, ...]

Usage:
  python scripts/build_players_index.py --top 200
"""

from __future__ import annotations
import json
import re
import argparse
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "public"

POSITIONS = ["QB", "RB", "WR", "TE"]

# --- Helpers -----------------------------------------------------------------

def load_json(p: Path) -> Any:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_json(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def norm_pos(x: str | None) -> str | None:
    if not x:
        return None
    x = x.upper()
    return x if x in {"QB","RB","WR","TE"} else None

def first_non_null(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def coalesce_number(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

# Generate simple alias forms: "First Last" → ["F. Last","Last"]
def make_aliases(full_name: str) -> List[str]:
    name = re.sub(r"\s+", " ", full_name.strip())
    parts = name.split(" ")
    aliases = []
    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        aliases.append(f"{first[0]}. {last}")
        aliases.append(last)
    return sorted(set(a for a in aliases if a.lower() != full_name.lower()))

# Robust half-PPR per-game scoring
def half_ppr_points_per_game(row: Dict[str, Any]) -> float:
    # Many shards carry a per-game field already; prefer those if present
    pg_candidates = [
        "ffpts_per_game", "fpts_pg", "half_ppr_pg", "ppr_points_per_game",
        "fantasy_points_per_game", "points_per_game"
    ]
    pg_val = first_non_null(row, pg_candidates)
    if pg_val is not None:
        return coalesce_number(pg_val)

    # Else compute per-game from raw totals if available
    # Try to find games played
    gp = first_non_null(row, ["games", "gp", "g", "games_played"], 0) or 0
    gp = coalesce_number(gp)

    # Accept common aliases
    get = lambda *keys: coalesce_number(first_non_null(row, list(keys), 0))

    pass_yds = get("pass_yds","passing_yards")
    pass_td  = get("pass_td","passing_tds")
    ints     = get("int","ints","interceptions")

    rush_yds = get("rush_yds","rushing_yards")
    rush_td  = get("rush_td","rushing_tds")

    rec_yds  = get("rec_yds","receiving_yards")
    rec_td   = get("rec_td","receiving_tds")
    rec      = get("receptions","rec","catches")

    two_pt   = get("two_pt","two_ptm","two_point_conversions")
    fumbles  = get("fumbles_lost","fum_lost","fumbles")

    # Half-PPR total
    total = (
        0.04 * pass_yds + 4.0 * pass_td - 2.0 * ints +
        0.10 * rush_yds + 6.0 * rush_td +
        0.10 * rec_yds  + 6.0 * rec_td  + 0.5 * rec +
        2.0 * two_pt - 2.0 * fumbles
    )
    if gp > 0:
        return total / gp
    return total  # fallback if games missing

def row_id(row: Dict[str, Any]) -> str | None:
    return first_non_null(row, ["player_id","id","gsis_id","pfr_id","nfl_id"])

def row_name(row: Dict[str, Any]) -> str | None:
    return first_non_null(row, ["name","player_name","full_name","display_name"])

def row_pos(row: Dict[str, Any]) -> str | None:
    return norm_pos(first_non_null(row, ["pos","position","position_group"]))

def row_team(row: Dict[str, Any]) -> str:
    team = first_non_null(row, ["team","team_abbr","recent_team","nfl_team"], "")
    return team or "FA"

# --- Main build ---------------------------------------------------------------

def collect_season_rows(season: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    base = PUBLIC / f"s{season}"
    for pos in POSITIONS:
        p = base / f"season_baselines_min_{pos}.json"
        if not p.exists():
            print(f"WARNING: missing {p}")
            continue
        try:
            data = load_json(p)
            if isinstance(data, list):
                for r in data:
                    rid = row_id(r)
                    nm = row_name(r)
                    rp = row_pos(r) or pos  # default to file pos if missing
                    if not rid or not nm or not rp:
                        continue
                    score = half_ppr_points_per_game(r)
                    rows.append({
                        "_score": score,
                        "id": rid,
                        "name": nm,
                        "pos": rp,
                        "team": row_team(r),
                    })
        except Exception as e:
            print(f"ERROR reading {p}: {e}")
    return rows

def dedupe_best(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Keep the highest score per player id (handles duplicates across files)
    best: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        rid = r["id"]
        if rid not in best or r["_score"] > best[rid]["_score"]:
            best[rid] = r
    return list(best.values())

def build_index(top_n: int) -> List[Dict[str, Any]]:
    meta_p = PUBLIC / "meta.json"
    meta = load_json(meta_p)
    season = int(meta.get("season"))
    rows = collect_season_rows(season)
    rows = dedupe_best(rows)
    rows.sort(key=lambda r: r["_score"], reverse=True)
    top = rows[:top_n]

    index = []
    for r in top:
        name = r["name"].strip()
        entry = {
            "id": r["id"],
            "name": name,
            "pos": r["pos"],
            "team": r["team"],
            "aliases": make_aliases(name),
        }
        index.append(entry)
    return index

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=200, help="How many players to include")
    args = ap.parse_args()

    idx = build_index(args.top)
    out_path = PUBLIC / "players" / "index.json"
    save_json(out_path, idx)
    print(f"Wrote {len(idx)} players → {out_path}")

if __name__ == "__main__":
    main()
