# scripts/prepare_raw_weekly.py
import os, json, gzip, pathlib, sys

def read_any(path):
    try:
        with open(path, "rb") as f:
            b = f.read()
    except FileNotFoundError:
        return None
    if not b or not b.strip():
        return None
    if b[:2] == b"\x1f\x8b":
        b = gzip.decompress(b)
    t = b.decode("utf-8", errors="replace").strip()
    # JSON first
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        # NDJSON fallback
        try:
            return [json.loads(ln) for ln in t.splitlines() if ln.strip()]
        except Exception:
            return None

def ensure_list(x):
    if isinstance(x, list): return x
    if isinstance(x, dict): return [x]
    return None

SEASON = int(os.environ.get("SEASON", "2025"))
WEEK   = int(os.environ.get("WEEK", "1"))
RAW_WEEKLY   = os.environ.get("RAW_WEEKLY", "").strip()
FALLBACK     = os.environ.get("FALLBACK_WEEKLY", "").strip()

candidates = [p for p in [
    RAW_WEEKLY, FALLBACK,
    f"week_{SEASON}_{WEEK}.json",   # old root-style name
    "latest_week.json"              # your existing file
] if p]

data = None
src  = None
for p in candidates:
    d = read_any(p)
    L = ensure_list(d) if d is not None else None
    if L:
        data, src = L, p
        break

if not data:
    sys.exit("No valid weekly JSON found. Provide RAW_WEEKLY/FALLBACK_WEEKLY or add week_{SEASON}_{WEEK}.json / latest_week.json")

# normalize a couple common aliases
def norm(r):
    r = dict(r)
    if "player_name" in r and "player" not in r: r["player"]   = r.pop("player_name")
    if "pos" in r and "position" not in r:       r["position"] = r.pop("pos")
    if "club" in r and "team" not in r:          r["team"]     = r.pop("club")
    r.setdefault("week", WEEK)
    return r

data = [norm(r) for r in data]

out_path = f"raw/weekly_usage_{SEASON}_w{WEEK:02}.json"
pathlib.Path("raw").mkdir(parents=True, exist_ok=True)
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False)
print(f"Wrote {out_path} from {src} with {len(data)} rows")
