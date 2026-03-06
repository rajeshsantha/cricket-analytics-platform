"""Build player-to-team mapping from Cricsheet JSON and save as JSON for dashboard."""
import json, glob, os

# Try repo-local data first (CI), then fall back to local dev path
script_dir = os.path.dirname(os.path.abspath(__file__))
local_raw = os.path.join(script_dir, "data", "raw_json")
dev_raw = os.path.expanduser("~/Datasets/t20_wc_2026_json")

if os.path.isdir(local_raw) and glob.glob(os.path.join(local_raw, "*.json")):
    src = local_raw
elif os.path.isdir(dev_raw) and glob.glob(os.path.join(dev_raw, "*.json")):
    src = dev_raw
else:
    print("No JSON source found. Checked:")
    print(f"  1. {local_raw}")
    print(f"  2. {dev_raw}")
    exit(1)

player_teams = {}
for f in sorted(glob.glob(os.path.join(src, "*.json"))):
    d = json.load(open(f))
    info = d.get("info", {})
    players = info.get("players", {})
    for team, plist in players.items():
        for p in plist:
            player_teams[p] = team  # last team seen (most recent match)

out = os.path.join(script_dir, "data", "player_teams.json")
with open(out, "w") as fp:
    json.dump(player_teams, fp, indent=2, sort_keys=True)

print(f"Saved {len(player_teams)} player→team mappings to {out}")
print(f"  Source: {src} ({len(glob.glob(os.path.join(src, '*.json')))} files)")
