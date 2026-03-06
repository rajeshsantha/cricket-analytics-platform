"""Build player-to-team mapping from Cricsheet JSON and save as JSON for dashboard."""
import json, glob, os

player_teams = {}
src = "/Users/rajeshsantha/Datasets/t20_wc_2026_json"
for f in sorted(glob.glob(os.path.join(src, "*.json"))):
    d = json.load(open(f))
    info = d.get("info", {})
    players = info.get("players", {})
    for team, plist in players.items():
        for p in plist:
            player_teams[p] = team  # last team seen (most recent match)

out = os.path.join(os.path.dirname(__file__), "data", "player_teams.json")
with open(out, "w") as fp:
    json.dump(player_teams, fp, indent=2, sort_keys=True)

print(f"Saved {len(player_teams)} player→team mappings to {out}")

