# reset_state.py — set last_id = 0 sesuai config.json
import json, os
CONF = "config.json"
with open(CONF,"r",encoding="utf-8") as f: c = json.load(f)
state_file = c["queue"].get("state_file",".harvest_ids_state.json")
with open(state_file,"w",encoding="utf-8") as f: json.dump({"last_id":0}, f)
print(f"[OK] {state_file} reset ke last_id=0")
