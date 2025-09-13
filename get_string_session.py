# get_string_session.py — generate Telethon StringSession (sekali saja)
import json, os
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

CONF = "config.json"
if not os.path.exists(CONF):
    raise SystemExit("config.json tidak ditemukan. Copy dari config.json.example dan isi api_id/api_hash.")

with open(CONF, "r", encoding="utf-8") as f:
    cfg = json.load(f)

api_id = int(cfg["userbot"]["api_id"])
api_hash = cfg["userbot"]["api_hash"]

print("Login untuk membuat StringSession (OTP akan dikirim ke Telegram).")
with TelegramClient(StringSession(), api_id, api_hash) as client:
    string = client.session.save()
    print("\n=== STRING SESSION ===")
    print(string)
    print("======================\n")
    # simpan otomatis ke config.json
    cfg["userbot"]["string_session"] = string
    with open(CONF, "w", encoding="utf-8") as w:
        json.dump(cfg, w, indent=2)
    print("StringSession disimpan ke config.json → userbot.string_session")
