# harvester_video_ids.py — pakai config.json + StringSession, harvest VIDEO IDs
import os, json, asyncio, time
from urllib.parse import urlparse

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from telethon.tl.functions.messages import ImportChatInviteRequest

CONF = "config.json"

def load_cfg():
    if not os.path.exists(CONF):
        raise SystemExit("config.json tidak ditemukan. Buat dari config.json.example.")
    with open(CONF, "r", encoding="utf-8") as f:
        return json.load(f)

def qpath(qdir, mid:int): 
    return os.path.join(qdir, f"{mid}.json")

def is_video(msg) -> bool:
    if getattr(msg, "video", None):
        return True
    doc = getattr(msg, "document", None)
    if not doc: 
        return False
    mt = (getattr(doc, "mime_type", "") or "").lower()
    if mt.startswith("video/"):
        return True
    # fallback cek ekstensi
    name = ""
    for attr in getattr(doc, "attributes", []) or []:
        name = getattr(attr, "file_name", "") or name
    name = (name or "").lower()
    return name.endswith((".mp4", ".mkv", ".mov", ".webm", ".avi"))

async def resolve_source(client, src_value:str, invite_link:str):
    # cast numeric -100...
    if src_value and src_value.startswith("-100") and src_value[4:].isdigit():
        try:
            return await client.get_entity(int(src_value))
        except Exception as e:
            print(f"[resolve] get_entity(int) gagal: {e}")
    # join via invite link
    if invite_link and invite_link.startswith("http"):
        path = urlparse(invite_link).path
        if path.startswith("/+"):
            code = path[2:]
            try:
                await client(ImportChatInviteRequest(code))
                print("[join] Berhasil join via invite link.")
            except UserAlreadyParticipantError:
                print("[join] Akun sudah jadi member.")
            except Exception as e:
                print(f"[join] Warning join: {e}")
        try:
            return await client.get_entity(invite_link)
        except Exception:
            pass
    # username/url publik
    if src_value:
        return await client.get_entity(src_value)
    raise SystemExit("Tidak bisa resolve sumber channel. Pastikan sudah join / set invite link.")

async def main():
    cfg = load_cfg()
    ub = cfg["userbot"]
    qcfg = cfg["queue"]

    api_id = int(ub["api_id"])
    api_hash = ub["api_hash"]
    string = ub["string_session"]
    if not string:
        raise SystemExit("userbot.string_session kosong. Jalankan: python3 get_string_session.py")

    queue_dir = qcfg.get("dir", "queue_ids")
    state_file = qcfg.get("state_file", ".harvest_ids_state.json")
    min_id = int(qcfg.get("min_id", 0))
    max_id = int(qcfg.get("max_id", 0))
    limit_per_run = int(qcfg.get("limit_per_run", 0))
    log_every = max(1, int(qcfg.get("log_every", 1)))

    os.makedirs(queue_dir, exist_ok=True)
    state = {"last_id": min_id}
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict): state.update(data)
        except Exception:
            pass

    async with TelegramClient(StringSession(string), api_id, api_hash) as client:
        src = await resolve_source(client, ub.get("source_channel","").strip(), ub.get("source_invite_link","").strip())

        print("== HARVEST VIDEO START ==")
        print(f"source={ub.get('source_channel') or ub.get('source_invite_link') or '<<resolved>>'}")
        print(f"offset(last_id)={state['last_id']}  min_id={min_id}  max_id={max_id or '∞'}")

        t0 = time.time()
        found = 0
        scanned = 0

        # gunakan min_id untuk ambil yang > last_id (lebih aman daripada offset_id)
        kwargs = dict(reverse=True, min_id=state["last_id"])
        if max_id > 0:
            kwargs["max_id"] = max_id

        try:
            async for m in client.iter_messages(src, **kwargs):
                state["last_id"] = m.id
                scanned += 1
                if not is_video(m):
                    if scanned % max(1, log_every * 10) == 0:
                        print(f"... scanned {scanned} messages, last_id={m.id}")
                    with open(state_file, "w", encoding="utf-8") as f: json.dump(state, f)
                    continue

                fp = qpath(queue_dir, m.id)
                if not os.path.exists(fp):
                    item = {"message_id": m.id, "caption": (m.message or "")[:1024]}
                    with open(fp, "w", encoding="utf-8") as f: json.dump(item, f, ensure_ascii=False)
                    found += 1
                    if found % log_every == 0:
                        print(f"HARVEST [{found}] msg_id={m.id} -> {fp}")
                    with open(state_file, "w", encoding="utf-8") as f: json.dump(state, f)
                    if limit_per_run and found >= limit_per_run:
                        print(f"LIMIT_PER_RUN reached: {found}")
                        break
        except FloodWaitError as fw:
            with open(state_file, "w", encoding="utf-8") as f: json.dump(state, f)
            secs = int(getattr(fw, "seconds", 5)) + 1
            print(f"[FloodWait] sleep {secs}s")
            await asyncio.sleep(secs)
        finally:
            with open(state_file, "w", encoding="utf-8") as f: json.dump(state, f)
            dt = time.time() - t0
            print(f"== HARVEST VIDEO DONE == new_queued={found} scanned={scanned} last_id={state['last_id']} time={dt:.1f}s")

if __name__ == "__main__":
    asyncio.run(main())
