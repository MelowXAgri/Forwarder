# bot_copy_from_ids.py — baca config.json, copyMessage tanpa label
import os, json, glob, time, random, asyncio
from telegram.ext import ApplicationBuilder
from telegram.error import RetryAfter, TimedOut

CONF = "config.json"

def load_cfg():
    if not os.path.exists(CONF):
        raise SystemExit("config.json tidak ditemukan.")
    with open(CONF, "r", encoding="utf-8") as f:
        return json.load(f)

def iter_queue(qdir):
    files = sorted(glob.glob(os.path.join(qdir,"*.json")), key=lambda p: int(os.path.basename(p).split(".")[0]))
    for p in files: 
        yield p

async def main():
    cfg = load_cfg()
    b = cfg["bot"]
    qdir = cfg["queue"]["dir"]

    token = b["token"]
    src_id = int(b["source_channel_id"])
    dst_id = int(b["dest_channel_id"])
    base_delay_ms   = int(b.get("base_delay_ms", 1000))
    jitter_ms       = int(b.get("jitter_ms", 400))
    per_min_cap     = int(b.get("per_min_cap", 50))
    daily_cap       = int(b.get("daily_cap", 1500))
    batch_size      = int(b.get("batch_size", 1000))
    batch_sleep_sec = int(b.get("batch_sleep_sec", 1800))
    log_every       = int(b.get("log_every", 1))

    app = ApplicationBuilder().token(token).build()
    bot = app.bot

    total = len(list(iter_queue(qdir)))
    print(f"== COPY START == items_in_queue={total} src={src_id} dst={dst_id}")

    state = {"sent_today":0,"last_day":0,"sent_this_minute":0,"minute_epoch":0}
    def tick_caps():
        day = int(time.time()//86400)
        if state["last_day"]!=day: state.update({"last_day":day,"sent_today":0})
        minute = int(time.time()//60)
        if state["minute_epoch"]!=minute: state.update({"minute_epoch":minute,"sent_this_minute":0})

    sent_total = sent_batch = 0
    t0 = time.time()

    try:
        for q in iter_queue(qdir):
            tick_caps()
            if daily_cap and state["sent_today"]>=daily_cap:
                print("[cap] daily cap reached"); break
            if per_min_cap and state["sent_this_minute"]>=per_min_cap:
                wait = 60 - (time.time()%60)
                print(f"[cap] per-minute reached, sleep {wait:.1f}s")
                await asyncio.sleep(wait+0.5); tick_caps()
            try:
                with open(q,"r",encoding="utf-8") as f: item=json.load(f)
                mid = int(item["message_id"])

                await bot.copy_message(chat_id=dst_id, from_chat_id=src_id, message_id=mid)

                os.remove(q)
                state["sent_today"]+=1; state["sent_this_minute"]+=1
                sent_total+=1; sent_batch+=1
                if sent_total % max(1,log_every) == 0:
                    print(f"COPY [{sent_total}/{total}] msg_id={mid} today={state['sent_today']} this_min={state['sent_this_minute']}")

                if sent_batch>=batch_size:
                    print(f"[batch] {batch_size} copied, sleep {batch_sleep_sec/60:.0f} min")
                    sent_batch = 0
                    await asyncio.sleep(batch_sleep_sec)

                delay = base_delay_ms + random.randint(-jitter_ms, jitter_ms)
                await asyncio.sleep(max(0.3, delay/1000.0))
            except RetryAfter as e:
                print(f"[429] RetryAfter {e.retry_after}s"); await asyncio.sleep(e.retry_after+1)
            except TimedOut:
                print("[timeout] retry"); await asyncio.sleep(1.0)
            except Exception as e:
                print(f"[copy error] {q}: {e}"); await asyncio.sleep(1.0)
    finally:
        print(f"== COPY DONE == sent_total={sent_total} time={time.time()-t0:.1f}s")
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
