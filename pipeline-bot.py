# pipeline-bot.py — otomatis: copy -> sleep -> harvest (config.json)
import os, json, asyncio, time

CONF = "config.json"
def cfg():
    with open(CONF,"r",encoding="utf-8") as f: return json.load(f)

async def run_stage(cmd, name):
    print(f"\n=== Stage: {name} ===")
    proc = await asyncio.create_subprocess_exec("python3", cmd,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    async for line in proc.stdout:
        print(line.decode().rstrip())
    rc = await proc.wait()
    print(f"=== {name} selesai (rc={rc}) ===\n")
    return rc

async def main():
    while True:
        c = cfg()
        sleep_sec = int(c["bot"].get("batch_sleep_sec", 1800))
        rc = await run_stage("bot_copy_from_ids.py", "COPY")
        if rc!=0:
            print("copy error; stop."); break
        print(f"Bot sleep {sleep_sec/60:.0f} menit. Saat bot sleep, harvester isi batch berikut.")
        t0 = time.time()
        while time.time()-t0 < sleep_sec:
            rc = await run_stage("harvester_video_ids.py", "HARVEST")
            if rc!=0:
                print("harvest error; stop."); break
            await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")

