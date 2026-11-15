"""壓力測試：在指定秒數內送出 N 筆叫車，等全部行程結束後自動檢查正確性。

    python simulate_riders.py --n 1000 --duration 60

需要先啟動 Redis、dispatcher（建議 2 個以上）與 simulator（建議 --cars 200 --speed 1200）。
"""
import argparse
import asyncio
import random
import sys
import time

import config as C
import verify
from rides import create_ride, random_route

DONE = {"completed", "failed", "aborted"}


async def main():
    C.utf8_console()
    ap = argparse.ArgumentParser(description="叫車壓力測試")
    ap.add_argument("--n", type=int, default=1000, help="叫車筆數")
    ap.add_argument("--duration", type=float, default=60, help="在幾秒內送完")
    ap.add_argument("--wait", type=float, default=300, help="送完後最多等幾秒讓行程跑完")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    r = C.get_redis()
    await C.ensure_group(r)
    await C.ensure_stations(r)
    stations = C.load_stations()

    online = await r.zcard(C.DRIVERS_ALL)
    consumers = await r.xinfo_consumers(C.STREAM_REQUESTS, C.GROUP)
    active = [c["name"] for c in consumers if c["idle"] < 5000]
    print(f"在線車輛 {online} 台｜dispatcher {len(active)} 個 {active}")
    if not online or not active:
        print("請先啟動 simulator.py 與 dispatcher.py 再跑壓力測試。")
        sys.exit(2)

    print(f"開始：{args.duration:g} 秒內送出 {args.n} 筆叫車（每秒約 {args.n / args.duration:.1f} 筆）")
    tids, lat_ms = [], []
    t0 = time.perf_counter()

    async def one(i):
        await asyncio.sleep(max(0.0, i * args.duration / args.n - (time.perf_counter() - t0)))
        s = time.perf_counter()
        ride = await create_ride(r, f"stress-{i}", *random_route(stations, rng), ratelimit=False)
        lat_ms.append((time.perf_counter() - s) * 1000)
        tids.append(ride["tid"])

    async def progress():
        while True:
            await asyncio.sleep(5)
            print(f"  {time.perf_counter() - t0:5.0f}s 已送出 {len(tids)} 筆")

    p = asyncio.create_task(progress())
    await asyncio.gather(*(one(i) for i in range(args.n)))
    p.cancel()
    sent_s = time.perf_counter() - t0
    lat_ms.sort()
    print(f"送出完成：{len(tids)} 筆，耗時 {sent_s:.1f} 秒｜叫車 API 延遲 中位數 {lat_ms[len(lat_ms) // 2]:.1f} ms、"
          f"P99 {lat_ms[int(len(lat_ms) * .99) - 1]:.1f} ms")

    print("等待所有行程結束…")
    deadline = time.perf_counter() + args.wait
    while True:
        pipe = r.pipeline(transaction=False)
        for t in tids:
            pipe.hget(C.k_trip(t), "status")
        left = sum(1 for s in await pipe.execute() if s not in DONE)
        print(f"  {time.perf_counter() - t0:5.0f}s 尚未結束 {left} 筆")
        if left == 0 or time.perf_counter() > deadline:
            break
        await asyncio.sleep(5)
    total_s = time.perf_counter() - t0

    ok, lines = await verify.run(r)
    head = [f"壓力測試報告　{time.strftime('%Y-%m-%d %H:%M:%S')}", "=" * 60,
            f"條件：{args.n} 筆叫車 / {args.duration:g} 秒｜車輛 {online} 台｜dispatcher {len(active)} 個",
            f"叫車延遲：中位數 {lat_ms[len(lat_ms) // 2]:.1f} ms、P99 {lat_ms[int(len(lat_ms) * .99) - 1]:.1f} ms｜"
            f"全部行程結束耗時 {total_s:.0f} 秒" + ("" if left == 0 else f"（仍有 {left} 筆未結束）"), ""]
    text = "\n".join(head + lines)
    print("\n" + text)
    (C.BASE / "reports").mkdir(exist_ok=True)
    (C.BASE / "reports" / "stress_report.txt").write_text(text + "\n", encoding="utf-8")
    await r.aclose()
    sys.exit(0 if ok and left == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
