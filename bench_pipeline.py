"""小實驗：200 台車的位置回報，逐筆送 vs Pipeline 批次送，差多少？

    python bench_pipeline.py

使用獨立的 bench:* key，不會動到正式資料，結束後自動清除。
"""
import asyncio
import random
import time

import config as C

N, ROUNDS = 200, 20


async def main():
    C.utf8_console()
    r = C.get_redis()
    script = r.register_script((C.BASE / "lua" / "update_position.lua").read_text(encoding="utf-8"))
    cars = [(f"bench-{i}", f"{random.uniform(121.5, 121.58):.6f}", f"{random.uniform(25.02, 25.09):.6f}") for i in range(N)]
    keys = lambda cid: ["bench:all", "bench:available", f"bench:driver:{cid}", f"bench:alive:{cid}"]

    await script(keys=keys("warm"), args=["warm", "121.5", "25.05", 5])

    t = time.perf_counter()
    for _ in range(ROUNDS):
        for cid, lon, lat in cars:
            await script(keys=keys(cid), args=[cid, lon, lat, 5])
    single = (time.perf_counter() - t) / ROUNDS * 1000

    t = time.perf_counter()
    for _ in range(ROUNDS):
        pipe = r.pipeline(transaction=False)
        for cid, lon, lat in cars:
            await script(keys=keys(cid), args=[cid, lon, lat, 5], client=pipe)
        await pipe.execute()
    piped = (time.perf_counter() - t) / ROUNDS * 1000

    async for k in r.scan_iter(match="bench:*", count=1000):
        await r.delete(k)
    await r.aclose()

    text = (f"{N} 台車回報一輪位置（平均 {ROUNDS} 輪）\n"
            f"  逐筆送出（{N} 次網路往返）：{single:8.1f} ms\n"
            f"  Pipeline（1 次網路往返）  ：{piped:8.1f} ms\n"
            f"  加速 {single / piped:.1f} 倍")
    print(text)
    (C.BASE / "reports").mkdir(exist_ok=True)
    (C.BASE / "reports" / "pipeline_bench.txt").write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(main())
