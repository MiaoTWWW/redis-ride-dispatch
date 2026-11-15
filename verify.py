"""正確性自動檢查（壓力測試結束後執行，也可以單獨跑）

    python verify.py

檢查四件事：
  A. 重播 stream:events：任何時刻每位司機最多只有一筆行程、每筆行程最多只有一位司機
  B. 目前快照：司機 Hash、行程 Hash、空車 GEO 三者互相吻合
  C. 派單紀錄：每次派到的都是「候選名單中，扣掉已被別人搶走的之後，最近的那一台」
  D. 計數器：stats 的數字與實際行程數、Stream 長度一致
"""
import asyncio
import re
import sys
import time
from collections import Counter

import config as C


async def xrange_all(r, key):
    last = "-"
    while True:
        batch = await r.xrange(key, min=last, max="+", count=5000)
        if not batch:
            return
        for mid, f in batch:
            yield mid, f
        last = "(" + batch[-1][0]


async def run(r):
    lines, ok_all = [], True

    def check(ok, title, detail=""):
        nonlocal ok_all
        ok_all &= bool(ok)
        lines.append(f"  [{'PASS' if ok else 'FAIL'}] {title}" + (f"　{detail}" if detail else ""))

    # ── A. 重播事件 ──
    holder = {}        # driver -> (tid, state)
    trip_driver = {}   # tid -> driver
    ev_count = Counter()
    violations = []
    async for mid, e in xrange_all(r, C.STREAM_EVENTS):
        ev, tid, d = e["ev"], e["tid"], e["driver"]
        ev_count[ev] += 1
        cur = holder.get(d)
        if ev == "offer":
            if cur:
                violations.append(f"{mid} {d} 手上已有行程 #{cur[0]}（{cur[1]}），卻又被派 #{tid}")
            if tid in trip_driver:
                violations.append(f"{mid} 行程 #{tid} 已派給 {trip_driver[tid]}，卻又派給 {d}")
            holder[d], trip_driver[tid] = (tid, "offered"), d
        elif ev in ("accept", "pickup", "complete", "reject", "timeout"):
            need = {"accept": "offered", "pickup": "to_pickup", "complete": "on_trip",
                    "reject": "offered", "timeout": "offered"}[ev]
            if cur != (tid, need):
                violations.append(f"{mid} {d} 的 {ev} #{tid} 不合法，當時狀態是 {cur}")
            if ev == "accept":
                holder[d] = (tid, "to_pickup")
            elif ev == "pickup":
                holder[d] = (tid, "on_trip")
            else:
                holder.pop(d, None)
                trip_driver.pop(tid, None)
        elif ev == "offline":
            if cur:
                trip_driver.pop(cur[0], None)
            holder.pop(d, None)
    n_events = sum(ev_count.values())
    lines.append(f"A. 重播 {n_events:,} 筆事件（派單 {ev_count['offer']:,}、接單 {ev_count['accept']:,}、"
                 f"拒單 {ev_count['reject']:,}、逾時 {ev_count['timeout']:,}、完成 {ev_count['complete']:,}）")
    check(not violations, "沒有任何司機同時被派兩筆行程、沒有行程同時派給兩位司機",
          f"違規 {len(violations)} 筆")
    lines += [f"         {v}" for v in violations[:10]]

    # ── B. 目前快照 ──
    trips, drivers = {}, {}
    keys_t, keys_d = [], []
    async for k in r.scan_iter(match="trip:*", count=1000):
        if re.fullmatch(r"trip:\d+", k):
            keys_t.append(k)
    async for k in r.scan_iter(match="driver:*", count=1000):
        if not k.startswith("driver:alive:"):
            keys_d.append(k)
    for keys, store in ((keys_t, trips), (keys_d, drivers)):
        for i in range(0, len(keys), 500):
            pipe = r.pipeline(transaction=False)
            for k in keys[i:i + 500]:
                pipe.hgetall(k)
            for k, h in zip(keys[i:i + 500], await pipe.execute()):
                store[k.split(":", 1)[1]] = h

    active = ("offered", "to_pickup", "on_trip")
    bad = []
    used = Counter()
    for tid, t in trips.items():
        if t["status"] in active:
            used[t["driver"]] += 1
            d = drivers.get(t["driver"], {})
            if d.get("trip") != tid or d.get("status") != t["status"]:
                bad.append(f"行程 #{tid}（{t['status']}）指向 {t['driver']}，但司機是 {d.get('status')}/#{d.get('trip')}")
    for did, d in drivers.items():
        if d.get("status") in active and trips.get(d.get("trip"), {}).get("driver") != did:
            bad.append(f"{did}（{d['status']}）指向 #{d.get('trip')}，但行程的司機不是他")
    dup = [d for d, n in used.items() if n > 1]
    check(not dup, "B. 進行中的行程沒有共用司機", f"進行中 {sum(used.values())} 筆")
    check(not bad, "   司機 Hash 與行程 Hash 互相吻合", f"不一致 {len(bad)} 筆")
    lines += [f"         {v}" for v in bad[:10]]

    pool = set(await r.zrange(C.DRIVERS_AVAILABLE, 0, -1))
    not_free = [d for d in pool if drivers.get(d, {}).get("status") != "available"]
    check(not not_free, "   空車清單（drivers:available）裡沒有非空車", f"清單 {len(pool)} 台，異常 {len(not_free)} 台")

    # ── C. 派單紀錄 ──
    n_log = n_rule = n_atomic = n_raced = 0
    async for _, f in xrange_all(r, C.STREAM_DISPATCH_LOG):
        n_log += 1
        cands = f["cands"].split(",") if f["cands"] else []
        skipped = f["skipped"].split(",") if f["skipped"] else []
        n_raced += bool(skipped)
        n_rule += cands[:len(skipped)] == skipped and len(cands) > len(skipped) and cands[len(skipped)] == f["driver"]
        n_atomic += f["atomic_nearest"] == f["driver"]
    check(n_rule == n_log, "C. 每次派單都是候選名單中最近、且尚未被搶走的空車", f"{n_rule:,} / {n_log:,}")
    if n_log:
        lines.append(f"         其中 {n_raced:,} 次最近的車已被別的 dispatcher 搶走，自動換下一台")
        lines.append(f"         Lua 在派單瞬間重查：{n_atomic:,} 次（{n_atomic / n_log:.1%}）仍是全場最近空車；"
                     f"其餘是查詢到派單之間車輛移動了")

    # ── D. 計數器 ──
    stats = {k: int(v) for k, v in (await r.hgetall(C.STATS)).items()}
    by = Counter(t["status"] for t in trips.values())
    seq = int(await r.get(C.SEQ_TRIP) or 0)
    n_matched = sum(1 for t in trips.values() if "assigned_ts" in t)
    xlen_trips = await r.xlen(C.STREAM_TRIPS)
    lines.append("D. 計數器")
    check(stats.get("requests", 0) == len(trips) == seq == ev_count["request"], "   叫車數：stats = 行程 Hash 數 = 流水號 = 事件數",
          f"{stats.get('requests', 0):,} / {len(trips):,} / {seq:,} / {ev_count['request']:,}")
    check(stats.get("matched", 0) == n_matched, "   媒合數：stats = 有 assigned_ts 的行程數",
          f"{stats.get('matched', 0):,} / {n_matched:,}")
    check(stats.get("completed", 0) == by["completed"] == xlen_trips == ev_count["complete"],
          "   完成數：stats = 行程狀態 = stream:trips 長度 = 事件數",
          f"{stats.get('completed', 0):,} / {by['completed']:,} / {xlen_trips:,} / {ev_count['complete']:,}")
    check(stats.get("failed", 0) == by["failed"] == ev_count["fail"], "   失敗數：stats = 行程狀態 = 事件數",
          f"{stats.get('failed', 0):,} / {by['failed']:,} / {ev_count['fail']:,}")
    check(stats.get("offers", 0) == ev_count["offer"] == n_log, "   派單次數：stats = 事件數 = 派單紀錄數",
          f"{stats.get('offers', 0):,} / {ev_count['offer']:,} / {n_log:,}")
    check(stats.get("rejects", 0) == ev_count["reject"] and stats.get("timeouts", 0) == ev_count["timeout"],
          "   拒單與逾時次數：stats = 事件數",
          f"拒單 {stats.get('rejects', 0):,}、逾時 {stats.get('timeouts', 0):,}")

    matched = stats.get("matched", 0)
    lines.append("摘要")
    lines.append(f"   行程狀態分佈：" + "、".join(f"{k} {v:,}" for k, v in sorted(by.items())))
    if matched:
        lines.append(f"   平均等車 {stats.get('total_wait_ms', 0) / matched / 1000:.2f} 秒｜"
                     f"派單成功率 {matched / max(matched + stats.get('failed', 0), 1):.1%}｜"
                     f"總車資 {stats.get('total_fare', 0):,} 元")
    lines.append("")
    lines.append("結果：" + ("全部通過 ✔" if ok_all else "有項目未通過 ✘"))
    return ok_all, lines


async def main():
    C.utf8_console()
    r = C.get_redis()
    ok, lines = await run(r)
    text = "\n".join([f"正確性檢查報告　{time.strftime('%Y-%m-%d %H:%M:%S')}", "=" * 60, *lines])
    print(text)
    (C.BASE / "reports").mkdir(exist_ok=True)
    (C.BASE / "reports" / "verify_report.txt").write_text(text + "\n", encoding="utf-8")
    await r.aclose()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
