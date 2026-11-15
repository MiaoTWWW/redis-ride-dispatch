"""司機模擬器（cmd 文字模式）

同時模擬 N 台車：每秒移動一步、用 Pipeline 批次回報位置與心跳、
收到派單後依機率接單／拒單／不理會，接單後開去載客再送到目的地。

用法：
    python simulator.py --cars 100 --speed 150
執行中可輸入指令（輸入 help 看說明）。
"""
import argparse
import asyncio
import json
import math
import random
import sys
import threading
import time

import config as C

CODE = {"available": "a", "offered": "o", "to_pickup": "p", "on_trip": "t"}

HELP = """指令：
  add N        增加 N 台車
  remove N     讓 N 台空車正常下線
  kill ID      讓某台車「斷線」（停止心跳，5 秒後由 TTL 過期偵測），例：kill car-003
  rate P       設定接單率 0~1（其餘一半拒單、一半不理會等逾時），例：rate 0.5
  speed V      設定車速 km/h
  status       立即顯示統計
  quit         全部下線並結束"""


class Car:
    __slots__ = ("id", "lon", "lat", "target", "status", "trip", "pickup", "dropoff",
                 "decide_at", "decision", "deciding")

    def __init__(self, cid, lon, lat):
        self.id, self.lon, self.lat = cid, lon, lat
        self.target = None
        self.status, self.trip = "available", ""
        self.pickup = self.dropoff = None
        self.decide_at, self.decision, self.deciding = 0.0, "", ""

    def move(self, meters):
        """朝 target 直線前進 meters 公尺。回傳是否已抵達。"""
        if not self.target:
            return True
        tlon, tlat = self.target
        dist = C.haversine_km(self.lon, self.lat, tlon, tlat) * 1000
        if dist <= meters or dist < 1:
            self.lon, self.lat = tlon, tlat
            return True
        f = meters / dist
        self.lon += (tlon - self.lon) * f
        self.lat += (tlat - self.lat) * f
        return False


class Simulator:
    def __init__(self, args):
        self.args = args
        self.speed = args.speed
        self.accept_rate = args.accept
        self.prefix = args.prefix
        self.cars: dict[str, Car] = {}
        self.dead: set[str] = set()
        self.seq = 0
        self.stations = C.load_stations()
        self.counts = {"accept": 0, "reject": 0, "ignore": 0, "pickup": 0, "complete": 0}
        self.tick_ms = 0.0
        self.running = True
        self.cmds: asyncio.Queue = asyncio.Queue()

    # ── 小工具 ──
    def random_point(self):
        if random.random() < 0.7:  # 多數空車在捷運站附近繞
            _, lon, lat = random.choice(self.stations)
            lon, lat = lon + random.gauss(0, 0.006), lat + random.gauss(0, 0.006)
        else:
            lon = random.uniform(C.BBOX[0], C.BBOX[2])
            lat = random.uniform(C.BBOX[1], C.BBOX[3])
        return min(max(lon, C.BBOX[0]), C.BBOX[2]), min(max(lat, C.BBOX[1]), C.BBOX[3])

    def log(self, msg):
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    async def add_cars(self, n):
        pipe = self.r.pipeline(transaction=False)
        for _ in range(n):
            self.seq += 1
            cid = f"{self.prefix}-{self.seq:03d}"
            car = Car(cid, *self.random_point())
            car.target = self.random_point()
            self.cars[cid] = car
            pipe.hsetnx(C.k_driver(cid), "rating", round(random.uniform(4.2, 5.0), 1))
        await pipe.execute()
        self.log(f"上線 {n} 台車，目前共 {len(self.cars)} 台")

    async def go_offline(self, car):
        for _ in range(3):
            tid = await self.r.hget(C.k_driver(car.id), "trip") or ""
            if await self.lua.offline(car.id, tid, force=True) != -1:
                break
        self.cars.pop(car.id, None)

    async def load_trip(self, car, tid):
        f = await self.r.hmget(C.k_trip(tid), "pickup_lon", "pickup_lat", "dropoff_lon", "dropoff_lat")
        if None in f:
            return False
        car.trip = tid
        car.pickup, car.dropoff = (float(f[0]), float(f[1])), (float(f[2]), float(f[3]))
        return True

    # ── 每秒一次 ──
    async def tick(self, dt):
        step = self.speed / 3.6 * dt
        arrived = {}
        live = [c for c in self.cars.values() if c.id not in self.dead]
        for car in live:
            arrived[car.id] = car.move(step)

        # 1) Pipeline：N 台車的位置 + 心跳，只要 1 次網路往返
        t0 = time.perf_counter()
        pipe = self.r.pipeline(transaction=False)
        for car in live:
            await self.lua.update_position(car.id, f"{car.lon:.6f}", f"{car.lat:.6f}", client=pipe)
        results = await pipe.execute()
        self.tick_ms = (time.perf_counter() - t0) * 1000

        # 2) 依 Redis 回傳的狀態決定每台車下一步
        now = time.monotonic()
        for car, (status, trip) in zip(live, results):
            prev = car.status
            car.status = status
            if status == "available":
                car.trip, car.deciding = "", ""
                if prev != "available" or arrived[car.id]:
                    car.target = self.random_point()
            elif status == "offered":
                car.target = None  # 停車看訂單
                if car.deciding != trip:
                    car.deciding = trip
                    car.decide_at = now + random.uniform(1.0, 3.0)
                    x = random.random()
                    rest = (1 - self.accept_rate) / 2
                    car.decision = "accept" if x < self.accept_rate else ("reject" if x < self.accept_rate + rest else "ignore")
                    if car.decision == "ignore":
                        self.counts["ignore"] += 1
                        self.log(f"{car.id} 收到行程 #{trip}，沒有回應（等 {C.OFFER_TTL} 秒逾時）")
                elif car.decision != "ignore" and now >= car.decide_at and car.decision:
                    decision, car.decision = car.decision, ""
                    if decision == "accept":
                        if await self.lua.accept(car.id, trip) == 1 and await self.load_trip(car, trip):
                            car.status, car.target = "to_pickup", car.pickup
                            self.counts["accept"] += 1
                            self.log(f"{car.id} 接單 #{trip}，前往上車點")
                    else:
                        if await self.lua.release(car.id, trip, "reject") == 1:
                            self.counts["reject"] += 1
                            self.log(f"{car.id} 拒絕 #{trip}")
            elif status == "to_pickup":
                if car.trip != trip and not await self.load_trip(car, trip):
                    continue
                car.target = car.pickup
                if (car.lon, car.lat) == car.pickup and await self.lua.pickup(car.id, trip) == 1:
                    car.status, car.target = "on_trip", car.dropoff
                    self.counts["pickup"] += 1
            elif status == "on_trip":
                if car.trip != trip and not await self.load_trip(car, trip):
                    continue
                car.target = car.dropoff
                if (car.lon, car.lat) == car.dropoff and await self.lua.complete(car.id, trip) == 1:
                    car.status, car.trip = "available", ""
                    car.target = self.random_point()
                    self.counts["complete"] += 1
                    self.log(f"{car.id} 完成 #{trip}")

        # 3) Pub/Sub：整批位置一次推給地圖
        payload = {"type": "map", "ts": int(time.time() * 1000),
                   "cars": [[c.id, round(c.lon, 5), round(c.lat, 5), CODE.get(c.status, "a")] for c in live]}
        await self.r.publish(C.CH_MAP, json.dumps(payload, separators=(",", ":")))

    def status_line(self):
        by = {"available": 0, "offered": 0, "to_pickup": 0, "on_trip": 0}
        for c in self.cars.values():
            if c.id not in self.dead:
                by[c.status] = by.get(c.status, 0) + 1
        k = self.counts
        return (f"車輛 {len(self.cars) - len(self.dead)}｜空車 {by['available']} 待接單 {by['offered']} "
                f"前往載客 {by['to_pickup']} 載客中 {by['on_trip']}｜累計 接單 {k['accept']} 拒單 {k['reject']} "
                f"未回應 {k['ignore']} 完成 {k['complete']}｜Pipeline {self.tick_ms:.1f} ms｜"
                f"車速 {self.speed:g} km/h 接單率 {self.accept_rate:.0%}")

    async def handle_cmd(self, line):
        parts = line.split()
        if not parts:
            return
        cmd, arg = parts[0].lower(), (parts[1] if len(parts) > 1 else "")
        try:
            if cmd == "add":
                await self.add_cars(int(arg or 10))
            elif cmd == "remove":
                idle = [c for c in self.cars.values() if c.status == "available" and c.id not in self.dead][:int(arg or 10)]
                for c in idle:
                    await self.go_offline(c)
                self.log(f"下線 {len(idle)} 台空車，目前共 {len(self.cars)} 台")
            elif cmd == "kill":
                if arg in self.cars:
                    self.dead.add(arg)
                    self.log(f"{arg} 停止心跳，{C.HEARTBEAT_TTL} 秒後 Redis 會讓 driver:alive:{arg} 過期")
                else:
                    self.log(f"找不到 {arg}")
            elif cmd == "rate":
                self.accept_rate = min(max(float(arg), 0.0), 1.0)
                self.log(f"接單率改為 {self.accept_rate:.0%}")
            elif cmd == "speed":
                self.speed = float(arg)
                await self.r.set(C.SIM_SPEED, self.speed)
                self.log(f"車速改為 {self.speed:g} km/h")
            elif cmd == "status":
                self.log(self.status_line())
            elif cmd in ("quit", "exit", "q"):
                self.running = False
            elif cmd == "help":
                print(HELP, flush=True)
            else:
                self.log("看不懂這個指令，輸入 help 看說明")
        except ValueError:
            self.log("參數格式不對，輸入 help 看說明")

    async def run(self):
        self.r = C.get_redis()
        self.lua = C.Lua(self.r)
        await self.r.ping()
        await self.r.set(C.SIM_SPEED, self.speed)
        print("=" * 64)
        print(" 即時派車模擬系統 ─ 司機模擬器")
        print(f" Redis: {C.REDIS_URL}")
        print("=" * 64)
        print(HELP)
        print("-" * 64, flush=True)
        await self.add_cars(self.args.cars)

        # 讀鍵盤輸入放在另一條執行緒，不擋住每秒的 tick
        loop = asyncio.get_running_loop()

        def reader():
            for line in sys.stdin:
                loop.call_soon_threadsafe(self.cmds.put_nowait, line.strip())
        threading.Thread(target=reader, daemon=True).start()

        last, n = time.monotonic(), 0
        try:
            while self.running:
                start = time.monotonic()
                while not self.cmds.empty():
                    await self.handle_cmd(self.cmds.get_nowait())
                await self.tick(start - last)
                last = start
                n += 1
                if n % self.args.report == 0:
                    self.log(self.status_line())
                await asyncio.sleep(max(0.0, self.args.interval - (time.monotonic() - start)))
        finally:
            self.log("全部車輛下線中…")
            for car in list(self.cars.values()):
                await self.go_offline(car)
            await self.r.aclose()


def main():
    C.utf8_console()
    ap = argparse.ArgumentParser(description="司機模擬器")
    ap.add_argument("--cars", type=int, default=100, help="車輛數（預設 100）")
    ap.add_argument("--speed", type=float, default=150, help="車速 km/h（模擬加速，預設 150）")
    ap.add_argument("--accept", type=float, default=0.8, help="接單率（預設 0.8）")
    ap.add_argument("--interval", type=float, default=1.0, help="回報間隔秒數")
    ap.add_argument("--report", type=int, default=5, help="每幾個 tick 印一次統計")
    ap.add_argument("--prefix", default="car", help="車輛 id 前綴")
    args = ap.parse_args()
    try:
        asyncio.run(Simulator(args).run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
