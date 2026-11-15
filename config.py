"""集中管理：Redis 連線、key 名稱、系統參數、Lua 腳本包裝。

所有程式（simulator / dispatcher / app / 壓力測試）都只從這裡取 key 名稱，
避免各寫各的造成打錯字。
"""
import csv
import math
import os
import sys
from pathlib import Path

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

BASE = Path(__file__).parent
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# ───────────── Key 名稱：<類別>:<id>[:<子項>] ─────────────
DRIVERS_ALL = "drivers:all"              # GEO   所有在線車輛（畫地圖用）
DRIVERS_AVAILABLE = "drivers:available"  # GEO   只放空車（派車搜尋用）
STATIONS = "stations"                    # GEO   捷運站（Open Data）
SEQ_TRIP = "seq:trip"                    # String 行程流水號
STREAM_REQUESTS = "stream:ride_requests"  # Stream 待派車佇列
GROUP = "dispatchers"                    # Consumer Group
STREAM_TRIPS = "stream:trips"            # Stream 已完成行程
STREAM_EVENTS = "stream:events"          # Stream 稽核紀錄（壓力測試重播用）
STREAM_DISPATCH_LOG = "stream:dispatch_log"  # Stream 每次派單的候選名單
QUEUE_DELAYED = "queue:delayed"          # ZSet  延遲重試佇列（score=可處理時間）
TRIPS_OFFERED = "trips:offered"          # ZSet  等待接單中的行程（score=期限）
STATS = "stats"                          # Hash  計數器
HOTSPOTS = "hotspots"                    # ZSet  熱門上車點
SURGE_CELLS = "surge:cells"              # ZSet  最近有叫車的格子（score=時間）
CH_MAP = "ch:map"                        # Pub/Sub 車輛位置批次推播
SIM_SPEED = "sim:speed_kmh"              # String 模擬車速（算 ETA 用）


def k_driver(i): return f"driver:{i}"
def k_alive(i): return f"driver:alive:{i}"
def k_trip(t): return f"trip:{t}"
def k_tried(t): return f"trip:{t}:tried"
def k_offer(t): return f"offer:{t}"
def k_ch_trip(t): return f"ch:trip:{t}"
def k_surge(cell): return f"surge:{cell}"
def k_ratelimit(uid): return f"ratelimit:rider:{uid}"


# ───────────── 系統參數 ─────────────
HEARTBEAT_TTL = 5          # 秒：心跳 key 的 TTL，過期視為離線
OFFER_TTL = 10             # 秒：司機接單期限
RADII_KM = [1, 3, 5, 5, 5]  # 搜尋半徑階段：1 → 3 → 5 km，5 km 再多等兩輪
RETRY_DELAY_MS = 2000      # 每次擴大半徑前等待
CANDIDATES = 5             # 每輪最多嘗試幾位候選司機
CLAIM_IDLE_MS = 30000      # 請求被取走超過這麼久沒 ACK，其他 dispatcher 可接手
RATE_LIMIT_S = 5           # 同一乘客幾秒內只能叫一次車
BASE_FARE = 70             # 起跳
PER_KM = 25                # 每公里
SURGE_TTL = 60             # 秒：需求計數視窗
CELL_LON = 0.005           # 約 500 m
CELL_LAT = 0.0045          # 約 500 m

# 模擬範圍（台北市中心）
BBOX = (121.495, 25.010, 121.585, 25.095)  # min_lon, min_lat, max_lon, max_lat


def get_redis() -> aioredis.Redis:
    return aioredis.from_url(REDIS_URL, decode_responses=True)


async def ensure_group(r):
    """建立 Stream 與 Consumer Group（已存在就略過）。"""
    try:
        await r.xgroup_create(STREAM_REQUESTS, GROUP, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def load_stations():
    with open(BASE / "data" / "mrt_stations.csv", encoding="utf-8") as f:
        return [(row["name"], float(row["lon"]), float(row["lat"])) for row in csv.DictReader(f)]


async def ensure_stations(r):
    pipe = r.pipeline(transaction=False)
    for name, lon, lat in load_stations():
        pipe.geoadd(STATIONS, (lon, lat, name))
    await pipe.execute()


def haversine_km(lon1, lat1, lon2, lat2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 6371.0088 * 2 * math.asin(math.sqrt(a))


def cell_of(lon, lat):
    return f"{math.floor(lon / CELL_LON)}:{math.floor(lat / CELL_LAT)}"


def cell_bounds(cell):
    ix, iy = (int(x) for x in cell.split(":"))
    return ix * CELL_LON, iy * CELL_LAT, (ix + 1) * CELL_LON, (iy + 1) * CELL_LAT


def surge_multiplier(demand, supply):
    """最近 60 秒叫車數 ÷ 格內空車數。超過 2 → ×1.5；超過 4 → ×2.0"""
    ratio = demand / max(supply, 1)
    if ratio > 4:
        return 2.0
    if ratio > 2:
        return 1.5
    return 1.0


def utf8_console():
    for s in (sys.stdout, sys.stderr):
        try:
            s.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


class Lua:
    """載入 lua/ 下所有腳本。每個方法負責把 KEYS / ARGV 排好。

    傳入 client=pipeline 時，腳本只會被排進 Pipeline，等 execute() 才一次送出。
    """

    def __init__(self, r):
        self.r = r
        self.s = {p.stem: r.register_script(p.read_text(encoding="utf-8")) for p in (BASE / "lua").glob("*.lua")}

    async def _run(self, name, keys, args, client=None):
        return await self.s[name](keys=keys, args=args, client=client or self.r)

    def update_position(self, did, lon, lat, client=None):
        return self._run("update_position",
                         [DRIVERS_ALL, DRIVERS_AVAILABLE, k_driver(did), k_alive(did)],
                         [did, lon, lat, HEARTBEAT_TTL], client)

    def dispatch(self, did, tid):
        return self._run("dispatch",
                         [k_driver(did), k_trip(tid), DRIVERS_AVAILABLE, k_offer(tid), TRIPS_OFFERED,
                          STREAM_EVENTS, k_tried(tid), STATS],
                         [did, tid, OFFER_TTL])

    def accept(self, did, tid):
        return self._run("accept",
                         [k_driver(did), k_trip(tid), k_offer(tid), TRIPS_OFFERED, STREAM_EVENTS, STATS],
                         [did, tid])

    def release(self, did, tid, reason):
        return self._run("release",
                         [k_driver(did), k_trip(tid), DRIVERS_ALL, DRIVERS_AVAILABLE, k_offer(tid), k_tried(tid),
                          STREAM_REQUESTS, TRIPS_OFFERED, STREAM_EVENTS, STATS],
                         [did, tid, reason])

    def pickup(self, did, tid):
        return self._run("pickup", [k_driver(did), k_trip(tid), STREAM_EVENTS], [did, tid])

    def complete(self, did, tid):
        return self._run("complete",
                         [k_driver(did), k_trip(tid), DRIVERS_ALL, DRIVERS_AVAILABLE, STREAM_TRIPS,
                          STREAM_EVENTS, STATS],
                         [did, tid])

    def fail(self, tid):
        return self._run("fail", [k_trip(tid), STREAM_EVENTS, STATS], [tid])

    def offline(self, did, tid, force=False):
        return self._run("offline",
                         [DRIVERS_ALL, DRIVERS_AVAILABLE, k_driver(did), k_alive(did), k_trip(tid), k_offer(tid),
                          k_tried(tid), STREAM_REQUESTS, TRIPS_OFFERED, STREAM_EVENTS, STATS],
                         [did, tid, 1 if force else 0])

    def promote(self):
        return self._run("promote", [QUEUE_DELAYED, STREAM_REQUESTS], [])
