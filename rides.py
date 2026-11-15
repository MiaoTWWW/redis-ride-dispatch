"""乘客叫車（app.py 與壓力測試共用同一份邏輯）。"""
import json
import random

import config as C


class RateLimited(Exception):
    pass


async def create_ride(r, rider, plon, plat, dlon, dlat, ratelimit=True):
    # 1) Rate limit：SET NX EX，5 秒內同一乘客只會成功一次
    if ratelimit and not await r.set(C.k_ratelimit(rider), 1, nx=True, ex=C.RATE_LIMIT_S):
        raise RateLimited()

    # 2) 行程流水號
    tid = str(await r.incr(C.SEQ_TRIP))

    # 3) 動態加價 + 最近的捷運站：用 Pipeline 一次往返問完
    cell = C.cell_of(plon, plat)
    x0, y0, x1, y1 = C.cell_bounds(cell)
    pipe = r.pipeline(transaction=False)
    pipe.incr(C.k_surge(cell))
    pipe.expire(C.k_surge(cell), C.SURGE_TTL, nx=True)      # 只在第一次設定，形成 60 秒視窗
    pipe.time()
    pipe.geosearch(C.DRIVERS_AVAILABLE, longitude=(x0 + x1) / 2, latitude=(y0 + y1) / 2,
                   width=0.5, height=0.5, unit="km")         # 這一格裡的空車
    pipe.geosearch(C.STATIONS, longitude=plon, latitude=plat, radius=3, unit="km", sort="ASC", count=1)
    demand, _, (sec, usec), supply, station = await pipe.execute()
    now = sec * 1000 + usec // 1000
    surge = C.surge_multiplier(demand, len(supply))
    station = station[0] if station else ""

    dist = round(C.haversine_km(plon, plat, dlon, dlat), 2)
    fare = round((C.BASE_FARE + C.PER_KM * dist) * surge)

    # 4) Transaction（MULTI/EXEC）：行程資料、排進佇列、計數器，全有或全無
    tx = r.pipeline(transaction=True)
    tx.hset(C.k_trip(tid), mapping={
        "tid": tid, "rider": rider, "status": "searching", "driver": "", "stage": 0,
        "pickup_lon": plon, "pickup_lat": plat, "dropoff_lon": dlon, "dropoff_lat": dlat,
        "requested_ts": now, "distance_km": dist, "surge": surge, "fare": fare, "station": station,
    })
    tx.xadd(C.STREAM_REQUESTS, {"tid": tid, "stage": 0})
    tx.hincrby(C.STATS, "requests", 1)
    if station:
        tx.zincrby(C.HOTSPOTS, 1, station)
    tx.zadd(C.SURGE_CELLS, {cell: now})
    tx.xadd(C.STREAM_EVENTS, {"ev": "request", "tid": tid, "driver": "", "ts": now})
    tx.publish(C.k_ch_trip(tid), json.dumps({"type": "trip", "ev": "searching", "tid": tid, "radius": C.RADII_KM[0],
                                             "surge": surge, "ts": now}))
    await tx.execute()
    return {"tid": tid, "fare": fare, "surge": surge, "distance_km": dist, "station": station}


def clamp(lon, lat):
    a, b, c, d = C.BBOX
    return min(max(lon, a), c), min(max(lat, b), d)


def random_route(stations, rng=random):
    """以捷運站為熱點產生一筆上車點／目的地（Open Data 的用途）。"""
    _, slon, slat = rng.choice(stations)
    plon, plat = clamp(slon + rng.gauss(0, 0.003), slat + rng.gauss(0, 0.003))
    _, tlon, tlat = rng.choice(stations)
    dlon, dlat = clamp(tlon + rng.gauss(0, 0.004), tlat + rng.gauss(0, 0.004))
    return plon, plat, dlon, dlat
