"""後端：FastAPI + WebSocket

    uvicorn app:app --port 8000
    乘客端   http://localhost:8000/
    儀表板   http://localhost:8000/dashboard
"""
import asyncio
import random
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import config as C
from rides import RateLimited, create_ride, random_route

STATIC = C.BASE / "static"
r = C.get_redis()
stations = C.load_stations()


@asynccontextmanager
async def lifespan(app):
    await C.ensure_group(r)
    await C.ensure_stations(r)
    yield
    await r.aclose()


app = FastAPI(title="即時派車模擬系統", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
async def rider_page():
    return FileResponse(STATIC / "rider.html")


@app.get("/dashboard")
async def dashboard_page():
    return FileResponse(STATIC / "dashboard.html")


class Point(BaseModel):
    lon: float = Field(ge=C.BBOX[0] - 0.05, le=C.BBOX[2] + 0.05)
    lat: float = Field(ge=C.BBOX[1] - 0.05, le=C.BBOX[3] + 0.05)


class RideRequest(BaseModel):
    rider: str = Field(min_length=1, max_length=40, pattern=r"^[\w-]+$")
    pickup: Point
    dropoff: Point


@app.post("/api/ride")
async def api_ride(req: RideRequest):
    try:
        return await create_ride(r, req.rider, req.pickup.lon, req.pickup.lat, req.dropoff.lon, req.dropoff.lat)
    except RateLimited:
        raise HTTPException(429, f"叫車太頻繁，請 {C.RATE_LIMIT_S} 秒後再試")


@app.post("/api/demo/burst")
async def api_burst(n: int = 10):
    """儀表板的「模擬叫車」按鈕：一次送出 n 筆隨機叫車。"""
    n = min(max(n, 1), 200)
    out = []
    for _ in range(n):
        ride = await create_ride(r, f"demo-{random.randrange(10**6)}", *random_route(stations), ratelimit=False)
        out.append(ride["tid"])
    return {"tids": out}


@app.get("/api/trip/{tid}")
async def api_trip(tid: int):
    trip = await r.hgetall(C.k_trip(tid))
    if not trip:
        raise HTTPException(404, "找不到這筆行程")
    if trip.get("driver") and trip["status"] in ("offered", "to_pickup", "on_trip"):
        pipe = r.pipeline(transaction=False)
        pipe.geopos(C.DRIVERS_ALL, trip["driver"])
        pipe.get(C.SIM_SPEED)
        pipe.hget(C.k_driver(trip["driver"]), "rating")
        pos, speed, rating = await pipe.execute()
        trip["driver_rating"] = rating
        if pos and pos[0]:
            lon, lat = pos[0]
            trip["driver_lon"], trip["driver_lat"] = lon, lat
            goal = "dropoff" if trip["status"] == "on_trip" else "pickup"
            km = C.haversine_km(lon, lat, float(trip[goal + "_lon"]), float(trip[goal + "_lat"]))
            trip["remaining_km"] = round(km, 2)
            trip["eta_s"] = round(km / float(speed or 40) * 3600)
    return trip


@app.get("/api/stations")
async def api_stations():
    return [{"name": n, "lon": lon, "lat": lat} for n, lon, lat in stations]


@app.get("/api/stats")
async def api_stats():
    pipe = r.pipeline(transaction=False)
    pipe.hgetall(C.STATS)
    pipe.zcard(C.DRIVERS_ALL)
    pipe.zcard(C.DRIVERS_AVAILABLE)
    pipe.xlen(C.STREAM_REQUESTS)
    pipe.zcard(C.QUEUE_DELAYED)
    pipe.zcard(C.TRIPS_OFFERED)
    pipe.zrevrange(C.HOTSPOTS, 0, 7, withscores=True)
    pipe.xrevrange(C.STREAM_TRIPS, count=8)
    pipe.xpending(C.STREAM_REQUESTS, C.GROUP)
    pipe.xinfo_consumers(C.STREAM_REQUESTS, C.GROUP)
    stats, online, available, qlen, delayed, offered, hot, recent, pending, consumers = await pipe.execute()
    s = {k: int(v) for k, v in stats.items()}
    matched, req = s.get("matched", 0), s.get("requests", 0)
    return {
        "stats": s,
        "online": online, "available": available,
        "queue_total": qlen, "pending": pending["pending"], "delayed": delayed, "offered": offered,
        "avg_wait_s": round(s.get("total_wait_ms", 0) / matched / 1000, 2) if matched else None,
        "match_rate": round(matched / (matched + s.get("failed", 0)), 4) if matched + s.get("failed", 0) else None,
        "requests": req,
        "hotspots": [{"name": n, "count": int(c)} for n, c in hot],
        "recent": [f for _, f in recent],
        "dispatchers": [{"name": c["name"], "pending": c["pending"], "idle_s": round(c["idle"] / 1000, 1)}
                        for c in consumers],
    }


@app.get("/api/surge")
async def api_surge():
    """動態加價熱力圖：最近 60 秒有人叫車的格子。"""
    sec, usec = await r.time()
    now = sec * 1000 + usec // 1000
    await r.zremrangebyscore(C.SURGE_CELLS, 0, now - C.SURGE_TTL * 1000)
    cells = await r.zrange(C.SURGE_CELLS, 0, -1)
    if not cells:
        return []
    pipe = r.pipeline(transaction=False)
    for c in cells:
        x0, y0, x1, y1 = C.cell_bounds(c)
        pipe.get(C.k_surge(c))
        pipe.geosearch(C.DRIVERS_AVAILABLE, longitude=(x0 + x1) / 2, latitude=(y0 + y1) / 2,
                       width=0.5, height=0.5, unit="km")
    res = await pipe.execute()
    out = []
    for i, c in enumerate(cells):
        demand, supply = int(res[2 * i] or 0), len(res[2 * i + 1])
        if demand:
            out.append({"cell": c, "bounds": C.cell_bounds(c), "demand": demand, "supply": supply,
                        "multiplier": C.surge_multiplier(demand, supply)})
    return out


@app.websocket("/ws")
async def ws(sock: WebSocket, tid: str = "", feed: int = 0, map: int = 1):
    """把 Redis Pub/Sub 轉給瀏覽器。
    預設訂閱 ch:map（車輛位置）；乘客端叫車後另開一條 tid=…&map=0 的連線只聽自己的行程，
    儀表板帶 feed=1 訂閱所有行程事件。
    """
    await sock.accept()
    p = r.pubsub()
    if map or not tid.isdigit():
        await p.subscribe(C.CH_MAP)
    if tid.isdigit():
        await p.subscribe(C.k_ch_trip(tid))
    if feed:
        await p.psubscribe("ch:trip:*")

    async def forward():
        async for m in p.listen():
            if m["type"] in ("message", "pmessage"):
                await sock.send_text(m["data"])

    async def watch_close():
        try:
            while True:
                await sock.receive_text()
        except WebSocketDisconnect:
            pass

    tasks = [asyncio.create_task(forward()), asyncio.create_task(watch_close())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in tasks:
            t.cancel()
        await p.aclose()
