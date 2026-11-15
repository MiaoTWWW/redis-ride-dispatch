-- 行程結束：on_trip → available，寫入完成紀錄
-- KEYS: 1 driver:{id}  2 trip:{tid}  3 drivers:all  4 drivers:available
--       5 stream:trips  6 stream:events  7 stats
-- ARGV: 1 id  2 tid
local id, tid = ARGV[1], ARGV[2]
if redis.call('HGET', KEYS[1], 'status') ~= 'on_trip' then return 0 end
if redis.call('HGET', KEYS[1], 'trip') ~= tid then return 0 end
if redis.call('HGET', KEYS[2], 'status') ~= 'on_trip' then return 0 end
if redis.call('HGET', KEYS[2], 'driver') ~= id then return 0 end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local f = redis.call('HMGET', KEYS[2], 'wait_ms', 'distance_km', 'fare', 'surge', 'station')
redis.call('HSET', KEYS[2], 'status', 'completed', 'completed_ts', now)

redis.call('HSET', KEYS[1], 'status', 'available', 'trip', '')
redis.call('HINCRBY', KEYS[1], 'trips_done', 1)
redis.call('HINCRBY', KEYS[1], 'earnings', f[3])
local pos = redis.call('GEOPOS', KEYS[3], id)
if pos[1] then redis.call('GEOADD', KEYS[4], pos[1][1], pos[1][2], id) end

redis.call('XADD', KEYS[5], '*', 'tid', tid, 'driver', id,
  'wait_s', string.format('%.1f', tonumber(f[1]) / 1000),
  'distance_km', f[2], 'fare', f[3], 'surge', f[4], 'station', f[5] or '')
redis.call('HINCRBY', KEYS[7], 'completed', 1)
redis.call('HINCRBY', KEYS[7], 'total_fare', f[3])
redis.call('XADD', KEYS[6], '*', 'ev', 'complete', 'tid', tid, 'driver', id, 'ts', now)
redis.call('PUBLISH', 'ch:trip:' .. tid,
  cjson.encode({ type = 'trip', ev = 'completed', tid = tid, driver = id, fare = tonumber(f[3]), ts = now }))
return 1
