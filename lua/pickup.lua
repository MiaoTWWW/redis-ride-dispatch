-- 抵達上車點：to_pickup → on_trip
-- KEYS: 1 driver:{id}  2 trip:{tid}  3 stream:events
-- ARGV: 1 id  2 tid
local id, tid = ARGV[1], ARGV[2]
if redis.call('HGET', KEYS[1], 'status') ~= 'to_pickup' then return 0 end
if redis.call('HGET', KEYS[1], 'trip') ~= tid then return 0 end
if redis.call('HGET', KEYS[2], 'status') ~= 'to_pickup' then return 0 end
if redis.call('HGET', KEYS[2], 'driver') ~= id then return 0 end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('HSET', KEYS[1], 'status', 'on_trip')
redis.call('HSET', KEYS[2], 'status', 'on_trip', 'pickup_ts', now)
redis.call('XADD', KEYS[3], '*', 'ev', 'pickup', 'tid', tid, 'driver', id, 'ts', now)
redis.call('PUBLISH', 'ch:trip:' .. tid,
  cjson.encode({ type = 'trip', ev = 'picked_up', tid = tid, driver = id, ts = now }))
return 1
