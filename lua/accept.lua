-- 司機接單：offered → to_pickup
-- 先確認 offer:{tid} 還在、而且值是自己，避免「逾時已改派，老司機才按接單」。
-- KEYS: 1 driver:{id}  2 trip:{tid}  3 offer:{tid}  4 trips:offered  5 stream:events  6 stats
-- ARGV: 1 id  2 tid
local id, tid = ARGV[1], ARGV[2]
if redis.call('GET', KEYS[3]) ~= id then return 0 end
if redis.call('HGET', KEYS[1], 'status') ~= 'offered' then return 0 end
if redis.call('HGET', KEYS[1], 'trip') ~= tid then return 0 end
if redis.call('HGET', KEYS[2], 'driver') ~= id then return 0 end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

redis.call('DEL', KEYS[3])
redis.call('ZREM', KEYS[4], tid)
redis.call('HSET', KEYS[1], 'status', 'to_pickup')
redis.call('HSET', KEYS[2], 'status', 'to_pickup', 'accepted_ts', now)

local wait = now - tonumber(redis.call('HGET', KEYS[2], 'requested_ts'))
-- 同一筆行程只在第一次媒合成功時計入統計（司機中途離線重派不重複計）
if redis.call('HSETNX', KEYS[2], 'assigned_ts', now) == 1 then
  redis.call('HSET', KEYS[2], 'wait_ms', wait)
  redis.call('HINCRBY', KEYS[6], 'matched', 1)
  redis.call('HINCRBY', KEYS[6], 'total_wait_ms', wait)
end
redis.call('XADD', KEYS[5], '*', 'ev', 'accept', 'tid', tid, 'driver', id, 'ts', now)
redis.call('PUBLISH', 'ch:trip:' .. tid,
  cjson.encode({ type = 'trip', ev = 'assigned', tid = tid, driver = id, wait_ms = wait, ts = now }))
return 1
