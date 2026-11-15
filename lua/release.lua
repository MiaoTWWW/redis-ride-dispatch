-- 拒單或逾時：offered → available，行程回到 searching 並重新排進佇列
-- reason = 'reject'  ：司機主動拒絕，offer 必須還在而且是自己
-- reason = 'timeout' ：offer 必須已經過期。若 offer 還在，代表行程已經改派給別人，
--                      這是一則過時的逾時通知，直接忽略（多個 dispatcher 都會收到同一則通知）。
-- KEYS: 1 driver:{id}  2 trip:{tid}  3 drivers:all  4 drivers:available  5 offer:{tid}
--       6 trip:{tid}:tried  7 stream:ride_requests  8 trips:offered  9 stream:events  10 stats
-- ARGV: 1 id  2 tid  3 reason
local id, tid, reason = ARGV[1], ARGV[2], ARGV[3]

if redis.call('HGET', KEYS[2], 'status') ~= 'offered' then return 0 end
if redis.call('HGET', KEYS[2], 'driver') ~= id then return 0 end

local cur = redis.call('GET', KEYS[5])
if reason == 'reject' then
  if cur ~= id then return 0 end
  redis.call('DEL', KEYS[5])
else
  if cur then return 0 end
end
redis.call('ZREM', KEYS[8], tid)

-- 司機放回空車清單（座標從 drivers:all 取）
if redis.call('HGET', KEYS[1], 'status') == 'offered' and redis.call('HGET', KEYS[1], 'trip') == tid then
  redis.call('HSET', KEYS[1], 'status', 'available', 'trip', '')
  local pos = redis.call('GEOPOS', KEYS[3], id)
  if pos[1] then redis.call('GEOADD', KEYS[4], pos[1][1], pos[1][2], id) end
end

-- 這位司機不再重派給同一筆行程
redis.call('SADD', KEYS[6], id)
redis.call('EXPIRE', KEYS[6], 3600)

redis.call('HSET', KEYS[2], 'status', 'searching', 'driver', '')
redis.call('XADD', KEYS[7], '*', 'tid', tid, 'stage', redis.call('HGET', KEYS[2], 'stage') or '0')

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('HINCRBY', KEYS[10], reason == 'reject' and 'rejects' or 'timeouts', 1)
redis.call('XADD', KEYS[9], '*', 'ev', reason, 'tid', tid, 'driver', id, 'ts', now)
redis.call('PUBLISH', 'ch:trip:' .. tid,
  cjson.encode({ type = 'trip', ev = reason, tid = tid, driver = id, ts = now }))
return 1
