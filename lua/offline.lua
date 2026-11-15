-- 司機離線：心跳 key 過期（或模擬器主動下線）時呼叫
-- 把車從兩個 GEO key 移除；如果他手上有行程，行程重新排隊或標記中止。
-- KEYS: 1 drivers:all  2 drivers:available  3 driver:{id}  4 driver:alive:{id}  5 trip:{tid}
--       6 offer:{tid}  7 trip:{tid}:tried  8 stream:ride_requests  9 trips:offered
--       10 stream:events  11 stats
-- ARGV: 1 id  2 呼叫端讀到的 tid（可為空字串）  3 force(1=主動下線)
-- 回傳: 1=已下線  0=不需處理（心跳又回來了/早已離線）  -1=tid 對不上，請重讀再呼叫
local id, tid, force = ARGV[1], ARGV[2], ARGV[3]

if force ~= '1' and redis.call('EXISTS', KEYS[4]) == 1 then return 0 end
local st = redis.call('HGET', KEYS[3], 'status')
if (not st) or st == 'offline' then return 0 end
if (redis.call('HGET', KEYS[3], 'trip') or '') ~= tid then return -1 end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

redis.call('ZREM', KEYS[1], id)
redis.call('ZREM', KEYS[2], id)
redis.call('DEL', KEYS[4])
redis.call('HSET', KEYS[3], 'status', 'offline', 'trip', '')
redis.call('HINCRBY', KEYS[11], 'offline_events', 1)
redis.call('XADD', KEYS[10], '*', 'ev', 'offline', 'tid', tid, 'driver', id, 'ts', now)

if tid ~= '' and redis.call('HGET', KEYS[5], 'driver') == id then
  local ts = redis.call('HGET', KEYS[5], 'status')
  if ts == 'offered' or ts == 'to_pickup' then
    -- 還沒載到客人：換一台車
    redis.call('DEL', KEYS[6])
    redis.call('ZREM', KEYS[9], tid)
    redis.call('SADD', KEYS[7], id)
    redis.call('EXPIRE', KEYS[7], 3600)
    redis.call('HSET', KEYS[5], 'status', 'searching', 'driver', '')
    redis.call('XADD', KEYS[8], '*', 'tid', tid, 'stage', redis.call('HGET', KEYS[5], 'stage') or '0')
    redis.call('PUBLISH', 'ch:trip:' .. tid,
      cjson.encode({ type = 'trip', ev = 'driver_lost', tid = tid, driver = id, ts = now }))
  elseif ts == 'on_trip' then
    redis.call('HSET', KEYS[5], 'status', 'aborted', 'aborted_ts', now)
    redis.call('HINCRBY', KEYS[11], 'aborted', 1)
    redis.call('PUBLISH', 'ch:trip:' .. tid,
      cjson.encode({ type = 'trip', ev = 'aborted', tid = tid, driver = id, ts = now }))
  end
end
return 1
