-- 原子派單：available → offered
-- 「檢查司機是空車」與「把他移出空車清單」在同一個腳本內完成，
-- 所以兩個 dispatcher 同時搶同一台車時，只有一個會成功。
-- KEYS: 1 driver:{id}  2 trip:{tid}  3 drivers:available  4 offer:{tid}
--       5 trips:offered  6 stream:events  7 trip:{tid}:tried  8 stats
-- ARGV: 1 id  2 tid  3 接單期限秒
-- 回傳: {1, 當下最近空車}=成功  {0, 原因}=這台車不行，換下一台  {-1, 原因}=行程已不需要派車
local id, tid, ttl = ARGV[1], ARGV[2], tonumber(ARGV[3])

if redis.call('HGET', KEYS[2], 'status') ~= 'searching' then return { -1, 'trip_not_searching' } end
if redis.call('HGET', KEYS[1], 'status') ~= 'available' then return { 0, 'driver_busy' } end
if not redis.call('ZSCORE', KEYS[3], id) then return { 0, 'not_in_pool' } end
if redis.call('SISMEMBER', KEYS[7], id) == 1 then return { 0, 'already_tried' } end

-- 驗證用：在「派單的這一瞬間」重新查一次誰是最近的空車，交給壓力測試統計
local plon = redis.call('HGET', KEYS[2], 'pickup_lon')
local plat = redis.call('HGET', KEYS[2], 'pickup_lat')
local n = redis.call('SCARD', KEYS[7])
local near = redis.call('GEOSEARCH', KEYS[3], 'FROMLONLAT', plon, plat, 'BYRADIUS', 50, 'km', 'ASC', 'COUNT', n + 1)
local nearest = ''
for _, m in ipairs(near) do
  if redis.call('SISMEMBER', KEYS[7], m) == 0 then nearest = m break end
end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

redis.call('ZREM', KEYS[3], id)
redis.call('HSET', KEYS[1], 'status', 'offered', 'trip', tid)
redis.call('HSET', KEYS[2], 'status', 'offered', 'driver', id, 'offered_ts', now)
redis.call('HINCRBY', KEYS[2], 'offers', 1)
redis.call('SET', KEYS[4], id, 'EX', ttl)
redis.call('ZADD', KEYS[5], now + ttl * 1000, tid)
redis.call('HINCRBY', KEYS[8], 'offers', 1)
redis.call('XADD', KEYS[6], '*', 'ev', 'offer', 'tid', tid, 'driver', id, 'ts', now)
redis.call('PUBLISH', 'ch:trip:' .. tid,
  cjson.encode({ type = 'trip', ev = 'offered', tid = tid, driver = id, ts = now }))
return { 1, nearest }
