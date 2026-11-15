-- 司機回報位置 + 心跳（模擬器每秒對每台車呼叫一次，用 Pipeline 批次送出）
-- 重點：只有「狀態是 available」才寫進空車清單。
--       若直接 GEOADD drivers:available，剛被派單的車下一秒又會被加回去而被派兩次。
-- KEYS: 1 drivers:all  2 drivers:available  3 driver:{id}  4 driver:alive:{id}
-- ARGV: 1 id  2 lon  3 lat  4 心跳 TTL 秒
local st = redis.call('HGET', KEYS[3], 'status')
if (not st) or st == 'offline' then
  st = 'available'
  redis.call('HSET', KEYS[3], 'status', 'available', 'trip', '')
end
redis.call('HSET', KEYS[3], 'lon', ARGV[2], 'lat', ARGV[3])
redis.call('GEOADD', KEYS[1], ARGV[2], ARGV[3], ARGV[1])
if st == 'available' then
  redis.call('GEOADD', KEYS[2], ARGV[2], ARGV[3], ARGV[1])
end
redis.call('SET', KEYS[4], '1', 'EX', ARGV[4])
return { st, redis.call('HGET', KEYS[3], 'trip') or '' }
