-- 找不到車：searching → failed（只有還在 searching 才會標記，重複呼叫無副作用）
-- KEYS: 1 trip:{tid}  2 stream:events  3 stats
-- ARGV: 1 tid
local tid = ARGV[1]
if redis.call('HGET', KEYS[1], 'status') ~= 'searching' then return 0 end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('HSET', KEYS[1], 'status', 'failed', 'failed_ts', now)
redis.call('HINCRBY', KEYS[3], 'failed', 1)
redis.call('XADD', KEYS[2], '*', 'ev', 'fail', 'tid', tid, 'driver', '', 'ts', now)
redis.call('PUBLISH', 'ch:trip:' .. tid, cjson.encode({ type = 'trip', ev = 'failed', tid = tid, ts = now }))
return 1
