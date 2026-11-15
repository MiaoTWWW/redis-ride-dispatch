-- 延遲佇列 → 叫車佇列
-- queue:delayed 是 Sorted Set，score = 可以處理的時間（毫秒）。
-- 「取出到期的成員」與「XADD 回 Stream」一次做完，多個 dispatcher 同時跑也不會重複搬。
-- KEYS: 1 queue:delayed  2 stream:ride_requests
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], 0, now, 'LIMIT', 0, 100)
for _, m in ipairs(due) do
  redis.call('ZREM', KEYS[1], m)
  local sep = string.find(m, ':', 1, true)
  redis.call('XADD', KEYS[2], '*', 'tid', string.sub(m, 1, sep - 1), 'stage', string.sub(m, sep + 1))
end
return #due
