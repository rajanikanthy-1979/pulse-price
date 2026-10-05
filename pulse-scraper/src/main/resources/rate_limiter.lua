local key = KEYS[1]
local capacity = tonumber(ARGV[1]) or 10
local refill_rate = tonumber(ARGV[2]) or 1
local requested = tonumber(ARGV[3]) or 1
local now = tonumber(ARGV[4])

-- Fallback to Redis server time if ARGV[4] was omitted
if not now then
    local time = redis.call('TIME')
    now = tonumber(time[1])
end

local bucket = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(bucket[1])
local last_updated = tonumber(bucket[2])

if tokens == nil or last_updated == nil then
    tokens = capacity
    last_updated = now
else
    local delta = math.max(0, now - last_updated)
    -- ADD tokens based on elapsed time (not multiplication)
    tokens = math.min(capacity, tokens + (delta * refill_rate))
    last_updated = now
end

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HSET', key, 'tokens', tokens, 'last_updated', last_updated)
    redis.call('EXPIRE', key, 3600)
    return 1
else
    redis.call('HSET', key, 'tokens', tokens, 'last_updated', last_updated)
    return 0
end