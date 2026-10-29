-- hold_inventory.lua
local drop_ts_str = redis.call('GET', KEYS[1])
if not drop_ts_str then
    return 'EVENT_NOT_CACHED'
end

local now_ms = tonumber(ARGV[2])
local drop_ms = tonumber(drop_ts_str)
if now_ms < drop_ms then
    return 'EVENT_NOT_OPEN'
end

-- Phase 1: Verify every requested seat is currently unheld and unsold in Redis
for i = 2, #KEYS do
    local current_val = redis.call('GET', KEYS[i])
    if current_val ~= false then
        return 'SEAT_TAKEN'
    end
end

-- Phase 2: All seats are free; acquire all seat locks with 660s safety-net TTL
local reservation_id = ARGV[1]
local seat_ttl_sec = tonumber(ARGV[3])

for i = 2, #KEYS do
    redis.call('SET', KEYS[i], reservation_id, 'EX', seat_ttl_sec)
end

return 'HELD'

