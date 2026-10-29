-- release_inventory.lua
local reservation_id = ARGV[1]
local released_count = 0

for i = 1, #KEYS do
    local current_holder = redis.call('GET', KEYS[i])
    if current_holder == reservation_id then
        redis.call('DEL', KEYS[i])
        released_count = released_count + 1
    end
end

return released_count

