-- confirm_inventory.lua
local reservation_id = ARGV[1]

for i = 1, #KEYS do
    local current_holder = redis.call('GET', KEYS[i])
    if current_holder == reservation_id or current_holder == false then
        redis.call('SET', KEYS[i], 'SOLD')
    end
end

return 'CONFIRMED'

