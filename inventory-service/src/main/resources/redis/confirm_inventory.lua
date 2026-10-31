-- confirm_inventory.lua
for i = 1, #KEYS do
    redis.call('SET', KEYS[i], 'SOLD')
end

return 'CONFIRMED'

