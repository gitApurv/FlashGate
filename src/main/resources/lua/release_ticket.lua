-- KEYS[1]: campaign stock key        (e.g., "campaign:{id}:stock")
-- KEYS[2]: campaign reserved users   (e.g., "campaign:{id}:users")
-- ARGV[1]: user_id

-- Only increment stock if the user was actually in the reserved set (idempotent release)
local removed = redis.call("SREM", KEYS[2], ARGV[1])
if removed == 1 then
    redis.call("INCR", KEYS[1])
    return 1
end
return 0
