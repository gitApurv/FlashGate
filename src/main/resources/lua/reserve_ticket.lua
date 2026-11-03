-- KEYS[1]: campaign stock key        (e.g., "campaign:{id}:stock")
-- KEYS[2]: campaign reserved users   (e.g., "campaign:{id}:users")
-- KEYS[3]: idempotency key           (e.g., "idemp:{idempotency_key}")
-- KEYS[4]: pending reconciliation    (e.g., "campaign:{id}:pending_sync")
-- ARGV[1]: user_id
-- ARGV[2]: reservation_id
-- ARGV[3]: current_timestamp_ms

-- 1. Check idempotency key first (return existing reservation_id if retry)
local existing_res = redis.call("GET", KEYS[3])
if existing_res then
    return existing_res
end

-- 2. Check if user already holds a reservation for this campaign
if redis.call("SISMEMBER", KEYS[2], ARGV[1]) == 1 then
    return "-1" -- ERR_ALREADY_RESERVED
end

-- 3. Verify available stock
local stock = tonumber(redis.call("GET", KEYS[1]))
if not stock or stock <= 0 then
    return "0" -- ERR_SOLD_OUT
end

-- 4. Atomically decrement stock, register user, store idempotency, and log pending sync
redis.call("DECR", KEYS[1])
redis.call("SADD", KEYS[2], ARGV[1])
redis.call("SET", KEYS[3], ARGV[2], "EX", 3600)
redis.call("HSET", KEYS[4], ARGV[2], ARGV[1] .. ":" .. ARGV[3])

return "1" -- SUCCESS
