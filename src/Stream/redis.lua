local lua51 = _VERSION:match '5%.1$'

local bitneg

if lua51 then
  bitneg = (require 'bit').bnot
else
  bitneg = function(v) return ~v end
end

return function (Stream, redis)
-- return function (Stream)
--   local redis = require 'redis'

  local function foldPairs(it, v)
    v = v or {}
    for i = 1, #it, 2 do
      v[it[i]] = it[i + 1]
    end
    return v
  end

  local function streamRedisResponse(reply)
    return Stream.fromArray(reply)
      :map(function(v)
        return foldPairs(v[2], { _id = v[1] })
      end)
  end

  local xtrim = redis.command('XTRIM')
  redis.commands.xtrim = function(client, key, n)
    if n < 0 then
      return xtrim(client, key, "MAXLEN", "~", bitneg(n))
    end
    return xtrim(client, key, "MAXLEN", n)
  end

  local xadd = redis.command('XADD')
  redis.commands.xadd = xadd
  -- redis.commands.xadd = function(client, key, ...)
  --   return xadd(client, key, "*", ...)
  -- end

  local xrevrange = redis.command('XREVRANGE', { response = streamRedisResponse })
  redis.commands.xrevrange = function(client, key, max, min, count)
    if count then
      return xrevrange(client, key, max or "+", min or "-", "COUNT", count)
    end
    return xrevrange(client, key, max or "+", min or "-")
  end

  local xrange = redis.command('XRANGE', { response = streamRedisResponse })

  redis.commands.xrange = function(client, key, min, max, count)
    if count then
      return xrange(client, key, min or "-", max or "+", "COUNT", count)
    end
    return xrange(client, key, min or "-", max or "+")
  end

  function Stream.setRedisClient(client)
    Stream.redis_client = client
  end

  function Stream.connectRedis(...)
    local client = redis.connect(...)

    Stream.redis_client = client

    function Stream.xrevrange(key, max, min, count)
      return client:xrevrange(key, max, min, count)
    end

    function Stream.xrange(key, max, min, count)
      return client:xrange(key, max, min, count)
    end

    function Stream:xadd(key)
      return self:transform(function(...)
        -- TODO return value?
        client:xadd(key, "*", ...)
        return ...
      end)
    end
  end


  -- local function streamSingleRedisResponse(reply)
  --   return Stream.fromArray(reply)
  --     :map(function(v)
  --       return { _id = v[1], data = v[2][2] }
  --     end)
  -- end

  -- redis.commands.zxrevrange = redis.command('XREVRANGE', {
  --     response = streamZephyrRedisResponse;
  --   })
  -- redis.commands.zxrange = redis.command('XRANGE', {
  --     response = streamZephyrRedisResponse;
  --   })
  return Stream
end
