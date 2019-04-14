return function(Stream, cjson)
	if not cjson then
		cjson = require 'cjson'
	end

	local function findJson(data, region_start)
		local _, region_start = data:find("%s*", region_start)
		region_start = region_start + 1
		local c = data:sub(region_start, region_start)
		if c == '"' then
			_, region_start, value = data:find('(%b"")', region_start)
		elseif c == '{' then
			_, region_start, value = data:find('(%b{})', region_start)
		elseif c == '[' then
			_, region_start, value = data:find('(%b[])', region_start)
		else
			_, region_start, value = data:find('(%S+)', region_start)
		end
		if value then
			return value, region_start + 1
		end
	end

	function Stream:json()
		self = self or Stream.fromFile(io.stdin)
		return Stream.fromFunction(function()
			-- local data = io.stdin:read("*a")
			local data = self:join()

			local region_start = 0
			local region_end = #data
			local value = nil

			while region_start < region_end do
				value, region_start = findJson(data, region_start)
				coroutine.yield(cjson.decode(value))
			end
		end)
	end

	function Stream:dumpJson()
		return self:map(function(v) return cjson.encode(v) end):print()
	end
end
