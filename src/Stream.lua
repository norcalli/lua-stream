local function class(init)
	local c = {}    -- a new class instance

	-- the class will be the metatable for all its objects,
	-- and they will look up their methods in it.
	c.__index = c

	-- expose a constructor which can be called by <classname>(<args>)
	local mt = {}
	c.instance = function(it)
		local obj = {}
		setmetatable(obj, c)
		obj.it = it
		return obj
	end
	mt.__call = function(class_tbl, ...)
		return c.init(...)
	end
	c.init = init
	c.isA = function(self)
		return getmetatable(self) == c
	end
	setmetatable(c, mt)
	return c
end

local STREAM_EXIT = {}

local function identity(...)
	return ...
end

local function call_if(fn, status, head, ...)
	-- if status == false or head == STREAM_EXIT then
	if not status then
		error(head)
		return false
	end
	-- if not status or head == STREAM_EXIT then
	if head == STREAM_EXIT then
		-- TODO evaluate error strategy
		-- Could potentially set a custom handler here.
		return false
	end
	return true, fn(head, ...)
end

local function try_call_if(fn, status, ...)
	-- if status == false or head == STREAM_EXIT then
	if not status then
		return false
	end
	return true, fn(status, ...)
end

local function call_if_iterator(fn, head, ...)
	if head == nil then
		return false, head
	end
	return true, head, fn(head, ...)
end

local function iterateThread(it, fn)
	while (call_if(fn, coroutine.resume(it))) do end
end

local function statusAndFilter(filter, status, ...)
	return status and filter(...)
end

local function iterateThreadWhile(it, fn, filter)
	while statusAndFilter(filter, call_if(fn, coroutine.resume(it))) do end
end

local Stream = class()

function Stream.fromFunction(fn)
	return Stream.instance(
		coroutine.create(function()
			fn()
			return STREAM_EXIT
		end))
end

-- function Stream.once(it)
--   return Stream.fromFunction(function()
--     coroutine.yield(it)
--   end)
-- end

-- TODO worth it for performance?
function Stream.once(...)
	local args = {...}
	return Stream.fromFunction(function()
		coroutine.yield(unpack(args))
	end)
end

Stream.init = Stream.once

function Stream.empty()
	return Stream.fromFunction(function() end)
end

function Stream.args(...)
	-- TODO optimize
	return Stream.fromArray({...})
end

-- function Stream.always(value)
--   return Stream.fromFunction(function()
--     while true do
--       coroutine.yield(value)
--     end
--   end)
-- end

-- TODO use all args? performance...
function Stream.always(...)
	local args = {...}
	return Stream.fromFunction(function()
		while true do
			coroutine.yield(unpack(args))
		end
	end)
end

local function transformThread(it, fn)
	return Stream.fromFunction(function()
		iterateThread(it, fn)
		-- while call_if(fn, coroutine.resume(it)) do end
	end)
end

function Stream.fromThread(it)
	return transformThread(it, coroutine.yield)
	-- return Stream.fromFunction(function()
	--   while call_if(coroutine.yield, coroutine.resume(it)) do end
	-- end)
end

function Stream.fromIterator(next, t, controlvar)
	return Stream.fromFunction(function()
		local status
		repeat
			status, controlvar = call_if_iterator(coroutine.yield, next(t,controlvar))
		until not status
	end)
end

function Stream.fromArray(it)
	-- return Stream.fromIterator(ipairs(it))
	return Stream.fromFunction(function()
		for i, v in ipairs(it) do
			coroutine.yield(v, i, it)
		end
	end)
end

function Stream.fromTable(it)
	-- return Stream.fromIterator(pairs(it))
	return Stream.fromFunction(function()
		for k, v in pairs(it) do
			coroutine.yield(v, k, it)
		end
	end)
end

function Stream.fromIterable(it, ...)
	local tt = type(it)
	if tt == 'thread' then
		return Stream.fromThread(it)
	elseif tt == 'function' then
		return Stream.fromIterator(it, ...)
	elseif tt == 'table' then
		if Stream.isA(it, Stream) then
			return it
		end
		if #it == 0 then
			return Stream.fromFunction(function() end)
		end
		if it[1] then
			return Stream.fromArray(it)
		end
		return Stream.fromTable(it)
	else
		return Stream(it)
	end
end

function Stream.fromLinearIterable(it, ...)
	local tt = type(it)
	if tt == 'thread' then
		return Stream.fromThread(it)
	elseif tt == 'function' then
		return Stream.fromIterator(it, ...)
	elseif tt == 'table' then
		if Stream.isA(it, Stream) then
			return it
		end
		if #it == 0 then
			return Stream.fromFunction(function() end)
		end
		if it[1] then
			return Stream.fromArray(it)
		end
		return Stream(it)
	else
		return Stream(it)
	end
end

function Stream.fromStateAction(seed, stateTransitionAction)
	return Stream.fromFunction(function()
		local stop = false
		while not stop do
			seed, stop = stateTransitionAction(seed)
		end
	end)
end

function Stream:resume()
	return call_if(identity, coroutine.resume(self.it))
end

function Stream:head()
	return self()
end

function Stream:__call()
	return select(2, self:resume())
end

function Stream:each(fn)
	if coroutine.status(self.it) == 'dead' then
		return
	end
	iterateThread(self.it, fn)
end

local NOOP = function() end

function Stream:consume()
	return self:each(NOOP)
end

function Stream:transform(fn)
	return transformThread(self.it, fn)
	-- local it = self.it
	-- return Stream.fromFunction(function()
	--   while call_if(fn, coroutine.resume(it)) do end
	-- end)
end

function Stream:map(fn)
	return self:transform(function(...)
		coroutine.yield(fn(...))
	end)
end

function Stream:filter(fn)
	fn = fn or identity
	return self:transform(function(...)
		if (fn(...)) then
			coroutine.yield(...)
		end
	end)
end

function Stream:filterNot(fn)
	fn = fn or identity
	return self:filter(negate(fn))
end

function Stream:concat(stream)
	assert(Stream.isA(stream), "only streams can be concat-ed")
	return Stream.fromFunction(function()
		iterateThread(self.it, coroutine.yield)
		iterateThread(stream.it, coroutine.yield)
	end)
end

function Stream:filterMap(fn)
	return self:transform(function(...)
		try_call_if(coroutine.yield, fn(...))
	end)
end

function Stream:flatMap(fn)
	fn = fn or identity
	return self:transform(function(...)
		local stream = fn(...)
		assert(Stream.isA(stream), "only streams can be flatMap-ed")
		stream:each(coroutine.yield)
	end)
end

function Stream:flatMapLinear(fn)
	fn = fn or identity
	return self:transform(function(...)
		local stream = fn(...)
		Stream.fromLinearIterable(stream):each(coroutine.yield)
	end)
end

Stream.flatMapI = Stream.flatMapLinear

function Stream:takeWhile(filterfn)
	filterfn = filterfn or identity
	return Stream.fromFunction(function()
		iterateThreadWhile(self.it,
			function(...)
				return filterfn(...), ...
			end,
			function(result, ...)
				if not result then
					return false
				end
				coroutine.yield(...)
				return true
			end)
	end)
end

local function negate(fn)
	return function(...)
		return not fn(...)
	end
end

function Stream:takeUntil(filterfn)
	return self:takeWhile(negate(filterfn))
end

function Stream:skipWhile(filterfn)
	return Stream.fromFunction(function()
		iterateThreadWhile(self.it,
			function(...)
				return filterfn(...), ...
			end,
			function(result, ...)
				if not result then
					coroutine.yield(...)
				end
				return result
			end)
		-- iterateThreadWhile(self.it, identity, filterfn)
		iterateThread(self.it, coroutine.yield)
	end)
end

function Stream:skipUntil(filterfn)
	return self:skipWhile(negate(filterfn))
end

function Stream:findL(filterfn)
	local it = self.it
	return Stream.fromFunction(function()
		iterateThreadWhile(it,
			function(...)
				return filterfn(...), ...
			end,
			function(result, ...)
				if result then
					-- Yield the first result that is true
					coroutine.yield(...)
					-- Returning true means the loop will stop
					return false
				end
				return true
			end)
	end)
end

function Stream:find(filterfn)
	return self:findL(filterfn)()
end

function Stream:contains(value)
	return self:find(function(v) return v == value end)
end

function Stream:containsL(value)
	return Stream.once(self:contains(value))
end

function Stream:skip(n)
	local it = self.it
	return Stream.fromFunction(function()
		iterateThread(it, function(...)
			if n > 0 then
				n = n - 1
				return
			end
			coroutine.yield(...)
		end)
	end)
end

function Stream:take(n)
	local it = self.it
	return Stream.fromFunction(function()
		iterateThreadWhile(it,
			function(...)
				if n <= 0 then return end
				n = n - 1
				coroutine.yield(...)
			end,
			function()
				return n > 0
			end)
	end)
end

function Stream:enumerate()
	local i = 0
	return self:transform(function(...)
		i = i + 1
		coroutine.yield(i, ...)
	end)
end

Stream.EXIT = STREAM_EXIT

function Stream:scan(init, fn)
	return Stream.fromFunction(function()
		iterateThread(self.it, function(...)
			init = fn(init, ...)
		end)
		-- fn(init, nil)
		fn(init, STREAM_EXIT)
	end)
end

function Stream:last()
	return Stream.fromFunction(function()
		-- TODO return all of the elements instead of just the first
		-- local last
		-- iterateThread(self.it, function(...)
		--   last = ...
		-- end)
		-- coroutine.yield(last)
		local last
		iterateThread(self.it, function(...)
			last = {...}
		end)
		coroutine.yield(unpack(last))
	end)
end

function Stream:lastL()
	return Stream.once(self:last())
end

function Stream:fold(init, fn)
	if init == STREAM_EXIT then
		return nil
	end
	self:each(function(...)
		init = fn(init, ...) or init
	end)
	return init
end

function Stream:foldL(init, fn)
	return Stream.once(self:fold(init, fn))
end

function Stream.f_add(Z, V)
	return Z + V
end

function Stream.f_max(a, b)
	if a < b then return b else return a end
end

function Stream.f_min(a, b)
	if a < b then return a else return b end
end

function Stream.f_concat(Z, V)
	return Z .. V
end

function Stream.f_append(Z, V)
	table.insert(Z, V)
	return Z
end

-- TODO figure out naming
function Stream.f_merge(Z, V, K)
	Z[K] = V
	return Z
end

function Stream.f_and(Z, V)
	return Z and V
end

function Stream.f_or(Z, V)
	return Z or V
end

function Stream:inspect(fn)
	fn = fn or print
	return self:transform(function(...)
		fn(...)
		coroutine.yield(...)
	end)
end

local function bind1(func, val1)
	return function (...)
		return func(val1, ...)
	end
end

local function preorderTraversal(fn, it, ...)
	local tt = type(it)
	-- print("PREORDER", tt, it, ...)
	if tt == 'thread' then
		iterateThread(it, bind1(preorderTraversal, fn))
	elseif tt == 'function' then
		while call_if_iterator(bind1(preorderTraversal, fn), it()) do end
	elseif tt == 'table' then
		if Stream.isA(it, Stream) then
			return it:each(bind1(preorderTraversal, fn))
		end
		fn(it, ...)
		if it[1] then
			for i, v in ipairs(it) do
				preorderTraversal(fn, v, i)
			end
			return
		end
		for k, v in pairs(it) do
			preorderTraversal(fn, v, k)
		end
		return
	else
		fn(it)
	end
end

function Stream:preorder()
	return self:transform(function(...)
		preorderTraversal(coroutine.yield, ...)
	end)
end

function Stream.fromPreorderTraversal(it)
	return Stream.fromFunction(function()
		preorderTraversal(coroutine.yield, it)
	end)
end

-- UTILITIES

function Stream:collect()
	return self:fold({}, Stream.f_append)
end

function Stream:collectL()
	return Stream.once(self:collect())
end

function Stream:collectDict()
	return self:fold({}, Stream.f_merge)
end

function Stream:collectDictL()
	return Stream.once(self:collectDict())
end

function Stream:sum()
	return self:fold(self:head(), Stream.f_add)
end

function Stream:sumL()
	return Stream.once(self:sum())
end

function Stream:count()
	local n = 0
	self:each(function()
		n = n + 1
	end)
	return n
end

function Stream:countL()
	return Stream.once(self:count())
end

function Stream:join(sep)
	if not sep then
		return self:fold("", Stream.f_concat)
	end
	local status, init = self:resume()
	if not status then
		return nil
	end
	-- This will cast things to string as necessary
	return self:fold(init, function(Z, V)
		return Z .. sep .. V
	end)
end

function Stream:joinL(sep)
	return Stream.once(self:join(sep))
end

local function lessThan(a,b)
	return a < b
end

local function greaterThan(a,b)
	return a > b
end

function Stream:maxBy(keyfn)
	assert(keyfn)
	local function cmpfn(a, b)
		return Stream.f_max(a, keyfn(b))
	end
	local init = self:head()
	if init == STREAM_EXIT then
		return nil
	end
	return self:fold(keyfn(init), cmpfn)
end

function Stream:maxByL(keyfn)
	return Stream.once(self:maxBy(keyfn))
end

function Stream:minBy(keyfn)
	assert(keyfn)
	local function cmpfn(a, b)
		return Stream.f_min(a, keyfn(b))
	end
	local init = self:head()
	if init == STREAM_EXIT then
		return nil
	end
	return self:fold(keyfn(init), cmpfn)
end

function Stream:minByL(keyfn)
	return Stream.once(self:minBy(keyfn))
end

function Stream:max()
	return self:fold(self(), Stream.f_max)
end

function Stream:maxL()
	return Stream.once(self:max())
end

function Stream:min()
	return self:fold(self(), Stream.f_min)
end

function Stream:minL()
	return Stream.once(self:min())
end

-- function Stream:zip(stream)
--   return self:take_while(function(...)
--     local a = {...}
--     local b = {stream:resume()}
--     local status = b[1]
--     if not status then
--     end
--     local b =
--     local status, ...
--   end)
--   return Stream.once(self:join(sep))
-- end

function Stream:cloned()
	if not self.data then
		self.data = self:collect()
	end
	return Stream.fromArray(self.data)
end

function Stream:print()
	return self:each(print)
end

-- TODO needed?
function Stream:chainCall(fn)
	return fn(self)
end

function Stream:__tostring()
	self:print()
	return ""
end

function Stream.import(fn, ...)
	fn(Stream, ...)
	return Stream
end

function Stream.require(module, ...)
	return Stream.import(require(module), ...)
end

-- EXPERIMENTAL aggregators

-- TODO optimize
local function hash(...)
	if select("#", ...) == 0 then
		return ...
	end
	return table.concat({...},"\0")
end

function Stream:uniqueBy(keyfn)
	keyfn = keyfn or identity
	return self:scan({}, function(Z, head, ...)
		if head == STREAM_EXIT then
			return
		end
		local key = hash(keyfn(head, ...))
		if Z[key] == nil then
			Z[key] = true
			coroutine.yield(head, ...)
		end
		return Z
	end)
end

function Stream:groupBy(keyfn)
	keyfn = keyfn or identity
	return self:scan({}, function(Z, head, ...)
		if head == STREAM_EXIT then
			for key, values in pairs(Z) do
				-- TODO optimize?
				coroutine.yield(Stream.fromArray(values):map(function(x) return unpack(x) end), key)
			end
			-- coroutine.yield(Z)
			return
		end
		local key = hash(keyfn(head, ...))
		local tableval = Z[key]
		if tableval == nil then
			Z[key] = { {head, ...} }
		else
			table.insert(tableval, {head, ...})
		end
		return Z
	end)
end

function Stream:sortBy(keyfn)
	-- TODO memoize?
	keyfn = keyfn or identity
	local function cmpfn(a, b)
		return keyfn(a) < keyfn(b)
	end
	return self:collectL()
		:flatMapLinear(function(arr)
			table.sort(arr, cmpfn)
			return arr
		end)
end

function Stream:sorted(cmpfn)
	cmpfn = cmpfn or lessThan
	return self:collectL()
		:flatMapLinear(function(arr)
			table.sort(arr, cmpfn)
			return arr
		end)
end

function Stream:reverse()
	return Stream.fromFunction(function()
		local arr = self:collect()
		for i=#arr,1,-1 do
			coroutine.yield(arr[i])
		end
	end)
end

-- function Stream:ordered_unique_by(keyfn, orderfn)
--   keyfn = keyfn or identity
--   return Stream.fromFunction(function()
--     local keys = {}
--     iterateThread(self.it, function(...)
--       local key = hash(keyfn(...))
--       local existing = keys[key]
--       if existing == nil then
--         keys[key] = true
--         coroutine.yield(...)
--       end
--     end)
--   end)
-- end

-- UTILITIES to generate streams

function Stream.seq(i, j, step)
	step = step or 1
	return Stream.fromFunction(function()
		if not j then
			while true do
				coroutine.yield(i)
				i = i + step
			end
		else
			for k = i,j,step do
				coroutine.yield(k)
			end
		end
	end)
end

function Stream.fromFileFn(file, fn)
	if type(file) == "string" then
		file = io.open(file, "r")
	end
	return Stream.fromFunction(function()
		fn(file)
		file:close()
	end)
end

function Stream.fromFile(file)
	return Stream.fromFileFn(file, function(file)
		coroutine.yield(file:read("*a"))
	end)
end

function Stream.fromFileLines(file)
	return Stream.fromFileFn(file, function(file)
		for line in file:lines() do
			coroutine.yield(line)
		end
	end)
end

function Stream.lines()
	return Stream.fromFunction(function()
		for line in io.lines() do
			coroutine.yield(line)
		end
	end)
end

-- TODO this won't close if the coroutine is exhausted. not
function Stream.fromShellLines(shell_cmd, ...)
	shell_cmd = string.format(shell_cmd, ...)
	return Stream.fromFunction(function()
		local command = io.popen(shell_cmd)
		for line in command:lines() do
			coroutine.yield(line)
		end
		command:close()
	end)
end

function Stream.fromShell(shell_cmd, ...)
	shell_cmd = string.format(shell_cmd, ...)
	return Stream.fromFunction(function()
		local command = io.popen(shell_cmd)
		coroutine.yield(command:read("*a"))
		command:close()
	end)
end

function Stream.gsplit(s, sep)
	return Stream.fromFunction(function()
		if s == '' or sep == '' then
			coroutine.yield(s)
			return
		end
		local lasti = 1
		for v,i in s:gmatch('(.-)'..sep..'()') do
			coroutine.yield(v)
			lasti = i
		end
		coroutine.yield(s:sub(lasti))
	end)
end

function Stream:gsplitBuffered(sep)
	local pattern = '(.-)'..sep..'()'
	return self:scan("", function(Z, V)
		if V == Stream.EXIT then
			local s = Z
			local lasti = 1
			for v, i in s:gmatch(pattern) do
				coroutine.yield(v)
				lasti = i
			end
			coroutine.yield(s:sub(lasti))
		else
			local s = Z .. V
			local lasti = 1
			for v, i in s:gmatch(pattern) do
				coroutine.yield(v)
				lasti = i
			end
			return s:sub(lasti)
		end
	end)
end

function Stream:linesBuffered()
	return self:gsplitBuffered("\n")
end

return Stream
