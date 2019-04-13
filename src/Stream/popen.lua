return function(Stream)
  local popen3 = require 'popen'
  local M = require 'posix.unistd'

  -- TODO pass options instead with stderr n' shit
  function Stream:popen(command, extra)
    if not Stream.isA(self) then
      self, command, extra = Stream.empty(), self, command
    end
    extra = extra or {}
    -- I tested various sizes, and this was the best.
    buffer_size = extra.buffer_size or 1024*1024*1024
    if type(command) == "string" then
      command = {"sh", "-c", command}
    end
    assert(type(command) == 'table' and command[1], 'popen command must be an array')
    local pid, r1, w2, w3 = popen3(command[1], {table.unpack(command, 2)}, extra)
    -- print(pid, r1, w2, w3)

    return Stream.fromFunction(function()
      self:each(function(input)
        local nbytes, err = M.write(r1, input)
        -- assert(nbytes ~= nil, "Failed to write to pipe: "..err)
        assert(nbytes ~= nil, err)
      end)
      M.close(r1)

      while true do
        local value, err = M.read(w2, buffer_size)
        assert(value, err)
        if value == "" then
          break
        end
        coroutine.yield(value)
      end
      M.close(w2)
      M.close(w3)


      local wait_pid, wait_cause, wait_status = (require "posix.sys.wait").wait(pid)
    end)
  end

  function Stream:popenLines(command, extra)
    return Stream.popen(self, command, extra):gsplitBuffered("\n")
  end

  function Stream:popenFull(command, extra)
    return Stream.popen(self, command, extra):joinL()
  end

  return Stream
end

