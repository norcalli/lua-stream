return function(Stream)
  local fetch = require 'fetch'

  function Stream:fetchPostChunks(url, extra)
    extra = extra or {}
    extra.body = function() return self() end
    local headers, stream = fetch.fetchStream(url, extra)
    return Stream.once(headers)
      :concat(Stream.fromIterator(stream:each_chunk()))
  end

  function Stream.fetchChunks(url, extra)
    local headers, stream = fetch.fetchStream(url, extra)
    return Stream.once(headers)
      :concat(Stream.fromIterator(stream:each_chunk()))
  end

  function Stream.fetch(url, extra)
    local headers, stream = fetch.fetchStream(url, extra)
    return Stream.once(headers)
      :concat(Stream.once(stream:get_body_as_string()))
  end

  return Stream
end
