package = "stream"
version = "0.1-1"
source = {
   url = "git://github.com/norcalli/lua-stream"
}
description = {
   homepage = "https://github.com/norcalli/lua-stream.git",
   license = "MIT"
}
dependencies = {
   "lua >= 5.1"
}
build = {
   type = "builtin",
   modules = {
      Stream = "src/Stream.lua",
      -- ["Stream.fetch"] = "Stream/fetch.lua",
      -- ["Stream.popen"] = "Stream/popen.lua",
      -- ["Stream.redis"] = "Stream/redis.lua"
   }
}
