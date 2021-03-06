package = "k-stream"
version = "dev-0"
source = {
   url = "git://github.com/norcalli/lua-stream"
}
description = {
   homepage = "https://github.com/norcalli/lua-stream.git",
   license = "MIT"
}
dependencies = {
   "lua >= 5.1, < 5.4"
}
build = {
   type = "builtin",
   modules = {
      Stream = "src/Stream.lua",
   }
}
