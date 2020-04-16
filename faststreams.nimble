mode = ScriptMode.Verbose

packageName   = "faststreams"
version       = "0.1.0"
author        = "Status Research & Development GmbH"
description   = "Nearly zero-overhead input/output streams for Nim"
license       = "Apache License 2.0"
skipDirs      = @["tests"]

requires "nim >= 1.2.0",
         "stew",
         "chronos"

task test, "Run all tests":
  exec "nim c -r --threads:off tests/all_tests"
  exec "nim c -r --threads:on tests/all_tests"

