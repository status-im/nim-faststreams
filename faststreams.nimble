mode = ScriptMode.Verbose

packageName   = "faststreams"
version       = "0.1.0"
author        = "Status Research & Development GmbH"
description   = "Nearly zero-overhead input/output streams for Nim"
license       = "Apache License 2.0"
skipDirs      = @["tests"]

requires "nim >= 0.17.0",
         "ranges",
         "std_shims"

import ospaths, strutils

task test, "Run tests":
  for filename in listFiles("tests"):
    if filename.startsWith("tests" / "test_") and filename.endsWith(".nim"):
      exec "nim c -r " & filename
      rmFile filename[0..^5]

