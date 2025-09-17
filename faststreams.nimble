mode = ScriptMode.Verbose

packageName   = "faststreams"
version       = "0.5.0"
author        = "Status Research & Development GmbH"
description   = "Nearly zero-overhead input/output streams for Nim"
license       = "Apache License 2.0"
skipDirs      = @["tests"]

requires "nim >= 1.6.0",
         "stew >= 0.2.0",
         "unittest2"

let nimc = getEnv("NIMC", "nim") # Which nim compiler to use
let lang = getEnv("NIMLANG", "c") # Which backend (c/cpp/js)
let flags = getEnv("NIMFLAGS", "") # Extra flags for the compiler
let verbose = getEnv("V", "") notin ["", "0"]

let cfg =
  " --styleCheck:usages --styleCheck:error" &
  (if verbose: "" else: " --verbosity:0 --hints:off") &
  " --skipParentCfg --skipUserCfg --outdir:build --nimcache:build/nimcache -f"

proc build(args, path: string) =
  exec nimc & " " & lang & " " & cfg & " " & flags & " " & args & " " & path

import strutils
proc run(args, path: string) =
  build args & " --mm:refc -r", path
  if (NimMajor, NimMinor) >= (2, 0):
    build args & " --mm:orc -r", path

    if (NimMajor, NimMinor) >= (2, 2) and defined(linux) and defined(amd64) and "danger" in args:
      # Test with AddressSanitizer
      build args & " --mm:orc -d:useMalloc --cc:clang --passc:-fsanitize=address --passl:-fsanitize=address --debugger:native -r", path

task test, "Run all tests":
  # TODO asyncdispatch backend is broken / untested
  # TODO chronos backend uses nested waitFor which is not supported
  for backend in ["-d:asyncBackend=none"]:
    for threads in ["--threads:off", "--threads:on"]:
      for mode in ["-d:debug", "-d:release", "-d:danger"]:
        run backend & " " & threads & " " & mode, "tests/all_tests"

task testChronos, "Run chronos tests":
  # TODO chronos backend uses nested waitFor which is not supported
  for backend in ["-d:asyncBackend=chronos"]:
    for threads in ["--threads:off", "--threads:on"]:
      for mode in ["-d:debug", "-d:release", "-d:danger"]:
        run backend & " " & threads & " " & mode, "tests/all_tests"
