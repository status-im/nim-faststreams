mode = ScriptMode.Verbose

packageName   = "faststreams"
version       = "0.6.0"
author        = "Status Research & Development GmbH"
description   = "Nearly zero-overhead input/output streams for Nim"
license       = "Apache License 2.0"
skipDirs      = @["tests"]

requires "nim >= 2.0.14",
         "stew >= 0.6.0",
         "unittest2 >= 0.3.0"

let nimc = getEnv("NIMC", "nim") # Which nim compiler to use
let lang = getEnv("NIMLANG", "c") # Which backend (c/cpp/js)
let flags = getEnv("NIMFLAGS", "") # Extra flags for the compiler
let verbose = getEnv("V", "") notin ["", "0"]
let platform = getEnv("PLATFORM", "")
let testArguments = [
  "-d:debug",
  "-d:release",
  "-d:danger",
]

from std/os import quoteShell

let cfg =
  " --styleCheck:usages --styleCheck:error" &
  (if verbose: "" else: " --verbosity:0") &
  " --skipParentCfg --skipUserCfg --outdir:build -f " &
  quoteShell("--nimcache:build/nimcache/$projectName")

proc build(args, path: string) =
  exec nimc & " " & lang & " " & cfg & " " & flags & " " & args & " " & path

proc run(args, path: string) =
  build args & " -r", path

task test, "Run all tests":
  # TODO asyncdispatch backend is broken / untested
  # TODO chronos backend uses nested waitFor which is not supported
  for backend in ["-d:asyncBackend=none"]:
    for threads in ["--threads:off", "--threads:on"]:
      for args in testArguments:
        run backend & " " & threads & " " & args & " --mm:refc", "tests/all_tests"
        run backend & " " & threads & " " & args & " --mm:orc", "tests/all_tests"

      # Nim CI runs `nimble test` as part of its important packages,
      # keep ASAN here as well so that Nim regressions are caught
      # https://github.com/nim-lang/Nim/blob/devel/testament/important_packages.nim
      if defined(linux) and defined(amd64):
        run backend & " " & threads &
          " -d:danger --mm:orc -d:useMalloc --cc:clang --debugger:native" &
          " --passC:-fsanitize=address --passL:-fsanitize=address",
          "tests/all_tests"

task testChronos, "Run chronos tests":
  # TODO chronos backend uses nested waitFor which is not supported
  for backend in ["-d:asyncBackend=chronos"]:
    for threads in ["--threads:off", "--threads:on"]:
      for args in testArguments:
        run backend & " " & threads & " " & args & " --mm:refc", "tests/all_tests"
        run backend & " " & threads & " " & args & " --mm:orc", "tests/all_tests"

task test_asan, "Run all tests with ASAN":
  if platform != "x86":
    # https://clang.llvm.org/docs/AddressSanitizer.html
    putEnv("ASAN_OPTIONS", "detect_leaks=0:detect_stack_use_after_return=1")
    # https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html
    putEnv("UBSAN_OPTIONS", "print_stacktrace=1")
    let asanArgs =
      " --mm:orc -d:useMalloc --cc:clang --debugger:native" &
      " --passC:-fsanitize=address,undefined" &
      " --passL:-fsanitize=address,undefined" &
      " --passC:-fno-sanitize-recover=undefined" &
      " --passC:-fno-sanitize-merge" &
      " --passC:-fno-omit-frame-pointer"
    # TODO asyncdispatch backend is broken / untested
    # TODO chronos backend uses nested waitFor which is not supported
    for backend in ["-d:asyncBackend=none"]:
      for threads in ["--threads:off", "--threads:on"]:
        for args in testArguments:
          run backend & " " & threads & " " & args & asanArgs, "tests/all_tests"
