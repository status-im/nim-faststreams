mode = ScriptMode.Verbose

packageName   = "faststreams"
version       = "0.3.0"
author        = "Status Research & Development GmbH"
description   = "Nearly zero-overhead input/output streams for Nim"
license       = "Apache License 2.0"
skipDirs      = @["tests"]

requires "nim >= 1.2.0",
         "stew",
         "chronos",
         "unittest2"

### Helper functions
proc test(args, path: string) =
  # Compilation language is controlled by TEST_LANG
  let lang = getEnv("TEST_LANG", "c")

  # nnkArglist was changed to nnkArgList, so can't always use --styleCheck:error
  # https://github.com/nim-lang/Nim/pull/17529
  # https://github.com/nim-lang/Nim/pull/19822
  let styleCheckStyle =
    if (NimMajor, NimMinor) < (1, 6):
      "hint"
    else:
      "error"

  var common_args = "-r -f " & getEnv("NIMFLAGS") &  " --hints:off --styleCheck:usages --styleCheck:" & styleCheckStyle

  if getEnv("NIMBUS_ENV_DIR") != "":
    common_args &= " --skipParentCfg"

  exec "nim " & lang & " " & args &
    " -d:asyncBackend=none " & common_args & " " & path
  exec "nim " & lang & " " & args &
    " -d:asyncBackend=chronos " & common_args & " " & path
  # TODO std backend is broken / untested
  # exec "nim " & lang & " " & args &
  #  " -d:asyncBackend=asyncdispatch " & common_args & " " & path

task test, "Run all tests":
  test "-d:debug   --threads:off", "tests/all_tests"
  test "-d:release --threads:off", "tests/all_tests"
  test "-d:danger  --threads:off", "tests/all_tests"
  test "-d:debug   --threads:on", "tests/all_tests"
  test "-d:release --threads:on", "tests/all_tests"
  test "-d:danger  --threads:on", "tests/all_tests"
