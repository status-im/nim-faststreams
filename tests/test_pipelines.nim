{.used.}

import
  std/[unittest, strutils, base64],
  ../faststreams/pipelines,
  ./base64 as fsBase64

include system/timers

type
  TestTimes = object
    fsPipeline: Nanos
    fsAsyncPipeline: Nanos
    stdFunctionCalls: Nanos

proc upcaseAllCharacters(i: InputStream, o: OutputStream) =
  while i.readable:
    o.write toUpperAscii(char i.read())

template timeit(timerVar: var Nanos, code: untyped) =
  let t0 = getTicks()
  code
  timerVar = int(getTicks() - t0) div 1000000

suite "pipelines":
  var loremIpsum = """
    Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod
    tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
    veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex
    ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
    velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat
    cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id
    est laborum.

  """.repeat(100)

  test "upper-case/base64 pipeline":
    var
      times: TestTimes
      stdRes: string
      fsRes: string

    timeIt times.fsPipeline:
      var memOut = memoryOutput()
      executePipeline(unsafeMemoryInput(loremIpsum),
                      upcaseAllCharacters,
                      base64encode,
                      base64decode,
                      memOut)
      fsRes = memOut.getOutput(string)

    timeIt times.stdFunctionCalls:
      stdRes = base64.decode(base64.encode(toUpperAscii(loremIpsum)))

    check fsRes == stdRes

