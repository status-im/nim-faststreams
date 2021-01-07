{.used.}

import
  # Std lib:
  std/[strutils, random, base64, terminal],
  # Other packages:
  testutils/unittests,
  # FastStreams modules:
  ../faststreams/[pipelines, multisync],
  # Testing modules:
  ./base64 as fsBase64

include system/timers

type
  TestTimes = object
    fsPipeline: Nanos
    fsAsyncPipeline: Nanos
    stdFunctionCalls: Nanos

proc upcaseAllCharacters(i: InputStream, o: OutputStream) {.fsMultiSync.} =
  let inputLen = i.len
  if inputLen.isSome:
    o.ensureRunway inputLen.get

  while i.readable:
    o.write toUpperAscii(i.read.char)

  close o

proc printTimes(t: TestTimes) =
  styledEcho "  cpu time [FS Sync  ]: ", styleBright, $t.fsPipeline, "ms"
  styledEcho "  cpu time [FS Async ]: ", styleBright, $t.fsAsyncPipeline, "ms"
  styledEcho "  cpu time [Std Lib  ]: ", styleBright, $t.stdFunctionCalls, "ms"

template timeit(timerVar: var Nanos, code: untyped) =
  let t0 = getTicks()
  code
  timerVar = int(getTicks() - t0) div 1000000

proc getOutput(sp: AsyncInputStream, T: type string): Future[string] {.async.} =
  # this proc is a quick hack to let the test pass
  # do not use it in production code
  let size = sp.totalUnconsumedBytes()
  if size > 0:
    var data = newSeq[byte](size)
    discard sp.readinto(data)
    result = cast[string](data)

procSuite "pipelines":
  let loremIpsum = """
    Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod
    tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
    veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex
    ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
    velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat
    cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id
    est laborum.

  """

  test "upper-case/base64 pipeline benchmark":
    var
      times: TestTimes
      stdRes: string
      fsRes: string
      fsAsyncRes: string

    let inputText = loremIpsum.repeat(5000)

    timeIt times.stdFunctionCalls:
      stdRes = base64.decode(base64.encode(toUpperAscii(inputText)))

    timeIt times.fsPipeline:
      fsRes = executePipeline(unsafeMemoryInput(inputText),
                              upcaseAllCharacters,
                              base64encode,
                              base64decode,
                              getOutput string)

    timeIt times.fsAsyncPipeline:
      fsAsyncRes = waitFor executePipeline(Async unsafeMemoryInput(inputText),
                                           upcaseAllCharacters,
                                           base64encode,
                                           base64decode,
                                           getOutput string)

    check fsAsyncRes == stdRes
    check fsRes == stdRes

    printTimes times

  asyncTest "upper-case/base64 async pipeline":
    let pipe = asyncPipe()
    let inputText = repeat(loremIpsum, 100)

    proc pipeFeeder(s: AsyncOutputStream) {.gcsafe, async.} =
      randomize 1234
      var pos = 0

      while pos != inputText.len:
        let bytesToWrite = rand(15)

        if bytesToWrite == 0:
          s.write inputText[pos]
          inc pos
        else:
          let endPos = min(pos + bytesToWrite, inputText.len)
          s.writeAndWait inputText[pos ..< endPos]
          pos = endPos

        let sleep = rand(50) - 45
        if sleep > 0:
          await sleepAsync(sleep.milliseconds)

      close s

    asyncCheck pipeFeeder(pipe.initWriter)

    let f = executePipeline(pipe.initReader,
                            upcaseAllCharacters,
                            base64encode,
                            base64decode,
                            getOutput string)

    let fsAsyncres = await f

    check fsAsyncRes == toUpperAscii(inputText)
