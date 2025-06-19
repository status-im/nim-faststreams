{.used.}

import
  os, unittest2, strutils, sequtils, random,
  ../faststreams, ../faststreams/textio

setCurrentDir currentSourcePath.parentDir

proc str(bytes: openArray[byte]): string =
  result = newStringOfCap(bytes.len)
  for b in items(bytes):
    result.add b.char

proc countLines(s: InputStream): Natural =
  for s in lines(s):
    inc result

proc readAll(s: InputStream): seq[byte] =
  while s.readable:
    result.add s.read

const
  asciiTableFile = "files" / "ascii_table.txt"
  asciiTableContents = slurp(asciiTableFile)

suite "input stream":
  template emptyInputTests(suiteName, setupCode: untyped) =
    suite suiteName & " empty inputs":
      setup setupCode

      test "input is not readable with read":
        check not input.readable
        when not defined(danger):
          expect Defect:
            echo "This read should not complete: ", input.read

      test "input is not readable with read(n)":
        check not input.readable(10)
        when not defined(danger):
          expect Defect:
            echo "This read should not complete: ", input.read(10)

      test "next returns none":
        check input.next.isNone

      test "input can be closed":
        input.close()

  emptyInputTests "memoryInput":
    var input = InputStream()

  emptyInputTests "unsafeMemoryInput":
    var str = ""
    var input = unsafeMemoryInput(str)

  emptyInputTests "fileInput":
    var input = fileInput("files" / "empty_file")

  emptyInputTests "memFileInput":
    var input = memFileInput("files" / "empty_file")

  template asciiTableFileTest(name: string, body: untyped) =
    suite name:
      test name & " of ascii table with regular pageSize":
        var input {.inject.} = fileInput(asciiTableFile)
        try:
          body
        finally:
          close input

      test name & " of ascii table with pageSize = 10":
        var input {.inject.} = fileInput(asciiTableFile, pageSize = 10)
        try:
          body
        finally:
          close input

      test name & " of ascii table with pageSize = 1":
        var input {.inject.} = fileInput(asciiTableFile, pageSize = 1)
        try:
          body
        finally:
          close input

  # TODO: fileInput with offset
  #        - in the middle of the
  #        - right at the end of the file
  #        - past the end of the file

  asciiTableFileTest "count lines":
    check countLines(input) == 34

  asciiTableFileTest "mixed read types":
    randomize(10000)

    var fileContents = ""

    while true:
      let r = rand(100)
      if r < 20:
        let readSize = 1 + rand(10)

        var buf = newSeq[byte](readSize)
        let bytesRead = input.readIntoEx(buf)
        fileContents.add buf.toOpenArray(0, bytesRead - 1).str

        if bytesRead < buf.len:
          break

      elif r < 50:
        let readSize = 6 + rand(10)

        if input.readable(readSize):
          fileContents.add input.read(readSize).str
        else:
          while input.readable:
            fileContents.add input.read.char
          break

      elif r < 60:
        # Test the ability to call readable() and read() multiple times from
        # the same scope.
        let readSize = 6 + rand(10)

        if input.readable(readSize):
          fileContents.add input.read(readSize).str
        else:
          while input.readable:
            fileContents.add input.read.char
          break

        if input.readable(readSize):
          fileContents.add input.read(readSize).str
        else:
          while input.readable:
            fileContents.add input.read.char
          break

      else:
        if input.readable:
          fileContents.add input.read.char

      # You can uncomment this to get earlier failure in the test:
      when false:
        require fileContents == asciiTableContents[0 ..< fileContents.len]

    check fileContents == asciiTableContents

  suite "misc":
    test "reading into empty buffer":
      var input = memoryInput([byte 1])

      var buf: seq[byte]
      check:
        # Reading into empty should succeed for open stream
        input.readIntoEx(buf) == 0
        input.readInto(buf)

      buf.setLen(1)
      check:
        input.readIntoEx(buf) == 1
        input.readIntoEx(buf) == 0
        not input.readInto(buf)
    test "missing file input":
      const fileName = "there-is-no-such-faststreams-file"

      check not fileExists(fileName)
      expect CatchableError: discard fileInput(fileName)

      check not fileExists(fileName)
      expect CatchableError: discard memFileInput(fileName)

      check not fileExists(fileName)

    test "can close nil InputStream":
      var v: InputStream
      v.close()

    test "non-blocking reads":
      let s = fileInput(asciiTableFile, pageSize = 100)
      if s.readable(20):
        s.withReadableRange(20, r):
          check r.len.get == 20
          check r.totalUnconsumedBytes == 20
          check r.readAll.len == 20

      if s.readable(20):
        s.withReadableRange(20, r):
          check r.len.get == 20
          check r.totalUnconsumedBytes == 20
          check r.readAll.len == 20

      check s.readable

      if s.readable(160):
        s.withReadableRange(160, r):
          check r.len.get == 160
          check r.totalUnconsumedBytes == 160
          check r.readAll.len == 160

      check s.readable

      if s.readable(20):
        s.withReadableRange(20, r):
          check r.len.get == 20
          check r.totalUnconsumedBytes == 20
          check r.readAll.len == 20

      check s.readable

    template drainTest(name: string, setup: untyped) =
      test "draining readable ranges " & name:
        setup
        check:
          s.readable(20)

        s.withReadableRange(20, r):
          var tmp: array[100, byte]

          check:
            r.drainBuffersInto(addr tmp[0], tmp.len) == 20
            @tmp == repeat(byte 5, 20) & repeat(byte 0, 80)

        check:
          s.readable(80)

        var tmp: array[10, byte]
        check:
          s.drainBuffersInto(addr tmp[0], tmp.len) == 10

          @tmp == repeat(byte 5, 10)

        s.withReadableRange(50, r):
          var tmp: array[100, byte]
          check:
            r.drainBuffersInto(addr tmp[0], tmp.len) == 50
            @tmp == repeat(byte 5, 50) & repeat(byte 0, 50)

        check:
          s.readable()

        check:
          s.drainBuffersInto(addr tmp[0], tmp.len) == 10
          @tmp == repeat(byte 5, 10)

        check:
          s.drainBuffersInto(addr tmp[0], tmp.len) == 10
          @tmp == repeat(byte 5, 10)

        check:
          not s.readable()

    drainTest "unsafeMemoryInput":
      var data = repeat(byte 5, 100)
      let s = unsafeMemoryInput(data)

    drainTest "memoryInput":
      var data = repeat(byte 5, 100)
      let s = memoryInput(data)

    test "simple":
      var input = repeat("1234 5678 90AB CDEF\n", 1000)
      var stream = unsafeMemoryInput(input)

      check:
        (stream.read(4) == "1234".toOpenArrayByte(0, 3))

  template posTest(name: string, setup: untyped) =
    test name:
      setup
      check input.readable
      check input.pos == 0
      discard input.read
      check input.pos == 1
      close input

  posTest "unsafeMemoryInput pos":
    var str = "hello"
    var input = unsafeMemoryInput(str)

  posTest "fileInput pos":
    var input = fileInput(asciiTableFile)

  posTest "memFileInput pos":
    var input = memFileInput(asciiTableFile)
