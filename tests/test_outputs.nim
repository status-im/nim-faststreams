{.used.}

import
  os, unittest2, random, strformat,
  stew/ranges/ptr_arith,
  ../faststreams, ../faststreams/textio

proc bytes(s: string): seq[byte] =
  result = newSeqOfCap[byte](s.len)
  for c in s: result.add byte(c)

proc bytes(s: cstring): seq[byte] =
  for c in s: result.add byte(c)

template bytes(c: char): byte = byte(c)
template bytes(b: seq[byte]): seq[byte] = b
template bytes[N, T](b: array[N, T]): seq[byte] = @b

proc repeat(b: byte, count: int): seq[byte] =
  result = newSeq[byte](count)
  for i in 0 ..< count: result[i] = b

const line = "123456789123456789123456789123456789\n\n\n\n\n"

proc randomBytes(n: int): seq[byte] =
  result.newSeq n
  for i in 0 ..< n:
    result[i] = byte(line[rand(line.len - 1)])

proc readAllAndClose(s: InputStream): seq[byte] =
  while s.readable:
    result.add s.read

  close(s)

import memfiles

suite "output stream":
  setup:
    var
      nimSeq: seq[byte] = @[]

      memStream = memoryOutput()
      smallPageSizeStream = memoryOutput(pageSize = 10)
      largePageSizeStream = memoryOutput(pageSize = 1000000)

      fileOutputPath = getTempDir() / "faststreams_testfile"
      unbufferedFileOutputPath = getTempDir() / "faststreams_testfile_unbuffered"

      fileStream = fileOutput(fileOutputPath)
      unbufferedFileStream = fileOutput(unbufferedFileOutputPath, pageSize = 0)

      bufferSize = 1000000
      buffer = alloc(bufferSize)
      streamWritingToExistingBuffer = unsafeMemoryOutput(buffer, bufferSize)

  teardown:
    close fileStream
    close unbufferedFileStream
    removeFile fileOutputPath
    removeFile unbufferedFileOutputPath
    dealloc buffer

  template output(val: auto) {.dirty.} =
    nimSeq.add bytes(val)
    memStream.write val
    smallPageSizeStream.write val
    largePageSizeStream.write val
    fileStream.write val
    unbufferedFileStream.write val
    streamWritingToExistingBuffer.write val

  template outputText(val: auto) =
    let valAsStr = $val
    nimSeq.add valAsStr.toOpenArrayByte(0, valAsStr.len - 1)

    memStream.writeText val
    smallPageSizeStream.writeText val
    largePageSizeStream.writeText val

    fileStream.writeText val
    unbufferedFileStream.writeText val

    streamWritingToExistingBuffer.writeText val

  template checkOutputsMatch(showResults = false,
                             skipUnbufferedFile = false) =
    close fileStream
    close unbufferedFileStream

    check fileExists(fileOutputPath) and
          fileExists(unbufferedFileOutputPath)

    let
      memStreamRes = memStream.getOutput
      readFileRes = readFile(fileOutputPath).string.bytes
      fileInputRes = fileInput(fileOutputPath).readAllAndClose
      memFileInputRes = memFileInput(fileOutputPath).readAllAndClose
      fileInputWithSmallPagesRes = fileInput(fileOutputPath, pageSize = 10).readAllAndClose

    when showResults:
      checkpoint "Nim seq result"
      checkpoint $nimSeq

      checkpoint "Writes to existing buffer result"
      checkpoint $makeOpenArray(cast[ptr byte](buffer),
                               streamWritingToExistingBuffer.pos)

      checkpoint "mem stream result"
      checkpoint $memStreamRes

      checkpoint "readFile result"
      checkpoint $readFileRes

      checkpoint "fileInput result"
      checkpoint $fileInputRes

      checkpoint "memFileInput result"
      checkpoint $memFileInputRes

      checkpoint "fileInput with small pageSize result"
      checkpoint $fileInputWithSmallPagesRes

    let outputsMatch =
      nimSeq == makeOpenArray(cast[ptr byte](buffer),
                              streamWritingToExistingBuffer.pos)
    check outputsMatch

    check nimSeq == memStreamRes
    check nimSeq == readFileRes
    check nimSeq == fileInputRes
    check nimSeq == memFileInputRes
    check nimSeq == fileInputWithSmallPagesRes

    when not skipUnbufferedFile:
      let unbufferedFileRes = readFile(unbufferedFileOutputPath).string.bytes
      check nimSeq == unbufferedFileRes

  test "no appends produce an empty output":
    checkOutputsMatch()

  test "write zero length slices":
    output ""
    output newSeq[byte]()
    var arr: array[0, byte]
    output arr

    check nimSeq.len == 0
    checkOutputsMatch()

  test "text output":
    for i in 1 .. 100:
      outputText i
      outputText " bottles on the wall"
      outputText '\n'

    checkOutputsMatch()

  test "cstrings":
    for i in 1 .. 100:
      output cstring("cstring sent by output ")
      output cstring("")
      outputText cstring("cstring sent by outputText ")
      outputText cstring("")

    checkOutputsMatch()

  test "memcpy":
    var x = 0x42'u8

    nimSeq.add x
    memStream.writeMemCopy x
    let memStreamRes = memStream.getOutput

    check memStreamRes == nimSeq

  template undelayedOutput(content: seq[byte]) {.dirty.} =
    nimSeq.add content
    streamWritingToExistingBuffer.write content

  test "delayed write":
    output "initial output\n"
    const delayedWriteContent = bytes "delayed write\n"

    var memCursor = memStream.delayFixedSizeWrite(delayedWriteContent.len)
    var fileCursor = fileStream.delayVarSizeWrite(delayedWriteContent.len + 50)

    let cursorStart = memStream.pos

    undelayedOutput delayedWriteContent

    var bytesWritten = 0
    for i, count in [2, 12, 342, 2121, 23, 1, 34012, 932]:
      output repeat(byte(i), count)
      bytesWritten += count
      check memStream.pos - cursorStart == bytesWritten

    memCursor.finalWrite delayedWriteContent
    fileCursor.finalWrite delayedWriteContent

    checkOutputsMatch(skipUnbufferedFile = true)

  test "float output":
    let basic: float64 = 12345.125
    let small: float32 = 12345.125
    let large: float64 = 9.99e+20
    let tiny: float64 = -2.25e-35

    outputText basic
    outputText small
    outputText large
    outputText tiny

    checkOutputsMatch()

proc writeBlock(data: openArray[byte], output: openArray[byte]): int =
  doAssert data.len <= output.len
  copyMem(unsafeAddr output[0], unsafeAddr data[0], data.len)
  data.len

suite "randomized tests":
  type
    WriteTypes = enum
      FixedSize
      VarSize
      Mixed

    DelayedWrite = object
      isFixedSize: bool
      fixedSizeCursor: WriteCursor
      varSizeCursor: VarSizeWriteCursor
      content: seq[byte]
      written: int

  proc randomizedCursorsTestImpl(stream: OutputStream,
                                 seed = 1000,
                                 iterations = 1000,
                                 minWriteSize = 500,
                                 maxWriteSize = 1000,
                                 writeTypes = Mixed,
                                 varSizeVariance = 50): seq[byte] =
    randomize seed

    var delayedWrites = newSeq[DelayedWrite]()
    let writeSizeSpread = maxWriteSize - minWriteSize

    for i in 0 ..< iterations:
      let decision = rand(100)

      if decision < 20:
        # Write at some random cursor
        if delayedWrites.len > 0:
          let
            i = rand(delayedWrites.len - 1)
            written = delayedWrites[i].written
            remaining = delayedWrites[i].content.len - written
            toWrite = min(rand(remaining) + 10, remaining)

          if delayedWrites[i].isFixedSize:
            delayedWrites[i].fixedSizeCursor.write delayedWrites[i].content[written ..< written + toWrite]

          delayedWrites[i].written += toWrite

          if remaining - toWrite == 0:
            if delayedWrites[i].isFixedSize:
              finalize delayedWrites[i].fixedSizeCursor
            else:
              finalWrite delayedWrites[i].varSizeCursor, delayedWrites[i].content

            if i != delayedWrites.len - 1:
              swap(delayedWrites[i], delayedWrites[^1])
            delayedWrites.setLen(delayedWrites.len - 1)

        continue

      let
        size = rand(writeSizeSpread) + minWriteSize
        randomBytes = randomBytes(size)

      if decision < 90:
        # Normal write
        result.add randomBytes
        stream.write randomBytes

      else:
        # Create cursor
        result.add randomBytes

        let isFixedSize = case writeTypes
          of FixedSize: true
          of VarSize: false
          of Mixed: rand(10) > 3

        if isFixedSize:
          let cursor = stream.delayFixedSizeWrite(randomBytes.len)

          delayedWrites.add DelayedWrite(
            fixedSizeCursor: cursor,
            content: randomBytes,
            written: 0,
            isFixedSize: true)
        else:
          let
            overestimatedBytes = rand(varSizeVariance)
            cursorSize = randomBytes.len + overestimatedBytes
            cursor = stream.delayVarSizeWrite(cursorSize)

          delayedWrites.add DelayedWrite(
            varSizeCursor: cursor,
            content: randomBytes,
            written: 0,
            isFixedSize: false)

    # Write all unwritten data to all outstanding cursors
    if stream != nil:
      for dw in mitems(delayedWrites):
        if dw.isFixedSize:
          let remaining = dw.content.len - dw.written
          dw.fixedSizeCursor.write dw.content[dw.written ..< dw.written + remaining]
          finalize dw.fixedSizeCursor
        else:
          dw.varSizeCursor.finalWrite dw.content

  template randomizedCursorsTest(streamExpr: OutputStreamHandle,
                                 writeTypesExpr: WriteTypes,
                                 varSizeVarianceExpr: int,
                                 customChecks: untyped = nil) =
    const testName = "randomized cursor test [" & astToStr(streamExpr) &
                     ";writes=" & $writeTypesExpr & ",variance=" & $varSizeVarianceExpr & "]"
    test testName:
      let s = streamExpr
      var referenceResult = randomizedCursorsTestImpl(stream = s,
                                                      writeTypes = writeTypesExpr,
                                                      varSizeVariance = varSizeVarianceExpr)

      when astToStr(customChecks) == "nil":
        let streamResult = s.getOutput()
        let resultsMatch = streamResult == referenceResult
        when false:
          if not resultsMatch:
            writeFile("reference-result.txt", referenceResult)
            writeFile("stream-result.txt", streamResult)
        check resultsMatch
      else:
        customChecks

      check referenceResult.len == s.pos

  randomizedCursorsTest(memoryOutput(), FixedSize, 0)
  randomizedCursorsTest(memoryOutput(), VarSize, 100)
  randomizedCursorsTest(memoryOutput(pageSize = 10), Mixed, 10)

  test "randomized file roundtrip":
    const randomBytesFileName = "random_bytes_file"

    var
      referenceBytes, restoredBytes: seq[byte]

    try:
      let output = fileOutput randomBytesFileName

      for i in 0 .. 3000:
        let bytes = randomBytes(rand(9999) + 1)
        referenceBytes.add bytes

        var openArraySize = rand(12000)
        if openArraySize >= bytes.len:
          # Make sure that sometimes `writeBlock` populates the entire span
          if i < 100: openArraySize = bytes.len
          output.advance writeBlock(bytes, output.getWritableBytes(openArraySize))
        else:
          output.write bytes

      close output

      let input = fileInput randomBytesFileName

      while input.readable(10000):
        let r = 1 + rand(9999)
        if r < 5000:
          restoredBytes.add input.read(8000)
          restoredBytes.add input.read(500)
        elif r < 7000:
          restoredBytes.add input.read(1)
          restoredBytes.add input.read(2)
          restoredBytes.add input.read(5)
          restoredBytes.add input.read(17)
          restoredBytes.add input.read(128)
        else:
          restoredBytes.add input.read(r)

      while input.readable:
        restoredBytes.add input.read

      close input

      doAssert referenceBytes == restoredBytes

    finally:
      if fileExists(randomBytesFileName):
        removeFile randomBytesFileName

