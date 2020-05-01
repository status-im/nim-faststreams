{.used.}

import
  os, unittest, random,
  stew/ranges/ptr_arith,
  ../faststreams, ../faststreams/textio

proc bytes(s: string): seq[byte] =
  result = newSeqOfCap[byte](s.len)
  for c in s: result.add byte(c)

template bytes(c: char): byte = byte(c)
template bytes(b: seq[byte]): seq[byte] = b
template bytes[N, T](b: array[N, T]): seq[byte] = @b

proc repeat(b: byte, count: int): seq[byte] =
  result = newSeq[byte](count)
  for i in 0 ..< count: result[i] = b

proc randomBytes(n: int): seq[byte] =
  result.newSeq n
  for i in 0 ..< n:
    result[i] = byte(rand(255))

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
    flush fileStream
    close fileStream

    flush unbufferedFileStream
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
                              streamWritingToExistingBuffer.pos) and
      nimSeq == memStreamRes and
      nimSeq == readFileRes and
      nimSeq == fileInputRes and
      nimSeq == memFileInputRes and
      nimSeq == fileInputWithSmallPagesRes

    check outputsMatch

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

  test "delayed write":
    output "initial output\n"
    const delayedWriteContent = bytes "delayed write\n"

    var cursor = memStream.delayFixedSizeWrite(delayedWriteContent.len)
    let cursorStart = memStream.pos

    nimSeq.add delayedWriteContent
    fileStream.write delayedWriteContent
    streamWritingToExistingBuffer.write delayedWriteContent

    var bytesWritten = 0
    for i, count in [2]: # 12, 342, 2121, 23, 1, 34012, 932]:
      output repeat(byte(i), count)
      bytesWritten += count
      check memStream.pos - cursorStart == bytesWritten

    cursor.finalWrite delayedWriteContent

    checkOutputsMatch(skipUnbufferedFile = true)

  test "multi-page delayed writes":
    randomize(1000)

    type
      DelayedWrite = object
        cursor: WriteCursor
        content: seq[byte]
        written: int

    var delayedWrites = newSeq[DelayedWrite]()

    for i in 0..50:
      let
        size = rand(8000) + 2000
        randomBytes = randomBytes(size)
        decision = rand(100)

      if decision < 70:
        # Write at some random cursor
        if delayedWrites.len == 0:
          continue

        let
          i = rand(delayedWrites.len - 1)
          written = delayedWrites[i].written
          remaining = delayedWrites[i].content.len - written
          toWrite = min(rand(remaining) + 10, remaining)

        delayedWrites[i].cursor.write delayedWrites[i].content[written ..< written + toWrite]
        delayedWrites[i].written += toWrite

        if remaining - toWrite == 0:
          finalize delayedWrites[i].cursor
          if i != delayedWrites.len - 1:
            swap(delayedWrites[i], delayedWrites[^1])
          delayedWrites.setLen(delayedWrites.len - 1)

      elif decision < 90:
        # Normal write
        memStream.write randomBytes
        nimSeq.add randomBytes

      else:
        # Create cursor
        nimSeq.add randomBytes
        delayedWrites.add DelayedWrite(
          cursor: memStream.delayFixedSizeWrite(randomBytes.len),
          content: randomBytes,
          written: 0)

      # Check that the stream position is consistently tracked at every step
      check nimSeq.len == memStream.pos

    # Write all unwritten data to all outstanding cursors
    for dw in mitems(delayedWrites):
      let remaining = dw.content.len - dw.written
      dw.cursor.write dw.content[dw.written ..< dw.written + remaining]
      finalize dw.cursor

    # The final outputs are the same
    check nimSeq == memStream.getOutput

