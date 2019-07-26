import
  os, unittest, random,
  stew/ranges/ptr_arith,
  ../faststreams

proc bytes(s: string): seq[byte] =
  result = newSeqOfCap[byte](s.len)
  for c in s: result.add byte(c)

template bytes(c: char): byte = byte(c)
template bytes(b: seq[byte]): seq[byte] = b

proc repeat(b: byte, count: int): seq[byte] =
  result = newSeq[byte](count)
  for i in 0 ..< count: result[i] = b

proc randomBytes(n: int): seq[byte] =
  result.newSeq n
  for i in 0 ..< n:
    result[i] = byte(rand(255))

suite "output stream":
  setup:
    var memStream = OutputStream.init
    var altOutput: seq[byte] = @[]
    var tempFilePath = getTempDir() / "faststreams_testfile"
    # var fileStream = OutputStream.init tempFilePath

    const bufferSize = 1000000
    var buffer = alloc(bufferSize)
    var existingBufferStream = OutputStream.init(buffer, bufferSize)

  teardown:
    removeFile tempFilePath

  template output(val: auto) {.dirty.} =
    altOutput.add bytes(val)

    memStream.append val
    # fileStream.append val
    existingBufferStream.append val

  template checkOutputsMatch =
    # fileStream.flush

    let
      # fileContents = readFile(tempFilePath).string.bytes
      memStreamContents = memStream.getOutput

    check altOutput == memStreamContents
    # check altOutput == fileContents
    check altOutput == makeOpenArray(cast[ptr byte](buffer),
                                     existingBufferStream.pos)

  test "no appends produce an empty output":
    checkOutputsMatch()

  test "string output":
    for i in 0 .. 1:
      output $i
      output " bottles on the wall"
      output '\n'

    checkOutputsMatch()

  test "delayed write":
    output "initial output\n"
    const delayedWriteContent = bytes "delayed write\n"

    var cursor = memStream.delayFixedSizeWrite(delayedWriteContent.len)
    let cursorStart = memStream.pos
    altOutput.add delayedWriteContent

    # fileStream.append delayedWriteContent
    existingBufferStream.append delayedWriteContent

    var totalBytesWritten = 0
    for i, count in [12, 342, 2121, 23, 1, 34012, 932]:
      output repeat(byte(i), count)
      totalBytesWritten += count
      check memStream.pos - cursorStart == totalBytesWritten

    cursor.endWrite delayedWriteContent

    checkOutputsMatch()

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

        delayedWrites[i].cursor.append delayedWrites[i].content[written ..< written + toWrite]
        delayedWrites[i].written += toWrite

        if remaining - toWrite == 0:
          dispose delayedWrites[i].cursor
          if i != delayedWrites.len - 1:
            swap(delayedWrites[i], delayedWrites[^1])
          delayedWrites.setLen(delayedWrites.len - 1)

      elif decision < 90:
        # Normal write
        memStream.append randomBytes
        altOutput.add randomBytes

      else:
        # Create cursor
        altOutput.add randomBytes
        delayedWrites.add DelayedWrite(
          cursor: memStream.delayFixedSizeWrite(randomBytes.len),
          content: randomBytes,
          written: 0)

      # Check that the stream position is consistently tracked at every step
      check altOutput.len == memStream.pos

    # Write all unwritten data to all outstanding cursors
    for dw in mitems(delayedWrites):
      let remaining = dw.content.len - dw.written
      dw.cursor.append dw.content[dw.written ..< dw.written + remaining]
      dispose dw.cursor

    # The final outputs are the same
    check altOutput == memStream.getOutput

