import
  unittest,
  ../faststreams

proc bytes(s: string): seq[byte] =
  result = newSeqOfCap[byte](s.len)
  for c in s: result.add byte(c)

template bytes(c: char): byte = byte(c)
template bytes(b: seq[byte]): seq[byte] = b

proc repeat(b: byte, count: int): seq[byte] =
  result = newSeq[byte](count)
  for i in 0 ..< count: result[i] = b

suite "output stream":
  setup:
    var stream = init OutputStream
    var altOutput: seq[byte] = @[]

  template output(val: auto) {.dirty.} =
    stream.append val
    altOutput.add bytes(val)

  test "string output":
    for i in 0 .. 1000:
      stream.appendNumber i
      altOutput.add bytes($i)

      output " bottles on the wall"
      output '\n'

    check stream.getOutput == altOutput

  test "delayed write":
    output "initial output\n"
    const delayedWriteContent = bytes "delayed write\n"

    var cursor = stream.delayFixedSizeWrite(delayedWriteContent.len)
    altOutput.add delayedWriteContent

    var totalBytesWritten = 0
    for i, count in [12, 342, 2121, 23, 1, 34012, 932]:
      output repeat(byte(i), count)
      totalBytesWritten += count
      check cursor.totalBytesWrittenAfterCursor == totalBytesWritten

    cursor.endWrite delayedWriteContent

    check stream.getOutput == altOutput
