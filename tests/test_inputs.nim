{.used.}

import
  os, unittest, strutils, stew/ranges/ptr_arith,
  ../faststreams

suite "input stream":
  test "empty input":
    var str = ""
    var i = unsafeMemoryInput(str)

    check:
      i.readable == false
      i.next.isNone

    expect Defect:
      echo i.read

  test "missing file input":
    const fileName = "there-is-no-such-faststreams-file-1"

    check not fileExists(fileName)
    expect CatchableError: discard fileInput(fileName)

    check not fileExists(fileName)
    expect CatchableError: discard memFileInput(fileName)

    check not fileExists(fileName)

  test "simple":
    var input = repeat("1234 5678 90AB CDEF\n", 1000)
    var stream = unsafeMemoryInput(input)

    check:
      (stream.read(4) == "1234".toOpenArrayByte(0, 3))

