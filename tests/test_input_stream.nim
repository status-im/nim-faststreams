import
  unittest, strutils, stew/ranges/ptr_arith,
  ../faststreams

suite "input stream":
  test "string input":
    var input = repeat("1234 5678 90AB CDEF\n", 1000)
    var stream = memoryInput(input)

    check:
      (stream.readBytes(4) == "1234".toOpenArrayByte(0, 3))

