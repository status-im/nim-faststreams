{.used.}

import os, unittest2, ../faststreams

const
  asciiTableFile = "files" / "ascii_table.txt"
  asciiTableFileAbsolute = currentSourcePath().parentDir / asciiTableFile

proc readAll(s: InputStream): string =
  while s.readable:
    result.add s.read.char

suite "Inputs":
  dualTest "readableNow":
    let s = memoryInput("abc")
    check readableNow(s)
    discard readAll(s)
    check not readableNow(s)

  dualTest "totalUnconsumedBytes":
    let s = memoryInput("abc")
    check totalUnconsumedBytes(s) == 3
    discard s.read()
    check totalUnconsumedBytes(s) == 2
    discard s.read()
    check totalUnconsumedBytes(s) == 1
    discard s.read()
    check totalUnconsumedBytes(s) == 0

  dualTest "len":
    let s = memoryInput("abc")
    check s.len == some 3.Natural
    discard s.read()
    check s.len == some 2.Natural
    discard s.read()
    check s.len == some 1.Natural
    discard s.read()
    check s.len == some 0.Natural

  dualTest "readable":
    let text = "abc"
    let s = memoryInput(text)
    for _ in 0 ..< text.len:
      check readable(s)
      discard s.read()
    check(not readable(s))

  dualTest "readable N":
    let s = memoryInput("abc")
    check readable(s, 0)
    check readable(s, 1)
    check readable(s, 2)
    check readable(s, 3)
    check(not readable(s, 4))

  dualTest "peek":
    let text = "abc"
    let s = memoryInput(text)
    for i in 0 ..< text.len:
      check s.peek() == text[i].byte
      discard s.read()

  dualTest "read":
    let text = "abc"
    let s = memoryInput(text)
    for i in 0 ..< text.len:
      check s.read() == text[i].byte

  dualTest "peekAt":
    let text = "abc"
    let s = memoryInput(text)
    check s.peekAt(0) == 'a'.byte
    check s.peekAt(1) == 'b'.byte
    check s.peekAt(2) == 'c'.byte

  dualTest "advance":
    let text = "abc"
    let s = memoryInput(text)
    for i in 0 ..< text.len:
      check s.peek() == text[i].byte
      advance(s)

  dualTest "readIntoEx":
    let text = "abc"
    let s = memoryInput(text)
    var ss = newSeq[byte](2)
    check readIntoEx(s, ss) == 2
    check ss == toOpenArrayByte("ab", 0, 1)
    check readIntoEx(s, ss) == 1
    check ss[0] == 'c'.byte

  dualTest "readInto":
    let text = "abc"
    let s = memoryInput(text)
    var ss = newSeq[byte](2)
    check readInto(s, ss) 
    check ss == toOpenArrayByte("ab", 0, 1)
    check(not readInto(s, ss))
    check ss[0] == 'c'.byte

  dualTest "pos":
    let text = "abc"
    let s = memoryInput(text)
    for i in 0 ..< text.len:
      check s.pos() == i
      discard s.read()

  dualTest "close":
    let s = memoryInput("abc")
    check readable(s)
    s.close()
    check(not readable(s))

  dualTest "unsafe read":
    let text = "abc"
    let s = unsafeMemoryInput(text)
    for i in 0 ..< text.len:
      check s.read() == text[i].byte

  dualTest "memFileInput relative path":
    let s = memFileInput(asciiTableFile)
    discard s.read()
    s.close()

  dualTest "memFileInput absolute path":
    let s = memFileInput(asciiTableFileAbsolute)
    discard s.read()
    s.close()

  dualTest "memFileInput content":
    const input = staticRead(asciiTableFileAbsolute)
    let ss = memoryInput(input)
    let s = memFileInput(asciiTableFileAbsolute)
    while readable(s):
      check s.read == ss.read
    check(not readable(ss))
    s.close()

  staticTest "memFileInput offset":
    const input = staticRead(asciiTableFileAbsolute)
    let ss = memoryInput(input)
    let offset = 10
    let s = memFileInput(asciiTableFileAbsolute, offset = offset)
    ss.advance offset
    while readable(s):
      check s.read == ss.read
    check(not readable(ss))
    s.close()

  staticTest "memFileInput mappedSize":
    const input = staticRead(asciiTableFileAbsolute)
    let ss = memoryInput(input)
    let mappedSize = 10
    let s = memFileInput(asciiTableFileAbsolute, mappedSize = mappedSize)
    while readable(s):
      check s.read == ss.read
    check s.pos == mappedSize
    s.close()

  staticTest "memFileInput offset & mappedSize":
    const input = staticRead(asciiTableFileAbsolute)
    let ss = memoryInput(input)
    let offset = 10
    let mappedSize = 10
    let s = memFileInput(asciiTableFileAbsolute, mappedSize, offset)
    ss.advance offset
    while readable(s):
      check s.read == ss.read
    check ss.pos == offset + mappedSize
    s.close()

suite "Outputs":
  dualTest "write byte":
    let s = memoryOutput()
    s.write('a'.byte)
    check s.getOutput() == @['a'.byte]

  dualTest "write char":
    let s = memoryOutput()
    s.write('a')
    check s.getOutput(string) == "a"

  dualTest "write byte seq":
    let s = memoryOutput()
    s.write(@['a'.byte, 'b'.byte])
    check s.getOutput() == @['a'.byte, 'b'.byte]

  dualTest "write string":
    let s = memoryOutput()
    s.write("ab")
    check s.getOutput(string) == "ab"
