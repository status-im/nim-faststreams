import unittest2, typetraits, ../faststreams

template test2(nameParam: string, body: untyped) =
  when nimvm:
    staticTest nameParam:
      body
  runtimeTest nameParam:
    body

proc readAll(s: InputStream): string =
  while s.readable:
    result.add s.read.char

suite "Inputs":
  test2 "readableNow":
    let s = memoryInput("abc")
    check readableNow(s)
    discard readAll(s)
    check not readableNow(s)

  test2 "totalUnconsumedBytes":
    let s = memoryInput("abc")
    check totalUnconsumedBytes(s) == 3
    discard s.read()
    check totalUnconsumedBytes(s) == 2
    discard s.read()
    check totalUnconsumedBytes(s) == 1
    discard s.read()
    check totalUnconsumedBytes(s) == 0

  test2 "len":
    let s = memoryInput("abc")
    check s.len == some 3.Natural
    discard s.read()
    check s.len == some 2.Natural
    discard s.read()
    check s.len == some 1.Natural
    discard s.read()
    check s.len == some 0.Natural

  test2 "readable":
    let text = "abc"
    let s = memoryInput(text)
    for _ in 0 ..< text.len:
      check readable(s)
      discard s.read()
    check(not readable(s))

  test2 "readable N":
    let s = memoryInput("abc")
    check readable(s, 0)
    check readable(s, 1)
    check readable(s, 2)
    check readable(s, 3)
    check(not readable(s, 4))

  test2 "peek":
    let text = "abc"
    let s = memoryInput(text)
    for i in 0 ..< text.len:
      check s.peek() == text[i].byte
      discard s.read()

  test2 "read":
    let text = "abc"
    let s = memoryInput(text)
    for i in 0 ..< text.len:
      check s.read() == text[i].byte

  test2 "peekAt":
    let text = "abc"
    let s = memoryInput(text)
    check s.peekAt(0) == 'a'.byte
    check s.peekAt(1) == 'b'.byte
    check s.peekAt(2) == 'c'.byte

  test2 "advance":
    let text = "abc"
    let s = memoryInput(text)
    for i in 0 ..< text.len:
      check s.peek() == text[i].byte
      advance(s)

  test2 "readIntoEx":
    let text = "abc"
    let s = memoryInput(text)
    var ss = newSeq[byte](2)
    check readIntoEx(s, ss) == 2
    check ss == toOpenArrayByte("ab", 0, 1)
    check readIntoEx(s, ss) == 1
    check ss[0] == 'c'.byte

  test2 "readInto":
    let text = "abc"
    let s = memoryInput(text)
    var ss = newSeq[byte](2)
    check readInto(s, ss) 
    check ss == toOpenArrayByte("ab", 0, 1)
    check(not readInto(s, ss))
    check ss[0] == 'c'.byte

  test2 "pos":
    let text = "abc"
    let s = memoryInput(text)
    for i in 0 ..< text.len:
      check s.pos() == i
      discard s.read()

  test2 "unsafe read":
    let text = "abc"
    let s = unsafeMemoryInput(text)
    for i in 0 ..< text.len:
      check s.read() == text[i].byte

suite "Outputs":
  test2 "write byte":
    let s = memoryOutput()
    s.write('a'.byte)
    check s.getOutput() == @['a'.byte]

  test2 "write char":
    let s = memoryOutput()
    s.write('a')
    check s.getOutput(string) == "a"

  test2 "write byte seq":
    let s = memoryOutput()
    s.write(@['a'.byte, 'b'.byte])
    check s.getOutput() == @['a'.byte, 'b'.byte]

  test2 "write string":
    let s = memoryOutput()
    s.write("ab")
    check s.getOutput(string) == "ab"

proc writeNimRepr*(stream: OutputStream, str: string) =
  stream.write '"'

  for c in str:
    if c == '"':
      stream.write ['\'', '"']
    else:
      stream.write c

  stream.write '"'

proc writeNimRepr*(stream: OutputStream, x: char) =
  stream.write ['\'', x, '\'']

proc writeNimRepr*(stream: OutputStream, x: int) =
  stream.write $x # Making this more optimal has been left
                   # as an exercise for the reader

proc writeNimRepr*[T](stream: OutputStream, obj: T) =
  stream.write typetraits.name(T)
  stream.write '('

  var firstField = true
  for name, val in fieldPairs(obj):
    if not firstField:
      stream.write ", "

    stream.write name
    stream.write ": "
    stream.writeNimRepr val

    firstField = false

  stream.write ')'

type
  ABC = object
    a: int
    b: char
    c: string

suite "Examples":
  test2 "readme example":
    var stream = memoryOutput()
    stream.writeNimRepr(ABC(a: 1, b: 'b', c: "str"))
    var repr = stream.getOutput(string)
    check repr == "ABC(a: 1, b: 'b', c: \"str\")"
