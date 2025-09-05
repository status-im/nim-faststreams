import unittest2, ../faststreams, ../faststreams/textio

template test2(nameParam: string, body: untyped) =
  when nimvm:
    staticTest nameParam:
      body
  runtimeTest nameParam:
    body

proc readAll(s: InputStream): string =
  while s.readable:
    result.add s.read.char

suite "TextIO":
  test2 "writeText int":
    let s = memoryOutput()
    var n = 123
    s.writeText(n)
    check s.getOutput(string) == "123"

  test2 "writeText uint":
    let s = memoryOutput()
    var n = 123'u64
    s.writeText(n)
    check s.getOutput(string) == "123"

  test2 "writeText float64":
    let s = memoryOutput()
    var n = 1.23'f64
    s.writeText(n)
    check s.getOutput(string) == "1.23"

  test2 "writeText float32":
    let s = memoryOutput()
    var n = 1.23'f32
    s.writeText(n)
    check s.getOutput(string) == "1.23"

  test2 "writeText string":
    let s = memoryOutput()
    var n = "abc"
    s.writeText(n)
    check s.getOutput(string) == "abc"

  test2 "writeHex":
    let s = memoryOutput()
    var n = "abc"
    s.writeHex(n)
    check s.getOutput(string) == "616263"

  test2 "readLine":
    let s = memoryInput("abc\ndef")
    check s.readLine() == "abc"
    check s.readLine() == "def"

  test2 "readUntil":
    let s = memoryInput("abc\ndef")
    check s.readUntil(@['d']).get == "abc\n"

  test2 "nextLine":
    let s = memoryInput("abc\ndef")
    check s.nextLine().get == "abc"
    check s.nextLine().get == "def"
    check s.nextLine() == none(string)

  test2 "lines":
    let s = memoryInput("abc\ndef")
    var found = newSeq[string]()
    for line in lines(s):
      found.add line
    check found == @["abc", "def"]
