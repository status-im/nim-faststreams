{.used.}

import unittest2, ../faststreams, ../faststreams/textio

proc readAll(s: InputStream): string =
  while s.readable:
    result.add s.read.char

suite "TextIO":
  dualTest "writeText int":
    let s = memoryOutput()
    var n = 123
    s.writeText(n)
    check s.getOutput(string) == "123"

  dualTest "writeText uint":
    let s = memoryOutput()
    var n = 123'u64
    s.writeText(n)
    check s.getOutput(string) == "123"

  dualTest "writeText float64":
    let s = memoryOutput()
    var n = 1.23'f64
    s.writeText(n)
    check s.getOutput(string) == "1.23"

  dualTest "writeText float32":
    let s = memoryOutput()
    var n = 1.23'f32
    s.writeText(n)
    check s.getOutput(string) == "1.23"

  dualTest "writeText string":
    let s = memoryOutput()
    var n = "abc"
    s.writeText(n)
    check s.getOutput(string) == "abc"

  dualTest "writeHex":
    let s = memoryOutput()
    var n = "abc"
    s.writeHex(n)
    check s.getOutput(string) == "616263"

  dualTest "readLine":
    let s = memoryInput("abc\ndef")
    check s.readLine() == "abc"
    check s.readLine() == "def"

  dualTest "readUntil":
    let s = memoryInput("abc\ndef")
    check s.readUntil(@['d']).get == "abc\n"

  dualTest "nextLine":
    let s = memoryInput("abc\ndef")
    check s.nextLine().get == "abc"
    check s.nextLine().get == "def"
    check s.nextLine() == none(string)

  dualTest "lines":
    let s = memoryInput("abc\ndef")
    var found = newSeq[string]()
    for line in lines(s):
      found.add line
    check found == @["abc", "def"]
