{.used.}

import
  typetraits, ../faststreams

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

block:
  var stream = memoryOutput()
  stream.writeNimRepr(ABC(a: 1, b: 'b', c: "str"))
  var repr = stream.getOutput(string)

  doAssert repr == "ABC(a: 1, b: 'b', c: \"str\")"

