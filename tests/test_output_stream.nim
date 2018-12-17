import
  unittest,
  ../faststreams

suite "output stream":
  test "string output":
    var s = init StringOutputStream
    var altOutput = ""

    for i in 0 .. 1000:
      s.appendNumber i
      s.append " bottles on the wall"
      s.append byte('\n')

      altOutput.add $i
      altOutput.add " bottles on the wall"
      altOutput.add '\n'

    let streamOutput = s.getOutput
    check streamOutput == altOutput

