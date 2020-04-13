import
  macros,
  input_stream, output_stream

export
  input_stream, output_stream

macro executePipeline*(start: InputStream, steps: varargs[untyped]) =
  var input = start
  result = newStmtList()

  for i in 0 .. steps.len - 2:
    var
      step = steps[i]
      outputVar = genSym(nskVar, "out")
      output = if i == steps.len - 2: steps[^1]
               else: newCall(bindSym"memoryOutput")

    result.add quote do:
      var `outputVar` = `output`
      `step`(`input`, `outputVar`)

    input = quote do:
      memoryInput(getOutput(`outputVar`))

  if defined(debugMacros) or defined(debugPipelines):
    echo result.repr

