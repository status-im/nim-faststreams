import
  macros,
  inputs, outputs

export
  inputs, outputs

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
      unsafeMemoryInput(getOutput(`outputVar`))

  if defined(debugMacros) or defined(debugPipelines):
    echo result.repr

