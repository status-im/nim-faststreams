import
  outputs

var fsStdOut* {.threadvar.}: OutputStream

proc initFsStdOut* =
  ## This proc must be called in each thread where
  ## the `fsStdOut` variable will be used.
  if fsStdOut == nil:
    fsStdOut = fileOutput(system.stdout)

initFsStdOut()

