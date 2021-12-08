const
  # To compile with async support, use `-d:asyncBackend=chronos|asyncdispatch`
  asyncBackend {.strdefine.} = "none"

const
  faststreams_async_backend {.strdefine.} = ""

when faststreams_async_backend != "":
  {.fatal: "use `-d:asyncBackend` instead".}

type
  CloseBehavior* = enum
    waitAsyncClose
    dontWaitAsyncClose

const
  debugHelpers* = defined(debugHelpers)
  fsAsyncSupport* = asyncBackend != "none"

when asyncBackend == "none":
  discard
elif asyncBackend == "chronos":
  import
    chronos

  export
    chronos

  template fsAwait*(f: Future): untyped =
    await f

elif asyncBackend == "asyncdispatch":
  import
    std/asyncdispatch

  export
    asyncdispatch

  template fsAwait*(awaited: Future): untyped =
    # TODO revisit after https://github.com/nim-lang/Nim/pull/12085/ is merged
    let f = awaited
    yield f
    if not isNil(f.error):
      raise f.error
    f.read

  type Duration* = int

else:
  {.fatal: "Unrecognized network backend: " & asyncBackend .}

when defined(danger):
  template fsAssert*(x) = discard
  template fsAssert*(x, msg) = discard
else:
  template fsAssert*(x) = doAssert(x)
  template fsAssert*(x, msg) = doAssert(x, msg)

template fsTranslateErrors*(errMsg: string, body: untyped) =
  try:
    body
  except IOError as err:
    raise err
  except Exception as err:
    if err[] of Defect:
      raise (ref Defect)(err)
    else:
      raise newException(IOError, errMsg, err)

template noAwait*(expr: untyped): untyped =
  expr

