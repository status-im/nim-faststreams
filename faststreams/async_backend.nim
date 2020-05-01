const
  faststreams_async_backend {.strdefine.} = "chronos"

type
  CloseBehavior* = enum
    waitAsyncClose
    dontWaitAsyncClose

when faststreams_async_backend == "chronos":
  import
    chronos

  export
    chronos

  template fsAwait*(f: Future): untyped =
    await f

elif faststreams_async_backend in ["std", "asyncdispatch"]:
  import
    std/[asyncfutures, asyncmacro]
  
  export
    asyncfutures, asyncmacro

  template fsAwait*(awaited: Future[T]): untyped =
    # TODO revisit after https://github.com/nim-lang/Nim/pull/12085/ is merged
    let f = awaited
    yield f
    if not isNil(f.error):
      raise f.error
    f.read

else:
  {.fatal: "Unrecognized network backend: " & faststreams_async_backend.}

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

