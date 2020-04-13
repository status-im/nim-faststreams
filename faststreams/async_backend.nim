const
  faststreams_async_backend {.strdefine.} = "chronos"

when faststreams_async_backend == "chronos":
  import chronos # import chronos/[asyncfutures2, asyncmacro2]
  export chronos # export asyncfutures2, asyncmacro2

  template faststreamsAwait*(f: Future): untyped =
    await f

elif faststreams_async_backend in ["std", "asyncdispatch"]:
  import std/[asyncfutures, asyncmacro]
  export asyncfutures, asyncmacro

  template faststreamsAwait*(awaited: Future[T]): untyped =
    # TODO revisit after https://github.com/nim-lang/Nim/pull/12085/ is merged
    let f = awaited
    yield f
    if not isNil(f.error):
      raise f.error
    f.read

else:
  {.fatal: "Unrecognized network backend: " & faststreams_async_backend.}

template raiseFaststreamsError*(errMsg: string, body: untyped) =
  try:
    body
  except CatchableError as err:
    raise newException(IOError, errMsg, err)

