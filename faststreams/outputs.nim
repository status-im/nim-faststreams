## Please note that the use of unbuffered streams comes with a number
## of restrictions:
##
## * Delayed writes are not supported.
## * Output consuming operations such as `getOutput`, `consumeOutputs` and
##   `consumeContiguousOutput` should not be used with them.
## * They cannot participate as intermediate steps in pipelines.

import
  deques, typetraits,
  stew/[ptrops, strings],
  buffers, async_backend

export
  initPageBuffers, CloseBehavior

when fsAsyncSupport:
  # Circular type refs prevent more targeted `when`
  type
    OutputStream* = ref object of RootObj
      vtable*: ptr OutputStreamVTable # This is nil for any memory output
      buffers*: PageBuffers           # This is nil for unsafe memory outputs
      span*: PageSpan
      spanEndPos*: Natural
      extCursorsCount: int
      closeFut: Future[void]          # This is nil before `close` is called
      when debugHelpers:
        name*: string

    AsyncOutputStream* {.borrow: `.`.} = distinct OutputStream

    WriteSyncProc* = proc (s: OutputStream, src: pointer, srcLen: Natural)
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    WriteAsyncProc* = proc (s: OutputStream, src: pointer, srcLen: Natural): Future[void]
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    FlushSyncProc* = proc (s: OutputStream)
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    FlushAsyncProc* = proc (s: OutputStream): Future[void]
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    CloseSyncProc* = proc (s: OutputStream)
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    CloseAsyncProc* = proc (s: OutputStream): Future[void]
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    OutputStreamVTable* = object
      writeSync*: WriteSyncProc
      writeAsync*: WriteAsyncProc
      flushSync*: FlushSyncProc
      flushAsync*: FlushAsyncProc
      closeSync*: CloseSyncProc
      closeAsync*: CloseAsyncProc

    MaybeAsyncOutputStream* = OutputStream | AsyncOutputStream

else:
  type
    OutputStream* = ref object of RootObj
      vtable*: ptr OutputStreamVTable # This is nil for any memory output
      buffers*: PageBuffers           # This is nil for unsafe memory outputs
      span*: PageSpan
      spanEndPos*: Natural
      extCursorsCount: int
      when debugHelpers:
        name*: string


    WriteSyncProc* = proc (s: OutputStream, src: pointer, srcLen: Natural)
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    FlushSyncProc* = proc (s: OutputStream)
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    CloseSyncProc* = proc (s: OutputStream)
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    OutputStreamVTable* = object
      writeSync*: WriteSyncProc
      flushSync*: FlushSyncProc
      closeSync*: CloseSyncProc

    MaybeAsyncOutputStream* = OutputStream

type
  WriteCursor* = object
    span: PageSpan
    stream: OutputStream

  LayeredOutputStream* = ref object of OutputStream
    destination*: OutputStream
    allowWaitFor*: bool

  OutputStreamHandle* = object
    s*: OutputStream

  VarSizeWriteCursor* = distinct WriteCursor

  FileOutputStream = ref object of OutputStream
    file: File
    allowAsyncOps: bool

template Sync*(s: OutputStream): OutputStream = s

when fsAsyncSupport:
  template Async*(s: OutputStream): AsyncOutputStream = AsyncOutputStream(s)

  template Sync*(s: AsyncOutputStream): OutputStream = OutputStream(s)
  template Async*(s: AsyncOutputStream): AsyncOutputStream = s

proc disconnectOutputDevice(s: OutputStream) =
  if s.vtable != nil:
    when fsAsyncSupport:
      if s.vtable.closeAsync != nil:
        s.closeFut = s.vtable.closeAsync(s)
      elif s.vtable.closeSync != nil:
        s.vtable.closeSync(s)
    else:
      if s.vtable.closeSync != nil:
        s.vtable.closeSync(s)

    s.vtable = nil

when fsAsyncSupport:
  template disconnectOutputDevice(s: AsyncOutputStream) =
    disconnectOutputDevice OutputStream(s)

template flushImpl(s: OutputStream, awaiter, writeOp, flushOp: untyped) =
  fsAssert s.extCursorsCount == 0
  if s.vtable != nil:
    if s.buffers != nil:
      let runway = s.span.len
      trackWrittenTo(s.buffers, s.span.startAddr)
      awaiter s.vtable.writeOp(s, nil, 0)
      s.span = getWritableSpan(s.buffers)
      s.spanEndPos = s.spanEndPos - runway + s.span.len

    if s.vtable.flushOp != nil:
      awaiter s.vtable.flushOp(s)

proc flush*(s: OutputStream) =
  flushImpl(s, noAwait, writeSync, flushSync)

proc close*(s: OutputStream,
            behavior = dontWaitAsyncClose)
           {.raises: [IOError, Defect].} =
  flush s
  disconnectOutputDevice(s)
  when fsAsyncSupport:
    if s.closeFut != nil:
      fsTranslateErrors "Stream closing failed":
        if behavior == waitAsyncClose:
          waitFor s.closeFut
        else:
          asyncCheck s.closeFut

template closeNoWait*(sp: MaybeAsyncOutputStream) =
  ## Close the stream without waiting even if's async.
  ## This operation will use `asyncCheck` internally to detect unhandled
  ## errors from the closing operation.
  close(InputStream(s), dontWaitAsyncClose)

# TODO
# The destructors are currently disabled because they seem to cause
# mysterious segmentation faults related to corrupted GC internal
# data structures.
#[
proc `=destroy`*(h: var OutputStreamHandle) {.raises: [Defect].} =
  if h.s != nil:
    if h.s.vtable != nil and h.s.vtable.closeSync != nil:
      try:
        h.s.vtable.closeSync(h.s)
      except IOError:
        # Since this is a destructor, there is not much we can do here.
        # If the user wanted to handle the error, they would have called
        # `close` manually.
        discard # TODO
    # h.s = nil
]#

converter implicitDeref*(h: OutputStreamHandle): OutputStream =
  h.s

template canExtendOutput(s: OutputStream): bool =
  # Streams writing to pre-allocated existing buffers cannot be grown
  s != nil and s.buffers != nil

proc addPage(s: OutputStream) =
  let
    nextPageSize = s.buffers.pageSize
    nextPage = s.buffers.addWritablePage(nextPageSize)
  s.span = nextPage.fullSpan
  s.spanEndPos += nextPageSize

template makeHandle*(sp: OutputStream): OutputStreamHandle =
  let s = sp
  OutputStreamHandle(s: s)

proc memoryOutput*(pageSize = defaultPageSize): OutputStreamHandle =
  fsAssert pageSize > 0
  # We are not creating an initial output page, because `ensureRunway`
  # can determine the most appropriate size.
  makeHandle OutputStream(buffers: initPageBuffers(pageSize))

proc unsafeMemoryOutput*(buffer: pointer, len: Natural): OutputStreamHandle =
  let buffer = cast[ptr byte](buffer)

  makeHandle OutputStream(
    span: PageSpan(startAddr: buffer, endAddr: offset(buffer, len)),
    spanEndPos: len)

proc ensureRunway*(s: OutputStream, neededRunway: Natural) =
  ## The hint provided in `ensureRunway` overrides any previous
  ## hint specified at stream creation with `pageSize`.
  let runway = s.span.len

  if neededRunway > runway:
    # If you use an unsafe memory output, you must ensure that
    # it will have a large enough size to hold the data you are
    # feeding to it.
    fsAssert s.buffers != nil, "Unsafe memory output of insufficient size"
    s.buffers.ensureRunway(s.span, neededRunway)
    s.spanEndPos = s.spanEndPos - runway + s.span.len

when fsAsyncSupport:
  template ensureRunway*(s: AsyncOutputStream, neededRunway: Natural) =
    ensureRunway OutputStream(s), neededRunway

template implementWrites*(buffersParam: PageBuffers,
                          srcParam: pointer,
                          srcLenParam: Natural,
                          dstDesc: static string,
                          writeStartVar, writeLenVar,
                          writeBlock: untyped) =
  let
    buffers = buffersParam
    writeStartVar = srcParam
    writeLenVar = srcLenParam

  template raiseError =
    raise newException(IOError, "Failed to write all bytes to " & dstDesc)

  if buffers != nil:
    for writeStartVar, writeLenVar in consumePageBuffers(s.buffers):
      let bytesWritten = writeBlock
      # TODO: Can we repair the buffers here?
      if bytesWritten != writeLenVar: raiseError()

  if writeLenVar > 0:
    fsAssert writeStartVar != nil
    let bytesWritten = writeBlock
    if bytesWritten != writeLenVar: raiseError()

proc writeFileSync(s: OutputStream, src: pointer, srcLen: Natural)
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
  var file = FileOutputStream(s).file

  implementWrites(s.buffers, src, srcLen, "FILE",
                  writeStartAddr, writeLen):
    file.writeBuffer(writeStartAddr, writeLen)

proc flushFileSync(s: OutputStream)
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
  flushFile FileOutputStream(s).file

proc closeFileSync(s: OutputStream)
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
  close FileOutputStream(s).file

when fsAsyncSupport:
  proc writeFileAsync(s: OutputStream, src: pointer, srcLen: Natural): Future[void]
                     {.nimcall, gcsafe, raises: [IOError, Defect].} =
    fsAssert FileOutputStream(s).allowAsyncOps
    writeFileSync(s, src, srcLen)
    result = newFuture[void]()
    fsTranslateErrors "Unexpected exception from merely completing a future":
      result.complete()

  proc flushFileAsync(s: OutputStream): Future[void]
                     {.nimcall, gcsafe, raises: [IOError, Defect].} =
    fsAssert FileOutputStream(s).allowAsyncOps
    flushFile FileOutputStream(s).file
    result = newFuture[void]()
    fsTranslateErrors "Unexpected exception from merely completing a future":
      result.complete()

let fileOutputVTable = when fsAsyncSupport:
  OutputStreamVTable(
    writeSync: writeFileSync,
    writeAsync: writeFileAsync,
    flushSync: flushFileSync,
    flushAsync: flushFileAsync,
    closeSync: closeFileSync)
else:
  OutputStreamVTable(
    writeSync: writeFileSync,
    flushSync: flushFileSync,
    closeSync: closeFileSync)

template vtableAddr*(vtable: OutputStreamVTable): ptr OutputStreamVTable =
  ## This is a simple work-around for the somewhat broken side
  ## effects analysis of Nim - reading from global let variables
  ## is considered a side-effect.
  {.noSideEffect.}:
    unsafeAddr vtable

proc fileOutput*(f: File,
                 pageSize = defaultPageSize,
                 allowAsyncOps = false): OutputStreamHandle
                {.raises: [IOError, Defect].} =
  makeHandle FileOutputStream(
    vtable: vtableAddr fileOutputVTable,
    buffers: initPageBuffers(pageSize),
    file: f,
    allowAsyncOps: allowAsyncOps)

proc fileOutput*(filename: string,
                 fileMode: FileMode = fmWrite,
                 pageSize = defaultPageSize,
                 allowAsyncOps = false): OutputStreamHandle
                {.raises: [IOError, Defect].} =
  fileOutput(open(filename, fileMode), pageSize, allowAsyncOps)

proc pos*(s: OutputStream): int =
  s.spanEndPos - s.span.len

when fsAsyncSupport:
  template pos*(s: AsyncOutputStream): int =
    pos OutputStream(s)

proc getBuffers*(s: OutputStream): PageBuffers =
  fsAssert s.buffers != nil
  s.buffers.trackWrittenTo s.span.startAddr
  return s.buffers

proc recycleBuffers*(s: OutputStream, buffers: PageBuffers) =
  if buffers != nil:
    s.buffers = buffers

    let len = buffers.queue.len
    if len > 0:
      if len > 1:
        buffers.queue.shrink(fromLast = len - 1)

      let bufferPage = buffers.queue[0]
      bufferPage.writtenTo = 0
      bufferPage.consumedTo = 0

      s.span = bufferPage.fullSpan
      s.spanEndPos = s.span.len
      return
  else:
    s.buffers = initPageBuffers(defaultPageSize)

  s.span = default(PageSpan)
  s.spanEndPos = 0

#
# Pre-conditions for `drainAllBuffers(Sync/Async)`
#  * The cursor has reached the current span end
#  * We are working with a vtable-enabled stream
#
# Post-conditions:
#  * All completed pages are written
#  * There is a fresh page ready for writing at the top
#    (we can reuse a previously existing page for this)
#  * The stream cursor is re-initialized at the start of the top page
#
proc drainAllBuffersSync(s: OutputStream, buf: pointer, bufSize: Natural) =
  s.vtable.writeSync(s, buf, bufSize)
  if s.buffers != nil:
    s.span = s.buffers.getWritableSpan()
    s.spanEndPos += s.span.len

when fsAsyncSupport:
  proc drainAllBuffersAsync(s: OutputStream, buf: pointer, bufSize: Natural) {.async.} =
    fsAwait s.vtable.writeAsync(s, buf, bufSize)
    s.span = s.buffers.getWritableSpan()
    s.spanEndPos += s.span.len

proc createCursor(s: OutputStream, size: int): WriteCursor =
  inc s.extCursorsCount

  let
    # The start address matches the current stream main cursor location
    startAddr = s.span.startAddr
    endAddr = offset(startAddr, size)

  result = WriteCursor(
    stream: s,
    span: PageSpan(startAddr: startAddr, endAddr: endAddr))

  # Adjust the stream main cursor to point past the end
  # of the newly created cursor:
  s.span.startAddr = endAddr

proc delayFixedSizeWrite*(s: OutputStream, cursorSize: Natural): WriteCursor =
  let runway = s.span.len
  if cursorSize <= runway:
    result = createCursor(s, cursorSize)
  elif runway == 0:
    # This is a special case of requesting a cursor right at the page boundary.
    # We can safely create a non-split cursor on a new page:
    let
      nextPageSize = nextAlignedSize(cursorSize, s.buffers.pageSize)
      nextPage = s.buffers.addWritablePage(nextPageSize)
      nextPageSpan = nextPage.fullSpan
      cursorEndAddr = offset(nextPageSpan.startAddr, cursorSize)

    inc s.extCursorsCount
    result = WriteCursor(stream: s,
                         span: PageSpan(startAddr: nextPageSpan.startAddr,
                                        endAddr: cursorEndAddr))

    s.span = PageSpan(startAddr: cursorEndAddr, endAddr: nextPageSpan.endAddr)
    s.spanEndPos += nextPageSize
  else:
    result = createCursor(s, runway)

    let
      runwayDeficit = cursorSize - runway
      nextPageSize = nextAlignedSize(runwayDeficit, s.buffers.pageSize)
      nextPage = s.buffers.addWritablePage(nextPageSize)
      nextPageSpan = nextPage.fullSpan

    s.span = PageSpan(startAddr: offset(nextPageSpan.startAddr, runwayDeficit),
                      endAddr: nextPageSpan.endAddr)

    # See the explanation about split cursors above
    nextPage.consumedTo = -runwayDeficit

    s.spanEndPos += nextPageSize

proc delayVarSizeWrite*(s: OutputStream, maxSize: Natural): VarSizeWriteCursor =
  ## Please note that using variable sized writes are not supported
  ## for unbuffered streams and unsafe memory inputs.
  fsAssert s.buffers != nil

  let runway = s.span.len
  if maxSize <= runway:
    let
      startAddr = s.span.startAddr
      endAddr = offset(startAddr, maxSize)

    inc s.extCursorsCount
    result = VarSizeWriteCursor WriteCursor(
      stream: s,
      span: PageSpan(startAddr: startAddr, endAddr: endAddr))

    s.buffers.splitLastPageAt(endAddr)
    s.span.startAddr = endAddr

  else:
    trackWrittenTo(s.buffers, s.span.startAddr)

    let
      nextPageSize = nextAlignedSize(maxSize, s.buffers.pageSize)
      nextPage = allocWritablePage(nextPageSize, maxSize)
      nextPageSpan = nextPage.fullSpan
      cursorEndAddr = offset(nextPageSpan.startAddr, maxSize)

    s.buffers.queue.addLast nextPage

    inc s.extCursorsCount
    result = VarSizeWriteCursor WriteCursor(
      stream: s,
      span: PageSpan(startAddr: nextPageSpan.startAddr,
                     endAddr: cursorEndAddr))

    s.span = PageSpan(startAddr: cursorEndAddr,
                      endAddr: nextPageSpan.endAddr)
    s.spanEndPos = s.spanEndPos - runway + nextPageSize

proc getWritableBytesOnANewPage(s: OutputStream, spanSize: Natural): ptr byte =
  fsAssert s.buffers != nil

  trackWrittenTo(s.buffers, s.span.startAddr)

  let
    nextPageSize = nextAlignedSize(spanSize, s.buffers.pageSize)
    nextPage = allocWritablePage(nextPageSize, spanSize)

  s.buffers.queue.addLast nextPage

  let retiredSpanRunway = s.span.len
  s.span = nextPage.fullSpan
  s.spanEndPos = s.spanEndPos - retiredSpanRunway + spanSize
  s.span.startAddr

template getWritableBytes*(sp: OutputStream, spanSizeParam: Natural): openArray[byte] =
  ## Returns a contiguous range of memory that the caller is free to populate fully
  ## or partially. The caller indicates how many bytes were written to the openArray
  ## by calling `advance(numberOfBytes)` once or multiple times. Advancing the stream
  ## past the allocated size is considered a defect. The typical usage pattern of this
  ## API looks as follows:
  ##
  ## stream.advance(myComponent.writeBlock(stream.getWritableBytes(maxBlockSize)))
  ##
  ## In the example, `writeBlock` would be a function returning the number of bytes
  ## written to the openArray.
  ##
  ## While it's not illegal to issue other writing operations to the stream during
  ## the `getWritetableBytes` -> `advance` sequence, doing this is not recommended
  ## because it will result in overwriting the same range of bytes.
  ##
  ## One limitation of this API is that the returned `openArray` will be considered
  ## read-only by Nim. You may need to use `unsafeAddr` in your writing functions
  ## to get around this limitation.
  let
    s = sp
    spanSize = spanSizeParam
    runway = s.span.len
    startAddr = if spanSize <= runway:
      s.span.startAddr
    else:
      getWritableBytesOnANewPage(s, spanSize)

  makeOpenArray(startAddr, spanSize)

proc advance*(s: OutputStream, bytesWrittenToWritableSpan: Natural) =
  ## Advance the stream write cursor.
  ## Typically used after a previous call to `getWritableBytes`.
  fsAssert bytesWrittenToWritableSpan <= s.span.len
  s.span.startAddr = offset(s.span.startAddr, bytesWrittenToWritableSpan)

proc finalize*(cursor: var WriteCursor) =
  fsAssert cursor.stream.extCursorsCount > 0
  dec cursor.stream.extCursorsCount

proc finalWrite*(cursor: var WriteCursor, data: openArray[byte]) =
  fsAssert data.len == cursor.span.len
  copyMem(cursor.span.startAddr, baseAddr(data), data.len)
  finalize cursor

proc finalWrite*(c: var VarSizeWriteCursor, data: openArray[byte]) =
  template cursor: auto = WriteCursor(c)

  let overestimatedBytes = cursor.span.len - data.len
  fsAssert overestimatedBytes >= 0

  WriteCursor(c).stream.spanEndPos -= overestimatedBytes

  for page in items(cursor.stream.buffers.queue):
    let baseAddr = page.allocationStart
    if cursor.span.startAddr == baseAddr:
      # This is page starting cursor
      page.consumedTo = overestimatedBytes
      copyMem(offset(baseAddr, overestimatedBytes), baseAddr(data), data.len)
      finalize cursor
      return

    if page.readableEnd == cursor.span.endAddr:
      # This is a page ending cursor
      page.writtenTo = distance(baseAddr, cursor.span.startAddr) + data.len
      copyMem(cursor.span.startAddr, baseAddr(data), data.len)
      finalize cursor
      return

  fsAssert false

proc tryMovingToNextPage(c: var WriteCursor) =
  # A split cursor is a fixed-size cursor that ended up on page boundary.
  #
  # Part of the cursor used the last few bytes of the first page and we've
  # left some empty space at the beginning of the second page.
  #
  # Even if the cursor size was very large, we've made sure the next
  # page is big enough to hold all the data. When we created the cursor,
  # we've taken a note regarding the number of bytes on the second page
  # that are reserved by writing them as a negative value for the page
  # `consumedTo`.
  #
  # All we need to do here is update the cursor span to point to the next
  # page and set the now final `endAddr`. The page `consumedTo` is updated
  # to 0 to indicate that the cursor has made the flip.
  #
  # If you are wondering, var-sized cursors cannot be split, because our
  # strategy is to always place them at the beggining or end of pages.
  #
  # When we try to create a var-sized cursor, we check if there are enough
  # bytes on the current page to contain the worst case scenario (the var
  # sized cursor has an upper size limit). If there are enough bytes, we
  # end the page prematurely (it will end up with an `writtenTo`). We can
  # then recycle the same memory for the next page that will use an adjusted
  # `consumedTo`. The `writtenTo` of the first page will be written when
  # the cursor is finalized and its final size becomes known.
  #
  # If there weren't enough bytes (a much more rare event), we allocate a
  # new page. We adjust the `writtenTo` of the current page to mark it's
  # premature end and we mark the cursor as special by writing a

  # The split cursor is definetely not on the last page, so we can iterate
  # only over the preceeding pages to find where it was:
  var prevPage = c.stream.buffers.queue[0]
  for i in 1 ..< c.stream.buffers.queue.len:
    let page = c.stream.buffers.queue[i]
    if c.span.endAddr == prevPage.allocationEnd and page.consumedTo < 0:
      # We found what we need, so let's get to business:
      c.span.startAddr = page.allocationStart
      c.span.endAddr = offset(c.span.startAddr, -page.consumedTo)
      page.consumedTo = 0
      return
    prevPage = page

  # We didn't find any page that this cursor was ending, so this is not
  # a split cursor. This means that the user just tried to write past the
  # pre-allocated cursor span, which is considered a Defect (a range error)
  fsAssert false, "Attempt to write past the end of a cursor"

template writeToNewSpanImpl(s: OutputStream, b: byte, awaiter, writeOp, drainOp: untyped) =
  if s.buffers == nil:
    fsAssert s.vtable != nil # This is an unsafe memory output and we've reached
                             # the end of the buffer which is range violation defect
    fsAssert s.vtable.writeOp != nil
    awaiter s.vtable.writeOp(s, unsafeAddr b, 1)
  elif s.vtable == nil or s.extCursorsCount > 0:
    # This is the main cursor of a stream, but we are either not
    # ready to flush due to outstanding delayed writes or this is
    # just a memory output stream. In both cases, we just need to
    # allocate more memory and continue writing:
    addPage(s)
    writeByte(s.span, b)
  else:
    trackWrittenToEnd(s.buffers)
    awaiter drainOp(s, nil, 0)
    writeByte(s.span, b)

proc write*(c: var WriteCursor, b: byte) =
  if atEnd(c.span):
    # The cursor has reached the end of its buffer, but it may be a
    # split cursor. If that's the case, the following function will
    # succeed. If that's not a split cursor, we'll raise a Defect.
    tryMovingToNextPage(c)

  writeByte(c.span, b)

proc writeToNewSpan(s: OutputStream, b: byte) =
  writeToNewSpanImpl(s, b, noAwait, writeSync, drainAllBuffersSync)

template write*(sp: OutputStream, b: byte) =
  let s = sp
  if hasRunway(s.span):
    writeByte(s.span, b)
  else:
    writeToNewSpan(s, b)

when fsAsyncSupport:
  proc write*(sp: AsyncOutputStream, b: byte) =
    let s = OutputStream sp
    if atEnd(s.span):
      addPage(s)
    writeByte(s.span, b)

  template writeAndWait*(sp: AsyncOutputStream, b: byte) =
    let s = OutputStream sp
    if hasRunway(s.span):
      writeByte(s.span, b)
    else:
      writeToNewSpanImpl(s, b, fsAwait, writeAsync, drainAllBuffersAsync)

  template write*(s: AsyncOutputStream, x: char) =
    write s, byte(x)

template write*(s: OutputStream|var WriteCursor, x: char) =
  bind write
  write s, byte(x)

proc writeToANewPage(s: OutputStream, bytes: openArray[byte]) =
  var
    runway = s.span.len
    inputPos = baseAddr(bytes)
    inputLen = bytes.len

  template reduceInput(delta: int) =
    inputPos = offset(inputPos, delta)
    inputLen -= delta

  if runway > 0:
    copyMem(s.span.startAddr, inputPos, runway)
    reduceInput runway

  fsAssert s.buffers != nil

  let nextPageSize = nextAlignedSize(inputLen, s.buffers.pageSize)
  let nextPage = s.buffers.addWritablePage(nextPageSize)

  s.span = nextPage.fullSpan
  s.spanEndPos += nextPageSize

  copyMem(s.span.startAddr, inputPos, inputLen)
  s.span.startAddr = offset(s.span.startAddr, inputLen)

template writeBytesImpl(s: OutputStream,
                        bytes: openArray[byte],
                        drainOp: untyped) =
  let inputLen = bytes.len
  if inputLen == 0: return

  # We have a short inlinable function handling the case when the input is
  # short enough to fit in the current page. We'll keep buffering until the
  # page is full:
  let runway = s.span.len
  if inputLen <= runway:
    copyMem(s.span.startAddr, baseAddr(bytes), inputLen)
    s.span.startAddr = offset(s.span.startAddr, inputLen)
  elif s.vtable == nil or s.extCursorsCount > 0:
    # We are not ready to flush, so we must create pending pages.
    # We'll try to create them as large as possible:
    s.writeToANewPage(bytes)
  else:
    trackWrittenTo(s.buffers, s.span.startAddr)
    drainOp

proc write*(s: OutputStream, bytes: openArray[byte]) =
  writeBytesImpl(s, bytes):
    drainAllBuffersSync(s, baseAddr(bytes), bytes.len)

proc write*(s: OutputStream, chars: openArray[char]) =
  write s, charsToBytes(chars)

proc write*(s: MaybeAsyncOutputStream, value: string) {.inline.} =
  write s, value.toOpenArrayByte(0, value.len - 1)

proc write*(s: OutputStream, value: cstring) =
  for c in value:
    write s, c

template memCopyToBytes(value: auto): untyped =
  type T = type(value)
  static: assert supportsCopyMem(T)
  let valueAddr = unsafeAddr value
  makeOpenArray(cast[ptr byte](valueAddr), sizeof(T))

proc writeMemCopy*(s: OutputStream, value: auto) =
  write s, memCopyToBytes(value)

when fsAsyncSupport:
  proc writeBytesAsyncImpl(sp: OutputStream,
                          bytes: openArray[byte]): Future[void] =
    let s = sp
    writeBytesImpl(s, bytes):
      return s.vtable.writeAsync(s, baseAddr(bytes), bytes.len)

  proc writeBytesAsyncImpl(s: OutputStream,
                          chars: openArray[char]): Future[void] =
    writeBytesAsyncImpl s, charsToBytes(chars)

  proc writeBytesAsyncImpl(s: OutputStream,
                          str: string): Future[void] =
    writeBytesAsyncImpl s, toOpenArray(str, 0, str.len - 1)

template writeAndWait*(s: OutputStream, value: untyped) =
  write s, value

when fsAsyncSupport:
  template writeAndWait*(sp: AsyncOutputStream, value: untyped) =
    bind writeBytesAsyncImpl

    let
      s = OutputStream sp
      f = writeBytesAsyncImpl(s, value)

    if f != nil:
      fsAwait(f)
      s.span = getWritableSpan s.buffers
      s.spanEndPos += s.span.len

  template writeMemCopyAndWait*(sp: AsyncOutputStream, value: auto) =
    writeAndWait(sp, memCopyToBytes(value))

proc writeBytesToCursor(c: var WriteCursor, bytes: openArray[byte]) =
  var
    runway = c.span.len
    inputPos = baseAddr(bytes)
    inputLen = bytes.len

  template reduceInput(delta: int) =
    inputPos = offset(inputPos, delta)
    inputLen -= delta

  if inputLen <= runway:
    copyMem(c.span.startAddr, inputPos, inputLen)
    c.span.startAddr = offset(c.span.startAddr, inputLen)
  else:
    # This must be a split cursor. We need to complete its first page first,
    # then switch to the second and continue the write there.
    copyMem(c.span.startAddr, baseAddr(bytes), runway)
    reduceInput runway
    # If this really is a split cursor, the following operation will succeed.
    # Otherwise, it will Defect and the conclusion is that this was a write
    # past the cursor end.
    c.tryMovingToNextPage()
    # On the next page, we have a new runway
    runway = c.span.len
    # The write shouldn't go past the end of the new runway
    fsAssert inputLen <= runway
    copyMem(c.span.startAddr, inputPos, inputLen)
    c.span.startAddr = offset(c.span.startAddr, inputLen)

template write*(c: var WriteCursor, bytes: openArray[byte]) =
  bind writeBytesToCursor
  writeBytesToCursor(c, bytes)

proc write*(c: var WriteCursor, chars: openArray[char]) {.inline.} =
  writeBytesToCursor(c, chars.toOpenArrayByte(0, chars.len - 1))

proc writeMemCopy*[T](c: var WriteCursor, value: T) =
  writeBytesToCursor(c, memCopyToBytes(value))

proc write*(c: var WriteCursor, str: string) =
  writeBytesToCursor(c, str.toOpenArrayByte(0, str.len - 1))

type
  OutputConsumingProc = proc (data: openArray[byte])
                             {.gcsafe, raises: [Defect].}

proc consumeOutputsImpl(s: OutputStream, consumer: OutputConsumingProc) =
  let runway = s.span.len

  fsAssert s.extCursorsCount == 0 and s.buffers != nil
  trackWrittenTo(s.buffers, s.span.startAddr)

  for pageReadableStart, pageLen in consumePageBuffers(s.buffers):
    consumer(makeOpenArray(pageReadableStart, pageLen))

  s.span = getWritableSpan(s.buffers)
  s.spanEndPos = s.spanEndPos - runway + s.span.len

template consumeOutputs*(s: OutputStream, bytesVar, body: untyped) =
  ## Please note that calling `consumeOutputs` on an unbuffered stream
  ## or an unsafe memory stream is considered a Defect.
  ##
  ## Before consuming the outputs, all outstanding delayed writes must
  ## be finalized.
  proc consumer(bytesVar: openArray[byte]) {.gcsafe, raises: [Defect].} =
    body

  consumeOutputsImpl(s, consumer)

proc consumeContiguousOutputImpl(s: OutputStream, consumer: OutputConsumingProc) =
  var
    runway = s.span.len
    contigiousBytes: string
      # this may remain null
      # We are using a string, because `newStringOfCap` doesn't zero out
      # the memory. TODO check if the same is true for `newSeqOfCap`.
    bytesPtr: ptr byte
    bytesLen: int

  fsAssert s.extCursorsCount == 0 and s.buffers != nil
  s.buffers.trackWrittenTo s.span.startAddr

  if s.buffers.queue.len == 1:
    let page = s.buffers.queue[0]
    bytesPtr = page.readableStart
    bytesLen = page.writtenTo - page.consumedTo
    # We need to reset the page to an empty state, so it can be reused
    page.consumedTo = 0
    page.writtenTo = 0
  else:
    contigiousBytes = newStringOfCap(s.buffers.totalBufferedBytes)

    for pageReadableStart, pageLen in consumePageBuffers(s.buffers):
      contigiousBytes.add makeOpenArray(cast[ptr char](pageReadableStart), pageLen)

    bytesPtr = cast[ptr byte](addr contigiousBytes[0])
    bytesLen = contigiousBytes.len

  consumer(makeOpenArray(bytesPtr, bytesLen))

  s.span = s.buffers.getWritableSpan()
  s.spanEndPos = s.spanEndPos - runway + s.span.len

template consumeContiguousOutput*(s: OutputStream, bytesVar, body: untyped) =
  ## Please note that calling `consumeContiguousOutput` on an unbuffered stream
  ## or an unsafe memory stream is considered a Defect.
  ##
  ## Before consuming the output, all outstanding delayed writes must
  ## be finalized.
  ##
  proc consumer(bytesVar: openArray[byte]) {.gcsafe, raises: [Defect].} =
    body

  consumeContiguousOutputImpl(s, consumer)

proc getOutput*(s: OutputStream, T: type string): string =
  ## Please note that calling `getOutput` on an unbuffered stream
  ## or an unsafe memory stream is considered a Defect.
  ##
  ## Before consuming the output, all outstanding delayed writes must be finalized.
  ##
  fsAssert s.extCursorsCount == 0 and s.buffers != nil
  s.buffers.trackWrittenTo s.span.startAddr

  if s.buffers.queue.len == 1:
    let page = s.buffers.queue[0]
    if page.consumedTo == 0:
      result.swap page.data[]
      result.setLen page.writtenTo
      # We clear the buffers, so the stream will be in pristine state.
      # The next write is going to create a fresh new starting page.
      s.buffers.queue.clear()
      return

  result = newStringOfCap(s.pos)
  for page in items(s.buffers.queue):
    result.add page.pageChars

template getOutput*(s: OutputStream, T: type seq[byte]): seq[byte] =
  cast[seq[byte]](s.getOutput(string))

template getOutput*(s: OutputStream): seq[byte] =
  cast[seq[byte]](s.getOutput(string))

when fsAsyncSupport:
  template getOutput*(s: AsyncOutputStream): seq[byte] =
    getOutput OutputStream(s)

  template getOutput*(s: AsyncOutputStream, T: type): untyped =
    getOutput OutputStream(s), T

  when fsAsyncSupport:
    template flush*(sp: AsyncOutputStream) =
      let s = OutputStream sp
      flushImpl(s, fsAwait, writeAsync, flushAsync)

    proc flushAsync*(s: AsyncOutputStream) {.async.} =
      flush s

    template close*(sp: AsyncOutputStream) =
      let s = OutputStream sp
      flush(Async s)
      disconnectOutputDevice(s)
      if s.closeFut != nil:
        fsAwait s.closeFut

    proc closeAsync*(s: AsyncOutputStream) {.async.} =
      close s
