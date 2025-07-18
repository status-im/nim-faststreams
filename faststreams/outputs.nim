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
  buffers, CloseBehavior

{.pragma: iocall, nimcall, gcsafe, raises: [IOError].}

when not declared(newSeqUninit): # nim 2.2+
  template newSeqUninit[T: byte](len: int): seq[byte] =
    newSeqUninitialized[byte](len)

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

    WriteSyncProc* = proc (s: OutputStream, src: pointer, srcLen: Natural) {.iocall.}
    WriteAsyncProc* = proc (s: OutputStream, src: pointer, srcLen: Natural): Future[void] {.iocall.}
    FlushSyncProc* = proc (s: OutputStream) {.iocall.}
    FlushAsyncProc* = proc (s: OutputStream): Future[void] {.iocall.}
    CloseSyncProc* = proc (s: OutputStream) {.iocall.}
    CloseAsyncProc* = proc (s: OutputStream): Future[void] {.iocall.}

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

    WriteSyncProc* = proc (s: OutputStream, src: pointer, srcLen: Natural) {.iocall.}
    FlushSyncProc* = proc (s: OutputStream) {.iocall.}
    CloseSyncProc* = proc (s: OutputStream) {.iocall.}

    OutputStreamVTable* = object
      writeSync*: WriteSyncProc
      flushSync*: FlushSyncProc
      closeSync*: CloseSyncProc

    MaybeAsyncOutputStream* = OutputStream

type
  WriteCursor* = object
    span: PageSpan
    spill: PageSpan
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

template prepareSpan(s: OutputStream, minLen: int) =
  s.span = s.buffers.prepare(s.buffers.nextAlignedSize(minLen))

  # Count the full span so that when span.len decreases as data gets written,
  # we can compute the total number of written bytes without having to
  # update another counter at each write
  s.spanEndPos += s.span.len

template commitSpan(s: OutputStream, span: PageSpan) =
  # Adjust position to discount the uncommitted bytes, now that the span is
  # being reset
  s.spanEndPos -= span.len
  s.buffers.commit(span)
  span.reset()

template flushImpl(s: OutputStream, awaiter, writeOp, flushOp: untyped) =
  fsAssert s.extCursorsCount == 0
  if s.vtable != nil:
    if s.buffers != nil:
      s.commitSpan(s.span)
      awaiter s.vtable.writeOp(s, nil, 0)

    if s.vtable.flushOp != nil:
      awaiter s.vtable.flushOp(s)

proc flush*(s: OutputStream) =
  flushImpl(s, noAwait, writeSync, flushSync)

proc close*(s: OutputStream,
            behavior = dontWaitAsyncClose)
           {.raises: [IOError].} =
  if s == nil:
    return

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
proc `=destroy`*(h: var OutputStreamHandle) {.raises: [].} =
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

template makeHandle*(sp: OutputStream): OutputStreamHandle =
  let s = sp
  OutputStreamHandle(s: s)

proc memoryOutput*(pageSize = defaultPageSize): OutputStreamHandle =
  fsAssert pageSize > 0
  # We are not creating an initial output page, because `ensureRunway`
  # can determine the most appropriate size.
  makeHandle OutputStream(buffers: PageBuffers.init(pageSize))

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
    s.commitSpan(s.span)
    s.prepareSpan(neededRunway)

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
    for span in s.buffers.consumeAll():
      let
        writeStartVar = span.startAddr
        writeLenVar = span.len
        bytesWritten = writeBlock
      # TODO: Can we repair the buffers here?
      if bytesWritten != writeLenVar: raiseError()

  if writeLenVar > 0:
    fsAssert writeStartVar != nil
    let bytesWritten = writeBlock
    if bytesWritten != writeLenVar: raiseError()

proc writeFileSync(s: OutputStream, src: pointer, srcLen: Natural)
                  {.iocall.} =
  var file = FileOutputStream(s).file

  implementWrites(s.buffers, src, srcLen, "FILE",
                  writeStartAddr, writeLen):
    file.writeBuffer(writeStartAddr, writeLen)

proc flushFileSync(s: OutputStream)
                  {.iocall.} =
  flushFile FileOutputStream(s).file

proc closeFileSync(s: OutputStream)
                  {.iocall.} =
  close FileOutputStream(s).file

when fsAsyncSupport:
  proc writeFileAsync(s: OutputStream, src: pointer, srcLen: Natural): Future[void]
                     {.iocall.} =
    fsAssert FileOutputStream(s).allowAsyncOps
    writeFileSync(s, src, srcLen)
    result = newFuture[void]()
    fsTranslateErrors "Unexpected exception from merely completing a future":
      result.complete()

  proc flushFileAsync(s: OutputStream): Future[void]
                     {.iocall.} =
    fsAssert FileOutputStream(s).allowAsyncOps
    flushFile FileOutputStream(s).file
    result = newFuture[void]()
    fsTranslateErrors "Unexpected exception from merely completing a future":
      result.complete()

const fileOutputVTable = when fsAsyncSupport:
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
  # https://github.com/nim-lang/Nim/issues/22389
  when (NimMajor, NimMinor, NimPatch) >= (2, 0, 12):
    addr vtable
  else:
    let vtable2 {.global.} = vtable
    {.noSideEffect.}:
      unsafeAddr vtable2

proc fileOutput*(f: File,
                 pageSize = defaultPageSize,
                 allowAsyncOps = false): OutputStreamHandle
                {.raises: [IOError].} =
  makeHandle FileOutputStream(
    vtable: vtableAddr fileOutputVTable,
    buffers: if pageSize > 0: PageBuffers.init(pageSize) else: nil,
    file: f,
    allowAsyncOps: allowAsyncOps)

proc fileOutput*(filename: string,
                 fileMode: FileMode = fmWrite,
                 pageSize = defaultPageSize,
                 allowAsyncOps = false): OutputStreamHandle
                {.raises: [IOError].} =
  fileOutput(open(filename, fileMode), pageSize, allowAsyncOps)

proc pos*(s: OutputStream): int =
  s.spanEndPos - s.span.len

when fsAsyncSupport:
  template pos*(s: AsyncOutputStream): int =
    pos OutputStream(s)

proc getBuffers*(s: OutputStream): PageBuffers =
  fsAssert s.buffers != nil
  s.commitSpan(s.span)

  s.buffers

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

      s.span = bufferPage.prepare()
      s.spanEndPos = s.span.len
      return
  else:
    s.buffers = PageBuffers.init(defaultPageSize)

  s.span = default(PageSpan)
  s.spanEndPos = 0

# Pre-conditions for `drainAllBuffers(Sync/Async)`
#  * The cursor has reached the current span end
#  * We are working with a vtable-enabled stream
#
# Post-conditions:
#  * All completed pages are written

proc drainAllBuffersSync(s: OutputStream, buf: pointer, bufSize: Natural) =
  s.vtable.writeSync(s, buf, bufSize)

when fsAsyncSupport:
  proc drainAllBuffersAsync(s: OutputStream, buf: pointer, bufSize: Natural) {.async.} =
    fsAwait s.vtable.writeAsync(s, buf, bufSize)

proc createCursor(s: OutputStream, span, spill: PageSpan): WriteCursor =
  inc s.extCursorsCount

  WriteCursor(stream: s, span: span, spill: spill)

proc delayFixedSizeWrite*(s: OutputStream, cursorSize: Natural): WriteCursor =
  let runway = s.span.len

  if s.buffers == nil:
    fsAssert cursorSize <= runway
    # Without buffers, we'll simply mark the part of the span as written and
    # move on
    createCursor(s, s.span.split(cursorSize), PageSpan())
  else:
    # Commit what's already been written to the local span, in case a flush
    # happens
    s.commitSpan(s.span)

    let capacity = s.buffers.capacity()
    if cursorSize <= capacity or capacity == 0:
      # A single page buffer will be enough to cover the cursor
      var span = s.buffers.reserve(cursorSize)
      s.spanEndPos += span.len

      createCursor(s, span, PageSpan())
    else:
      # There is some capacity in the page buffer but not enough for the full
      # cursor - create a split cursor that references two memory areas
      var
        span = s.buffers.reserve(capacity)
        spill = s.buffers.reserve(cursorSize - capacity)
      s.spanEndPos += span.len + spill.len


      createCursor(s, span, spill)

proc delayVarSizeWrite*(s: OutputStream, maxSize: Natural): VarSizeWriteCursor =
  ## Please note that using variable sized writes are not supported
  ## for unbuffered streams and unsafe memory inputs.
  fsAssert s.buffers != nil

  VarSizeWriteCursor s.delayFixedSizeWrite(maxSize)

proc getWritableBytesOnANewPage(s: OutputStream, spanSize: Natural): ptr byte =
  fsAssert s.buffers != nil
  s.commitSpan(s.span)

  s.prepareSpan(spanSize)

  s.span.startAddr

template getWritableBytes*(sp: OutputStream, spanSizeParam: Natural): var openArray[byte] =
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
  s.span.advance(bytesWrittenToWritableSpan)

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
    s.commitSpan(s.span)
    s.prepareSpan(s.buffers.pageSize)
    write(s.span, b)
  else:
    s.commitSpan(s.span)
    awaiter drainOp(s, nil, 0)
    s.prepareSpan(s.buffers.pageSize)
    write(s.span, b)

proc writeToNewSpan(s: OutputStream, b: byte) =
  writeToNewSpanImpl(s, b, noAwait, writeSync, drainAllBuffersSync)

template write*(sp: OutputStream, b: byte) =
  let s = sp
  if hasRunway(s.span):
    write(s.span, b)
  else:
    writeToNewSpan(s, b)

when fsAsyncSupport:
  proc write*(sp: AsyncOutputStream, b: byte) =
    let s = OutputStream sp
    if atEnd(s.span):
      s.commitSpan(s.span)
      s.prepareSpan()

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
  let runway = s.span.len
  fsAssert bytes.len > runway

  if runway > 0:
    s.span.write(bytes.toOpenArray(0, runway - 1))

  s.commitSpan(s.span)
  s.prepareSpan(bytes.len - runway)

  s.span.write(bytes.toOpenArray(runway, bytes.len - 1))

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
    s.span.write(bytes)
  elif s.vtable == nil or s.extCursorsCount > 0:
    # We are not ready to flush, so we must create pending pages.
    # We'll try to create them as large as possible:

    s.writeToANewPage(bytes)
  else:
    s.commitSpan(s.span)
    drainOp

proc write*(s: OutputStream, bytes: openArray[byte]) =
  writeBytesImpl(s, bytes):
    drainAllBuffersSync(s, baseAddr(bytes), bytes.len)

proc write*(s: OutputStream, chars: openArray[char]) =
  write s, chars.toOpenArrayByte(0, chars.high())

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
    writeBytesAsyncImpl s, chars.toOpenArrayByte(0, chars.high())

  proc writeBytesAsyncImpl(s: OutputStream,
                          str: string): Future[void] =
    writeBytesAsyncImpl s, str.toOpenArrayByte(0, str.high())

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

  template writeMemCopyAndWait*(sp: AsyncOutputStream, value: auto) =
    writeAndWait(sp, memCopyToBytes(value))

proc write*(c: var WriteCursor, bytes: openArray[byte]) =
  var remaining = bytes.len
  if remaining > 0:
    let written = min(remaining, c.span.len)
    c.span.write(bytes.toOpenArray(0, written - 1))

    if c.span.len == 0 and c.stream.buffers != nil:
      c.stream.commitSpan(c.span)

    if written < remaining:
      # c.span is full - commit it and move to the spill
      # Reaching this point implies we have buffers backing the span
      remaining -= written

      c.span = c.spill
      c.spill.reset()

      c.span.write(bytes.toOpenArray(written, bytes.len - 1))

      if c.span.len == 0 and c.stream.buffers != nil:
        c.stream.commitSpan(c.span)

proc write*(c: var WriteCursor, b: byte) =
  write(c, [b])

proc write*(c: var WriteCursor, chars: openArray[char]) =
  write(c, chars.toOpenArrayByte(0, chars.len - 1))

proc writeMemCopy*[T](c: var WriteCursor, value: T) =
  write(c, memCopyToBytes(value))

proc write*(c: var WriteCursor, str: string) =
  write(c, str.toOpenArrayByte(0, str.len - 1))

proc finalize*(c: var WriteCursor) =
  fsAssert c.stream.extCursorsCount > 0

  dec c.stream.extCursorsCount

proc finalWrite*(c: var WriteCursor, data: openArray[byte]) =
  c.write(data)
  finalize c

proc write*(c: var VarSizeWriteCursor, b: byte) {.borrow.}
proc write*(c: var VarSizeWriteCursor, bytes: openArray[byte]) {.borrow.}
proc write*(c: var VarSizeWriteCursor, chars: openArray[char]) {.borrow.}
proc writeMemCopy*[T](c: var VarSizeWriteCursor, value: T) =
  writeMemCopy(WriteCursor(c), value)

proc write*(c: var VarSizeWriteCursor, str: string)  {.borrow.}

proc finalize*(c: var VarSizeWriteCursor) =
  fsAssert WriteCursor(c).stream.extCursorsCount > 0

  dec WriteCursor(c).stream.extCursorsCount

  if WriteCursor(c).span.startAddr != nil:
    WriteCursor(c).stream.commitSpan(WriteCursor(c).span)

  if WriteCursor(c).spill.startAddr != nil:
    WriteCursor(c).stream.commitSpan(WriteCursor(c).spill)

proc finalWrite*(c: var VarSizeWriteCursor, data: openArray[byte]) =
  c.write(data)
  finalize c

type
  OutputConsumingProc = proc (data: openArray[byte])
                             {.gcsafe, raises: [].}

proc consumeOutputsImpl(s: OutputStream, consumer: OutputConsumingProc) =
  fsAssert s.extCursorsCount == 0 and s.buffers != nil
  s.commitSpan(s.span)

  for span in s.buffers.consumeAll():
    var span = span # var for template
    consumer(span.data())

  s.spanEndPos = 0

template consumeOutputs*(s: OutputStream, bytesVar, body: untyped) =
  ## Please note that calling `consumeOutputs` on an unbuffered stream
  ## or an unsafe memory stream is considered a Defect.
  ##
  ## Before consuming the outputs, all outstanding delayed writes must
  ## be finalized.
  proc consumer(bytesVar: openArray[byte]) {.gcsafe, raises: [].} =
    body

  consumeOutputsImpl(s, consumer)

proc consumeContiguousOutputImpl(s: OutputStream, consumer: OutputConsumingProc) =
  fsAssert s.extCursorsCount == 0 and s.buffers != nil
  s.commitSpan(s.span)

  if s.buffers.queue.len == 1:
    var span = s.buffers.consume()
    consumer(span.data())
  else:
    var bytes = newSeqUninit[byte](s.pos)
    var pos = 0

    if bytes.len > 0:
      for span in s.buffers.consumeAll():
        copyMem(addr bytes[pos], span.startAddr, span.len )
        pos += span.len

    consumer(bytes)

  s.spanEndPos = 0

template consumeContiguousOutput*(s: OutputStream, bytesVar, body: untyped) =
  ## Please note that calling `consumeContiguousOutput` on an unbuffered stream
  ## or an unsafe memory stream is considered a Defect.
  ##
  ## Before consuming the output, all outstanding delayed writes must
  ## be finalized.
  ##
  proc consumer(bytesVar: openArray[byte]) {.gcsafe, raises: [].} =
    body

  consumeContiguousOutputImpl(s, consumer)

proc getOutput*(s: OutputStream, T: type string): string =
  ## Consume data written so far to the in-memory page buffer - this operation
  ## is meaningful only for `memoryOutput` and leaves the page buffer empty.
  ##
  ## Please note that calling `getOutput` on an unbuffered stream
  ## or an unsafe memory stream is considered a Defect.
  ##
  ## Before consuming the output, all outstanding delayed writes must be finalized.
  ##
  fsAssert s.extCursorsCount == 0 and s.buffers != nil
  s.commitSpan(s.span)

  result = newStringOfCap(s.pos)
  s.spanEndPos = 0

  for span in s.buffers.consumeAll():
    when compiles(span.data().toOpenArrayChar(0, len - 1)):
      result.add span.data().toOpenArrayChar(0, len - 1)
    else:
      let p = cast[ptr char](span.startAddr)
      result.add makeOpenArray(p, span.len)

proc getOutput*(s: OutputStream, T: type seq[byte]): seq[byte] =
  ## Consume data written so far to the in-memory page buffer - this operation
  ## is meaningful only for `memoryOutput` and leaves the page buffer empty.
  ##
  ## Please note that calling `getOutput` on an unbuffered stream
  ## or an unsafe memory stream is considered a Defect.
  ##
  ## Before consuming the output, all outstanding delayed writes must be finalized.
  fsAssert s.extCursorsCount == 0 and s.buffers != nil
  s.commitSpan(s.span)

  if s.buffers.queue.len == 1:
    let page = s.buffers.queue[0]
    if page.consumedTo == 0:
      # "move" the buffer to the caller - swap works for all nim versions and gcs
      result.swap page.store[]
      result.setLen page.writtenTo
      # We clear the buffers, so the stream will be in pristine state.
      # The next write is going to create a fresh new starting page.
      s.buffers.queue.clear()
      s.spanEndPos = 0
      return

  result = newSeqUninit[byte](s.pos)
  s.spanEndPos = 0

  var pos = 0
  for span in s.buffers.consumeAll():
    copyMem(addr result[pos], span.startAddr, span.len)
    pos += span.len

template getOutput*(s: OutputStream): seq[byte] =
  s.getOutput(seq[byte])

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

    proc close*(sp: AsyncOutputStream) =
      let s = OutputStream sp
      if s != nil:
        flush(Async s)
        disconnectOutputDevice(s)
        if s.closeFut != nil:
          fsAwait s.closeFut

    proc closeAsync*(s: AsyncOutputStream) {.async.} =
      close s
