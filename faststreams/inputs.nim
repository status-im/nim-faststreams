import
  os, memfiles, options,
  stew/[ptrops],
  async_backend, buffers

export
  options, CloseBehavior

when fsAsyncSupport:
  # Circular type refs prevent more targeted `when`
  type
    InputStream* = ref object of RootObj
      vtable*: ptr InputStreamVTable # This is nil for unsafe memory inputs
      buffers*: PageBuffers          # This is nil for unsafe memory inputs
      span*: PageSpan
      spanEndPos*: Natural
      closeFut*: Future[void]      # This is nil before `close` is called
      when debugHelpers:
        name*: string


    AsyncInputStream* {.borrow: `.`.} = distinct InputStream

    ReadSyncProc* = proc (s: InputStream, dst: pointer, dstLen: Natural): Natural
                        {.nimcall, gcsafe, raises: [IOError, Defect].}

    ReadAsyncProc* = proc (s: InputStream, dst: pointer, dstLen: Natural): Future[Natural]
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    CloseSyncProc* = proc (s: InputStream)
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    CloseAsyncProc* = proc (s: InputStream): Future[void]
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    GetLenSyncProc* = proc (s: InputStream): Option[Natural]
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    InputStreamVTable* = object
      readSync*: ReadSyncProc
      closeSync*: CloseSyncProc
      getLenSync*: GetLenSyncProc
      readAsync*: ReadAsyncProc
      closeAsync*: CloseAsyncProc

    MaybeAsyncInputStream* = InputStream | AsyncInputStream

else:
  type
    InputStream* = ref object of RootObj
      vtable*: ptr InputStreamVTable # This is nil for unsafe memory inputs
      buffers*: PageBuffers          # This is nil for unsafe memory inputs
      span*: PageSpan
      spanEndPos*: Natural
      when debugHelpers:
        name*: string


    ReadSyncProc* = proc (s: InputStream, dst: pointer, dstLen: Natural): Natural
                        {.nimcall, gcsafe, raises: [IOError, Defect].}

    CloseSyncProc* = proc (s: InputStream)
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    GetLenSyncProc* = proc (s: InputStream): Option[Natural]
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

    InputStreamVTable* = object
      readSync*: ReadSyncProc
      closeSync*: CloseSyncProc
      getLenSync*: GetLenSyncProc

    MaybeAsyncInputStream* = InputStream

type
  LayeredInputStream* = ref object of InputStream
    source*: InputStream
    allowWaitFor*: bool

  InputStreamHandle* = object
    s*: InputStream

  MemFileInputStream = ref object of InputStream
    file: MemFile

  FileInputStream = ref object of InputStream
    file: File

template Sync*(s: InputStream): InputStream = s

when fsAsyncSupport:
  template Async*(s: InputStream): AsyncInputStream = AsyncInputStream(s)

  template Sync*(s: AsyncInputStream): InputStream = InputStream(s)
  template Async*(s: AsyncInputStream): AsyncInputStream = s

proc disconnectInputDevice(s: InputStream) =
  # TODO
  # Document the behavior that closeAsync is preferred
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
  template disconnectInputDevice(s: AsyncInputStream) =
    disconnectInputDevice InputStream(s)

proc preventFurtherReading(s: InputStream) =
  s.vtable = nil
  s.span = default(PageSpan)

when fsAsyncSupport:
  template preventFurtherReading(s: AsyncInputStream) =
    preventFurtherReading InputStream(s)

template makeHandle*(sp: InputStream): InputStreamHandle =
  let s = sp
  InputStreamHandle(s: s)

proc close*(s: InputStream,
            behavior = dontWaitAsyncClose)
           {.raises: [IOError, Defect].} =
  ## Closes the stream. Any resources associated with the stream
  ## will be released and no further reading will be possible.
  ##
  ## If the underlying input device requires asynchronous closing
  ## and `behavior` is set to `waitAsyncClose`, this proc will use
  ## `waitFor` to block until the async operation completes.
  s.disconnectInputDevice()
  s.preventFurtherReading()
  when fsAsyncSupport:
    if s.closeFut != nil:
      fsTranslateErrors "Stream closing failed":
        if behavior == waitAsyncClose:
          waitFor s.closeFut
        else:
          asyncCheck s.closeFut

when fsAsyncSupport:
  template close*(sp: AsyncInputStream) =
    ## Starts the asychronous closing of the stream and returns a future that
    ## tracks the closing operation.
    let s = InputStream sp
    disconnectInputDevice(s)
    preventFurtherReading(s)
    if s.closeFut != nil:
      fsAwait s.closeFut

template closeNoWait*(sp: MaybeAsyncInputStream) =
  ## Close the stream without waiting even if's async.
  ## This operation will use `asyncCheck` internally to detect unhandled
  ## errors from the closing operation.
  close(InputStream(s), dontWaitAsyncClose)

# TODO
# The destructors are currently disabled because they seem to cause
# mysterious segmentation faults related to corrupted GC internal
# data structures.
#[
proc `=destroy`*(h: var InputStreamHandle) {.raises: [Defect].} =
  if h.s != nil:
    if h.s.vtable != nil and h.s.vtable.closeSync != nil:
      try:
        h.s.vtable.closeSync(h.s)
      except IOError:
        # Since this is a destructor, there is not much we can do here.
        # If the user wanted to handle the error, they would have called
        # `close` manually.
        discard # TODO
    # TODO ATTENTION!
    # Uncommenting the following line will lead to a GC heap corruption.
    # Most likely this leads to Nim collecting some object prematurely.
    # h.s = nil
    # We work-around the problem through more indirect incapacitatation
    # of the stream object:
    h.s.preventFurtherReading()
]#

converter implicitDeref*(h: InputStreamHandle): InputStream =
  ## Any `InputStreamHandle` value can be implicitly converted to an
  ## `InputStream` or an `AsyncInputStream` value.
  h.s

template vtableAddr*(vtable: InputStreamVTable): ptr InputStreamVTable =
  # This is a simple work-around for the somewhat broken side
  # effects analysis of Nim - reading from global let variables
  # is considered a side-effect.
  {.noSideEffect.}:
    unsafeAddr vtable

let memFileInputVTable = InputStreamVTable(
  closeSync: proc (s: InputStream)
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
    try:
      close MemFileInputStream(s).file
    except OSError as err:
      raise newException(IOError, "Failed to close file", err)
  ,
  getLenSync: proc (s: InputStream): Option[Natural]
                   {.nimcall, gcsafe, raises: [IOError, Defect].} =
    some s.span.len
)

proc memFileInput*(filename: string, mappedSize = -1, offset = 0): InputStreamHandle
                  {.raises: [IOError].} =
  ## Creates an input stream for reading the contents of a memory-mapped file.
  ##
  ## Using this API will provide better performance than `fileInput`,
  ## but this comes at a cost of higher address space usage which may
  ## be problematic when working with extremely large files.
  ##
  ## All parameters are forwarded to Nim's memfiles.open function:
  ##
  ## ``filename``
  ##  The name of the file to read.
  ##
  ## ``mappedSize`` and ``offset``
  ##  can be used to map only a slice of the file.
  ##
  ## ``offset`` must be multiples of the PAGE SIZE of your OS
  ##  (usually 4K or 8K, but is unique to your OS)

  # Nim's memfiles module will fail to map an empty file,
  # but we don't consider this a problem. The stream will
  # be in non-readable state from the start.
  try:
    let fileSize = getFileSize(filename)
    if fileSize == 0:
      return makeHandle InputStream()

    let
      memFile = memfiles.open(filename,
                              mode = fmRead,
                              mappedSize = mappedSize,
                              offset = offset)
      head = cast[ptr byte](memFile.mem)
      mappedSize = memFile.size

    makeHandle MemFileInputStream(
      vtable: vtableAddr memFileInputVTable,
      span: PageSpan(
        startAddr: head,
        endAddr: offset(head, mappedSize)),
      spanEndPos: mappedSize,
      file: memFile)
  except OSError as err:
    raise newException(IOError, err.msg, err)

func getNewSpan(s: InputStream) =
  let buffers = s.buffers
  fsAssert buffers != nil
  s.span = buffers.obtainReadableSpan(s.spanEndPos)

func getNewSpanOrDieTrying(s: InputStream) =
  getNewSpan s
  fsAssert s.span.hasRunway

proc readableNow*(s: InputStream): bool =
  if s.span.hasRunway: return true
  getNewSpan s
  s.span.hasRunway

when fsAsyncSupport:
  template readableNow*(s: AsyncInputStream): bool =
    readableNow InputStream(s)

  proc readOnce*(sp: AsyncInputStream): Future[Natural] {.async.} =
    let s = InputStream(sp)
    fsAssert s.buffers != nil and s.vtable != nil

    result = fsAwait s.vtable.readAsync(s, nil, 0)

    if s.buffers.eofReached:
      disconnectInputDevice(s)

    if result > 0 and s.span.len == 0:
      getNewSpan s

  proc timeoutToNextByteImpl(s: AsyncInputStream,
                            deadline: Future): Future[bool] {.async.} =
    let readFut = s.readOnce
    fsAwait readFut or deadline
    if not readFut.finished:
      readFut.cancel()
      return true
    else:
      return false

  template timeoutToNextByte*(sp: AsyncInputStream, deadline: Future): bool =
    let s = sp
    if readableNow(s):
      true
    else:
      fsAwait timeoutToNextByteImpl(s, deadline)

  template timeoutToNextByte*(sp: AsyncInputStream, timeout: Duration): bool =
    let s = sp
    if readableNow(s):
      true
    else:
      fsAwait timeoutToNextByteImpl(s, sleepAsync(timeout))

  proc closeAsync*(s: AsyncInputStream) {.async.} =
    close s

  template totalUnconsumedBytes*(s: AsyncInputStream): Natural =
    ## Alias for InputStream.totalUnconsumedBytes
    totalUnconsumedBytes InputStream(s)

func getBestContiguousRunway(s: InputStream): Natural =
  result = s.span.len
  if result == 0 and s.buffers != nil:
    getNewSpan s
    result = s.span.len

func totalUnconsumedBytes*(s: InputStream): Natural =
  ## Returns the number of bytes that are currently sitting within the stream
  ## buffers and that can be consumed with `read` or `advance`.
  let
    localRunway = s.span.len
    runwayInBuffers = if s.buffers == nil:
                        0
                      elif s.buffers.fauxEofPos != 0:
                        s.buffers.fauxEofPos - s.spanEndPos
                      else:
                        s.buffers.totalBufferedBytes

  if localRunway == 0 and runwayInBuffers > 0:
    getNewSpan s

  localRunway + runwayInBuffers

proc limitReadableRange(s: InputStream, rangeLen: Natural): Natural =
  s.vtable = nil

  let runway = s.span.len
  if rangeLen > runway:
    fsAssert s.buffers != nil
    return s.buffers.setFauxEof(rangeLen - runway + s.spanEndPos)
  else:
    s.span.endAddr = offset(s.span.startAddr, rangeLen)
    let bytesToUnconsume = runway - rangeLen
    s.spanEndPos -= bytesToUnconsume
    if s.buffers != nil:
      s.buffers.queue.peekFirst.consumedTo -= bytesToUnconsume
      return s.buffers.setFauxEof(s.spanEndPos)

template withReadableRange*(sp: MaybeAsyncInputStream,
                            rangeLen: Natural,
                            rangeStreamVarName, blk: untyped) =
  let
    s = InputStream sp
    vtable = s.vtable
    origEndAddr = s.span.endAddr
    origEof = limitReadableRange(s, rangeLen)

  try:
    let rangeStreamVarName {.inject.} = s
    blk
  finally:
    s.vtable = vtable
    s.span.endAddr = origEndAddr
    if s.buffers != nil: restoreEof(s.buffers, origEof)

let fileInputVTable = InputStreamVTable(
  readSync: proc (s: InputStream, dst: pointer, dstLen: Natural): Natural
                 {.nimcall, gcsafe, raises: [IOError, Defect].} =
    let file = FileInputStream(s).file
    implementSingleRead(s.buffers, dst, dstLen,
                        {partialReadIsEof},
                        readStartAddr, readLen):
      file.readBuffer(readStartAddr, readLen)
  ,
  getLenSync: proc (s: InputStream): Option[Natural]
                   {.nimcall, gcsafe, raises: [IOError, Defect].} =
    let
      s = FileInputStream(s)
      runway = s.totalUnconsumedBytes

    let preservedPos = getFilePos(s.file)
    setFilePos(s.file, 0, fspEnd)
    let endPos = getFilePos(s.file)
    setFilePos(s.file, preservedPos)

    some Natural(endPos - preservedPos + runway)
  ,
  closeSync: proc (s: InputStream)
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
    try:
      close FileInputStream(s).file
    except OSError as err:
      raise newException(IOError, "Failed to close file", err)
)

proc fileInput*(file: File,
                offset = 0,
                pageSize = defaultPageSize): InputStreamHandle
               {.raises: [IOError, OSError].} =
  ## Creates an input stream for reading the contents of a file
  ## through Nim's `io` module.
  ##
  ## Parameters:
  ##
  ## ``file``
  ##  The file to read.
  ##
  ## ``offset``
  ##  Initial position in the file where reading should start.
  ##

  if offset != 0:
    setFilePos(file, offset)

  makeHandle FileInputStream(
    vtable: vtableAddr fileInputVTable,
    buffers: initPageBuffers(pageSize),
    file: file)

proc fileInput*(filename: string,
                offset = 0,
                pageSize = defaultPageSize): InputStreamHandle
               {.raises: [IOError, OSError].} =
  ## Creates an input stream for reading the contents of a file
  ## through Nim's `io` module.
  ##
  ## Parameters:
  ##
  ## ``filename``
  ##  The name of the file to read.
  ##
  ## ``offset``
  ##  Initial position in the file where reading should start.
  ##
  let file = system.open(filename, fmRead)
  return fileInput(file, offset, pageSize)

proc unsafeMemoryInput*(mem: openArray[byte]): InputStreamHandle =
  let head = cast[ptr byte](mem)

  makeHandle InputStream(
    span: PageSpan(
      startAddr: head,
      endAddr: offset(head, mem.len)),
    spanEndPos: mem.len)

proc unsafeMemoryInput*(str: string): InputStreamHandle =
  unsafeMemoryInput str.toOpenArrayByte(0, str.len - 1)

proc len*(s: InputStream): Option[Natural] {.raises: [Defect, IOError].} =
  if s.vtable == nil:
    some s.totalUnconsumedBytes
  elif s.vtable.getLenSync != nil:
    s.vtable.getLenSync(s)
  else:
    none Natural

when fsAsyncSupport:
  template len*(s: AsyncInputStream): Option[Natural] =
    len InputStream(s)

func memoryInput*(buffers: PageBuffers): InputStreamHandle =
  var spanEndPos = Natural 0
  var span = if buffers.len == 0: default(PageSpan)
             else: buffers.obtainReadableSpan(spanEndPos)

  makeHandle InputStream(buffers: buffers,
                         span: span,
                         spanEndPos: spanEndPos)

func memoryInput*(data: openArray[byte]): InputStreamHandle =
  let stream = if data.len > 0:
    let
      buffers = initPageBuffers(data.len)
      page = buffers.addWritablePage(data.len)
      pageSpan = page.fullSpan

    copyMem(pageSpan.startAddr, baseAddr(data), data.len)

    InputStream(buffers: buffers,
                span: pageSpan,
                spanEndPos: data.len)
  else:
    InputStream()

  makeHandle stream

func memoryInput*(data: openArray[char]): InputStreamHandle =
  memoryInput charsToBytes(data)

proc resetBuffers*(s: InputStream, buffers: PageBuffers) =
  # This should be used only on safe memory input streams
  fsAssert s.vtable == nil and s.buffers != nil and buffers.len > 0
  s.spanEndPos = 0
  s.buffers = buffers
  getNewSpan s

proc continueAfterRead(s: InputStream, bytesRead: Natural): bool =
  # Please note that this is extracted into a proc only to reduce the code
  # that ends up inlined into async procs by `bufferMoreDataImpl`.
  # The inlining itself is required to support the await-free operation of
  # the `readable` APIs.

  # The read might have been incomplete which signals the EOF of the stream.
  # If this is the case, we disconnect the input device which prevents any
  # further attempts to read from it:
  if s.buffers.eofReached:
    disconnectInputDevice(s)

  if bytesRead > 0:
    getNewSpan s
    return true
  else:
    return false

template bufferMoreDataImpl(s, awaiter, readOp: untyped): bool =
  # This template is always called when the current page has been
  # completely exhausted. It should produce `true` if more data was
  # successfully buffered, so reading can continue.
  #
  # The vtable will be `nil` for a memory stream and `vtable.readOp`
  # will be `nil` for a memFile. If we've reached here, this is the
  # end of the memory buffer, so we can signal EOF:
  if s.buffers == nil:
    false
  else:
    # There might be additional pages in our buffer queue. If so, we
    # just jump to the next one:
    getNewSpan s
    if hasRunway(s.span):
      true
    elif s.vtable != nil and s.vtable.readOp != nil:
      # We ask our input device to populate our page queue with newly
      # read pages. The state of the queue afterwards will tell us if
      # the read was successful. In `continueAfterRead`, we examine if
      # EOF was reached, but please note that some data might have been
      # read anyway:
      continueAfterRead(s, awaiter s.vtable.readOp(s, nil, 0))
    else:
      false

proc bufferMoreDataSync(s: InputStream): bool =
  # This proc exists only to avoid inlining of the code of
  # `bufferMoreDataImpl` into `readable` (which in turn is
  # a template inlined in the user code).
  bufferMoreDataImpl(s, noAwait, readSync)

template readable*(sp: InputStream): bool =
  ## Checks whether reading more data from the stream is possible.
  ##
  ## If there is any unconsumed data in the stream buffers, the
  ## operation returns `true` immediately. You can call `read`
  ## or `peek` afterwards to consume or examine the next byte
  ## in the stream.
  ##
  ## If the stream buffers are empty, the operation may block
  ## until more data becomes available. The end of the stream
  ## may be reached at this point, which will be indicated by
  ## a `false` return value. Any attempt to call `read` or
  ## `peek` afterwards is considered a `Defect`.
  ##
  ## Please note that this API is intended for stream consumers
  ## who need to consume the data one byte at a time. A typical
  ## usage will be the following:
  ##
  ## ```nim
  ## while stream.readable:
  ##   case stream.peek.char
  ##   of '"':
  ##     parseString(stream)
  ##   of '0'..'9':
  ##     parseNumber(stream)
  ##   of '\':
  ##     discard stream.read # skip the slash
  ##     let escapedChar = stream.read
  ## ```
  ##
  ## Even though the user code consumes the data one byte at a time,
  ## in the majority of cases this consist of simply incrementing a
  ## pointer within the stream buffers. Only when the stream buffers
  ## are exhausted, a new read operation will be executed throught
  ## the stream input device which may repopulate the buffers with
  ## fresh data. See `Stream Pages` for futher discussion of this.

  # This is a template, because we want the pointer check to be
  # inlined at the call sites. Only if it fails, we call into the
  # larger non-inlined proc:
  let s = sp
  hasRunway(s.span) or bufferMoreDataSync(s)

when fsAsyncSupport:
  template readable*(sp: AsyncInputStream): bool =
    ## Async version of `readable`.
    ## The intended API usage is the same. Instead of blocking, an async
    ## stream will use `await` while waiting for more data.
    let s = InputStream sp
    if hasRunway(s.span):
      true
    else:
      bufferMoreDataImpl(s, fsAwait, readAsync)

func continueAfterReadN(s: InputStream,
                        runwayBeforeRead, bytesRead: Natural) =
  if runwayBeforeRead == 0 and bytesRead > 0:
    getNewSpan s

template readableNImpl(s, n, awaiter, readOp: untyped): bool =
  let runway = totalUnconsumedBytes(s)
  if runway >= n:
    true
  elif s.buffers == nil or s.vtable == nil or s.vtable.readOp == nil:
    false
  else:
    var
      res = false
      bytesRead = Natural 0
      bytesDeficit = n - runway

    while true:
      bytesRead += awaiter s.vtable.readOp(s, nil, bytesDeficit)

      if s.buffers.eofReached:
        disconnectInputDevice(s)
        res = bytesRead >= bytesDeficit
        break

      if bytesRead >= bytesDeficit:
        res = true
        break

    continueAfterReadN(s, runway, bytesRead)
    res

proc readable*(s: InputStream, n: int): bool =
  ## Checks whether reading `n` bytes from the input stream is possible.
  ##
  ## If there is enough unconsumed data in the stream buffers, the
  ## operation will return `true` immediately. You can use `read`,
  ## `peek`, `read(n)` or `peek(n)` afterwards to consume up to the
  ## number of verified bytes. Please note that consuming more bytes
  ## will be considered a `Defect`.
  ##
  ## If the stream buffers do not contain enough data, the operation
  ## may block until more data becomes available. The end of the stream
  ## may be reached at this point, which will be indicated by a `false`
  ## return value. Please note that the stream might still contain some
  ## unconsumed bytes after `readable(n)` returned false. You can use
  ## `totalUnconsumedBytes` or a combination of `readable` and `read`
  ## to consume the remaining bytes if desired.
  ##
  ## If possible, prefer consuming the data one byte at a time. This
  ## ensures the most optimal usage of the stream buffers. Even after
  ## calling `readable(n)`, it's still preferrable to continue with
  ## `read` instead of `read(n)` because the later may require the
  ## resulting bytes to be copied to a freshly allocated sequence.
  ##
  ## In the situation where the consumed bytes need to be copied to
  ## an existing external buffer, `readInto` will provide the best
  ## performance instead.
  ##
  ## Just like `readable`, this operation will invoke reads on the
  ## stream input device only when necessary. See `Stream Pages`
  ## for futher discussion of this.
  readableNImpl(s, n, noAwait, readSync)

when fsAsyncSupport:
  template readable*(sp: AsyncInputStream, np: int): bool =
    ## Async version of `readable(n)`.
    ## The intended API usage is the same. Instead of blocking, an async
    ## stream will use `await` while waiting for more data.
    let
      s = InputStream sp
      n = np

    readableNImpl(s, n, fsAwait, readAsync)

template peek*(sp: InputStream): byte =
  let s = sp
  if hasRunway(s.span):
    s.span.startAddr[]
  else:
    getNewSpanOrDieTrying s
    s.span.startAddr[]

when fsAsyncSupport:
  template peek*(s: AsyncInputStream): byte =
    peek InputStream(s)

func readFromNewSpan(s: InputStream): byte =
  getNewSpanOrDieTrying s
  result = s.span.startAddr[]
  bumpPointer s.span

template read*(sp: InputStream): byte =
  let s = sp
  if hasRunway(s.span):
    let res = s.span.startAddr[]
    bumpPointer(s.span)
    res
  else:
    readFromNewSpan s

when fsAsyncSupport:
  template read*(s: AsyncInputStream): byte =
    read InputStream(s)

proc peekAt*(s: InputStream, pos: int): byte {.inline.} =
  # TODO implement page flipping
  let peekHead = offset(s.span.startAddr, pos)
  fsAssert cast[uint](peekHead) < cast[uint](s.span.endAddr)
  return peekHead[]

when fsAsyncSupport:
  template peekAt*(s: AsyncInputStream, pos: int): byte =
    peekAt InputStream(s), pos

proc advance*(s: InputStream) =
  if hasRunway(s.span):
    bumpPointer s.span
  else:
    getNewSpan s

proc advance*(s: InputStream, n: Natural) =
  # TODO This is silly, implement it properly
  for i in 0 ..< n:
    advance s

when fsAsyncSupport:
  template advance*(s: AsyncInputStream) =
    advance InputStream(s)

  template advance*(s: AsyncInputStream, n: Natural) =
    advance InputStream(s), n

proc drainBuffersInto*(s: InputStream, dstAddr: ptr byte, dstLen: Natural): Natural =
  var
    dst = dstAddr
    remainingBytes = dstLen
    runway = s.span.len

  if runway >= remainingBytes:
    copyMem(dst, s.span.startAddr, remainingBytes)
    s.span.bumpPointer remainingBytes
    return dstLen
  elif runway > 0:
    copyMem(dst, s.span.startAddr, runway)
    dst = offset(dst, runway)
    remainingBytes -= runway

  if s.buffers != nil:
    # Since we reached the end of the current page,
    # we have to do the equivalent of `getNewSpan`:

    if s.buffers.len > 0 and s.buffers[0].pageLen == 0:
      discard s.buffers.popFirst()

    for page in consumePages(s.buffers):
      let
        pageStart = page.readableStart
        pageLen = page.writtenTo - page.consumedTo

      # There are two possible scenarios ahead:
      # 1) We'll either stop at this page in which case our span will
      #    point to the end of the page (so, it's fully consumed)
      # 2) We are going to copy the entire page to the destination
      #    buffer and we'll continue (so, it's fully consumed again)
      page.consumedTo = page.writtenTo

      if pageLen > remainingBytes:
        # This page has enough data to fill the rest of the buffer:
        copyMem(dst, pageStart, remainingBytes)

        # This page is partially consumed now and we must set our
        # span to point to its remaining contents:
        s.span = PageSpan(startAddr: offset(pageStart, remainingBytes),
                          endAddr: page.readableEnd)

        # We also need to know how much our position in the stream
        # has advanced:
        let bytesDrainedFromBuffers = dstLen - runway
        s.spanEndPos += bytesDrainedFromBuffers + s.span.len

        # We return the length of the buffer, which means that is
        # has been fully populated:
        return dstLen
      else:
        copyMem(dst, pageStart, pageLen)
        remainingBytes -= pageLen
        dst = offset(dst, pageLen)

  # We've completerly drained the current span and all the buffers,
  # so we set the span to a pristine state that will trigger a new
  # read on the next interaction with the stream.
  s.span = default(PageSpan)

  # We failed to populate the entire buffer
  return dstLen - remainingBytes

template readIntoExImpl(s: InputStream,
                        dst: ptr byte, dstLen: Natural,
                        awaiter, readOp: untyped): Natural =
  let totalBytesDrained = drainBuffersInto(s, dst, dstLen)
  var bytesDeficit = (dstLen - totalBytesDrained)

  if bytesDeficit > 0 and s.vtable != nil:
    var adjustedDst = offset(dst, totalBytesDrained)

    while true:
      let newBytesRead = awaiter s.vtable.readOp(s, adjustedDst, bytesDeficit)

      s.spanEndPos += newBytesRead
      bytesDeficit -= newBytesRead

      if s.buffers.eofReached:
        disconnectInputDevice(s)
        break

      if bytesDeficit == 0:
        break

      adjustedDst = offset(adjustedDst, newBytesRead)

  dstLen - bytesDeficit

proc readIntoEx*(s: InputStream, dst: var openArray[byte]): int =
  ## Read data into the destination buffer.
  ##
  ## Returns the number of bytes that were successfully
  ## written to the buffer. The function will return a
  ## number smaller than the buffer length only if EOF
  ## was reached before the buffer was fully populated.
  let dstAddr = addr dst[0]
  let dstLen = dst.len
  readIntoExImpl(s, dstAddr, dstLen, noAwait, readSync)

proc readInto*(s: InputStream, target: var openArray[byte]): bool =
  ## Read data into the destination buffer.
  ##
  ## Returns `false` if EOF was reached before the buffer
  ## was fully populated. if you need precise information
  ## regarding the number of bytes read, see `readIntoEx`.
  s.readIntoEx(target) == target.len

when fsAsyncSupport:
  template readIntoEx*(sp: AsyncInputStream, dst: var openArray[byte]): int =
    let s = InputStream(sp)
    # BEWARE! `openArrayToPair` here is needed to avoid
    # double evaluation of the `dst` expression:
    let (dstAddr, dstLen) = openArrayToPair(dst)
    readIntoExImpl(s, dstAddr, dstLen, fsAwait, readAsync)

  template readInto*(sp: AsyncInputStream, dst: var openArray[byte]): bool =
    ## Asynchronously read data into the destination buffer.
    ##
    ## Returns `false` if EOF was reached before the buffer
    ## was fully populated. if you need precise information
    ## regarding the number of bytes read, see `readIntoEx`.
    ##
    ## If there are enough bytes already buffered by the stream,
    ## the expression will complete immediately.
    ## Otherwise, it will await more bytes to become available.

    let s = InputStream(sp)
    # BEWARE! `openArrayToPair` here is needed to avoid
    # double evaluation of the `dst` expression:
    let (dstAddr, dstLen) = openArrayToPair(dst)
    readIntoExImpl(s, dstAddr, dstLen, fsAwait, readAsync) == dstLen

template useHeapMem(_: Natural) =
  var buffer: seq[byte]

  template allocMem(n: Natural): ptr byte =
    buffer.setLen(n)
    addr buffer[0]

template useStackMem(n: static Natural) =
  var buffer: array[n + 1, byte]

  template allocMem(_: Natural): ptr byte =
    addr buffer[0]

template readNImpl(sp: InputStream,
                   np: Natural,
                   createAllocMemOp: untyped): openArray[byte] =
  let
    s = sp
    n = np
    runway = getBestContiguousRunway(s)

  # Since Nim currently doesn't allow the `makeOpenArray` calls bellow
  # to appear in different branches of an if statement, the code must
  # be written in this branch-free linear fashion. The `dataCopy` seq
  # may remain empty in the case where we use stack memory or return
  # an `openArray` from the existing span.
  var startAddr: ptr byte

  block:
    # This defines the `allocMem` operation used below.
    # See `useHeapMem` and `useStackMem` for the possible definitions.
    # We are creating a block scope in order to allow multiple usages
    # of `read` within a single scope (avoiding duplicate definitions
    # of `allocMem`)
    createAllocMemOp(np)

    if n > runway:
      startAddr = allocMem(n)
      let drained {.used.} = drainBuffersInto(s, startAddr, n)
      fsAssert drained == n
    else:
      startAddr = s.span.startAddr
      bumpPointer s.span, n

  makeOpenArray(startAddr, n)

template read*(sp: InputStream, np: static Natural): openArray[byte] =
  const n = np
  when n < maxStackUsage:
    readNImpl(sp, n, useStackMem)
  else:
    readNImpl(sp, n, useHeapMem)

template read*(s: InputStream, n: Natural): openArray[byte] =
  readNImpl(s, n, useHeapMem)

when fsAsyncSupport:
  template read*(s: AsyncInputStream, n: Natural): openArray[byte] =
    read InputStream(s), n

proc lookAheadMatch*(s: InputStream, data: openArray[byte]): bool =
  for i in 0 ..< data.len:
    if s.peekAt(i) != data[i]:
      return false

  return true

when fsAsyncSupport:
  template lookAheadMatch*(s: AsyncInputStream, data: openArray[byte]): bool =
    lookAheadMatch InputStream(s)

proc next*(s: InputStream): Option[byte] =
  if readable(s):
    result = some read(s)

when fsAsyncSupport:
  template next*(sp: AsyncInputStream): Option[byte] =
    let s = sp
    if readable(s):
      some read(s)
    else:
      none byte

proc pos*(s: InputStream): int {.inline.} =
  s.spanEndPos - s.span.len

when fsAsyncSupport:
  template pos*(s: AsyncInputStream): int =
    pos InputStream(s)
