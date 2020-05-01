import
  deques,
  stew/[ptrops, ranges/ptr_arith],
  async_backend

type
  PageSpan* = object
    startAddr*, endAddr*: ptr byte

  Page* = object
    startOffset*: Natural
    endOffset*: Natural
    data*: ref string

  PageRef* = ref Page

  PageBuffers* = ref object
    pageSize*: Natural
    maxWriteSize*: Natural
    backPressureLimit*: Natural

    queue*: Deque[PageRef]
    getters: seq[Future[void]]
    putters: seq[Future[void]]

    eofReached: bool

    totalBytesRead*: Natural
    totalBytesWritten*: Natural

const
  nimPageSize* = 4096
  pageMetadataSize* = offsetof(Page, data)
  nimAllocatorMetadataSize* = 32
    # TODO: Get this legally from the Nim allocator.
    # The goal is to make perfect page-aligned allocations
    # that get fast O(0) treatment.
  defaultPageSize* = 4096 - (pageMetadataSize + nimAllocatorMetadataSize)
  maxStackUsage* = 16384

proc openArrayToPair*(a: var openarray[byte]): (ptr byte, Natural) =
  (addr a[0], Natural(a.len))

template pageBaseAddr*(page: PageRef): ptr byte =
  cast[ptr byte](addr page.data[][0])

func pageStartAddr*(page: PageRef): ptr byte =
  offset(cast[ptr byte](addr page.data[][0]), page.startOffset)

func pageEndAddr*(page: PageRef): ptr byte =
  offset(cast[ptr byte](addr page.data[][0]), page.endOffset)

func pageLen*(page: PageRef): Natural =
  page.endOffset - page.startOffset

template pageChars*(page: PageRef): untyped =
  let baseAddr = cast[ptr UncheckedArray[char]](pageBaseAddr(page))
  toOpenArray(baseAddr, page.startOffset, page.endOffset - 1)

func span*(page: PageRef, writable: static[bool] = false): PageSpan =
  let baseAddr = page.pageBaseAddr
  PageSpan(startAddr: offset(baseAddr, page.startOffset),
           endAddr: offset(baseAddr, when writable: page.data[].len
                                     else: page.endOffset))

template writableSpan*(page: PageRef): PageSpan =
  span(page, writable = true)

func initPageBuffers*(pageSize: Natural,
                      maxWriteSize = high(int)): PageBuffers =
  if pageSize > 0:
    return PageBuffers(pageSize: pageSize,
                       maxWriteSize: maxWriteSize)

template allocRef[T: not ref](x: T): ref T =
  let res = new type(x)
  res[] = x
  res

func addWritablePage*(buffers: PageBuffers, pageSize: Natural): PageRef =
  result = PageRef(data: allocRef newString(pageSize),
                   endOffset: pageSize)
  buffers.queue.addLast result

func getWritablePage*(buffers: PageBuffers,
                      preferredSize: Natural): PageRef =
  if buffers.queue.len == 1:
    let recycledPage = buffers.queue.peekLast
    if recycledPage.endOffset == 0 and
       recycledPage.data[].len == preferredSize:
      recycledPage.endOffset = recycledPage.data[].len
      return recycledPage

  return addWritablePage(buffers, preferredSize)

func addWritablePage*(buffers: PageBuffers): PageRef =
  buffers.addWritablePage(buffers.pageSize)

template getWritableSpan*(buffers: PageBuffers): PageSpan =
  getWritablePage(buffers, buffers.pageSize).span(writable = true)

proc getReadableSpan*(buffers: PageBuffers): PageSpan =
  if buffers.queue.len > 1:
    discard buffers.queue.popFirst

  buffers.queue[0].span

func ensureRunway*(buffers: PageBuffers, neededRunway: Natural): PageSpan =
  doAssert buffers.queue.len == 0
  buffers.pageSize = neededRunway
  getWritableSpan(buffers)

template len*(buffers: PageBuffers): Natural =
  buffers.queue.len

func totalBufferredBytes*(buffers: PageBuffers): Natural =
  for i in 1 ..< buffers.queue.len:
    result += buffers.queue[i].pageLen

template popFirst*(buffers: PageBuffers): PageRef =
  buffers.queue.popFirst

template `[]`*(buffers: PageBuffers, idx: Natural): PageRef =
  buffers.queue[idx]

func splitLastPageAt*(buffers: PageBuffers, address: ptr byte) =
  var
    topPage = buffers.queue.peekLast
    newPage = PageRef()
    splitPosition = distance(topPage.pageBaseAddr, address)

  newPage[] = topPage[]
  topPage.endOffset = splitPosition
  newPage.startOffset = splitPosition

  buffers.queue.addLast newPage

func endLastPageAt*(buffers: PageBuffers, address: ptr byte) =
  if buffers != nil and buffers.queue.len > 0:
    var topPage = buffers.queue.peekLast
    topPage.endOffset = distance(topPage.pageBaseAddr, address)

func nextAlignedSize*(minSize, pageSize: Natural): Natural =
  # TODO: This is not perfectly accurate. Revisit later
  ((minSize div pageSize) + 1) * pageSize

iterator consumePages*(buffers: PageBuffers): PageRef =
  doAssert buffers != nil

  var recycledPage: PageRef
  while buffers.queue.len > 0:
    var page = peekFirst(buffers.queue)

    # TODO: what if the body throws an exception?
    # Should we do anything with the consumed page?
    yield page

    if page.data[].len == buffers.pageSize:
      recycledPage = page

    discard buffers.queue.popFirst

  if recycledPage != nil:
    recycledPage.startOffset = 0
    recycledPage.endOffset = 0
    buffers.queue.addLast recycledPage

iterator consumePageBuffers*(buffers: PageBuffers): (ptr byte, Natural) =
  for page in consumePages(buffers):
    yield (page.pageStartAddr,
           Natural(page.endOffset - page.startOffset))

template wasEofReached*(buffers: PageBuffers): bool =
  buffers.eofReached

# BEWARE! These templates violate the double evaluation
#         safety measures in order to produce better inlined
#         code. We are using a `var` type to make it harder
#         to accidentally misuse them.
template len*(span: var PageSpan): Natural =
  distance(span.startAddr, span.endAddr)

template atEnd*(span: var PageSpan): bool =
  span.startAddr == span.endAddr

template hasRunway*(span: var PageSpan): bool =
  span.startAddr != span.endAddr

template bumpPointer*(span: var PageSpan, numberOfBytes: Natural = 1) =
  span.startAddr = offset(span.startAddr, numberOfBytes)

template writeByte*(span: var PageSpan, val: byte) =
  span.startAddr[] = val
  span.startAddr = offset(span.startAddr, 1)

template charsToBytes*(chars: openArray[char]): untyped =
  bind makeOpenArray
  var charsStart = unsafeAddr chars[0]
  makeOpenArray(cast[ptr byte](charsStart), chars.len)

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
      buffers.totalBytesWritten += bytesWritten

  if srcLen > 0:
    doAssert src != nil
    let bytesWritten = writeBlock
    if bytesWritten != writeLenVar: raiseError()
    # TODO: Fix this after removing the unbuffered streams
    if buffers != nil:
      buffers.totalBytesWritten += bytesWritten

type
  ReadFlag* = enum
    partialReadIsEof
    zeroReadIsNotEof

  ReadFlags* = set[ReadFlag]

template implementSingleRead*(buffersParam: PageBuffers,
                              dstParam: pointer,
                              dstLenParam: Natural,
                              flags: static ReadFlags,
                              readStartVar, readLenVar,
                              readBlock: untyped): Natural =
  var
    buffers = buffersParam
    readStartVar = dstParam
    readLenVar = dstLenParam
    bytesRead: Natural

  if readStartVar != nil:
    bytesRead = readBlock
  else:
    let
      bestPageSize = nextAlignedSize(readLenVar, buffers.pageSize)
      page = getWritablePage(buffers, bestPageSize)

    readStartVar = page.pageStartAddr
    readLenVar = page.endOffset - page.startOffset

    # TODO: what if we exit with an exception here?
    # Are the side-effects of `getWritablePage` above OK to keep?
    bytesRead = readBlock
    page.endOffset = page.startOffset + bytesRead

  if (bytesRead == 0 and zeroReadIsNotEof notin flags) or
     (partialReadIsEof in flags and bytesRead < readLenVar):
    buffers.eofReached = true
  else:
    buffers.totalBytesRead += bytesRead

  bytesRead

