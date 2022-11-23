import
  deques,
  stew/ptrops,
  async_backend

export
  deques

type
  PageSpan* = object
    startAddr*, endAddr*: ptr byte

  Page* = object
    consumedTo*: int
    writtenTo*: Natural
    data*: ref string

  PageRef* = ref Page

  PageBuffers* = ref object
    pageSize*: Natural
    maxBufferedBytes*: Natural

    queue*: Deque[PageRef]
    when fsAsyncSupport:
      waitingReader*: Future[void]
      waitingWriter*: Future[void]

    eofReached*: bool
    fauxEofPos*: Natural

const
  nimPageSize* = 4096
  nimAllocatorMetadataSize* = 32
    # TODO: Get this legally from the Nim allocator.
    # The goal is to make perfect page-aligned allocations
    # that go through a fast O(0) path in the allocator.
  defaultPageSize* = 4096 - nimAllocatorMetadataSize
  maxStackUsage* = 16384

when debugHelpers:
  proc describeBuffers*(context: static string, buffers: PageBuffers) =
    debugEcho context, " :: buffers"
    for page in buffers.queue:
      debugEcho " page ", page.data[][page.consumedTo ..<
                                      min(page.consumedTo + 16, page.writtenTo)]
      debugEcho "  len = ", page.data[].len
      debugEcho "  start = ", page.consumedTo
      debugEcho "  written to = ", page.writtenTo

  func contents*(buffers: PageBuffers): string =
    for page in buffers.queue:
      result.add page.data[][page.consumedTo ..< page.writtenTo - 1]
else:
  template describeBuffers*(context: static string, buffers: PageBuffers) =
    discard

func openArrayToPair*(a: var openArray[byte]): (ptr byte, Natural) =
  (addr a[0], Natural(a.len))

template allocationStart*(page: PageRef): ptr byte =
  cast[ptr byte](addr page.data[][0])

func readableStart*(page: PageRef): ptr byte =
  offset(cast[ptr byte](addr page.data[][0]), page.consumedTo)

func readableEnd*(page: PageRef): ptr byte =
  offset(cast[ptr byte](addr page.data[][0]), page.writtenTo)

template writableStart*(page: PageRef): ptr byte =
  readableEnd(page)

func allocationEnd*(page: PageRef): ptr byte =
  offset(cast[ptr byte](addr page.data[][0]), page.data[].len)

func pageLen*(page: PageRef): Natural =
  page.writtenTo - page.consumedTo

template pageChars*(page: PageRef): untyped =
  let baseAddr = cast[ptr UncheckedArray[char]](allocationStart(page))
  toOpenArray(baseAddr, page.consumedTo, page.writtenTo - 1)

func obtainReadableSpan*(buffers: PageBuffers,
                         currentSpanEndPos: var Natural): PageSpan =
  if buffers.queue.len == 0:
    return default(PageSpan)

  var page = buffers.queue[0]
  var unconsumedLen = page.writtenTo - page.consumedTo
  if unconsumedLen == 0:
    if buffers.queue.len > 1:
      discard buffers.queue.popFirst
      page = buffers.queue[0]
      unconsumedLen = page.writtenTo - page.consumedTo
    else:
      return default(PageSpan)

  let
    baseAddr = page.allocationStart
    startAddr = offset(baseAddr, page.consumedTo)

  let usableLen = if buffers.fauxEofPos != 0:
    let maxSize = buffers.fauxEofPos - currentSpanEndPos
    if maxSize < unconsumedLen:
      maxSize
    else:
      unconsumedLen
  else:
    unconsumedLen

  page.consumedTo += usableLen
  currentSpanEndPos += usableLen

  PageSpan(startAddr: startAddr,
           endAddr: offset(startAddr, usableLen))

func writableSpan*(page: PageRef): PageSpan =
  let baseAddr = allocationStart(page)
  PageSpan(startAddr: offset(baseAddr, page.writtenTo),
           endAddr: offset(baseAddr, page.data[].len))

func fullSpan*(page: PageRef): PageSpan =
  let baseAddr = page.allocationStart
  PageSpan(startAddr: baseAddr, endAddr: offset(baseAddr, page.data[].len))

func initPageBuffers*(pageSize: Natural,
                      maxBufferedBytes: Natural = 0): PageBuffers =
  # TODO: remove the unbuferred streams
  if pageSize > 0:
    return PageBuffers(pageSize: pageSize,
                       maxBufferedBytes: maxBufferedBytes)

template allocRef[T: not ref](x: T): ref T =
  let res = new type(x)
  res[] = x
  res

func trackWrittenToEnd*(buffers: PageBuffers) =
  if buffers.queue.len > 0:
    let page = buffers.queue.peekLast
    page.writtenTo = page.data[].len

func trackWrittenTo*(buffers: PageBuffers, spanHeadPos: ptr byte) =
  if buffers != nil and buffers.queue.len > 0:
    var topPage = buffers.queue.peekLast
    topPage.writtenTo = distance(topPage.allocationStart, spanHeadPos)

proc setFauxEof*(buffers: PageBuffers, pos: Natural): Natural =
  result = buffers.fauxEofPos
  buffers.fauxEofPos = pos

proc restoreEof*(buffers: PageBuffers, pos: Natural) =
  buffers.fauxEofPos = pos

template allocWritablePage*(pageSize: Natural, writtenToParam: Natural = 0): auto =
  PageRef(data: allocRef newString(pageSize),
          writtenTo: writtenToParam)

func addWritablePage*(buffers: PageBuffers, pageSize: Natural): PageRef =
  trackWrittenToEnd(buffers)
  result = allocWritablePage(pageSize)
  buffers.queue.addLast result

func getWritablePage*(buffers: PageBuffers,
                      preferredSize: Natural): PageRef =
  if buffers.queue.len > 0:
    let lastPage = buffers.queue.peekLast
    if lastPage.writtenTo < lastPage.data[].len:
      return lastPage

  return addWritablePage(buffers, preferredSize)

func addWritablePage*(buffers: PageBuffers): PageRef =
  buffers.addWritablePage(buffers.pageSize)

template getWritableSpan*(buffers: PageBuffers): PageSpan =
  let page = getWritablePage(buffers, buffers.pageSize)
  writableSpan(page)

func stringFromBytes(src: pointer, srcLen: Natural): string =
  result = newString(srcLen)
  copyMem(addr result[0], src, srcLen)

func nextAlignedSize*(minSize, pageSize: Natural): Natural =
  # TODO: This is not perfectly accurate. Revisit later
  ((minSize div pageSize) + 1) * pageSize

func appendUnbufferedWrite*(buffers: PageBuffers,
                            src: pointer, srcLen: Natural) =
  if buffers.queue.len == 0:
    buffers.queue.addLast PageRef(
      data: allocRef stringFromBytes(src, srcLen),
      writtenTo: srcLen)
  else:
    var
      src = src
      srcLen = srcLen
      lastPage = buffers.queue.peekLast
      lastPageLen = lastPage.data[].len
      unusedBytes = lastPageLen - lastPage.writtenTo

    if unusedBytes > 0:
      let unusedBytesStart = offset(addr lastPage.data[0], lastPage.writtenTo)
      if unusedBytes >= srcLen:
        copyMem(unusedBytesStart, src, srcLen)
        lastPage.writtenTo += srcLen
        return
      else:
        copyMem(unusedBytesStart, src, unusedBytes)
        lastPage.writtenTo = lastPageLen
        src = offset(src, unusedBytes)
        srcLen -= unusedBytes

    let nextPageSize = nextAlignedSize(srcLen, buffers.pageSize)
    let nextPage = buffers.addWritablePage(nextPageSize)

    copyMem(addr nextPage.data[0], src, srcLen)
    nextPage.writtenTo = srcLen

template hasDelayedWritesAtPageStart(page: PageRef): bool =
  page.consumedTo < 0

func ensureRunway*(buffers: PageBuffers,
                   currentHeadPos: var PageSpan,
                   neededRunway: Natural) =
  if currentHeadPos.startAddr == nil:
    # This is a brand new stream, just like we recomend.
    let page = buffers.addWritablePage(neededRunway)
    currentHeadPos = page.fullSpan
  else:
    # This is a more complicated path that should almost never
    # trigger in practice in a typically implemented code that
    # calls `ensureRunway` at the beggining of a transformation.
    fsAssert buffers.queue.len > 0
    let currPage = buffers.queue.peekLast

    if currPage.hasDelayedWritesAtPageStart:
      # There is not much we can do here. The outstanding cursors
      # may point to the current page. We won't honor the runway
      # request.
      return

    let
      oldData = currPage.data
      bytesWrittenToCurrPage = distance(currPage.readableStart,
                                        currentHeadPos.startAddr)
      replacementPageSize = neededRunway + bytesWrittenToCurrPage

    currPage.data = allocRef newString(replacementPageSize)
    currPage.consumedTo = bytesWrittenToCurrPage
    currPage.writtenTo = bytesWrittenToCurrPage

    # We copy the old data over the new page
    copyMem(addr currPage.data[][0], addr oldData[][0],
            bytesWrittenToCurrPage)

    currentHeadPos = currPage.writableSpan

template len*(buffers: PageBuffers): Natural =
  buffers.queue.len

func totalBufferedBytes*(buffers: PageBuffers): Natural =
  for i in 0 ..< buffers.queue.len:
    result += buffers.queue[i].pageLen

func canAcceptWrite*(buffers: PageBuffers, writeSize: Natural): bool =
  true or # TODO Remove this line
  buffers.queue.len == 0 or
  buffers.maxBufferedBytes == 0 or
  buffers.totalBufferedBytes < buffers.maxBufferedBytes

template popFirst*(buffers: PageBuffers): PageRef =
  buffers.queue.popFirst

template `[]`*(buffers: PageBuffers, idx: Natural): PageRef =
  buffers.queue[idx]

func splitLastPageAt*(buffers: PageBuffers, address: ptr byte) =
  var
    topPage = buffers.queue.peekLast
    splitPosition = distance(topPage.allocationStart, address)
    newPage = PageRef(
      data: topPage.data,
      consumedTo: splitPosition,
      writtenTo: splitPosition)

  topPage.writtenTo = splitPosition
  buffers.queue.addLast newPage

iterator consumePages*(buffers: PageBuffers): PageRef =
  fsAssert buffers != nil

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
    recycledPage.consumedTo = 0
    recycledPage.writtenTo = 0
    buffers.queue.addLast recycledPage

iterator consumePageBuffers*(buffers: PageBuffers): (ptr byte, Natural) =
  for page in consumePages(buffers):
    yield (page.readableStart,
           Natural(page.writtenTo - page.consumedTo))

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
  chars.toOpenArrayByte(0, chars.len - 1)

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

    readStartVar = writableStart(page)
    readLenVar = page.data[].len - page.writtenTo

    # TODO: what if we exit with an exception here?
    # Are the side-effects of `getWritablePage` above OK to keep?
    bytesRead = readBlock
    page.writtenTo += bytesRead

  if (bytesRead == 0 and zeroReadIsNotEof notin flags) or
     (partialReadIsEof in flags and bytesRead < readLenVar):
    buffers.eofReached = true

  bytesRead

