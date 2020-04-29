import
  deques,
  stew/[ptrops, ranges/ptr_arith],
  async_backend

type
  PageKind* = enum
    userPage
    stringPage
    mallocPage

  PageSpan* = object
    startAddr*, endAddr*: ptr byte

  Page* = object
    startOffset*: Natural
    endOffset*: Natural
    case kind*: PageKind
    of userPage, mallocPage:
      bufferStart, bufferEnd: ptr byte
    of stringPage:
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

func pageBaseAddr*(page: PageRef): ptr byte =
  if page.kind == stringPage:
    cast[ptr byte](addr page.data[][0])
  else:
    page.bufferStart

func pageStartAddr*(page: PageRef): ptr byte =
  if page.kind == stringPage:
    offset(cast[ptr byte](addr page.data[][0]), page.startOffset)
  else:
    offset(page.bufferStart, page.startOffset)

func pageEndAddr*(page: PageRef): ptr byte =
  if page.kind == stringPage:
    offset(cast[ptr byte](addr page.data[][0]), page.endOffset)
  else:
    offset(page.bufferStart, page.endOffset)

template pageChars*(page: PageRef): untyped =
  let baseAddr = cast[ptr UncheckedArray[char]](pageBaseAddr(page))
  toOpenArray(baseAddr, page.startOffset, page.endOffset - 1)

func span*(page: PageRef, writable: static[bool] = false): PageSpan =
  if page.kind == stringPage:
    let baseAddr = cast[ptr byte](addr page.data[][0])
    PageSpan(startAddr: offset(baseAddr, page.startOffset),
             endAddr: offset(baseAddr, when writable: page.data[].len
                                       else: page.endOffset))
  else:
    PageSpan(startAddr: offset(page.bufferStart, page.startOffset),
             endAddr: when writable: page.bufferEnd
                      else: offset(page.bufferStart, page.endOffset))

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

func getWritablePage*(buffers: PageBuffers): PageRef =
  # TODO: The semantics of this func are quite unusual
  #       I should find a more appropriate name
  if buffers.queue.len == 0:
    result = PageRef(kind: stringPage,
                     data: allocRef newString(buffers.pageSize),
                     endOffset: buffers.pageSize)
    buffers.queue.addLast result
  else:
    result = buffers.queue[0]

func addWritablePage*(buffers: PageBuffers, pageSize: Natural): PageRef =
  result = PageRef(kind: stringPage,
                   data: allocRef newString(pageSize),
                   endOffset: pageSize)
  buffers.queue.addLast result

func addWritablePage*(buffers: PageBuffers): PageRef =
  buffers.addWritablePage(buffers.pageSize)

template getWritableSpan*(buffers: PageBuffers): PageSpan =
  getWritablePage(buffers).span(writable = true)

func ensureRunway*(buffers: PageBuffers, neededRunway: Natural): PageSpan =
  doAssert buffers.queue.len == 0
  buffers.pageSize = neededRunway
  getWritableSpan(buffers)

template len*(buffers: PageBuffers): int =
  buffers.queue.len

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

func trackPageWrite*(page: PageRef, bytesWritten: Natural) {.inline.} =
  page.endOffset = page.startOffset + bytesWritten

template writeToSpan*(buffersParam: PageBuffers,
                      spanVarName, writeExpr: untyped) =
  var
    buffers = buffersParam
    page = buffers.getWritablePage
    spanVarName = page.writableSpan

  # TODO: what if we exit with an exception here?
  # Are the side-effects of `getWritablePage` above OK to keep?

  let bytesWritten = writeExpr
  trackPageWrite(page, bytesWritten)

  if bytesWritten == 0:
    buffers.eofReached = true

func nextAlignedSize*(minSize, pageSize: Natural): Natural =
  # TODO: This is not perfectly accurate. Revisit later
  ((minSize div pageSize) + 1) * pageSize

template consumeAllPages*(buffersParam: PageBuffers,
                          pageAddrVar, pageLenVar, body: untyped) =
  let buffers = buffersParam
  doAssert buffers != nil

  var recycledPage: PageRef
  for page in buffers.queue:
    let
      pageAddrVar = page.pageStartAddr
      pageLenVar = page.endOffset - page.startOffset

    if page.kind == stringPage and page.data[].len == buffers.pageSize:
      recycledPage = page

    # TODO: what if the body throws an exception?
    # Should we do anything with the remaining pages?
    body

  buffers.queue.clear()

  if recycledPage != nil:
    recycledPage.startOffset = 0
    recycledPage.endOffset = 0
    buffers.queue.addLast recycledPage

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

