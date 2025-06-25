import
  std/[deques, options],
  stew/ptrops,
  async_backend

export
  deques

type
  PageSpan* = object
    ## View into memory area backed by a Page, allowing efficient access.
    ##
    ## Unlike openArray, lifetime must be managed manually
    ## Similar to UncheckedArray, range checking is generally not performed.
    ##
    ## The end address points to the memory immediately after the last entry,
    ## meaning that when startAddr == endAddr, the span is empty.
    ## endAddr is used instead of length so that when advancing, only a single
    ## pointer needs to be updated.
    startAddr*, endAddr*: ptr byte

  Page* = object
    ## A Page is a contiguous fixed-size memory area for managing a stream of
    ## bytes.
    ##
    ## Each page is divided into input and output sequences - the output
    ## sequence is the part prepared for writing while the input is the
    ## part already written and waiting to be read.
    ##
    ## As data is written, the output sequence is moved to the input sequence
    ## by adjusting the `writtenTo` counter. Similarly, as data is read from the
    ## input sequence, `consumedTo` is updated to reflect the number of bytes read.
    ##
    ## A page may also be created to reserve a part of the output sequence for
    ## delayed writing, such as when a length prefix is written after producing
    ## the data. Such a page will have `reservedTo` set, and as long as such a
    ## marker exists, the following pages will not be made available for
    ## consuming.
    ##
    ## Data is typically read and written in batches represented by a PageSpan,
    ## where each batch can be bulk-processed efficiently.
    consumedTo*: int
      ## Number of bytes consumed from the input sequence
    writtenTo*: Natural
      ## Number of bytes written to the input sequence
    reservedTo*: Natural
      ## Number of bytes reserved for future writing
    data*: ref seq[byte]
      ## Memory backing the page - allocated once and never resized to maintain
      ## pointer stability. Multiple pages may share the same buffer, which
      ## specially happens when reserving parts of a page for delayed writing.

  PageRef* = ref Page

  PageBuffers* = ref object
    ## PageBuffers is a memory management structure designed for efficient
    ## buffering of streaming data. It divides the buffer into pages (blocks of
    ## memory), which are managed in a queue.
    ##
    ## This approach is suitable in scenarios where data arrives or is consumed
    ## in chunks of varying size, such as when decoding a network stream or file
    ## into structured data.
    ##
    ## Pages are kept in a deque. New pages are prepared as needed when writing,
    ## and fully consumed pages are removed after reading.
    ##
    ## Pages can also be reserved to delay the writing of a prefix while data
    ## is being buffered. In such cases, writing can continue but reading will
    ## be blocked until the reservation is committed.
    pageSize*: Natural
      ## Default size of freshly allocated pages - pages are typically of at
      ## least this size but may also be larger

    maxBufferedBytes*: Natural
      ## TODO: currently unusued

    queue*: Deque[PageRef]

    when fsAsyncSupport:
      waitingReader*: Future[void]
      waitingWriter*: Future[void]

    eofReached*: bool

const
  nimPageSize* = 4096
  nimAllocatorMetadataSize* = 32
    # TODO: Get this legally from the Nim allocator.
    # The goal is to make perfect page-aligned allocations
    # that go through a fast O(0) path in the allocator.
  defaultPageSize* = 4096 - nimAllocatorMetadataSize
  maxStackUsage* = 16384

when not declared(newSeqUninit): # nim 2.2+
  template newSeqUninit[T: byte](len: int): seq[byte] =
    newSeqUninitialized[byte](len)

when debugHelpers:
  proc describeBuffers*(context: static string, buffers: PageBuffers) =
    debugEcho context, " :: buffers"
    for page in buffers.queue:
      debugEcho " page ", page.data[][page.consumedTo ..<
                                      min(page.consumedTo + 16, page.writtenTo)]
      debugEcho "  len = ", page.data[].len
      debugEcho "  start = ", page.consumedTo
      debugEcho "  written to = ", page.writtenTo

  func contents*(buffers: PageBuffers): seq[byte] =
    for page in buffers.queue:
      result.add page.data[][page.consumedTo ..< page.writtenTo - 1]
else:
  template describeBuffers*(context: static string, buffers: PageBuffers) =
    discard

func openArrayToPair*(a: var openArray[byte]): (ptr byte, Natural) =
  if a.len > 0:
    (addr a[0], Natural(a.len))
  else:
    (nil, Natural(0))

template allocRef[T: not ref](x: T): ref T =
  let res = new type(x)
  res[] = x
  res

func nextAlignedSize*(minSize, pageSize: Natural): Natural =
  if pageSize == 0:
    minSize
  else:
    max(((minSize + pageSize - 1) div pageSize) * pageSize, pageSize)

# BEWARE! These templates violate the double evaluation
#         safety measures in order to produce better inlined
#         code. We are using a `var` type to make it harder
#         to accidentally misuse them.
template len*(span: var PageSpan): Natural =
  distance(span.startAddr, span.endAddr)

func `$`*(span: PageSpan): string =
  # avoid repr(ptr byte) since it can crash if the pointee is gone
  var span = span
  "[startAddr: " & repr(pointer(span.startAddr)) & ", len: " & $span.len & "]"

template atEnd*(span: var PageSpan): bool =
  span.startAddr == span.endAddr

template hasRunway*(span: var PageSpan): bool =
  not span.atEnd()

template advance*(span: var PageSpan, numberOfBytes: Natural = 1) =
  span.startAddr = offset(span.startAddr, numberOfBytes)

template split*(span: var PageSpan, pos: Natural): PageSpan =
  let other = PageSpan(startAddr: span.startAddr, endAddr: offset(span.startAddr, pos))
  span.advance(pos)
  other

template read*(span: var PageSpan): byte =
  let b = span.startAddr[]
  span.advance(1)
  b

func read*(span: var PageSpan, val: var openArray[byte]) {.inline.} =
  if val.len > 0: # avoid accessing addr val[0] when it's empty
    copyMem(addr val[0], span.startAddr, val.len)
    span.advance(val.len)

func write*(span: var PageSpan, val: openArray[byte]) {.inline.} =
  if val.len > 0: # avoid accessing addr val[0] when it's empty
    copyMem(span.startAddr, unsafeAddr val[0], val.len)
    span.advance(val.len)

template write*(span: var PageSpan, val: byte) =
  span.startAddr[] = val
  span.startAddr = offset(span.startAddr, 1)

template data*(span: var PageSpan): var openArray[byte] =
  makeOpenArray(span.startAddr, span.len())

template allocationStart*(page: PageRef): ptr byte =
  baseAddr page.data[]

func readableStart*(page: PageRef): ptr byte {.inline.} =
  offset(page.allocationStart(), page.consumedTo)

func readableEnd*(page: PageRef): ptr byte {.inline.} =
  offset(page.allocationStart(), page.writtenTo)

func reservedEnd*(page: PageRef): ptr byte {.inline.} =
  offset(page.allocationStart(), page.reservedTo)

template writableStart*(page: PageRef): ptr byte =
  readableEnd(page)

func allocationEnd*(page: PageRef): ptr byte {.inline.} =
  offset(page.allocationStart(), page.data[].len)

func reserved*(page: PageRef): bool {.inline.} =
  page.reservedTo > 0

func len*(page: PageRef): Natural {.inline} =
  ## The number of bytes that can be read from this page, ie what would be
  ## returned by `consume`.
  page.writtenTo - page.consumedTo

func capacity*(page: PageRef): Natural {.inline.} =
  ## The amount of bytes that can be written to this page, ie what would be
  ## returned by `prepare`.
  page.data[].len - page.writtenTo

template data*(pageParam: PageRef): openArray[byte] =
  ## Currnet input sequence, or what would be returned by `consume`
  let page = pageParam
  var baseAddr = cast[ptr UncheckedArray[byte]](allocationStart(page))
  toOpenArray(baseAddr, page.consumedTo, page.writtenTo - 1)

func prepare*(page: PageRef): PageSpan =
  ## Return a span representing the output sequence of this page, ie the
  ## of space available for writing, limited to `len` bytes.
  ##
  ## After writing data to the span, `commit` should be called with the number
  ## of bytes written (or the advanced span).
  ##
  ## `len` must not be greater than `page.capacity()`.
  let baseAddr = allocationStart(page)
  PageSpan(startAddr: offset(baseAddr, page.writtenTo),
           endAddr: offset(baseAddr, page.data[].len))

func prepare*(page: PageRef, len: Natural): PageSpan =
  ## Return a span representing the output sequence of this page, ie the
  ## of space available for writing, limited to `len` bytes.
  ##
  ## After writing data to the span, `commit` should be called with the number
  ## of bytes written (or the advanced span).
  ##
  ## `len` must not be greater than `page.capacity()`.
  fsAssert len <= page.capacity()
  let
    baseAddr = allocationStart(page)
    startAddr = offset(baseAddr, page.writtenTo)
    endAddr = offset(startAddr, len)
  PageSpan(startAddr: startAddr, endAddr: endAddr)

func contains*(page: PageRef, address: ptr byte): bool {.inline.} =
  let span = page.prepare()
  address >= span.startAddr and address <= span.endAddr

func commit*(page: PageRef) =
  ## Mark the span returned by `prepare` as committed, allowing it to be
  ## accessed by `consume`
  page.reservedTo = 0
  page.writtenTo = page.data[].len()

func commit*(page: PageRef, len: Natural) =
  ## Mark `len` prepared bytes as committed, allowing them to be accessed by
  ## `consume`. `len` may be fewer bytes than were returned by `prepare`.
  fsAssert len <= page.capacity()
  page.reservedTo = 0
  page.writtenTo += len

func consume*(page: PageRef, maxLen = Natural.high()): PageSpan =
  ## Consume up to `maxLen` bytes committed to this page returning the
  ## corresponding span. May return a span shorter than `maxLen`.
  ##
  ## The span remains valid until the next consume call.
  let
    startAddr = page.readableStart()
    bytes = min(page.len, maxLen)
    endAddr = offset(startAddr, bytes)

  page.consumedTo += bytes

  PageSpan(startAddr: startAddr, endAddr: endAddr)

func consume*(page: PageRef): PageSpan =
  ## Consume all bytes committed to this page returning the corresponding span.
  ##
  ## The span remains valid until the next consume call.

  page.consume(page.len)

func unconsume(page: PageRef, len: int) =
  ## Return bytes from the last `consume` call to the Page, making them available
  ## for reading again.
  fsAssert len < page.consumedTo, "cannot unconsume more bytes than were consumed"
  page.consumedTo -= len

func init*(_: type PageRef, data: ref seq[byte], pos: int): PageRef =
  fsAssert data != nil and data[].len > 0
  fsAssert pos <= data[].len
  PageRef(data: data, consumedTo: pos, writtenTo: pos)

func init*(_: type PageRef, pageSize: Natural): PageRef =
  fsAssert pageSize > 0
  PageRef(data: allocRef newSeqUninit[byte](pageSize))

func nextAlignedSize*(buffers: PageBuffers, len: Natural): Natural =
  nextAlignedSize(len, buffers.pageSize)

func capacity*(buffers: PageBuffers): int =
  if buffers.queue.len > 0:
    buffers.queue.peekLast.capacity()
  else:
    0

func prepare*(buffers: PageBuffers): PageSpan =
  ## Return the largest contiguous writable memory area currently available or
  ## allocate a new page if there is no space.
  ##
  ## `capacity` can be used to check how much space is available allocation-free
  ## before calling `prepare`.
  ##
  ## Calling `prepare` multiple times without a `commit` in between will result
  ## in the same span being returned.
  if buffers.queue.len() == 0 or buffers.queue.peekLast().capacity() == 0:
    buffers.queue.addLast PageRef.init(buffers.pageSize)

  buffers.queue.peekLast().prepare()

func prepare*(buffers: PageBuffers, minLen: Natural): PageSpan =
  ## Return a contiguous span of at least `minLen` bytes, switching to a
  ## new page if need be.
  ##
  ## The span returned by `prepare` remains valid until the next call to either
  ## `reserve` or `commit`.
  ##
  ## Calling prepare may result in space being wasted when the desired allocation
  ## does not not fit in the current page. Use `prepare()` to avoid this situation
  ## or call `prepare` with your best guess of the total maximum memory that will
  ## be needed.
  ##
  ## Calling `prepare` multiple times without a `commit` in between will result
  ## in the same span being returned.
  if buffers.queue.len() == 0 or buffers.queue.peekLast().capacity() < minLen:
      # Create a new buffer for the desired runway, ending the current buffer
      # potentially without using its entire space because existing write
      # cursors may point to the memory inside it.
      # In git history, one can find code that moves data from the existing page
      # data to the new buffer - this would be a good idea if it wasn't for the
      # fact that it would invalidate pointers to the previous buffer.

    buffers.queue.addLast PageRef.init(buffers.nextAlignedSize(minLen))

  buffers.queue.peekLast().prepare()

func reserve*(buffers: PageBuffers, len: Natural): PageSpan =
  ## Reserve a span in a page that is frozen for reading until the span is
  ## committed. The reserved span must be committed even if it's not written to,
  ## to release subsequent writes for reading.
  ##
  ## `reserve` is used to create a writeable area whose size and contents are
  ## unknown at the time of reserve but whose maximum size is bounded and
  ## reasonable.
  ##
  ## Calling `reserve` multiple times will result in a new span being returned
  ## every time - each reserved span must be committed separately.
  ##
  ## If len is 0, no reservation is made.
  if len == 0:
    # If the reservation is empty, we'd end up with overlapping pages in the
    # buffer which would make finding the span (to commit it) tricky
    return PageSpan()

  if buffers.queue.len() == 0 or buffers.queue.peekLast().capacity() < len:
    buffers.queue.addLast PageRef.init(nextAlignedSize(len, buffers.pageSize))

  let page = buffers.queue.peekLast()
  page.reservedTo = page.writtenTo + len

  # The next `prepare` / `reserve` will go into a fresh part
  buffers.queue.addLast PageRef.init(page.data, page.reservedTo)

  let
    startAddr = page.writableStart
    endAddr = offset(startAddr, len) # Same as reservedTo

  PageSpan(startAddr: startAddr, endAddr: endAddr)

func commit*(buffers: PageBuffers, len: Natural) =
  ## Commit `len` bytes from a span previously given by the last call to `prepare`.
  fsAssert buffers.queue.len > 0, "Must call prepare with at least `len` bytes"
  buffers.queue.peekLast().commit(len)

func commit*(buffers: PageBuffers, span: PageSpan) =
  ## Commit a span previously return by `prepare` or `reserve`, committing all
  ## bytes that were written to the span.
  ##
  ## Spans received from `reserve` may be committed in any order - the data will
  ## be made available to `consume` in `reserve` order as soon as all preceding
  ## reservations have been committed.
  fsAssert span.endAddr >= span.startAddr, "Buffer overrun when writing span"

  if span.startAddr == nil: # zero-length reservations
    return

  var span = span

  for ridx in 1..buffers.queue.len():
    let page = buffers.queue[^ridx]

    if span.startAddr in page:
      if ridx < buffers.queue.len():
        # A non-reserved page may share end address with the resered address of
        # the preceding page if the reservation exactly covers the end of a page
        # and no allocation has been done yet
        let page2 = buffers.queue[^(ridx + 1)]
        if page2.reservedTo > 0 and span.endAddr == page2.reservedEnd():
          page2.commit(distance(page2.writableStart(), span.startAddr))
          return

      page.commit(distance(page.writableStart(), span.startAddr))
      return

  fsAssert false, "Could not find page to commit"

func consumable*(buffers: PageBuffers): int =
  ## Total number of bytes ready to be consumed - it may take several calls to
  ## `consume` to get all of them!
  var len = 0
  for b in buffers.queue:
    if b.reservedTo > 0:
      break

    len += b.len
  len

func consume*(buffers: PageBuffers, maxLen = int.high()): PageSpan =
  ## Return a span representing up to `maxLen` bytes from a single page. The
  ## return span is valid until the next call to `consume`.
  ##
  ## Calling code should make no assumptions about the number of `consume` calls
  ## needed to consume a corresponding amount of commits - ie commits may be
  ## split and consolidated as optimizations and features are implemented.
  var page: PageRef
  while buffers.queue.len > 0:
    let tmp = buffers.queue.peekFirst()
    if tmp.reservedTo > 0: # Page has been reserved and is waiting for a commit
      break

    if tmp.len() > 0:
      page = tmp
      break

    discard buffers.queue.popFirst()

  if page == nil:
    PageSpan() # No ready pages
  else:
    page.consume(maxLen)

func unconsume*(buffers: PageBuffers, bytes: Natural) =
  ## Return bytes that were given by the previous call to `consume`
  fsAssert buffers.queue.len > 0
  buffers.queue.peekFirst.consumedTo -= bytes

func write*(buffers: PageBuffers, val: openArray[byte]) =
  if val.len > 0:
    var written = 0
    if buffers.capacity() > 0: # Use up whatever space is left in the current page
      var span = buffers.prepare()
      written = min(span.len, val.len)

      span.write(val.toOpenArray(0, written - 1))
      buffers.commit(written)

    if written < val.len: # Put the rest in a fresh page
      let remaining = val.len - written
      var span = buffers.prepare(remaining)
      span.write(val.toOpenArray(written, val.len - 1))
      buffers.commit(remaining)

func write*(buffers: PageBuffers, val: byte) =
  buffers.write([val])

func init*(_: type PageBuffers, pageSize: Natural, maxBufferedBytes: Natural = 0): PageBuffers =
  fsAssert pageSize > 0
  PageBuffers(pageSize: pageSize, maxBufferedBytes: maxBufferedBytes)

template pageBytes*(page: PageRef): var openArray[byte] {.deprecated: "data".} =
  page.data()

template pageChars*(page: PageRef): var openArray[char] {.deprecated: "data".} =
  var baseAddr = cast[ptr UncheckedArray[char]](allocationStart(page))
  toOpenArray(baseAddr, page.consumedTo, page.writtenTo - 1)

func pageLen*(page: PageRef): Natural {.deprecated: "len".} =
  page.writtenTo - page.consumedTo

func writableSpan*(page: PageRef): PageSpan {.deprecated: "prepare".} =
  page.prepare()

func fullSpan*(page: PageRef): PageSpan {.deprecated: "data or prepare".} =
  let baseAddr = page.allocationStart
  PageSpan(startAddr: baseAddr, endAddr: offset(baseAddr, page.data[].len))

template bumpPointer*(span: var PageSpan, numberOfBytes: Natural = 1) {.deprecated: "advance".}=
  span.advance(numberOfBytes)
template writeByte*(span: var PageSpan, val: byte) {.deprecated: "write".} =
  span.write(val)

func obtainReadableSpan*(buffers: PageBuffers, maxLen: Option[Natural]): PageSpan {.deprecated: "consume".} =
  buffers.consume(maxLen.get(int.high()))

func returnReadableSpan*(buffers: PageBuffers, bytes: Natural) {.deprecated: "unconsume".} =
  fsAssert buffers.queue.len > 0
  buffers.queue.peekFirst.consumedTo -= bytes

func initPageBuffers*(pageSize: Natural,
                      maxBufferedBytes: Natural = 0): PageBuffers {.deprecated: "init".} =
  if pageSize == 0:
    nil
  else:
    PageBuffers.init(pageSize, maxBufferedBytes)

func trackWrittenToEnd*(buffers: PageBuffers) {.deprecated: "commit".} =
  if buffers != nil and buffers.queue.len > 0:
    buffers.commit(buffers.queue.peekLast.capacity())

func trackWrittenTo*(buffers: PageBuffers, spanHeadPos: ptr byte) {.deprecated: "commit".} =
  # Compatibility hack relying on commit ignoring `endAddr` for now
  if buffers != nil and spanHeadPos != nil:
    buffers.commit(PageSpan(startAddr: spanHeadPos))

template allocWritablePage*(pageSize: Natural, writtenToParam: Natural = 0): auto {.deprecated: "PageRef.init".} =
  PageRef(data: allocRef newSeqUninit[byte](pageSize),
          writtenTo: writtenToParam)

func addWritablePage*(buffers: PageBuffers, pageSize: Natural): PageRef {.deprecated: "prepare".} =
  trackWrittenToEnd(buffers)
  result = allocWritablePage(pageSize)
  buffers.queue.addLast result

func getWritablePage*(buffers: PageBuffers,
                      preferredSize: Natural): PageRef {.deprecated: "prepare".} =
  if buffers.queue.len > 0:
    let lastPage = buffers.queue.peekLast
    if lastPage.writtenTo < lastPage.data[].len:
      return lastPage

  return addWritablePage(buffers, preferredSize)

func addWritablePage*(buffers: PageBuffers): PageRef {.deprecated: "prepare".} =
  buffers.addWritablePage(buffers.pageSize)

template getWritableSpan*(buffers: PageBuffers): PageSpan {.deprecated: "prepare".} =
  let page = getWritablePage(buffers, buffers.pageSize)
  writableSpan(page)

func appendUnbufferedWrite*(buffers: PageBuffers,
                            src: pointer, srcLen: Natural) {.deprecated: "write".} =
  if srcLen > 0:
    buffers.write(makeOpenArray(cast[ptr byte](src), srcLen))

func ensureRunway*(buffers: PageBuffers,
                   currentHeadPos: var PageSpan,
                   neededRunway: Natural) {.deprecated: "prepare".} =
  # End writing to the current buffer (if any) and create a new one of the given
  # length
  if currentHeadPos.startAddr != nil:
    buffers.commit(currentHeadPos)
  currentHeadPos = buffers.prepare(neededRunway)

  let page =
    if currentHeadPos.startAddr == nil:
      # This is a brand new stream, just like we recomend.
      buffers.addWritablePage(neededRunway)
    else:
      # Create a new buffer for the desired runway, ending the current buffer
      # potentially without using its entire space because existing write
      # cursors may point to the memory inside it.
      # In git history, one can find code that moves data from the existing page
      # data to the new buffer - this is not a bad idea in general but breaks
      # when write cursors are in play
      trackWrittenTo(buffers, currentHeadPos.startAddr)
      let page = allocWritablePage(neededRunway)
      buffers.queue.addLast page
      page

  currentHeadPos = page.fullSpan

template len*(buffers: PageBuffers): Natural =
  buffers.queue.len

func totalBufferedBytes*(buffers: PageBuffers): Natural {.deprecated: "consumable".} =
  for i in 0 ..< buffers.queue.len:
    result += buffers.queue[i].pageLen

func canAcceptWrite*(buffers: PageBuffers, writeSize: Natural): bool {.deprecated: "Unimplemented".} =
  true or # TODO Remove this line
  buffers.queue.len == 0 or
  buffers.maxBufferedBytes == 0 or
  buffers.totalBufferedBytes < buffers.maxBufferedBytes

template popFirst*(buffers: PageBuffers): PageRef =
  buffers.queue.popFirst

template `[]`*(buffers: PageBuffers, idx: Natural): PageRef =
  buffers.queue[idx]

func splitLastPageAt*(buffers: PageBuffers, address: ptr byte) {.deprecated: "reserve".} =
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
    if page.len > 0:
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

template charsToBytes*(chars: openArray[char]): untyped {.deprecated: "multiple evaluation!".} =
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
    var span = buffers.prepare(buffers.nextAlignedSize(readLenVar))

    readStartVar = span.startAddr
    readLenVar = span.len

    # readBlock may raise meaning that the prepared span might be get recycled
    # in a future read, even if readBlock wrote some data in it - we have no
    # way of knowing how much though!
    bytesRead = readBlock
    buffers.commit(bytesRead)

  if (bytesRead == 0 and zeroReadIsNotEof notin flags) or
     (partialReadIsEof in flags and bytesRead < readLenVar):
    buffers.eofReached = true

  bytesRead
