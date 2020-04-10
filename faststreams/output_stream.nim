import
  deques, typetraits,
  stew/[ptrops, strings, ranges/ptr_arith]

type
  OutputPage = object
    buffer: string
    startOffset: int

  # We inherit from RootObj because in layered streams
  # the stream itself is often used as an `outputDevice`
  OutputStreamObj = object of RootObj
    cursor*: WriteCursor
    pages: Deque[OutputPage]
    endPos: int
    extCursorsCount: int
    pageSize: int
    maxWriteSize*: int
    minWriteSize*: int
    outputDevice*: RootRef
    vtable*: ptr OutputStreamVTable

  OutputStream* = ref OutputStreamObj

  WritePageProc* = proc (s: OutputStream, page: openarray[byte])
                        {.nimcall, gcsafe, raises: [IOError, Defect].}

  FlushProc* = proc (s: OutputStream)
                    {.nimcall, gcsafe, raises: [IOError, Defect].}

  OutputStreamVTable* = object
    writePage*: WritePageProc
    flush*: FlushProc

  WriteCursor* = object
    head, bufferEnd: ptr byte
    stream: OutputStream

  VarSizeWriteCursor* = distinct WriteCursor

  FileOutput = ref object of RootObj
    file: File

const
  nimAllocatorMetadataSize* = 0
    # TODO: Get this from Nim's allocator.
    # The goal is to make perfect page-aligned allocations
  defaultPageSize = 4096 - nimAllocatorMetadataSize - 1 # 1 byte for the null terminator

proc createWriteCursor*[R, T](x: var array[R, T]): WriteCursor =
  let startAddr = cast[ptr byte](addr x[0])
  WriteCursor(head: startAddr, bufferEnd: offset(startAddr, sizeof x))

template canExtendOutput(s: OutputStream): bool =
  # Streams writing to pre-allocated existing buffers cannot be grown
  s != nil and s.pageSize > 0

template isExternalCursor(c: var WriteCursor): bool =
  # Is this the original stream cursor or is it one created by a "delayed write"
  addr(c) != addr(c.stream.cursor)

func runway*(c: var WriteCursor): int {.inline.} =
  distance(c.head, c.bufferEnd)

proc flipPage(s: OutputStream) =
  s.cursor.head = cast[ptr byte](addr s.pages[s.pages.len - 1].buffer[0])
  # TODO: There is an assumption here and elsewhere that `s.pages[^1]` has
  # a length equal to `s.pageSize`
  s.cursor.bufferEnd = cast[ptr byte](offset(s.cursor.head, s.pageSize))
  s.endPos += s.pageSize

proc addPage(s: OutputStream) =
  s.pages.addLast OutputPage(buffer: newString(s.pageSize),
                             startOffset: 0)
  s.flipPage

proc implementOutputStream*(outputDevice: RootRef,
                            vtable: ptr OutputStreamVTable,
                            pageSize = defaultPageSize,
                            maxWriteSize = high(int),
                            minWriteSize = 1): OutputStream =
  ## This proc is intented for use by module that implement
  ## their own flavours of OutputStream by providing a custom
  ## VTable. Such modules should export easier to use high-level
  ## constructors intended for end-users.
  result = OutputStream(
    pageSize: pageSize,
    maxWriteSize: maxWriteSize,
    minWriteSize: minWriteSize,
    pages: initDeque[OutputPage](),
    vtable: vtable,
    outputDevice: outputDevice)

  result.addPage
  result.cursor.stream = result

proc memoryOutput*(pageSize = defaultPageSize): OutputStream =
  implementOutputStream(outputDevice = nil, vtable = nil, pageSize = pageSize)

proc memoryOutput*(buffer: pointer, len: int): OutputStream =
  result = OutputStream()
  let buffer = cast[ptr byte](buffer)
  result.cursor.head = buffer
  result.cursor.bufferEnd = offset(buffer, len)
  result.cursor.stream = result
  result.endPos = len

let FileStreamVTable = OutputStreamVTable(
  writePage: proc (s: OutputStream, data: openarray[byte]) {.nimcall, gcsafe.} =
    var output = FileOutput(s.outputDevice)
    var written = output.file.writeBuffer(unsafeAddr data[0], data.len)
    if written != data.len:
      raise newException(IOError, "Failed to write OutputStream page.")
  ,
  flush: proc (s: OutputStream) {.nimcall, gcsafe.} =
    var output = FileOutput(s.outputDevice)
    flushFile output.file
)

template vtableAddr*(vtable: OutputStreamVTable): ptr OutputStreamVTable =
  ## This is a simple work-around for the somewhat broken side
  ## effects analysis of Nim - reading from global let variables
  ## is considered a side-effect.
  {.noSideEffect.}:
    unsafeAddr vtable

proc fileOutput*(filename: string,
                 fileMode: FileMode = fmWrite,
                 pageSize = defaultPageSize): OutputStream {.
  raises: [IOError, Defect]
.} =
  implementOutputStream FileOutput(file: open(filename, fileMode)),
                        vtable = vtableAddr FileStreamVTable,
                        pageSize = pageSize

proc pos*(s: OutputStream): int =
  s.endPos - s.cursor.runway

proc safeWritePage(s: OutputStream, data: openarray[byte]) {.inline.} =
  if data.len > 0: s.vtable.writePage(s, data)

proc writePages(s: OutputStream, skipLast = 0) =
  assert s.vtable != nil
  for i in 0 ..< s.pages.len - skipLast:
    s.safeWritePage s.pages[i].buffer.toOpenArrayByte(0, s.pages[i].buffer.len - 1)

proc writePartialPage(s: OutputStream, page: var OutputPage) =
  assert s.vtable != nil
  let
    unwrittenBytes = s.cursor.runway
    pageEndPos = s.pageSize - unwrittenBytes - 1
    pageStartPos = page.startOffset

  s.safeWritePage page.buffer.toOpenArrayByte(pageStartPos, pageEndPos)
  s.endPos -= unwrittenBytes

  page.startOffset = 0
  s.flipPage

proc flush*(s: OutputStream) =
  doAssert s.extCursorsCount == 0
  if s.vtable != nil:
    # We write all pages except the last one
    s.writePages(skipLast = 1)
    # Then we erase them from the list
    s.pages.shrink(fromFirst = s.pages.len - 1)
    # Then we write the current page, which is probably incomplete
    s.writePartialPage s.pages[0]
    # Finally, we flush
    s.vtable.flush(s)

proc writePendingPagesAndLeaveOne(s: OutputStream) {.inline.} =
  s.writePages
  s.pages.shrink(fromFirst = s.pages.len - 1)
  s.pages[0].startOffset = 0
  s.flipPage

proc tryFlushing(s: OutputStream) {.inline.} =
  # Pre-conditions:
  #  * The cursor has reached the current buffer end
  #
  # Post-conditions:
  #  * All completed pages are written
  #  * There is a fresh page ready for writing at the top
  #    (we can reuse a previously existing page for this)
  #  * The head and bufferEnd pointers point to the new top page
  if s.vtable != nil and s.extCursorsCount == 0:
    s.writePendingPagesAndLeaveOne
  else:
    s.addPage

func endAddr(s: string): ptr byte {.inline.} =
  let a = unsafeAddr s[0]
  offset(cast[ptr byte](a), s.len)

template startAddr(s: string): ptr byte =
  cast[ptr byte](unsafeAddr s[0])

func boundingAddrs(s: string): (ptr byte, ptr byte) {.inline.} =
  (startAddr s, endAddr s)

proc findNextPage(c: var WriteCursor): int =
  let cursorBufferEnd = c.bufferEnd
  for i in 0 .. c.stream.pages.len - 2:
    let pageEnd = endAddr c.stream.pages[i].buffer
    if cursorBufferEnd == pageEnd:
      return i + 1

  doAssert false # There is no next page the cursor can move to

proc moveToPage(c: var WriteCursor, p: var OutputPage) =
  doAssert p.startOffset > 0
  c.head = cast[ptr byte](unsafeAddr p.buffer[0])
  c.bufferEnd = offset(c.head, p.startOffset)
  p.startOffset = 0

proc moveToNextPage(c: var WriteCursor) =
  c.moveToPage c.stream.pages[c.findNextPage()]

proc append*(c: var WriteCursor, b: byte) =
  if c.head == c.bufferEnd:
    doAssert c.stream.canExtendOutput
    if c.isExternalCursor:
      c.moveToNextPage()
    else:
      c.stream.tryFlushing()

  c.head[] = b
  c.head = offset(c.head, 1)

template append*(c: var WriteCursor, x: char) =
  bind append
  c.append byte(x)

proc writeDataAsPages(s: OutputStream, data: ptr byte, dataLen: int) =
  var
    data = data
    dataLen = dataLen

  if dataLen > s.pageSize:
    if dataLen < s.maxWriteSize:
      s.vtable.writePage(s, makeOpenArray(data, dataLen))
      s.endPos += dataLen
      return

    while dataLen > s.pageSize:
      s.vtable.writePage(s, makeOpenArray(data, s.pageSize))
      data = offset(data, s.pageSize)
      dec dataLen, s.pageSize
      s.endPos += s.pageSize

  copyMem(s.cursor.head, data, dataLen)
  s.cursor.head = offset(s.cursor.head, dataLen)

proc newStringFromBytes(input: ptr byte, inputLen: int): string =
  assert inputLen > 0
  result = newString(inputLen)
  copyMem(addr result[0], input, inputLen)

proc handleLongAppend*(c: var WriteCursor, bytes: openarray[byte]) =
  var
    pageRemaining = c.runway
    inputPos = unsafeAddr bytes[0]
    inputLen = bytes.len
    stream = c.stream

  template reduceInput(delta: int) =
    inputPos = offset(inputPos, delta)
    inputLen -= delta

  # Since the input is longer, we first make sure that the top-most
  # page is filled to the top:
  doAssert c.stream.canExtendOutput
  copyMem(c.head, inputPos, pageRemaining)
  reduceInput pageRemaining

  if c.isExternalCursor:
    var
      totalPages = stream.pages.len
      nextPageIdx = c.findNextPage

    while nextPageIdx < totalPages:
      let
        pageStart = startAddr stream.pages[nextPageIdx].buffer
        pageRunway = stream.pages[nextPageIdx].startOffset
        pageLen = stream.pageSize

      doAssert pageRunway > 0
      stream.pages[nextPageIdx].startOffset = 0

      if pageRunway < pageLen:
        doAssert inputLen <= pageRunway
        copyMem(pageStart, inputPos, inputLen)
        c.head = offset(pageStart, inputLen)
        c.bufferEnd = offset(pageStart, pageRunway)
        return
      else:
        if inputLen <= pageLen:
          copyMem(pageStart, inputPos, inputLen)
          c.head = offset(pageStart, inputLen)
          c.bufferEnd = offset(pageStart, pageLen)
          return
        else:
          copyMem(pageStart, inputPos, pageLen)
          reduceInput pageLen
          inc nextPageIdx

    doAssert false # If we reached here, this means that we've ran out
                   # of pages, so this is a write past the end of the
                   # pre-allocated space for the delayed write.

  elif c.stream.vtable != nil and c.stream.extCursorsCount == 0:
    # This stream has an output device and we are ready to flush
    # all the pending pages. One fresh page will be left on top.
    # The input is yet to be written:
    stream.writePendingPagesAndLeaveOne
    # This will directly send our input to the output device.
    # Since the output device has a preference for pageSize and
    # maxWriteSize, we'll send some full pages to it and then
    # some bytes will be written to the fresh page created above:
    stream.writeDataAsPages(inputPos, inputLen)
  else:
    # We are not ready to flush, so we must create pending pages.
    # We'll try to create them as large as possible:
    let maxPageSize = c.stream.maxWriteSize

    # We know how much the endPos will advance, but please note that
    # it may be corrected later if we end up writing a portion of the
    # input to an incomplete page:
    stream.endPos += inputLen

    # Try to create big pages until we have more data:
    while inputLen > maxPageSize:
      stream.pages.addLast OutputPage(
        buffer: newStringFromBytes(inputPos, maxPageSize),
        startOffset: 0)
      reduceInput maxPageSize

    # Here the remaining input is smaller than a max page, but it may be
    # still larger than a regular page. If this is the case, we just create
    # one final oversized page and then we leave one empty fresh page where
    # the writing will continue:
    if inputLen > c.stream.pageSize:
      stream.pages.addLast OutputPage(
        buffer: newStringFromBytes(inputPos, inputLen),
        startOffset: 0)
      stream.addPage
    else:
      # We don't have enough remaining bytes for a full page, so we'll just
      # allocate a new empty page and we'll write the remaining input there.
      # This will also reset the cursor to the start of the page:
      stream.addPage
      copyMem(c.head, inputPos, inputLen)
      c.head = offset(c.head, inputLen)
      # We must correct endPos, because it must mark the end of the top-most
      # page. Since `addPage` advances the endPos as well and our remaining
      # input was written to the newly created page, our initial increase of
      # endPos was overestimated:
      stream.endPos -= inputLen

proc append*(c: var WriteCursor, bytes: openarray[byte]) {.inline.} =
  # We have a short inlinable function handling the case when the input is
  # short enough to fit in the current page. We'll keep buffering until the
  # page is full:
  let
    pageRemaining = c.runway
    inputLen = bytes.len

  if inputLen == 0: return
  if inputLen <= pageRemaining:
    copyMem(c.head, unsafeAddr bytes[0], inputLen)
    c.head = offset(c.head, inputLen)
  else:
    handleLongAppend(c, bytes)

proc append*(c: var WriteCursor, chars: openarray[char]) {.inline.} =
  var charsStart = unsafeAddr chars[0]
  c.append makeOpenArray(cast[ptr byte](charsStart), chars.len)

template appendMemCopy*[T](c: var WriteCursor, value: T) =
  bind append
  static: assert supportsCopyMem(T)
  let valueAddr = unsafeAddr value
  c.append makeOpenArray(cast[ptr byte](valueAddr), sizeof(value))

template append*(c: var WriteCursor, str: string) =
  bind append
  c.append str.toOpenArrayByte(0, str.len - 1)

template append*(s: OutputStream, value: auto) =
  bind append
  s.cursor.append value

template appendMemCopy*(s: OutputStream, value: auto) =
  bind append
  s.cursor.append value

proc getOutput*(s: OutputStream, T: type string): string =
  doAssert s.vtable == nil and s.extCursorsCount == 0 and s.pageSize > 0

  s.pages[s.pages.len - 1].buffer.setLen(s.pageSize - s.cursor.runway)

  if s.pages.len == 1 and s.pages[0].startOffset == 0:
    result.swap s.pages[0].buffer
  else:
    result = newStringOfCap(s.pos)
    for page in items(s.pages):
      result.add page.buffer.toOpenArray(page.startOffset.int,
                                         page.buffer.len - 1)

template getOutput*(s: OutputStream, T: type seq[byte]): seq[byte] =
  cast[seq[byte]](s.getOutput(string))

template getOutput*(s: OutputStream): seq[byte] =
  cast[seq[byte]](s.getOutput(string))

proc finishPageEarly(s: OutputStream, unwrittenBytes: int) {.inline.} =
  s.pages[s.pages.len - 1].buffer.setLen(s.pageSize - unwrittenBytes)
  s.endPos -= unwrittenBytes
  s.tryFlushing()

proc createCursor(s: OutputStream, size: int): WriteCursor =
  inc s.extCursorsCount

  result = WriteCursor(head: s.cursor.head,
                       bufferEnd: offset(s.cursor.head, size),
                       stream: s)

  s.cursor.head = result.bufferEnd

proc delayFixedSizeWrite*(s: OutputStream, size: Natural): WriteCursor =
  let remainingBytesInPage = s.cursor.runway
  if size <= remainingBytesInPage:
    result = s.createCursor(size)
  else:
    result = s.createCursor(remainingBytesInPage)
    var size = size - remainingBytesInPage
    s.endPos += size
    while size > s.pageSize:
      s.pages.addLast OutputPage(buffer: newString(s.pageSize),
                                 startOffset: s.pageSize)
      size -= s.pageSize

    s.pages.addLast OutputPage(buffer: newString(s.pageSize),
                               startOffset: size)

    let (pageStart, pageEnd) = boundingAddrs s.pages[s.pages.len - 1].buffer
    s.cursor.head = offset(pageStart, size)
    s.cursor.bufferEnd = pageEnd
    s.endPos += (s.pageSize - size)

proc delayVarSizeWrite*(s: OutputStream, maxSize: Natural): VarSizeWriteCursor =
  doAssert maxSize < s.pageSize
  s.finishPageEarly s.cursor.runway
  VarSizeWriteCursor s.createCursor(maxSize)

proc finalize*(cursor: var WriteCursor) =
  doAssert cursor.stream.extCursorsCount > 0
  dec cursor.stream.extCursorsCount

proc writeAndFinalize*(cursor: var WriteCursor, data: openarray[byte]) =
  doAssert data.len == cursor.runway
  copyMem(cursor.head, unsafeAddr data[0], data.len)
  finalize cursor

proc writeAndFinalize*(c: var VarSizeWriteCursor, data: openarray[byte]) =
  template cursor: auto = WriteCursor(c)

  for page in mitems(cursor.stream.pages):
    if unsafeAddr(page.buffer[0]) == cursor.head:
      let overestimatedBytes = cursor.runway - data.len
      doAssert overestimatedBytes >= 0
      page.startOffset = overestimatedBytes
      copyMem(offset(cursor.head, overestimatedBytes), unsafeAddr data[0], data.len)
      finalize cursor
      return

  doAssert false

