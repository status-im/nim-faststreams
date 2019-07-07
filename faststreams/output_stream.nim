import
  deques, stew/ranges/ptr_arith, stew/strings

type
  OutputPage = object
    buffer: string
    startOffset: int

  OutputStream* = object of RootObj
    cursor: WriteCursor
    pages: Deque[OutputPage]
    endPos: int
    vtable*: ptr OutputStreamVTable
    outputDevice*: RootRef
    extCursorsCount: int
    pageSize: int
    maxWriteSize*: int

  WriteCursor* = object
    head, bufferEnd: ptr byte
    stream: OutputStreamVar

  FileOutput = ref object of RootObj
    file: File

  OutputStreamVar* = ref OutputStream

  # Keep this temporary for backward-compatibility
  DelayedWriteCursor* = WriteCursor
  VarSizeWriteCursor* = distinct WriteCursor

  OutputStreamVTable* = object
    writePage*: proc (s: OutputStreamVar, page: openarray[byte])
                     {.nimcall, gcsafe, raises: [IOError, Defect] .}

    flush*: proc (s: OutputStreamVar)
                 {.nimcall, gcsafe, raises: [IOError, Defect].}

const
  allocatorMetadata = 0 # TODO: Get this from Nim's allocator.
                        # The goal is to make perfect page-aligned allocations
  defaultPageSize = 4096 - allocatorMetadata - 1 # 1 byte for the null terminator

func canExtendOutput(c: var WriteCursor): bool {.inline.} =
  # ATTENTION!
  # The `var` modifier is intentional here. Nim tries to optimise small values
  # such as WriteCursors by copying them instead of passing them by reference.
  # This will break the address checks below.
  #
  # Only the original stream cursor is allowed to write past its buffer end by
  # allocating new memory pages. Streams writing to pre-allocated existing
  # buffers cannot be grown as well:
  unsafeAddr(c) == unsafeAddr(c.stream.cursor) and c.stream.pageSize > 0

func remainingBytesToWrite*(c: var WriteCursor): int {.inline.} =
  distance(c.head, c.bufferEnd)

proc flipPage(s: OutputStreamVar) =
  s.cursor.head = cast[ptr byte](addr s.pages[s.pages.len - 1].buffer[0])
  s.cursor.bufferEnd = cast[ptr byte](shift(s.cursor.head, s.pageSize))
  s.endPos += s.pageSize

proc addPage(s: OutputStreamVar) =
  s.pages.addLast OutputPage(buffer: newString(s.pageSize),
                             startOffset: 0)
  s.flipPage

proc initWithSinglePage*(s: OutputStreamVar, pageSize, maxWriteSize: int) =
  s.pageSize = pageSize
  s.maxWriteSize = maxWriteSize
  s.pages = initDeque[OutputPage]()
  s.addPage
  s.cursor.stream = s

proc init*(T: type OutputStream,
           pageSize = defaultPageSize): ref OutputStream =
  new result
  result.initWithSinglePage pageSize, high(int)

let FileStreamVTable = OutputStreamVTable(
  writePage: proc (s: OutputStreamVar, data: openarray[byte]) {.nimcall, gcsafe.} =
    var output = FileOutput(s.outputDevice)
    var written = output.file.writeBuffer(unsafeAddr data[0], data.len)
    if written != data.len:
      raise newException(IOError, "Failed to write OutputStream page.")
  ,
  flush: proc (s: OutputStreamVar) {.nimcall, gcsafe.} =
    var output = FileOutput(s.outputDevice)
    flushFile output.file
)

proc init*(T: type OutputStream,
           filename: string,
           pageSize = defaultPageSize): ref OutputStream =
  new result
  result.outputDevice = FileOutput(file: open(filename, fmWrite))
  result.vtable = unsafeAddr FileStreamVTable
  result.initWithSinglePage pageSize, high(int)

proc init*(T: type OutputStream,
           buffer: pointer, len: int): ref OutputStream =
  new result
  let buffer = cast[ptr byte](buffer)
  result.cursor.head = buffer
  result.cursor.bufferEnd = buffer.shift(len)
  result.cursor.stream = result
  result.endPos = len

proc pos*(s: OutputStreamVar): int =
  s.endPos - s.cursor.remainingBytesToWrite

proc safeWritePage(s: OutputStreamVar, data: openarray[byte]) {.inline.} =
  if data.len > 0: s.vtable.writePage(s, data)

proc writePages(s: OutputStreamVar, skipLast = 0) =
  assert s.vtable != nil
  for i in 0 ..< s.pages.len - skipLast:
    s.safeWritePage s.pages[i].buffer.toOpenArrayByte(0, s.pages[i].buffer.len - 1)

proc writePartialPage(s: OutputStreamVar, page: var OutputPage) =
  assert s.vtable != nil
  let
    unwrittenBytes = s.cursor.remainingBytesToWrite
    pageEndPos = s.pageSize - unwrittenBytes - 1
    pageStartPos = page.startOffset

  s.safeWritePage page.buffer.toOpenArrayByte(pageStartPos, pageEndPos)
  s.endPos -= unwrittenBytes

  page.startOffset = 0
  s.flipPage

proc flush*(s: OutputStreamVar) =
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

proc writePendingPagesAndLeaveOne(s: OutputStreamVar) {.inline.} =
  s.writePages
  s.pages.shrink(fromFirst = s.pages.len - 1)
  s.pages[0].startOffset = 0
  s.flipPage

proc tryFlushing(s: OutputStreamVar) {.inline.} =
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

proc append*(c: var WriteCursor, b: byte) =
  if c.head == c.bufferEnd:
    doAssert c.canExtendOutput
    c.stream.tryFlushing()

  c.head[] = b
  c.head = shift(c.head, 1)

template append*(c: var WriteCursor, x: char) =
  bind append
  c.append byte(x)

proc writeDataAsPages(s: OutputStreamVar, data: ptr byte, dataLen: int) =
  var
    data = data
    dataLen = dataLen

  if dataLen < s.maxWriteSize:
    s.vtable.writePage(s, makeOpenArray(data, dataLen))
    s.endPos += dataLen
    return

  while dataLen > s.pageSize:
    s.vtable.writePage(s, makeOpenArray(data, s.pageSize))
    data = shift(data, s.pageSize)
    dec dataLen, s.pageSize
    s.endPos += s.pageSize

  copyMem(s.cursor.head, data, dataLen)
  s.cursor.head = shift(s.cursor.head, dataLen)

proc newStringFromBytes(input: ptr byte, inputLen: int): string =
  assert inputLen > 0
  result = newString(inputLen)
  copyMem(addr result[0], input, inputLen)

proc handleLongAppend*(c: var WriteCursor, bytes: openarray[byte]) =
  var
    pageRemaining = c.remainingBytesToWrite
    inputPos = unsafeAddr bytes[0]
    inputLen = bytes.len

  template reduceInput(delta: int) =
    inputPos = shift(inputPos, delta)
    inputLen -= delta

  # Since the input is longer, we first make sure that the top-most
  # page is filled to the top:
  doAssert c.canExtendOutput
  copyMem(c.head, inputPos, pageRemaining)
  reduceInput pageRemaining

  if c.stream.vtable != nil and c.stream.extCursorsCount == 0:
    # This stream has an output device and we are ready to flush
    # all the pending pages. One fresh page will be left on top.
    # The input is yet to be written:
    c.stream.writePendingPagesAndLeaveOne
    # This will directly send our input to the output device.
    # Since the output device has a preference for pageSize and
    # maxWriteSize, we'll send some full pages to it and then
    # some bytes will be written to the fresh page created above:
    c.stream.writeDataAsPages(inputPos, inputLen)
  else:
    # We are not ready to flush, so we must create pending pages.
    # We'll try to create them as large as possible:
    let maxPageSize = c.stream.maxWriteSize

    # We know how much the endPos will advance, but please note that
    # it may be corrected later if we end up writing a portion of the
    # input to an incomplete page:
    c.stream.endPos += inputLen

    # Try to create big pages until we have more data:
    while inputLen > maxPageSize:
      c.stream.pages.addLast OutputPage(
        buffer: newStringFromBytes(inputPos, maxPageSize),
        startOffset: 0)
      reduceInput maxPageSize

    # Here the remaining input is smaller than a max page, but it may be
    # still larger than a regular page. If this is the case, we just create
    # one final oversized page and then we leave one empty fresh page where
    # the writing will continue:
    if inputLen > c.stream.pageSize:
      c.stream.pages.addLast OutputPage(
        buffer: newStringFromBytes(inputPos, inputLen),
        startOffset: 0)
      c.stream.addPage
    else:
      # We don't have enough remaining bytes for a full page, so we'll just
      # allocate a new empty page and we'll write the remaining input there.
      # This will also reset the cursor to the start of the page:
      c.stream.addPage
      copyMem(c.head, inputPos, inputLen)
      c.head = shift(c.head, inputLen)
      # We must correct endPos, because it must mark the end of the top-most
      # page. Since `addPage` advances the endPos as well and our remaining
      # input was written to the newly created page, our initial increase of
      # endPos was overestimated:
      c.stream.endPos -= inputLen

proc append*(c: var WriteCursor, bytes: openarray[byte]) {.inline.} =
  # We have a short inlinable function handling the case when the input is
  # short enough to fit in the current page. We'll keep buffering until the
  # page is full:
  let
    pageRemaining = c.remainingBytesToWrite
    inputPos = unsafeAddr bytes[0]
    inputLen = bytes.len

  if inputLen <= pageRemaining:
    copyMem(c.head, inputPos, inputLen)
    c.head = shift(c.head, inputLen)
  else:
    handleLongAppend(c, bytes)

proc append*(c: var WriteCursor, chars: openarray[char]) {.inline.} =
  var charsStart = unsafeAddr chars[0]
  c.append makeOpenArray(cast[ptr byte](charsStart), chars.len)

template appendMemCopy*(c: var WriteCursor, value: auto) =
  bind append
  # TODO: add a check that this is a trivial type
  c.append makeOpenArray(cast[ptr byte](unsafeAddr(value)), sizeof(value))

template append*(c: var WriteCursor, str: string) =
  bind append
  c.append str.toOpenArrayByte(0, str.len - 1)

template append*(s: OutputStreamVar, value: auto) =
  bind append
  s.cursor.append value

template appendMemCopy*(s: OutputStreamVar, value: auto) =
  bind append
  s.cursor.append value

proc getOutput*(s: OutputStreamVar, T: type string): string =
  doAssert s.vtable == nil and s.extCursorsCount == 0 and s.pageSize > 0

  s.pages[s.pages.len - 1].buffer.setLen(s.pageSize - s.cursor.remainingBytesToWrite)

  if s.pages.len == 1 and s.pages[0].startOffset == 0:
    result.swap s.pages[0].buffer
  else:
    result = newStringOfCap(s.pos)
    for page in s.pages:
      result.add page.buffer.toOpenArray(page.startOffset.int,
                                         page.buffer.len - 1)

template getOutput*(s: OutputStreamVar, T: type seq[byte]): seq[byte] =
  cast[seq[byte]](s.getOutput(string))

proc getOutput*(s: OutputStreamVar): seq[byte] =
  # TODO: is the extra copy here optimized away?
  # Turning this proc into a template creates problems at the moment.
  s.getOutput(seq[byte])

proc finishPageEarly(s: OutputStreamVar, unwrittenBytes: int) {.inline.} =
  s.pages[s.pages.len - 1].buffer.setLen(s.pageSize - unwrittenBytes)
  s.endPos -= unwrittenBytes
  s.tryFlushing()

proc createCursor(s: OutputStreamVar, size: int): WriteCursor =
  inc s.extCursorsCount

  result = WriteCursor(head: s.cursor.head,
                       bufferEnd: s.cursor.head.shift(size),
                       stream: s)

  s.cursor.head = result.bufferEnd

proc delayFixedSizeWrite*(s: OutputStreamVar, size: int): WriteCursor =
  let remainingBytesInPage = s.cursor.remainingBytesToWrite
  if size > remainingBytesInPage:
    doAssert size < s.pageSize
    s.finishPageEarly remainingBytesInPage

  s.createCursor(size)

proc delayVarSizeWrite*(s: OutputStreamVar, maxSize: int): VarSizeWriteCursor =
  doAssert maxSize < s.pageSize
  s.finishPageEarly s.cursor.remainingBytesToWrite
  VarSizeWriteCursor s.createCursor(maxSize)

proc dispose*(cursor: var WriteCursor) =
  doAssert cursor.stream.extCursorsCount > 0
  dec cursor.stream.extCursorsCount

proc endWrite*(cursor: var WriteCursor, data: openarray[byte]) =
  doAssert data.len == cursor.remainingBytesToWrite
  copyMem(cursor.head, unsafeAddr data[0], data.len)
  dispose cursor

proc endWrite*(c: var VarSizeWriteCursor, data: openarray[byte]) =
  template cursor: auto = WriteCursor(c)

  for page in mitems(cursor.stream.pages):
    if unsafeAddr(page.buffer[0]) == cursor.head:
      let overestimatedBytes = cursor.remainingBytesToWrite - data.len
      doAssert overestimatedBytes >= 0
      page.startOffset = overestimatedBytes
      copyMem(cursor.head.shift(overestimatedBytes), unsafeAddr data[0], data.len)
      dispose cursor
      return

  doAssert false

