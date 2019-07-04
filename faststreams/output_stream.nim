import
  deques, stew/ranges/ptr_arith, stew/strings

type
  OutputStreamVTable = tuple
    prepareOutput: proc (s: ptr OutputStream, size: int) {.nimcall.}
    finish: proc (s: ptr OutputStream) {.nimcall.}

  OutputPage = object
    buffer: string
    startOffset: int16
    delayedWrites: int16

  OutputStream* = object
    cursor: WriteCursor
    pages: Deque[OutputPage]
    endPos: int
    vtable: ptr OutputStreamVTable
    outputDevice: RootRef

  WriteCursor* = object
    head, bufferEnd: ptr byte
    stream: OutputStreamVar
    absPageIdx: int

  OutputStreamVar* = ref OutputStream

  # Keep this temporary for backward-compatibility
  DelayedWriteCursor* = WriteCursor

const
  allocatorMetadata = 0 # TODO: Get this from Nim's allocator.
                        # The goal is to make perfect page-aligned allocations
  pageSize = 4096 - allocatorMetadata - 1 # 1 byte for the null terminator

func remainingBytesToWrite*(c: WriteCursor): int {.inline.} =
  distance(c.head, c.bufferEnd)

template relToAbsPageIdx(s: OutputStreamVar, idx: int): int =
  # original code: s.firstPage + idx
  idx - s.cursor.absPageIdx - 1

template absToRelPageIdx(s: OutputStreamVar, idx: int): int =
  # original code: idx - s.firstPage
  idx + s.cursor.absPageIdx + 1

func relPage(c: WriteCursor): int {.inline.} =
  c.stream.absToRelPageIdx c.absPageIdx

proc addPage(s: OutputStreamVar) =
  s.pages.addLast OutputPage(buffer: newString(pageSize),
                             delayedWrites: 0,
                             startOffset: 0)
  s.cursor.head = cast[ptr byte](addr s.pages[s.pages.len - 1].buffer[0])
  s.cursor.bufferEnd = cast[ptr byte](shift(s.cursor.head, pageSize))
  s.endPos += pageSize

proc init*(T: type OutputStream): ref OutputStream =
  new result
  result.vtable = nil
  result.pages = initDeque[OutputPage]()
  result.addPage()
  result.cursor.absPageIdx = -1
  result.cursor.stream = result

proc pos*(s: OutputStreamVar): int =
  s.endPos - s.cursor.remainingBytesToWrite

proc tryFlushing(s: OutputStreamVar) =
  # TODO This is relevant when writing to files and layered streams (e.g. zip)
  # Post-conditions:
  #  * All completed pages are written
  #  * There is a fresh page ready for writing at the top
  #    (we can reuse a previously existing page for this)
  #  * The head and bufferEnd pointers point to the new top page
  s.addPage()

template isDelayedWrite(c: WriteCursor): bool =
  c.absPageIdx >= 0

proc append*(c: var WriteCursor, b: byte) =
  if c.head == c.bufferEnd:
    # Delayed write cursors are not allowed to reach
    # the end of the buffer and allocate new pages:
    doAssert(not c.isDelayedWrite)
    c.stream.tryFlushing()

  c.head[] = b
  c.head = shift(c.head, 1)

template append*(c: var WriteCursor, x: char) =
  bind append
  c.append byte(x)

proc append*(c: var WriteCursor, bytes: openarray[byte]) =
  # TODO: this can use copyMem
  for b in bytes:
    c.append b

proc append*(c: var WriteCursor, chars: openarray[char]) =
  # TODO: this can use copyMem
  for x in chars:
    c.append byte(x)

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

proc flush*(s: OutputStreamVar) =
  s.vtable.finish(addr s[])

proc getOutput*(s: OutputStreamVar, T: type string): string =
  s.pages[s.pages.len - 1].buffer.setLen(pageSize - s.cursor.remainingBytesToWrite)

  if s.pages.len == 1 and s.pages[0].startOffset == 0:
    result.swap s.pages[0].buffer
  else:
    result = newStringOfCap(s.pos)
    for page in s.pages:
      doAssert page.delayedWrites == 0
      result.add page.buffer.toOpenArray(page.startOffset.int,
                                         page.buffer.len - 1)

template getOutput*(s: OutputStreamVar, T: type seq[byte]): seq[byte] =
  cast[seq[byte]](s.getOutput(string))

proc getOutput*(s: OutputStreamVar): seq[byte] =
  # TODO: is the extra copy here optimized away?
  # Turning this proc into a template creates problems at the moment.
  s.getOutput(seq[byte])

proc flushDelayedPages*(s: OutputStreamVar) =
  for i in 0 .. s.pages.len - 2:
    if s.pages[i].delayedWrites > 0: return
    # TODO:
    # Send to output

proc delayFixedSizeWrite*(s: OutputStreamVar, size: int): WriteCursor =
  doAssert size < pageSize

  let remainingBytesInPage = s.cursor.remainingBytesToWrite
  if size > remainingBytesInPage:
    s.pages[s.pages.len - 1].buffer.setLen(pageSize - remainingBytesInPage)
    s.endPos -= remainingBytesInPage
    s.tryFlushing()

  let curPageIdx = s.pages.len - 1
  inc s.pages[curPageIdx].delayedWrites

  result = WriteCursor(head: s.cursor.head,
                       bufferEnd: s.cursor.head.shift(size),
                       absPageIdx: s.relToAbsPageIdx(curPageIdx),
                       stream: s)

  s.cursor.head = result.bufferEnd

proc delayVarSizeWrite*(s: OutputStreamVar, maxSize: int): WriteCursor =
  # TODO
  discard

proc decRef(x: var int16): int16 =
  result = x - 1
  doAssert result >= 0
  x = result

proc totalBytesWrittenAfterCursor*(cursor: WriteCursor): int =
  template s: auto = cursor.stream

  let
    relPageIdx = cursor.relPage
    spanningPagesTotal = (cursor.stream.pages.len - relPageIdx) * pageSize
    deductedFromFirstPage = distance(unsafeAddr s.pages[relPageIdx].buffer[0],
                                     cursor.bufferEnd)
    deductedFromLastPage = s.cursor.remainingBytesToWrite

  spanningPagesTotal - deductedFromFirstPage - deductedFromLastPage

proc endWrite*(cursor: WriteCursor, data: openarray[byte]) =
  doAssert data.len == cursor.remainingBytesToWrite

  copyMem(cursor.head, unsafeAddr data[0], data.len)
  if cursor.stream.pages[cursor.relPage].delayedWrites.decRef <= 0:
    cursor.stream.flushDelayedPages()

# Any stream

proc appendNumberImpl(s: OutputStreamVar, number: BiggestInt) =
  # TODO: don't allocate
  s.append $number

proc appendNumberImpl(s: OutputStreamVar, number: BiggestUInt) =
  # TODO: don't allocate
  s.append $number

template toBiggestRepr(i: SomeUnsignedInt): BiggestUInt =
  BiggestUInt(i)

template toBiggestRepr(i: SomeSignedInt): BiggestInt =
  BiggestInt(i)

template appendNumber*(s: OutputStreamVar, i: SomeInteger) =
  # TODO: specify radix/base
  appendNumberImpl(s, toBiggestRepr(i))

