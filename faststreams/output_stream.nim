import
  deques, ranges/ptr_arith, std_shims/strings

type
  OutputStreamVTable = tuple
    prepareOutput: proc (s: ptr OutputStream, size: int) {.nimcall.}
    finish: proc (s: ptr OutputStream) {.nimcall.}

  OutputPage = object
    buffer: string
    startOffset: int16
    delayedWrites: int16

  OutputStream* = object
    head, bufferEnd: ptr byte
    pages: Deque[OutputPage]
    firstPage: int
    endPos: int
    outputResource: pointer
    vtable: ptr OutputStreamVTable

  DelayedWriteCursor* = object
    head, bufferEnd: ptr byte
    stream: OutputStreamVar
    page: int

  OutputStreamVar* = ref OutputStream

const
  allocatorMetadata = 0 # TODO: Get this from Nim's allocator.
                        # The goal is to make perfect page-aligned allocations
  pageSize = 4096 - allocatorMetadata - 1 # 1 byte for the null terminator

proc addPage(s: OutputStreamVar) =
  s.pages.addLast OutputPage(buffer: newString(pageSize),
                             delayedWrites: 0,
                             startOffset: 0)
  s.head = cast[ptr byte](addr s.pages[s.pages.len - 1].buffer[0])
  s.bufferEnd = cast[ptr byte](shift(s.head, pageSize))
  s.endPos += pageSize

proc init*(T: type OutputStream): ref OutputStream =
  new result
  result.vtable = nil
  result.pages = initDeque[OutputPage]()
  result.addPage()

proc pos*(s: OutputStreamVar): int =
  s.endPos - distance(s.head, s.bufferEnd)

proc tryFlushing(s: OutputStreamVar) =
  # TODO This is relevant when writing to files and layered streams (e.g. zip)
  # Post-conditions:
  #  * All completed pages are written
  #  * There is a fresh page ready for writing at the top
  #    (we can reuse a previously existing page for this)
  #  * The head and bufferEnd pointers point to the new top page
  s.addPage()

proc append*(s: OutputStreamVar, b: byte) =
  if s.head == s.bufferEnd:
    s.tryFlushing()

  s.head[] = b
  s.head = shift(s.head, 1)

template append*(s: OutputStreamVar, c: char) =
  bind append
  append s, byte(c)

proc append*(s: OutputStreamVar, bytes: openarray[byte]) =
  # TODO: this can use copyMem
  for b in bytes:
    s.append b

proc append*(s: OutputStreamVar, chars: openarray[char]) =
  # TODO: this can use copyMem
  for c in chars:
    s.append byte(c)

template append*(s: OutputStreamVar, str: string) =
  s.append str.toOpenArrayByte(0, str.len - 1)

proc flush*(s: OutputStreamVar) =
  s.vtable.finish(addr s[])

proc getOutput*(s: OutputStreamVar, T: type string): string =
  s.pages[s.pages.len - 1].buffer.setLen(pageSize - distance(s.head, s.bufferEnd))

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

proc delayFixedSizeWrite*(s: OutputStreamVar, size: int): DelayedWriteCursor =
  doAssert size < pageSize

  let remainingBytesInPage = distance(s.head, s.bufferEnd)
  if size > remainingBytesInPage:
    s.pages[s.pages.len - 1].buffer.setLen(pageSize - remainingBytesInPage)
    s.endPos -= remainingBytesInPage
    s.tryFlushing()

  let curPageIdx = s.pages.len - 1
  inc s.pages[curPageIdx].delayedWrites

  result = DelayedWriteCursor(head: s.head, bufferEnd: s.head.shift(size),
                              page: s.firstPage + curPageIdx, stream: s)

  s.head = result.bufferEnd

proc delayVarSizeWrite*(s: OutputStreamVar, maxSize: int): DelayedWriteCursor =
  # TODO
  discard

proc decRef(x: var int16): int16 =
  result = x - 1
  doAssert result >= 0
  x = result

proc totalBytesWrittenAfterCursor*(cursor: DelayedWriteCursor): int =
  template s: auto = cursor.stream

  let
    spanningPagesTotal = (cursor.stream.pages.len - cursor.page) * pageSize
    deductedFromFirstPage = distance(unsafeAddr s.pages[cursor.page].buffer[0],
                                     cursor.bufferEnd)
    deductedFromLastPage = distance(s.head, s.bufferEnd)

  spanningPagesTotal - deductedFromFirstPage - deductedFromLastPage

proc endWrite*(cursor: DelayedWriteCursor, data: openarray[byte]) =
  if data.len != distance(cursor.head, cursor.bufferEnd):
    doAssert false

  copyMem(cursor.head, unsafeAddr data[0], data.len)
  if cursor.stream.pages[cursor.page - cursor.stream.firstPage].delayedWrites.decRef <= 0:
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

