import
  memfiles, options,
  stew/[ptrops, ranges/ptr_arith],
  async_backend

type
  InputStream* = ref object of RootObj
    vtable*: ptr InputStreamVTable
    head*: ptr byte
    pageSize*: int
    bufferSize: int
    bufferStart, bufferEnd: ptr byte
    bufferEndPos: int

  LayeredInputStream* = ref object of InputStream
    subStream*: InputStream

  InputStreamHandle* = object
    s*: InputStream

  AsyncInputStream* {.borrow: `.`.} = distinct InputStream

  ReadSyncProc* = proc (s: InputStream, buffer: ptr byte, bufSize: int): int
                       {.nimcall, gcsafe, raises: [IOError, Defect].}

  ReadAsyncProc* = proc (s: InputStream, buffer: ptr byte, bufSize: int): Future[int]
                        {.nimcall, gcsafe, raises: [IOError, Defect].}

  CloseSyncProc* = proc (s: InputStream)
                        {.nimcall, gcsafe, raises: [IOError, Defect].}

  CloseAsyncProc* = proc (s: InputStream): Future[void]
                         {.nimcall, gcsafe, raises: [IOError, Defect].}

  GetLenSyncProc* = proc (s: InputStream): int
                         {.nimcall, gcsafe, raises: [IOError, Defect].}

  InputStreamVTable* = object
    readSync*: ReadSyncProc
    readAsync*: ReadAsyncProc
    closeSync*: CloseSyncProc
    closeAsync*: CloseAsyncProc
    getLenSync*: GetLenSyncProc

  FileInputStream = ref object of InputStream
    file: MemFile

const
  lengthUnknown* = -1
  debugHelpers = false
  nimAllocatorMetadataSize* = 0
    # TODO: Get this from Nim's allocator.
    # The goal is to make perfect page-aligned allocations
  # defaultPageSize = 4096 - nimAllocatorMetadataSize

proc close*(s: var InputStream) {.raises: [IOError, Defect].} =
  if s != nil:
    if s.vtable != nil and s.vtable.closeSync != nil:
      s.vtable.closeSync(s)
    s = nil

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
    h.s = nil

converter implicitDeref*(h: InputStreamHandle): InputStream =
  h.s

let FileStreamVTable = InputStreamVTable(
  closeSync: proc (s: InputStream)
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
    try:
      close FileInputStream(s).file
    except OSError as err:
      raise newException(IOError, "Failed to close file", err)
  ,
  getLenSync: proc (s: InputStream): int
                   {.nimcall, gcsafe, raises: [IOError, Defect].} =
    distance(s.head, s.bufferEnd)
)

template vtableAddr*(vtable: InputStreamVTable): ptr InputStreamVTable =
  ## This is a simple work-around for the somewhat broken side
  ## effects analysis of Nim - reading from global let variables
  ## is considered a side-effect.
  {.noSideEffect.}:
    unsafeAddr vtable

proc fileInput*(filename: string): InputStreamHandle =
  let
    memFile = memfiles.open(filename)
    head = cast[ptr byte](memFile.mem)
    fileSize = memFile.size

  var stream = FileInputStream(
    vtable: vtableAddr FileStreamVTable,
    head: head,
    bufferEnd: offset(head, fileSize),
    bufferEndPos: fileSize,
    file: memFile)

  when debugHelpers:
    stream.bufferStart = head

  InputStreamHandle(s: stream)

proc memoryInput*(mem: openarray[byte]): InputStreamHandle =
  let head = unsafeAddr mem[0]
  InputStreamHandle(s: InputStream(
    head: head,
    bufferEnd: offset(head, mem.len),
    bufferEndPos: mem.len))

proc memoryInput*(str: string): InputStreamHandle =
  memoryInput str.toOpenArrayByte(0, str.len - 1)

# TODO: Is this used, should we deprecate it?
proc endPos*(s: InputStream): int =
  doAssert s.vtable == nil or s.vtable.getLenSync != nil
  return s.bufferEndPos

# TODO The return type here could be Option[Natural] if Nim had
# the Option[range] optimisation that will make it equvalent to `int`.
proc len*(s: InputStream): int {.raises: [Defect, IOError].} =
  if s.vtable == nil:
    distance(s.head, s.bufferEnd)
  elif s.vtable.getLenSync != nil:
    s.vtable.getLenSync(s)
  else:
    lengthUnknown

template len*(s: AsyncInputStream): int =
  len InputStream(s)

proc bufferMoreDataSync(s: InputStream): bool =
  # Returns true if more data was successfully buffered
  if s.vtable == nil or s.vtable.readSync == nil:
    return false

  let bytesRead = s.vtable.readSync(s, s.bufferStart, s.bufferSize)
  if bytesRead == 0:
    # TODO close the input device
    s.vtable = nil
    return false
  else:
    s.bufferEnd = offset(s.bufferStart, bytesRead)
    s.bufferEndPos += bytesRead
    return true

proc bufferMoreDataAsync(s: AsyncInputStream): Future[bool] {.async.} =
  # Returns true if more data was successfully buffered
  return false

proc readable*(s: InputStream): bool =
  if s.head != s.bufferEnd:
    true
  else:
    s.bufferMoreDataSync()

template readable*(sp: AsyncInputStream): bool =
  let s = sp
  if s.head != s.bufferEnd:
    true
  else:
    faststreamsAwait bufferMoreDataAsync(s)

proc readable*(s: InputStream, n: int): bool =
  if distance(s.head, s.bufferEnd) >= n:
    return true

  if s.vtable == nil:
    return false

  # TODO
  doAssert false, "Multi-buffer reading will be implemented later"

template readable*(sp: AsyncInputStream, n: int): bool =
  let s = sp

  if distance(s.head, s.bufferEnd) >= n:
    return true

  if s.vtable == nil:
    return false

  # TODO
  doAssert false, "Multi-buffer reading will be implemented later"

template close*(s: AsyncInputStream) =
  close InputStream(s)

proc peek*(s: InputStream): byte {.inline.} =
  doAssert s.head != s.bufferEnd
  return s.head[]

template peek*(s: AsyncInputStream): byte =
  peek InputStream(s)

proc peekAt*(s: InputStream, pos: int): byte {.inline.} =
  # TODO implement page flipping
  let peekHead = offset(s.head, pos)
  doAssert cast[uint](peekHead) < cast[uint](s.bufferEnd)
  return peekHead[]

template peekAt*(s: AsyncInputStream, pos: int): byte =
  peekAt InputStream(s)

when debugHelpers:
  proc showPosition*(s: InputStream) =
    echo "head at ", distance(s.bufferStart, s.head), "/",
                     distance(s.bufferStart, s.bufferEnd)

proc advance*(s: InputStream) =
  if s.head != s.bufferEnd:
    s.head = offset(s.head, 1)
  else:
    discard s.bufferMoreDataSync()

template advance*(sp: AsyncInputStream) =
  let s = sp
  if s.head != s.bufferEnd:
    s.head = offset(s.head, 1)
  else:
    discard faststreamsAwait(bufferMoreDataAsync(s))

proc read*(s: InputStream): byte =
  result = s.peek()
  advance s

template read*(sp: AsyncInputStream): byte =
  let s = sp
  let res = s.peek()
  advance(s)
  res

proc checkReadAhead(s: InputStream, n: int): ptr byte =
  result = s.head
  doAssert distance(s.head, s.bufferEnd) >= n
  s.head = offset(s.head, n)

template read*(s: InputStream, n: int): auto =
  makeOpenArray(checkReadAhead(s, n), n)

proc next*(s: InputStream): Option[byte] =
  if readable(s):
    result = some read(s)

template next*(sp: AsyncInputStream): Option[byte] =
  let s = sp
  if readable(s):
    some read(s)
  else:
    none byte

proc bufferPos(s: InputStream, pos: int): ptr byte =
  let offsetFromEnd = pos - s.bufferEndPos
  doAssert offsetFromEnd < 0
  result = offset(s.bufferEnd, offsetFromEnd)
  doAssert result >= s.bufferStart

proc pos*(s: InputStream): int {.inline.} =
  s.bufferEndPos - distance(s.head, s.bufferEnd)

template pos*(s: AsyncInputStream): int =
  pos InputStream(s)

proc firstAccessiblePos*(s: InputStream): int {.inline.} =
  s.bufferEndPos - distance(s.bufferStart, s.bufferEnd)

proc `[]`*(s: InputStream, pos: int): byte {.inline.} =
  s.bufferPos(pos)[]

proc rewind*(s: InputStream, delta: int) =
  s.head = offset(s.head, -delta)
  doAssert s.head >= s.bufferStart

proc rewindTo*(s: InputStream, pos: int) {.inline.} =
  s.head = s.bufferPos(pos)

