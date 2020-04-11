import
  memfiles, options,
  stew/[ptrops, ranges/ptr_arith]

type
  InputStream* = ref object of RootObj
    vtable*: ptr InputStreamVTable
    head*: ptr byte
    bufferSize: int
    bufferStart, bufferEnd: ptr byte
    bufferEndPos: int
    inputDevice*: RootRef

  LayeredInputStream* = ref object of InputStream
    subStream*: InputStream

  InputStreamHandle* = object
    s*: InputStream

  AsciiInputStream* = distinct InputStream
  Utf8InputStream* = distinct InputStream

  ReadSyncProc* = proc (s: InputStream, buffer: ptr byte, bufSize: int): int
                       {.nimcall, gcsafe, raises: [IOError, Defect].}

  ReadAsyncCallback* = proc (s: InputStream, bytesRead: int)
                            {.nimcall, gcsafe, raises: [Defect].}

  ReadAsyncProc* = proc (s: InputStream,
                         buffer: ptr byte, bufSize: int,
                         cb: ReadAsyncCallback)
                        {.nimcall, gcsafe, raises: [IOError, Defect].}

  CloseSyncProc* = proc (s: InputStream)
                        {.nimcall, gcsafe, raises: [IOError, Defect].}

  CloseAsyncCallback* = proc (s: InputStream)
                             {.nimcall, gcsafe, raises: [IOError, Defect].}

  CloseAsyncProc* = proc (s: InputStream)
                         {.nimcall, gcsafe, raises: [IOError, Defect].}

  InputStreamVTable* = object
    readSync*: ReadSyncProc
    readAsync*: ReadAsyncProc
    closeSync*: CloseSyncProc
    closeAsync*: CloseAsyncProc
    hasKnownLen*: bool

  FileInputStream = ref object of InputStream
    file: MemFile

const
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
  hasKnownLen: true
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

proc endPos*(s: InputStream): int =
  doAssert s.vtable == nil or s.vtable.hasKnownLen
  return s.bufferEndPos

proc syncRead(s: InputStream): bool =
  if s.vtable == nil or s.vtable.readSync == nil:
    return true

  let bytesRead = s.vtable.readSync(s, s.bufferStart, s.bufferSize)
  if bytesRead == 0:
    # TODO close the input device
    s.inputDevice = nil
    s.vtable = nil
    return true
  else:
    s.bufferEnd = offset(s.bufferStart, bytesRead)
    s.bufferEndPos += bytesRead
    return false

proc ensureBytes*(s: InputStream, n: int): bool =
  if distance(s.head, s.bufferEnd) >= n:
    return true

  if s.vtable == nil:
    return false

  # TODO
  doAssert false, "Multi-buffer reading will be implemented later"

proc eof*(s: InputStream): bool =
  if s.head != s.bufferEnd:
    return false

  return s.syncRead()

proc peek*(s: InputStream): byte {.inline.} =
  doAssert s.head != s.bufferEnd
  return s.head[]

when debugHelpers:
  proc showPosition*(s: InputStream) =
    echo "head at ", distance(s.bufferStart, s.head), "/",
                     distance(s.bufferStart, s.bufferEnd)

proc advance*(s: InputStream) =
  if s.head != s.bufferEnd:
    s.head = offset(s.head, 1)
  discard s.syncRead()

proc read*(s: InputStream): byte =
  result = s.peek()
  advance s

proc checkReadAhead(s: InputStream, n: int): ptr byte =
  result = s.head
  doAssert distance(s.head, s.bufferEnd) >= n
  s.head = offset(s.head, n)

template readBytes*(s: InputStream, n: int): auto =
  makeOpenArray(checkReadAhead(s, n), n)

proc next*(s: InputStream): Option[byte] =
  if not s.eof:
    result = some s.read()

proc bufferPos(s: InputStream, pos: int): ptr byte =
  let offsetFromEnd = pos - s.bufferEndPos
  doAssert offsetFromEnd < 0
  result = offset(s.bufferEnd, offsetFromEnd)
  doAssert result >= s.bufferStart

proc pos*(s: InputStream): int {.inline.} =
  s.bufferEndPos - distance(s.head, s.bufferEnd)

proc firstAccessiblePos*(s: InputStream): int {.inline.} =
  s.bufferEndPos - distance(s.bufferStart, s.bufferEnd)

proc `[]`*(s: InputStream, pos: int): byte {.inline.} =
  s.bufferPos(pos)[]

proc rewind*(s: InputStream, delta: int) =
  s.head = offset(s.head, -delta)
  doAssert s.head >= s.bufferStart

proc rewindTo*(s: InputStream, pos: int) {.inline.} =
  s.head = s.bufferPos(pos)

template pos*(s: AsciiInputStream|Utf8InputStream): int =
  pos InputStream(s)

template eof*(s: AsciiInputStream|Utf8InputStream): bool =
  eof InputStream(s)

template close*(s: AsciiInputStream|Utf8InputStream) =
  close InputStream(s)

template advance*(s: AsciiInputStream) =
  advance InputStream(s)

template peek*(s: AsciiInputStream): char =
  char InputStream(s).peek()

template read*(s: var AsciiInputStream): char =
  char InputStream(s).read()

template next*(s: var AsciiInputStream): Option[char] =
  cast[Option[char]](InputStream(s).next())

