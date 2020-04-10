import
  memfiles, options,
  stew/[ptrops, ranges/ptr_arith]

type
  # We inherit from RootObj because in layered streams
  # the stream itself is often used as an `outputDevice`
  InputStreamObj = object of RootObj
    head*: ptr byte
    bufferSize: int
    bufferStart, bufferEnd: ptr byte
    bufferEndPos: int
    inputDevice*: RootRef
    vtable*: ptr InputStreamVTable

  InputStream* = ref InputStreamObj

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

  CloseStreamProc* = proc (s: InputStream)
                          {.nimcall, gcsafe, raises: [IOError, Defect].}

  InputStreamVTable* = object
    readSync*: ReadSyncProc
    readAsync*: ReadAsyncProc
    closeStream*: CloseStreamProc
    hasKnownLen*: bool

  FileInput = ref object of RootObj
    file: MemFile

const
  debugHelpers = false
  nimAllocatorMetadataSize* = 0
    # TODO: Get this from Nim's allocator.
    # The goal is to make perfect page-aligned allocations
  defaultPageSize = 4096 - nimAllocatorMetadataSize

let FileStreamVTable = InputStreamVTable(
  readSync: nil,
  readAsync: nil,
  closeStream: proc (s: InputStream)
               {.nimcall, gcsafe, raises: [IOError, Defect].} =
    try:
      close FileInput(s.inputDevice).file
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

proc fileInput*(filename: string): InputStream =
  let
    inputDevice = FileInput(file: memfiles.open(filename))
    head = cast[ptr byte](inputDevice.file.mem)
    fileSize = inputDevice.file.size

  result = InputStream(
    head: head,
    bufferEnd: offset(head, fileSize),
    bufferEndPos: fileSize,
    inputDevice: inputDevice,
    vtable: vtableAddr FileStreamVTable)

  when debugHelpers:
    result.bufferStart = result.head

proc implementInputStream*(vtable: ptr InputStreamVTable,
                           inputDevice: RootRef,
                           pageSize = defaultPageSize): InputStream =
  # TODO: We need to allocate memory here and start reading
  InputStream(inputDevice: inputDevice,
              vtable: vtable)

proc memoryInput*(mem: openarray[byte]): InputStream =
  let head = unsafeAddr mem[0]
  InputStream(
    head: head,
    bufferEnd: offset(head, mem.len),
    bufferEndPos: mem.len)

proc memoryInput*(str: string): InputStream =
  memoryInput str.toOpenArrayByte(0, str.len - 1)

proc endPos*(s: InputStream): int =
  doAssert s.vtable == nil or s.vtable.hasKnownLen
  return s.bufferEndPos

proc syncRead(s: InputStream): bool =
  let bytesRead = s.vtable.readSync(s, s.bufferStart, s.bufferSize)
  if bytesRead == 0:
    # TODO close the input device
    s.inputDevice = nil
    return true
  else:
    s.bufferEnd = offset(s.bufferStart, bytesRead)
    s.bufferEndPos += bytesRead
    return false

proc ensureBytes*(s: InputStream, n: int): bool =
  if distance(s.head, s.bufferEnd) >= n:
    return true

  if s.inputDevice == nil:
    return false

  # TODO
  doAssert false, "Multi-buffer reading will be implemented later"

proc eof*(s: InputStream): bool =
  if s.head != s.bufferEnd:
    return false

  if s.inputDevice == nil:
    return true

  return s.syncRead()

#proc eob*(s: InputStream): bool {.inline.} =
#  s.head != s.bufferEnd

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
  elif s.inputDevice != nil:
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

# TODO: use a destructor once we migrate to Nim 0.20
# TODO: It's not appropriate for this to raise
proc close*(s: InputStream) {.raises: [IOError, Defect].} =
  if s.vtable != nil:
    s.vtable.closeStream(s)

template pos*(s: AsciiInputStream|Utf8InputStream): int =
  InputStream(s).pos

template eof*(s: AsciiInputStream|Utf8InputStream): bool =
  InputStream(s).eof

#template eob*(s: AsciiInputStream|Utf8InputStream): bool =
#  InputStream(s).eob

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

