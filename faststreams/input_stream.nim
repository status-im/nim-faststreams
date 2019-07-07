import
  memfiles, options, stew/ranges/ptr_arith

const
  pageSize = 4096
  debugHelpers = false

type
  StreamReader = proc (bufferStart: ptr byte, bufferSize: int): int
  # TODO: use openarray once it's supported
  AsyncStreamReader = StreamReader # proc (bufferStart: ptr byte, bufferSize: int): Future[int]
  CloseStreamProc = proc ()

  ByteStream* = object of RootObj
    head*: ptr byte
    bufferSize: int
    bufferStart, bufferEnd: ptr byte
    cursorsList: ptr StreamCursor
    reader: StreamReader
    asyncReader: AsyncStreamReader
    closeStream: CloseStreamProc
    bufferEndPos: int

  StreamCursor* = object
    head, bufferEnd: ptr byte
    nextCursor: ptr StreamCursor

  BufferedStream*[size: static int] = object of ByteStream
    buffer: array[size, byte]

  AsciiStream* = distinct ByteStream
  UnicodeStream* = distinct ByteStream
  ObjectStream*[T] = distinct ByteStream

  # TODO: ByteStreamVar should become a `var` short-lived pointer once this is supported
  ByteStreamVar* = ref ByteStream
  AsciiStreamVar* = ref AsciiStream

proc openFile*(filename: string): ByteStreamVar =
  new result
  var memFile = memfiles.open(filename)
  result.head = cast[ptr byte](memFile.mem)
  when debugHelpers:
    result.bufferStart = result.head
  result.bufferEnd = result.head.shift memFile.size
  result.bufferEndPos = memFile.size
  result.closeStream = proc = close memFile

proc init*(T: type ByteStream,
           mem: openarray[byte],
           reader = StreamReader(nil),
           asyncReader = AsyncStreamReader(nil)): ByteStream =
  # TODO: the result should use `from mem` once it's supported
  result.head = unsafeAddr mem[0]
  result.bufferEnd = result.head.shift mem.len
  result.reader = reader
  result.asyncReader = asyncReader

proc memoryStream*(mem: openarray[byte]): ByteStreamVar = # TODO: mark the result with `from mem`
  new result
  result[] = ByteStream.init(mem)

proc memoryStream*(str: string): ByteStreamVar = # TODO: mark the result with `from str`
  new result
  result[] = init(ByteStream, str.toOpenArrayByte(0, str.len - 1))

proc init*(T: type BufferedStream,
           reader = StreamReader(nil),
           asyncReader = AsyncStreamReader(nil)): BufferedStream =
  result.init result.buffer, reader, asyncReader

proc syncRead(s: var ByteStream): bool =
  let bytesRead = s.reader(s.bufferStart, s.bufferSize)
  if bytesRead == 0:
    s.reader = nil
    return true
  else:
    s.bufferEnd = s.bufferStart.shift bytesRead
    s.bufferEndPos += bytesRead
    return false

proc ensureBytes*(s: var ByteStream, n: int): bool =
  if distance(s.head, s.bufferEnd) >= n:
    return true

  if s.reader == nil:
    return false

  doAssert false, "Multi-buffer reading will be implemented later"

proc eof*(s: var ByteStream): bool =
  if s.head != s.bufferEnd:
    return false

  if s.reader == nil:
    return true

  return s.syncRead()

proc eob*(s: ByteStream): bool {.inline.} =
  s.head != s.bufferEnd

proc peek*(s: ByteStream): byte {.inline.} =
  doAssert s.head != s.bufferEnd
  return s.head[]

when debugHelpers:
  proc showPosition*(s: ByteStream) =
    echo "head at ", distance(s.bufferStart, s.head), "/",
                     distance(s.bufferStart, s.bufferEnd)

proc advance*(s: var ByteStream) =
  if s.head != s.bufferEnd:
    s.head = s.head.shift 1
  elif s.reader != nil:
    discard s.syncRead()

proc read*(s: var ByteStream): byte =
  result = s.peek()
  advance s

proc checkReadAhead(s: ByteStreamVar, n: int): ptr byte =
  result = s.head
  doAssert distance(s.head, s.bufferEnd) >= n
  s.head = s.head.shift(n)

template readBytes*(s: ByteStreamVar, n: int): auto =
  makeOpenArray(checkReadAhead(s, n), n)

proc next*(s: var ByteStream): Option[byte] =
  if not s.eof:
    result = some s.read()

proc bufferPos(s: ByteStream, pos: int): ptr byte =
  let shiftFromEnd = pos - s.bufferEndPos
  doAssert shiftFromEnd < 0
  result = s.bufferEnd.shift shiftFromEnd
  doAssert result >= s.bufferStart

proc pos*(s: ByteStream): int {.inline.} =
  s.bufferEndPos - distance(s.head, s.bufferEnd)

proc firstAccessiblePos*(s: ByteStream): int {.inline.} =
  s.bufferEndPos - distance(s.bufferStart, s.bufferEnd)

proc `[]`*(s: ByteStream, pos: int): byte {.inline.} =
  s.bufferPos(pos)[]

proc rewind*(s: var ByteStream, delta: int) =
  s.head = s.head.shift(-delta)
  doAssert s.head >= s.bufferStart

proc rewindTo*(s: var ByteStream, pos: int) {.inline.} =
  s.head = s.bufferPos(pos)

# TODO: use a destructor once we migrate to Nim 0.20
proc close*(s: var ByteStream) =
  if s.closeStream != nil:
    s.closeStream()

# TODO: Use `distrinct with` once it's supported
template pos*(s: AsciiStream|UnicodeStream|ObjectStream): int =
  ByteStream(s).pos

template eof*(s: var (AsciiStream|UnicodeStream|ObjectStream)): bool =
  ByteStream(s).eof

template eob*(s: var (AsciiStream|UnicodeStream|ObjectStream)): bool =
  ByteStream(s).eob

template close*(s: AsciiStream|UnicodeStream|ObjectStream) =
  close ByteStream(s)

template advance*(s: var AsciiStream) =
  advance ByteStream(s)

template peek*(s: AsciiStream): char =
  char ByteStream(s).peek()

template read*(s: var AsciiStream): char =
  char ByteStream(s).read()

template next*(s: var AsciiStream): Option[char] =
  cast[Option[char]](ByteStream(s).next())

