import
  ranges/ptr_arith

type
  OutputStreamVTable = tuple
    prepareOutput: proc (s: ptr OutputStream, size: int) {.nimcall.}
    finish: proc (s: ptr OutputStream) {.nimcall.}

  OutputStream* = object {.inheritable.}
    head, bufferEnd: ptr byte
    vtable: ptr OutputStreamVTable

  MemoryOutputStream*[T] = object of OutputStream
    output: T

  StringOutputStream* = MemoryOutputStream[string]
  BytesOutputStream* = MemoryOutputStream[seq[byte]]

  FileOutputStream* = object of OutputStream
    file: File

const
  pageSize = 4096

proc prepareOutputImpl[T](s: ptr OutputStream, size: int) =
  let s = cast[ptr T](s)
  let currentSize = s.output.len
  s.output.setLen(s.output.len + pageSize)
  s.head = cast[ptr byte](addr s.output[currentSize])
  s.bufferEnd = cast[ptr byte](shift(addr s.output[0], s.output.len))

proc finishImpl[T](s: ptr OutputStream) =
  let s = cast[ptr T](s)
  s.output.setLen(s.output.len - distance(s.head, s.bufferEnd))

proc init*(T: type MemoryOutputStream): T =
  var vtable {.global.} = (prepareOutputImpl[T], finishImpl[T])
  result.vtable = addr vtable
  result.vtable.prepareOutput(addr result, pageSize)

proc append*(s: var OutputStream, b: byte) =
  if s.head == s.bufferEnd:
    s.vtable.prepareOutput(addr s, pageSize)

  s.head[] = b
  s.head = shift(s.head, 1)

template append*(s: var OutputStream, c: char) =
  s.append byte(c)

proc flush*(s: var OutputStream) =
  s.vtable.finish(addr s)

proc getOutput*(s: var MemoryOutputStream): auto =
  flush s
  shallow s.output
  return s.output

proc append*(s: var OutputStream, bytes: openarray[byte]) =
  # TODO: this can use copyMem
  for b in bytes:
    s.append b

proc append*(s: var OutputStream, chars: openarray[char]) =
  # TODO: this can use copyMem
  for c in chars:
    s.append byte(c)

template append*(s: var OutputStream, str: string) =
  s.append str.toOpenArrayByte(0, str.len - 1)

# Any stream

proc appendNumberImpl(s: var OutputStream, number: BiggestInt) =
  # TODO: don't allocate
  s.append $number

proc appendNumberImpl(s: var OutputStream, number: BiggestUInt) =
  # TODO: don't allocate
  s.append $number

template toBiggestRepr(i: SomeUnsignedInt): BiggestUInt =
  BiggestUInt(i)

template toBiggestRepr(i: SomeSignedInt): BiggestInt =
  BiggestInt(i)

template appendNumber*(s: var OutputStream, i: SomeInteger) =
  # TODO: specify radix/base
  appendNumberImpl(s, toBiggestRepr(i))

