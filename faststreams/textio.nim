import
  stew/ptrops,
  inputs, outputs, buffers, async_backend, multisync,
  system/formatfloat

template matchingIntType(T: type int64): type = uint64
template matchingIntType(T: type int32): type = uint32
template matchingIntType(T: type uint64): type = int64
template matchingIntType(T: type uint32): type = int32

# To reduce the produce code bloat, we will compile the integer
# handling functions only for the native type of the platform.
# Smaller int types will be automatically promoted to the native.
# On a 32-bit platforms, we'll also compile support for 64-bit types.
when sizeof(int) == sizeof(int64):
  type
    CompiledIntTypes = int64
    PromotedIntTypes = int8|int16|int32
    CompiledUIntTypes = uint64
    PromotedUintTypes = uint8|uint16|uint32
else:
  type
    CompiledIntTypes = int32|int64
    PromotedIntTypes = int8|int16
    CompiledUIntTypes = uint32|uint64
    PromotedUintTypes = uint8|uint16

# The following code implements writing numbers to a stream without going
# through Nim's `$` operator which will allocate memory.
# It's based on some speed comparisons of different methods presented here:
# http://www.zverovich.net/2013/09/07/integer-to-string-conversion-in-cplusplus.html

const
  digitsTable = block:
    var s = ""
    for i in 0..99:
      if i < 10: s.add '0'
      s.add $i
    s

  maxLen = ($BiggestInt.high).len + 4 # null terminator, sign

proc writeText*(s: OutputStream, x: CompiledUIntTypes) =
  var
    num: array[maxLen, char]
    pos = num.len

  template writeByteInReverse(c: char) =
    dec pos
    num[pos] = c

  var val = x
  while val > 99:
    # Integer division is slow so do it for a group of two digits instead
    # of for every digit. The idea comes from the talk by Alexandrescu
    # "Three Optimization Tips for C++".
    let base100digitIdx = (val mod 100) * 2
    val = val div 100

    writeByteInReverse digitsTable[base100digitIdx + 1]
    writeByteInReverse digitsTable[base100digitIdx]

  when true:
    if val < 10:
      writeByteInReverse char(ord('0') + val)
    else:
      let base100digitIdx = val * 2
      writeByteInReverse digitsTable[base100digitIdx + 1]
      writeByteInReverse digitsTable[base100digitIdx]
  else:
    # Alternative idea:
    # We now know enough to write digits directly to the stream.
    if val < 10:
      write s, byte(ord('\0') + val)
    else:
      let base100digitIdx = val * 2
      write s, digitsTable[base100digitIdx]
      write s, digitsTable[base100digitIdx + 1]

  write s, num.toOpenArray(pos, static(num.len - 1))

proc writeText*(s: OutputStream, x: CompiledIntTypes) =
  type MatchingUInt = matchingIntType typeof(x)

  if x < 0:
    s.write '-'
    # The `0 - x` trick below takes care of one corner case:
    # How do we get the abs value of low(int)?
    # The naive `-x` triggers an overflow, because low(int8)
    # is -128, while high(int8) is 127.
    writeText(s, MatchingUInt(0) - MatchingUInt(x))
  else:
    writeText(s, MatchingUInt(x))

when defined(c):

  proc writeText*(s: OutputStream, x: float64|float32|float) =
    ## Write the floating point number to the output stream. It has less overhead
    ## than `$` because it is directly written to the stream without
    ## allocating a standard Nim 'string'.
    ##
    ## Because this procedure uses stdlib, it is only active when Nim compiles
    ## with the 'c' flag. Otherwise, float types are passed to the generic writeText
    ## procedure.
    var buffer: array[65, char]
    let blen = writeFloatToBuffer(buffer, x)
    write s, buffer.toOpenArray(0, blen - 1)

template writeText*(s: OutputStream, str: string) =
  write s, str

template writeText*(s: OutputStream, val: auto) =
  write s, $val

proc writeHex*(s: OutputStream, bytes: openArray[byte]) =
  const hexChars = "0123456789abcdef"

  for b in bytes:
    s.write hexChars[int b shr 4 and 0xF]
    s.write hexChars[int b and 0xF]

proc writeHex*(s: OutputStream, chars: openArray[char]) =
  writeHex s, charsToBytes(chars)

const
  NewLines* = {'\r', '\n'}
  Digits* = {'0'..'9'}

proc readLine*(s: InputStream, keepEol = false): string {.fsMultiSync.} =
  fsAssert readableNow(s)

  while s.readable:
    let c = s.peek.char
    if c in NewLines:
      if keepEol:
        result.add c
        if c == '\r' and s.readable and s.peek.char == '\n':
          result.add s.read.char
      else:
        advance s
        if c == '\r' and s.readable and s.peek.char == '\n':
          advance s
      return

    result.add s.read.char

proc readUntil*(s: InputStream,
                sep: openArray[char]): Option[string] =
  fsAssert readableNow(s)
  var res = ""
  while s.readable(sep.len):
    if s.lookAheadMatch(charsToBytes(sep)):
      return some(res)
    res.add s.read.char

template nextLine*(sp: InputStream, keepEol = false): Option[string] =
  let s = sp
  if s.readable:
    some readLine(s, keepEol)
  else:
    none string

iterator lines*(s: InputStream, keepEol = false): string =
  while s.readable:
    yield readLine(s, keepEol)

proc readUnsignedInt*(s: InputStream, T: type[CompiledUIntTypes]): T =
  fsAssert s.readable and s.peek.char in Digits

  template eatDigitAndPeek: char =
    advance s
    if not s.readable: return
    s.peek.char

  var c = s.peek.char
  result = T(ord(c) - ord('0'))
  c = eatDigitAndPeek()
  while c.isDigit:
    # TODO: How do we handle the possible overflow here?
    result = result * 10 + T(ord(c) - ord('0'))
    c = eatDigitAndPeek()

template readUnsignedInt*(s: InputStream): uint =
  readUnsignedInt(s, uint)

proc readSignedInt*(s: InputStream, T: type[CompiledIntTypes]): Option[T] =
  if s.readable:
    let maybeSign = s.read.peek
    if maybeSign in {'-', '+'}:
      if not s.readable(2) or s.peekAt(1).char notin Digits:
        return
      else:
        advance s
    elif maybeSign notin Digits:
      return

    type UIntType = matchingIntType T
    let uintVal = readUnsignedInt(s, UIntType)

    if maybeSign == '-':
      if uintVal > UIntType(max(T)) + 1:
        return # Overflow. We've consumed part of the stream though.
               # TODO: Should we rewind it to a previous state?
      return some cast[T](UIntType(0) - uintVal)
    else:
      if uintVal > UIntType(max(T)):
        return # Overflow. We've consumed part of the stream though.
               # TODO: Should we rewind it to a previous state?
      return some T(uintVal)

