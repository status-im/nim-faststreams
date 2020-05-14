import
  stew/ptrops,
  inputs, outputs, buffers, async_backend, multisync,
  math

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
    CompiledIntTypes = int|int64
    PromotedIntTypes = int8|int16
    CompiledUIntTypes = uint|uint64
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

when defined(aa):
  proc writeText*(s: OutputStream, x: float64|float) =  # this version mostly works, but does not account for rounding error
    var whole = x.int
    writeText(s, whole)
    var frac = abs(x - whole.float)
    if frac == 0.0:
      return
    write s, '.'
    var limit: byte = 8
    while (frac != 0.0) and (limit > 0):
      limit -= 1
      frac *=  10.0
      whole = frac.int
      frac -= whole.float
      write s, char(ord('0') + whole)

when defined(bb):
  proc writeText*(s: OutputStream, x: float64|float) =   # this version just prints the integer part
    writeText(s, x.int)

when defined(cc):
  proc writeText*(s: OutputStream, x: float64|float) =   # this version uses ftoa of stdlibc
    proc floatToStr(x: float): string {.magic: "FloatToStr", noSideEffect.}
    writeText(s, floatToStr(x))

when defined(dd):
  proc writeText*(s: OutputStream, x: float64|float) =   # this version uses ftoa of stdlibc
    write s, $x

when defined(ee):
  proc writeText*(s: OutputStream, x: float64|float) =  # this version mostly works, but does not account for rounding error
    if x == 0.0:
      write s, "0.0"
      return
    if x < 0.0:
      write s, '-'
    var frac = x.abs
    var place = log10(frac).int
    if place < 0:
      place = 0
    frac /= pow(10.0, place.float)
    var whole: int
    while (frac != 0.0) and (place > -100):
      if place == -1:
        write s, '.'
      whole = frac.int
      write s, char(ord('0') + whole)
      frac = (frac - whole.float) * 10.0
      place -= 1

when defined(ff):
  proc writeText*(s: OutputStream, x: float64|float) =  # handles rounding
    const PRECISION_LIMIT = 16
    const DECIMAL_POINT: uint8 = 255
    if x == 0.0:
      write s, "0.0"
      return
    var digits: array[322, byte]
    var index: int = 0
    if x < 0.0:
      write s, '-'
    var precision: int = 0
    var firstDigitFound = false
    var decimalPointIndex = -1
    var frac = x.abs
    var place = log10(frac).int
    if place < 0:
      place = 0
    frac /= pow(10.0, place.float)
    var whole: uint8
    #
    # run through the digits:
    #
    while (frac != 0.0) and (place > -317) and (precision < PRECISION_LIMIT):
      if place == -1:
        digits[index] = DECIMAL_POINT
        decimalPointIndex = index
        index += 1
      whole = frac.uint8
      digits[index] = whole
      index += 1
      if firstDigitFound:
        precision += 1
      else:
        if whole != 0.uint8:
          firstDigitFound = true
          precision += 1
      frac = (frac - whole.float) * 10.0
      place -= 1
    #
    # place any missing decimal points as a key marker
    #
    if decimalPointIndex == -1:
      # if decimal point not expressed yet, then add it
      index += place + 1
      digits[index] = DECIMAL_POINT
      decimalPointIndex = index
      index += 1
    index -= 1
    #
    # remove trailing digits and adjust precision back if need be:
    #
    while (digits[index] == 0) and (index > 0):
      precision -= 1
      index -= 1
    #
    # if still at precision limit, then do rounding:
    #
    if precision >= PRECISION_LIMIT:
      while index > 0:
        if digits[index] == DECIMAL_POINT:
          index -= 1
          continue
        elif digits[index] >= 5:
          digits[index - 1] += 1
          digits[index] = 0
          if digits[index - 1] < 10:
            break
        else:
          digits[index] = 0
          break
        index -= 1
    #
    # remove trailing digits and adjust precision back if need be:
    #
    while (digits[index] == 0) and (index > 0):
      precision -= 1
      index -= 1
    #
    # express to the stream
    #
    if index < decimalPointIndex:
      index = decimalPointIndex
    for i in 0 .. index:
      let d = digits[i]
      if d == DECIMAL_POINT:
        write s, '.'
      elif d == 10:
        write s, "10"
      else:
        write s, char(ord('0') + d)
    if digits[index] == DECIMAL_POINT:
      write s, '0'

template writeText*(s: OutputStream, str: string) =
  write s, str

template writeText*(s: OutputStream, val: auto) =
  write s, $val

proc writeHex*(s: OutputStream, bytes: openarray[byte]) =
  const hexChars = "0123456789abcdef"

  for b in bytes:
    s.write hexChars[int b shr 4 and 0xF]
    s.write hexChars[int b and 0xF]

proc writeHex*(s: OutputStream, chars: openarray[char]) =
  writeHex s, charsToBytes(chars)

const
  NewLines* = {'\r', '\n'}
  Digits* = {'0'..'9'}

proc readLine*(s: InputStream, keepEol = false): TaintedString =
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
                sep: openarray[char]): Option[TaintedString] =
  fsAssert readableNow(s)
  var res = ""
  while s.readable(sep.len):
    if s.lookAheadMatch(charsToBytes(sep)):
      return some(res)
    res.add s.read.char

template nextLine*(sp: InputStream, keepEol = false): Option[TaintedString] =
  let s = sp
  if s.readable:
    some readLine(s, keepEol)
  else:
    none string

iterator lines*(s: InputStream, keepEol = false): TaintedString =
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

