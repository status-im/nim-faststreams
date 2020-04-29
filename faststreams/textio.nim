import
  stew/ptrops,
  inputs, outputs, buffers

# The following code implements writing numbers to a stream without going
# through Nim's `$` operator which will allocate memory.
# It's based on some speed comparisons of different methods presented here:
# http://www.zverovich.net/2013/09/07/integer-to-string-conversion-in-cplusplus.html

# TODO Maybe the `writeText` proc shouldn't be instantiated for every integer
# type, but only for the largest "native" one. We can promote the rest with
# a template.

const
  digitsTable = block:
    var s = ""
    for i in 0..99:
      if i < 10: s.add '0'
      s.add $i
    s

  maxLen = ($BiggestInt.high).len + 4 # null terminator, sign

proc writeText*(s: OutputStream, x: SomeUnsignedInt) =
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

proc writeText*(s: OutputStream, x: SomeSignedInt) =
  # TODO: Determine this accurately
  type MatchingUInt = BiggestUInt

  if x < 0:
    s.write '-'
    # The `0 - x` trick below takes care of one corner case:
    # How do we get the abs value of low(int)?
    # The naive `-x` triggers an overflow, because low(int8)
    # is -128, while high(int8) is 127.
    writeText(s, MatchingUInt(0) - MatchingUInt(x))
  else:
    writeText(s, MatchingUInt(x))

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

