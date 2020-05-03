import
  ../faststreams

template cbBase(a, b): untyped = [
  'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
  'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
  'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
  'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
  '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', a, b]

const
  cb64 = cbBase('+', '/')
  invalidChar = 255
  paddingByte = byte('=')

template encodeSize(size: int): int = (size * 4 div 3) + 6

import
  ../faststreams/buffers

proc base64encode*(i: InputStream, o: OutputStream) {.fsMultiSync.} =
  var
    n: uint32
    b: uint32

  template inputByte(exp: untyped) =
    b = uint32(i.read)
    n = exp

  template outputChar(x: typed) =
    o.write cb64[x and 63]

  let inputLen = i.len
  if inputLen.isSome:
    o.ensureRunway encodeSize(inputLen.get)

  while i.readable(3):
    inputByte(b shl 16)
    inputByte(n or b shl 8)
    inputByte(n or b shl 0)
    outputChar(n shr 18)
    outputChar(n shr 12)
    outputChar(n shr 6)
    outputChar(n shr 0)

  if i.readable:
    inputByte(b shl 16)
    if i.readable:
      inputByte(n or b shl 8)
      outputChar(n shr 18)
      outputChar(n shr 12)
      outputChar(n shr 6)
      o.write paddingByte
    else:
      outputChar(n shr 18)
      outputChar(n shr 12)
      o.write paddingByte
      o.write paddingByte

  close o

proc initDecodeTable*(): array[256, char] =
  # computes a decode table at compile time
  for i in 0 ..< 256:
    let ch = char(i)
    var code = invalidChar
    if ch >= 'A' and ch <= 'Z': code = i - 0x00000041
    if ch >= 'a' and ch <= 'z': code = i - 0x00000047
    if ch >= '0' and ch <= '9': code = i + 0x00000004
    if ch == '+' or ch == '-': code = 0x0000003E
    if ch == '/' or ch == '_': code = 0x0000003F
    result[i] = char(code)

const
  decodeTable = initDecodeTable()

proc base64decode*(i: InputStream, o: OutputStream) {.fsMultiSync.} =
  proc decodeSize(size: int): int =
    return (size * 3 div 4) + 6

  proc raiseInvalidChar(c: byte, pos: int) {.noReturn.} =
    raise newException(ValueError,
      "Invalid base64 format character `" & char(c) & "` at location " & $pos & ".")

  template inputChar(x: untyped) =
    let c = i.read()
    let x = int decodeTable[c]
    if x == invalidChar:
      raiseInvalidChar(c, i.pos - 1)

  template outputChar(x: untyped) =
    o.write char(x and 255)

  let inputLen = i.len
  if inputLen.isSome:
    o.ensureRunway decodeSize(inputLen.get)

  # hot loop: read 4 characters at at time
  while i.readable(8):
    inputChar(a)
    inputChar(b)
    inputChar(c)
    inputChar(d)
    outputChar(a shl 2 or b shr 4)
    outputChar(b shl 4 or c shr 2)
    outputChar(c shl 6 or d shr 0)

  if i.readable(4):
    inputChar(a)
    inputChar(b)
    outputChar(a shl 2 or b shr 4)

    if i.peek == paddingByte:
      let next = i.peekAt(1)
      if next != paddingByte:
        raiseInvalidChar(next, i.pos + 1)
    else:
      inputChar(c)
      outputChar(b shl 4 or c shr 2)
      if i.peek != paddingByte:
        inputChar(d)
        outputChar(c shl 6 or d shr 0)
  elif i.readable:
    raise newException(ValueError, "The input stream has insufficient nymber of bytes for base64 decoding")

  close o

