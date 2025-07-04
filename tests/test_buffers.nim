{.used.}

import unittest2, ../faststreams/buffers
import std/random

const bytes256 = block:
  var v: array[256, byte]
  for i in 0 ..< 256:
    v[i] = byte i
  v

proc consumeAll(buf: PageBuffers): seq[byte] =
  for page in buf.consumePages():
    result.add page.data()

# Note: these tests count `consume` calls as a way to make sure we don't waste
# buffers - however, the number of consume calls is not part of the stable API
# and therefore, tests may have to change in the future
suite "PageBuffers":
  test "prepare/commit/consume":
    let pageSize = 8
    let buf = PageBuffers.init(8)
    let input = bytes256[0 .. 31]

    # Write in one go
    buf.write(input)

    var res: seq[byte]

    var r = buf.consume()
    res.add r.data()
    check res == input

    # Buffer should now be empty
    check buf.consumable() == 0

  test "prepare/commit/consume with partial writes and reads":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    let input = bytes256[0 .. 15] # 16 bytes, 2 pages

    # Write in two steps
    var w1 = buf.prepare(8)
    w1.write(input[0 .. 7])
    buf.commit(8)

    var w2 = buf.prepare(8)
    w2.write(input[8 .. 15])
    buf.commit(8)

    # Consume part of first write
    var r = buf.consume(4)
    check @(r.data()) == input[0 .. 3]

    r = buf.consume()
    check @(r.data()) == input[4 .. 7]

    r = buf.consume()
    check @(r.data()) == input[8 .. 15]

    # Buffer should now be empty
    check buf.consumable() == 0

  test "reserve/commit/consume blocks until commit":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    let input = bytes256[0 .. 7]

    var w = buf.reserve(8)
    w.write(input)
    # Not committed yet, should not be readable
    check buf.consumable() == 0

    buf.commit(w)
    # Now it should be readable
    var r = buf.consume()
    check @(r.data()) == input

  test "reserve/prepare/commit/consume interleaved":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    let a = bytes256[0 .. 7]
    let b = bytes256[8 .. 15]

    var w1 = buf.reserve(8)
    w1.write(a)
    var w2 = buf.prepare(8)
    w2.write(b)
    buf.commit(w2)
    # Only b is committed, but a is reserved and not yet committed, so nothing is readable
    check buf.consumable() == 0

    buf.commit(w1)

    # Now both a and b should be readable, in order
    var r = buf.consume()
    check @(r.data()) == a
    r = buf.consume()
    check @(r.data()) == b
    check buf.consumable() == 0

  test "multiple small writes and reads, crossing page boundaries":
    let pageSize = 4
    let buf = PageBuffers.init(pageSize)
    let input = bytes256[0 .. 11] # 12 bytes, 3 pages

    # Write 3 times, 4 bytes each
    for i in 0 .. 2:
      var w = buf.prepare(4)
      w.write(input[(i * 4) ..< (i * 4 + 4)])
      buf.commit(4)

    # Read 6 times, 2 bytes each
    var res: seq[byte]
    for i in 0 .. 5:
      var r = buf.consume(2)
      res.add r.data()
    check res == input

    # Buffer should now be empty
    check buf.consumable() == 0

  test "unconsume restores data":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    let input = bytes256[0 .. 7]

    var w = buf.prepare(8)
    w.write(input)
    buf.commit(8)

    var r = buf.consume(8)
    check @(r.data()) == input

    # Unconsume 4 bytes
    buf.unconsume(4)
    var r2 = buf.consume(4)
    check @(r2.data()) == input[4 .. 7]

    # Buffer should now be empty
    check buf.consumable() == 0

  test "prepare with more than page size allocates larger page":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    let input = bytes256[0 .. 15] # 16 bytes

    var w = buf.prepare(16)
    w.write(input)
    buf.commit(16)

    var r = buf.consume(16)
    check @(r.data()) == input

  test "reserve with more than page size allocates larger page":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    let input = bytes256[0 .. 15] # 16 bytes

    var w = buf.reserve(16)
    w.write(input)
    buf.commit(w)

    var r = buf.consume(16)
    check @(r.data()) == input

  test "mix of prepare, reserve, commit, and consume with random order":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    var expected: seq[byte]
    var written: seq[(string, seq[byte], PageSpan)]
    randomize(1000)

    # Randomly choose prepare or reserve, then commit in random order
    for i in 0 .. 3:
      let d = bytes256[(i * 8) ..< (i * 8 + 8)]
      if rand(1) == 0:
        var w = buf.prepare(8)
        w.write(d)
        written.add(("prepare", d, w))
        buf.commit(8)
      else:
        var w = buf.reserve(8)
        w.write(d)
        written.add(("reserve", d, w))

    # Commit in random order
    var idxs = @[0, 1, 2, 3]
    idxs.shuffle()
    for i in idxs:
      let (kind, d, w) = written[i]
      if kind == "reserve":
        buf.commit(w)

    # All data should be readable in original order
    for i in 0 .. 3:
      expected.add(bytes256[(i * 8) ..< (i * 8 + 8)])
    check consumeAll(buf) == expected

  test "consumePages iterator yields all data and recycles last page":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    let input = bytes256[0 .. 23] # 24 bytes, 3 pages

    for i in 0 .. 2:
      var w = buf.prepare()
      w.write(input[i * 8 ..< ((i + 1) * 8)])
      buf.commit(w)

    var seen: seq[byte]
    for page in buf.consumePages():
      seen.add page.data()

    check seen == input
    # After consuming, the buffer should have one recycled page
    check buf.queue.len == 1
    check buf.queue.peekLast.consumedTo == 0
    check buf.queue.peekLast.writtenTo == 0

  test "consumePageBuffers yields correct pointers and lengths":
    let buf = PageBuffers.init(8)
    let input = bytes256[0 .. 15] # 16 bytes, 2 pages

    var w = buf.prepare()
    w.write(input[0 .. 7])
    buf.commit(w)
    w = buf.prepare()
    w.write(input[8 .. 15])
    buf.commit(w)

    var seen: seq[byte]
    for (p, len) in buf.consumePageBuffers():
      let s = cast[ptr UncheckedArray[byte]](p)
      for i in 0 ..< int(len):
        seen.add s[i]
    check seen == input

  test "commit less than prepared within a single page":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    let input = bytes256[0 .. 7]

    var w = buf.prepare(8)
    w.write(input)
    # Only commit 4 bytes
    buf.commit(4)

    var r = buf.consume()
    check @(r.data()) == input[0 .. 3]
    # The rest should not be available
    check buf.consumable() == 0

    # To write more, must call prepare again
    var w2 = buf.prepare(4)
    w2.write(input[4 .. 7])
    buf.commit(4)
    r = buf.consume()
    check @(r.data()) == input[4 .. 7]
    check buf.consumable() == 0

  test "commit less than reserved, then reserve again in same page":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    let input = bytes256[0 .. 7]

    var w = buf.reserve(8)
    w.write(input[0 .. 3])
    # Commit only 4 bytes
    buf.commit(w)
    # The committed reserve should be available also
    check buf.consumable() == 4

    var w2 = buf.reserve(4)
    w2.write(input[4 .. 7])
    buf.commit(w2)

    var r = buf.consume()
    check @(r.data()) == input[0 .. 3]
    r = buf.consume()
    check @(r.data()) == input[4 .. 7]
    check buf.consumable() == 0

  test "commit less than prepared, crossing page boundary":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    let input = bytes256[0 .. 15] # 16 bytes, 2 writes

    var w = buf.prepare(16)
    w.write(input)
    buf.commit(10)

    var r = buf.consume()
    check @(r.data()) == input[0 .. 9]
    # The rest should not be available
    check buf.consumable() == 0

    # To write more, must call prepare again
    var w2 = buf.prepare(6)
    w2.write(input[10 .. 15])
    buf.commit(6)
    r = buf.consume()
    check @(r.data()) == input[10 .. 15]
    check buf.consumable() == 0

  test "commit less than reserved, crossing page boundary, then reserve again":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    let input = bytes256[0 .. 15] # 16 bytes

    var w = buf.reserve(16)
    w.write(input[0 .. 9])
    # Commit only first 10 bytes
    buf.commit(w)
    check buf.consumable() == 10

    # Must reserve again
    var w2 = buf.reserve(6)
    w2.write(input[10 .. 15])
    buf.commit(w2)
    var r = buf.consume()
    check @(r.data()) == input[0 .. 9]
    r = buf.consume()
    check @(r.data()) == input[10 .. 15]
    check buf.consumable() == 0

  test "commit less than prepared/reserved, then interleave with new writes":
    let pageSize = 8
    let buf = PageBuffers.init(pageSize)
    let a = bytes256[0 .. 7]
    let b = bytes256[8 .. 15]

    # Prepare 8, commit 4
    var w1 = buf.prepare(8)
    w1.write(a)
    buf.commit(4)

    # Must prepare again
    var w1b = buf.prepare(4)
    w1b.write(a[4 .. 7])
    buf.commit(4)

    # Reserve 8, commit 4
    var w2 = buf.reserve(8)
    w2.write(b[0 .. 3])
    buf.commit(w2)

    check buf.consumable() == 12

    # Must reserve again
    var w2b = buf.reserve(4)
    w2b.write(b[4 .. 7])
    buf.commit(w2b)

    # All data should be readable in order
    var r = buf.consume()
    check @(r.data()) == a
    r = buf.consume()
    check @(r.data()) == b[0 .. 3]
    r = buf.consume()
    check @(r.data()) == b[4 .. 7]
    check buf.consumable() == 0
