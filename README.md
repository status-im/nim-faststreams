# nim-faststreams

[![Build Status (Travis)](https://img.shields.io/travis/status-im/nim-faststreams/master.svg?label=Linux%20/%20macOS "Linux/macOS build status (Travis)")](https://travis-ci.org/status-im/nim-faststreams)
[![Windows build status (Appveyor)](https://img.shields.io/appveyor/ci/nimbus/nim-faststreams/master.svg?label=Windows "Windows build status (Appveyor)")](https://ci.appveyor.com/project/nimbus/nim-faststreams)
[![License: Apache](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
![Stability: experimental](https://img.shields.io/badge/stability-experimental-orange.svg)
![Github action](https://github.com/status-im/nim-faststreams/workflows/nim-faststreams%20CI/badge.svg)

FastStreams is a highly efficient library for all your I/O needs.

It offers nearly zero-overhead synchronous and asynchronous streams
for handling inputs and outputs of various types:

* Memory inputs and outputs for serialization frameworks and parsers
* File inputs and outputs
* Pipes and Process I/O
* Networking

The library aims to provide a common interface between all stream types
that allows the application code to be easily portable to different back-end
event loops. In particular, [Chronos](https://github.com/status-im/nim-chronos)
and [AsyncDispatch](https://nim-lang.org/docs/asyncdispatch.html)
are already supported. It's envisioned that the library will also
gain support for the Nginx event loop to allow the creation of web
applications running as Nginx run-time modules and the [SeaStar event loop](http://seastar.io/)
for the development of extremely low-latency services taking advantage
of [kernel-bypass networking](https://blog.cloudflare.com/kernel-bypass/).

## What does zero-overhead mean?

Even though FastStreams support multiple stream types, the API is designed
in a way that allows the read and write operations to be handled without any
dynamic dispatch in the majority of cases.

In particular, reading from a `memoryInput` or writing to a `memoryOutput`
will have similar performance to a loop iterating over an `openarray` or
another loop populating a pre-allocated `string`. `memFileInput` offers
the same performance characteristics when working with files. The idiomatic
use of the APIs with the rest of the stream types will result in a highly
efficient memory allocation patterns and zero-copy performance in a great
variety of real-world use cases such as:

* Parsers for data formats and protocols employing formal grammars
* Block ciphers
* Compressors and decompressors
* Stream multiplexers

The zero-copy behavior and low-memory usage is maintained even when multiple
streams are layered on top of each other while back-pressure is properly
accounted for. This makes FastStreams ideal for implementing highly-flexible
networking stacks such as [LibP2P](https://github.com/status-im/nim-libp2p).

## The key ideas in the FastStreams design

FastStreams is heavily inspired by the `System.IO.Pipelines` API which was
developed and released by Microsoft in 2018 and is considered the result of
multiple years of evolution over similar APIs shipped in previous SDKs.

We highly recommend reading the following two articles which provide an in-depth
explanation for the benefits of the design:

* https://blog.marcgravell.com/2018/07/pipe-dreams-part-1.html
* https://blog.marcgravell.com/2018/07/pipe-dreams-part-2.html

Here, we'll only summarize the main insights:

### Obtaining data from the input device is not the same as consuming it.

When protocols and formats are layered on top of each other, it's highly
inconvenient to handle a read operation that can return an arbitrary amount
of data. If not enough data was returned, you may need to copy the available
bytes into a local buffer and then repeat the reading operation until enough
data is gathered and the local buffer can be processed. On the other hand,
if more data was received, you need to complete the current stage of processing
and then somehow feed the remaining bytes into the next stage of processing
(e.g this might be a nested format or a different parsing branch in the formal
 grammar of the protocol). Both of these scenarios require logic that is
difficult to write correctly and results in unnecessary copying of the input
bytes.

A major difference in the FastStreams design is that the arbitrary-length
data obtained from the input device is managed by the stream itself and you
are provided with an API allowing you to control precisely how much data
is consumed from the stream. Consuming the buffered content does not invoke
costly asynchronous calls and you are allowed to peek at the stream contents
before deciding which step to take next (something crucial for handling formal
grammars). Thus, using the FastStreams API results in code that is both highly
efficient and easy to author.

### Higher efficiency is possible if we say goodbye to the good old single buffer.

The buffering logic inside the stream divides the data into "pages" which
are allocated with a known fast path in the Nim allocator and which can be
efficiently transferred between streams and threads in the layered streams
scenario or in IPC mechanisms such as `AsyncChannel`. The consuming code can
be aware of this, but doesn't need to. The most idiomatic usage of the API
handles the buffer switching logic automatically for the user.

Nevertheless, the buffering logic can be configured for unbuffered reads
and writes and it supports efficiently various common real-world patterns
such as:

* Length prefixes

  To handle protocols with length prefixes without any memory overhead,
  the output streams support "delayed writes" where a portion of the
  stream content is specified only after the prefixed content is written
  to the stream.

* Block compressors and Block ciphers

  These can benefit significantly from a more precise control over
  the stride of the buffered pages which can be configured to match
  the block size of the encoder.

* Content with known length

  Some streams have a known length which allows us to accurately estimate
  the size of the transformed content. The `len` and `ensureRunway` APIs
  make sure such cases are handled as optimally as possible.

## Basic API usage

The FastStreams API consists of ony few major object types:

### `InputStream`

An `InputStream` manages a particular input device. The library offers out
of the box the following input stream types:

* `fileInput`

  For reading files through the familiar `fread` API from the C run-time.

* `memFileInput`

  For reading memory mapped files which provides best performance.

* `unsafeMemoryInput`

  For handling strings, sequences and openarrays as an input stream. <br />
  You are responsible for ensuring that the backing buffer won't be invalidated
  while the stream is being used.

* `memoryInput`

  Primarily used to consume the contents written to a previously populated
  output stream, but it can also be used to consume the contents of strings
  and sequences in a memory-safe way (by creating a copy).

* `pipeInput` (async)

  For arbitrary conmmunication between a produced and a consumer.

* `chronosInput` (async)

  Enabled by importing `faststreams/chronos_adapters`. <br />
  It can represent any Chronos `Transport` as an input stream.

* `asyncSocketInput` (async)

  Enabled by importing `faststreams/std_adapters`. <br />
  Allows using Nim's standard library `AsyncSocket` type as an input stream.

You can extend the library with new `InputStream` types without modifying it.
Please see the inline code documentation of `InputStreamVTable` for more details.

All of the above APIs are possible constructors for creating an `InputStream`.
The stream instances will manage their resources through destructors, but you
might want to `close` them explicitly in async context or when you need to
handle the possible errors from the closing operation.

Here is an example usage:

```nim
import
  faststreams/inputs

var
  jsonString = "[1, 2, 3]"
  jsonNodes = parseJson(unsafeMemoryInput(jsonString))
  moreNodes = parseJson(fileInput("data.json"))
```

The example above assumes we might have a `parseJson` function accepting an
`InputStream`. Here how this function could be defined:

```nim
import
  faststreams/inputs

proc scanString(stream: InputStream): JsonToken {.fsMultiSync.} =
  result = newStringToken()

  advance stream # skip the opening quote

  while stream.readable:
    let nextChar = stream.read.char
    case nextChar
    of '\'':
      if stream.readable:
        let escaped = stream.read.char
        case escaped
        of 'n': result.add '\n'
        of 't': result.add '\t'
        else: result.add escaped
      else:
        error(UnexpectedEndOfFile)
    of '"'
      return
    else:
      result.add nextChar

  error(UnexpectedEndOfFile)

proc nextToken(stream: InputStream): JsonToken {.fsMultiSync.} =
  while stream.readable:
    case stream.peek.char
    of '"':
      result = scanString(stream)
    of '0'..'9':
      result = scanNumber(stream)
    of 'a'..'z', 'A'..'Z', '_':
      result = scanIdentifier(stream)
    of '{':
      advance stream # skip the character
      result = objectStartToken
    ...

  return eofToken

proc parseJson(stream: InputStream): JsonNode {.fsMultiSync.} =
  while (let token = nextToken(stream); token != eofToken):
    case token
    of numberToken:
      result = newJsonNumber(token.num)
    of stringToken:
      result = newJsonString(token.str)
    of objectStartToken:
      result = parseObject(stream)
    ...
```

The above example is nothing but a toy program, but we can already see many
usage patterns of the `InputStream` type. For a more sophisticated and complete
implementation of a JSON parser, please see the [nim-json-serialization](https://github.com/status-im/nim-json-serialization)
package.

As we can see from the example above, calling `stream.read` should always be
preceded by a call to `stream.readable`. When the stream is in the readable
state, we can also `peek` at the next character before we decide how to
proceed. Besides calling `read`, we can also mark the data as consumed by
calling `stream.advance`.

The above APIs demonstrate how you can consume the data one byte at the time.
Common wisdom might tell you that this should be inefficient, but that's not
the case with FastStreams. The loop `while stream.readable: stream.read` will
compile to very efficient inlined code that performs nothing more than pointer
increments and comparisons. This will be true even when working with async
streams.

The `readable` check is the only place where our code may block (or await).
Only when all the data in the stream buffers have been consumed, the stream
will invoke a new read operation on the backing input device and this may
repopulate the buffers with an arbitrary number of new bytes.

Sometimes, you need to check whether the stream contains at least a specific
number of bytes. You can use the `stream.readable(N)` API to achieve this.

Reading multiple bytes at once is then possible with `stream.read(N)`, but
if you need to store the bytes in an object field or another long-term storage
location, consider using `stream.readInto(destination)` which may result in
zero-copy operation. It can also be used to implement unbuffered reading.

#### `AsyncInputStream` and `fsMultiSync`

An astute reader might have wondered what is the purpose of the custom pragma
`fsMultiSync` used in the examples above? It is a simple macro generating an
additional `async` copy of our stream processing functions where all the input
types are replaced by their async counterparts (e.g. `AsyncInputStream`) and
the return type is wrapped in a `Future` as usual.

The standard API of `InputStream` and `AsyncInputStream` is exactly the same.
Operations such as `readable` will just invoke `await` behind the scenes, but
there is one key difference - the `await` will be triggered only when there
is not enough data already stored in the stream buffers. Thus, in the great
majority of cases, we avoid the high cost of instantiating a `Future` and
yielding control to the event loop.

We highly recommend implementing most of your stream processing code through
the `fsMultiSync` pragma. This ensures the best possible performance and makes
the code more easily testable (e.g. with inputs stored on disk). FastStreams
ships with a set of fuzzing tools that will help you ensure that your code
behaves correctly with arbitrary data and/or arbitrary interruption points.

Nevertheless, if you need a more traditional async API, please be aware that
all of the functions discussed in this README also have an `*Async` suffix
form that returns a `Future` (e.g. `readableAsync`, `readAsync`, etc).

One exception to the above rule is the helper `stream.timeoutToNextByte(t)`
which can be used to detect situations where your communicating party is
failing to send data in time. It accepts a `Duration` or an existing deadline
`Future` and it's usually used like this:

```nim
proc performHandshake(c: Connection): bool {.async.} =
  if c.inputStream.timeoutToNextByte(HANDSHAKE_TIMEOUT):
    # The other party didn't send us anything in time,
    # We close the connection:
    close c
    return false

  while c.inputStream.readable:
    ...
```

It is assumed that in traditional async code, timeouts will be managed more
explicitly with `sleepAsync` and the `or` operator defined over futures.

#### Range-restricted reads

Protocols transmitting serialized payloads often provide information regarding
the size of the payload. When you invoke the deserialization routine, it's
preferable if the provided boundaries are treated like an "end of file" marker
for the deserializer. FastStreams provides an easy way to achieve this without
extra copies and memory allocations through the `withReadableRange` facility.
Here is a typical usage:

```nim
proc decodeFrame(s: AsyncInputStream, DecodedType: type): Option[DecodedType] =
  if not s.readable(4):
    return

  let lengthPrefix = toInt32 s.read(4)
  if s.readable(lengthPrefix):
    s.withReadableRange(lengthPrefix, range):
      range.readValue(Json, DecodedType)
```

Please note that the above example uses the [nim-serialization library](https://github.com/status-im/nim-serialization/)

Simply, inside the `withReadableRange` block, `range` becomes a stream for
which `s.readable` will return `false` as soon as the Json parser has consumed
the specified number of bytes.

Furthermore, `withReadableRange` guarantees that all stream operations within
the block will be non-blocking, so it will transform the `AsyncInputStream`
into a regular `InputStream`. Depending on the complexity of the stream
processing functions, this will often lead to significant performance gains.

### `OutputStream` and `AsyncOutputStream`

An `OutputStream` manages a particular output device. The library offers out
of the box the following output stream types:

* `writeFileOutput`

  For writing files through the familiar `fwrite` API from the C run-time.

* `memoryOutput`

  For building a `string` or a `seq[byte]` result.

* `unsafeMemoryOutput`

  For writing to an arbitrary existing buffer. <br />
  You are responsible for ensuring that the backing buffer won't be invalidated
  while the stream is being used.

* `pipeOutput` (async)

  For arbitrary conmmunication between a produced and a consumer.

* `chronosOutput` (async)

  Enabled by importing `faststreams/chronos_adapters`. <br />
  It can represent any Chronos `Transport` as an input stream.

* `asyncSocketOutput` (async)

  Enabled by importing `faststreams/std_adapters`. <br />
  Allows using Nim's standard library `AsyncSocket` type as an output stream.

You can extend the library with new `OutputStream` types without modifying it.
Please see the inline code documentation of `OutputStreamVTable` for more details.

All of the above APIs are possible constructors for creating an `OutputStream`.
The stream instances will manage their resources through destructors, but you
might want to `close` them explicitly in async context or when you need to
handle the possible errors from the closing operation.

Here is an example usage:

```nim
import
  faststreams/outputs

type
  ABC = object
    a: int
    b: char
    c: string

var stream = memoryOutput()
stream.writeNimRepr(ABC(a: 1, b: 'b', c: "str"))
var repr = stream.getOutput(string)
```

The `writeNimRepr` in the above example is not part of the library, but
let's see how it can be implemented:

```nim
import
  typetraits, faststreams/outputs

proc writeNimRepr*(stream: OutputStream, str: string) =
  stream.write '"'

  for c in str:
    if c == '"':
      stream.write ['\'', '"']
    else:
      stream.write c

  stream.write '"'

proc writeNimRepr*(stream: OutputStream, x: char) =
  stream.write ['\'', x, '\'']

proc writeNimRepr*(stream: OutputStream, x: int) =
  stream.write $x # Making this more optimal has been left
                  # as an exercise for the reader

proc writeNimRepr*[T](stream: OutputStream, obj: T) =
  stream.write typetraits.name(T)
  stream.write '('

  var firstField = true
  for name, val in fieldPairs(obj):
    if not firstField:
      stream.write ", "

    stream.write name
    stream.write ": "
    stream.writeNimRepr val

    firstField = false

  stream.write ')'
```

When the stream is created, its output buffers will be initialized with a
single page of `pageSize` bytes (specified at stream creation). Calls to
`write` will just populate this page until it becomes full and only then
it would be sent to the output device.

As the example demonstrates, a `memoryOutput` will continue buffering
pages until they can be finally concatenated and returned in `stream.getOutput`.
If the output fits within a single page, it will be efficiently moved to
the `getOutput` result. When the output size is known upfront you can ensure
that this optimization is used by calling `stream.ensureRunway` before any
writes, but please note that the library is free to ignore this hint in async
context or if a maximum memory usage policy is specified.

In a non-memory stream, any writes larger than a page or issued through the
`writeNow` API will be sent to the output device immediately.

Please note that even in async context, `write` will complete immediately.
To handle back-pressure properly, use `stream.flush` or `stream.waitForConsumer`
which will ensure that the buffered data is drained to a specified number of
bytes before continuing. The rationale here is that introducing an interruption
point at every `write` produces less optimal code, but if this is desired you
can use the `stream.writeAndWait` API.

#### Delayed Writes

Many protocols and formats employ fixed-size and variable-size length prefixes
that have been tradionally difficult to handle because they require you to
either measure the size of the content before writing it to the stream, or
even worse, serialize it to a memory buffer in order to determine its size.

FastStreams supports handling such length prefixes with a zero-copy mechanism
that doesn't require additional memory allocations. `stream.delayFixedSizeWrite`
and `stream.delayVarSizeWrite` are APIs that return a `WriteCursor` object that
can be used to implement a delayed write to the stream. After obtaining the
write cursor you can take a note of the current `pos` in the stream and then
continue issuing `stream.write` operations normally. After all of the content
is written, you obtain `pos` again to determine the final value of the length
prefix. Throughout the whole time, you are free to call `write` on the cursor
to populate the "hole" left in the stream with bytes, but at the end you must
call `finalize` to unlock the stream for flushing. You can also perform the
finalization in one step with `finalWrite` (the one-step approach is manatory
for variable-size prefixes).

### `Pipeline`

(This section is a stub and it will be expanded with more details in the future)

A `Pipeline` represents a chain of transformations that should be applied to a
stream. It starts with an `InputStream` followed by one or more transformation
steps and ending with a result.

Each transformation step is a function of the kind:

```nim
type PipelineStep* = proc (i: InputStream, o: OutputStream)
                          {.gcsafe, raises: [Defect, CatchableError].}
```

A result obtaining operation is a function of the kind:

```nim
type PipelineResultProc*[T] = proc (i: InputStream): T
                                   {.gcsafe, raises: [Defect, CatchableError].}
```

Please note that `stream.getOutput` is an example of such a function.

Pipelnes can be created with the `cretePipeline` API or executed in place with
`executePipeline`. If the first input source is async, then the whole pipeline
with be executing asynchronously which can result in a much lower memory usage.

The pipeline transformation steps are usually employing the `fsMultiSync`
pragma to make them usable in both synchronous and asynchronous scenarios.

Please note that the above higher-level APIs are just about simplifying the
instantiation of multiple `Pipe` objects that can be used to hook input and
output streams in arbitrary ways.

A stream multiplexer for example is likely to rely on the lower-level `Pipe`
objects and the underlying `PageBuffers` directly.

## License

Licensed and distributed under either of

* MIT license: [LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT

or

* Apache License, Version 2.0, ([LICENSE-APACHEv2](LICENSE-APACHEv2) or http://www.apache.org/licenses/LICENSE-2.0)

at your option. This file may not be copied, modified, or distributed except according to those terms.
