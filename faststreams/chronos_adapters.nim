import
  chronos,
  inputs, outputs, buffers, multisync

export
  chronos, fsMultiSync

{.pragma: iocall, nimcall, gcsafe, raises: [IOError].}

type
  ChronosInputStream* = ref object of InputStream
    transport: StreamTransport
    allowWaitFor: bool

  ChronosOutputStream* = ref object of OutputStream
    transport: StreamTransport
    allowWaitFor: bool

const
  readingErrMsg = "Failed to read from Chronos transport"
  writingErrMsg = "Failed to write to Chronos transport"
  closingErrMsg = "Failed to close Chronos transport"
  writeIncompleteErrMsg = "Failed to write all bytes to Chronos transport"

proc chronosCloseWait(t: StreamTransport)
                     {.async, raises: [IOError].} =
  fsTranslateErrors closingErrMsg:
    await t.closeWait()

proc chronosReadOnce(s: ChronosInputStream,
                     dst: pointer, dstLen: Natural): Future[Natural] {.async.} =
  fsTranslateErrors readingErrMsg:
    return implementSingleRead(s.buffers, dst, dstLen, ReadFlags {},
                               readStartAddr, readLen):
      await s.transport.readOnce(readStartAddr, readLen)

proc chronosWrites(s: ChronosOutputStream, src: pointer, srcLen: Natural) {.async.} =
  fsTranslateErrors writeIncompleteErrMsg:
    implementWrites(s.buffers, src, srcLen, "StreamTransport",
                    writeStartAddr, writeLen):
      await s.transport.write(writeStartAddr, writeLen)

# TODO: Use the Raising type here
const chronosInputVTable = InputStreamVTable(
  readSync: proc (s: InputStream, dst: pointer, dstLen: Natural): Natural
                 {.iocall.} =
    fsTranslateErrors "Unexpected exception from Chronos async macro":
      var cs = ChronosInputStream(s)
      fsAssert cs.allowWaitFor
      return waitFor chronosReadOnce(cs, dst, dstLen)
  ,
  readAsync: proc (s: InputStream, dst: pointer, dstLen: Natural): Future[Natural]
                  {.iocall.} =
    fsTranslateErrors "Unexpected exception from merely forwarding a future":
      return chronosReadOnce(ChronosInputStream s, dst, dstLen)
  ,
  closeSync: proc (s: InputStream)
                  {.iocall.} =
    fsTranslateErrors closingErrMsg:
      s.closeFut = ChronosInputStream(s).transport.close()
  ,
  closeAsync: proc (s: InputStream): Future[void]
                   {.iocall.} =
    chronosCloseWait ChronosInputStream(s).transport
)

func chronosInput*(s: StreamTransport,
                   pageSize = defaultPageSize,
                   allowWaitFor = false): AsyncInputStream =
  AsyncInputStream ChronosInputStream(
    vtable: vtableAddr chronosInputVTable,
    buffers: initPageBuffers(pageSize),
    transport: s,
    allowWaitFor: allowWaitFor)

const chronosOutputVTable = OutputStreamVTable(
  writeSync: proc (s: OutputStream, src: pointer, srcLen: Natural)
                  {.iocall.} =
    var cs = ChronosOutputStream(s)
    fsAssert cs.allowWaitFor
    waitFor chronosWrites(cs, src, srcLen)
  ,
  writeAsync: proc (s: OutputStream, src: pointer, srcLen: Natural): Future[void]
                   {.iocall.} =
    chronosWrites(ChronosOutputStream s, src, srcLen)
  ,
  closeSync: proc (s: OutputStream)
                  {.iocall.} =
    fsTranslateErrors closingErrMsg:
      s.closeFut = close ChronosOutputStream(s).transport
  ,
  closeAsync: proc (s: OutputStream): Future[void]
                   {.iocall.} =
    chronosCloseWait ChronosOutputStream(s).transport
)

func chronosOutput*(s: StreamTransport,
                    pageSize = defaultPageSize,
                    allowWaitFor = false): AsyncOutputStream =
  AsyncOutputStream ChronosOutputStream(
    vtable: vtableAddr(chronosOutputVTable),
    buffers: initPageBuffers(pageSize),
    transport: s,
    allowWaitFor: allowWaitFor)

