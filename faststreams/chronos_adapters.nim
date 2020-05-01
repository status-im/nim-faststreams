import
  chronos,
  inputs, outputs, buffers, multisync

export
  chronos, fsMultiSync

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

proc chronosCloseWait(t: StreamTransport) {.async, raises: [Defect, IOError].} =
  fsTranslateErrors closingErrMsg:
    await t.closeWait()

proc chronosReadOnce(s: ChronosInputStream, dst: pointer, dstLen: Natural): Future[Natural]
                    {.raises: [IOError, Defect], async.} =
  fsTranslateErrors readingErrMsg:
    return implementSingleRead(s.buffers, dst, dstLen, {},
                               readStartAddr, readLen):
      await s.transport.readOnce(readStartAddr, readLen)

proc chronosWrites(s: ChronosOutputStream, src: pointer, srcLen: Natural)
                  {.raises: [IOError, Defect], async.} =
  fsTranslateErrors writeIncompleteErrMsg:
    implementWrites(s.buffers, src, srcLen, "StreamTransport"
                    writeStartAddr, writeLen):
      await s.transport.write(writeStartAddr, writeLen)

# TODO: Use the Raising type here
let ChronosInputStreamVTable = InputStreamVTable(
  readSync: proc (s: InputStream, dst: pointer, dstLen: Natural): Natural
                 {.nimcall, gcsafe, raises: [IOError, Defect].} =
    var cs = ChronosInputStream(s)
    doAssert cs.allowWaitFor
    waitFor chronosReadOnce(cs, dst, dstLen)
  ,
  readAsync: proc (s: InputStream, dst: pointer, dstLen: Natural): Future[Natural]
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
    chronosReadOnce(ChronosInputStream s, dst, dstLen)
  ,
  closeSync: proc (s: InputStream)
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
    fsTranslateErrors closingErrMsg:
      s.closeFut = ChronosInputStream(s).transport.close()
  ,
  closeAsync: proc (s: InputStream): Future[void]
                   {.nimcall, gcsafe, raises: [IOError, Defect].} =
    chronosCloseWait ChronosInputStream(s).transport
)

func chronosInput*(s: StreamTransport,
                   pageSize = buffers.defaultPageSize,
                   allowWaitFor = false): InputStreamHandle =
  InputStreamHandle(s: ChronosInputStream(
    vtable: vtableAddr ChronosInputStreamVTable,
    pageSize: pageSize,
    allowWaitFor: allowWaitFor))

let ChronosOutputStreamVTable = OutputStreamVTable(
  writeSync: proc (s: OutputStream, src: pointer, srcLen: Natural)
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
    var cs = ChronosOutputStream(s)
    doAssert cs.allowWaitFor
    waitFor chronosWrites(cs, src, srcLen)
  ,
  writeAsync: proc (s: OutputStream, src: pointer, srcLen: Natural): Future[void]
                   {.nimcall, gcsafe, raises: [IOError, Defect].} =
    chronosWrites(ChronosOutputStream s, src, srcLen)
  ,
  closeSync: proc (s: OutputStream)
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
    fsTranslateErrors closingErrMsg:
      s.closeFut = close ChronosOutputStream(s).transport
  ,
  closeAsync: proc (s: OutputStream): Future[void]
                   {.nimcall, gcsafe, raises: [IOError, Defect].} =
    chronosCloseWait ChronosOutputtream(s).transport
)

func chronosOutput*(s: StreamTransport,
                    pageSize = buffers.defaultPageSize,
                    allowWaitFor = false): OutputStreamHandle =
  var stream = ChronosOutputStream(
    vtable: vtableAddr(SnappyStreamVTable),
    pageSize: pageSize,
    minWriteSize: 1,
    maxWriteSize: high(int),
    transport: s,
    allowWaitFor: allowWaitFor)

  stream.initWithSinglePage()

  OutputStreamHandle(s: stream)

