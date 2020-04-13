import
  chronos,
  input_stream, output_stream, multisync

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

proc fsCloseWait(t: StreamTransport) {.async, raises: [Defect, IOError].} =
  raiseFaststreamsError closingErrMsg:
    await t.closeWait()

proc fsReadOnce(t: StreamTransport,
                buffer: ptr byte, bufSize: int): Future[int] {.async, raises: [Defect, IOError].} =
  raiseFaststreamsError readingErrMsg:
    return t.readOnce(pointer(buffer), bufSize)

# TODO: Use the Raising type here
let ChronosInputStreamVTable = InputStreamVTable(
  readSync: proc (s: InputStream, buffer: ptr byte, bufSize: int): int
                 {.nimcall, gcsafe, raises: [IOError, Defect].} =
    var cs = ChronosInputStream(s)
    doAssert cs.allowWaitFor
    raiseFaststreamsError readingErrMsg:
      return waitFor cs.transport.readOnce(pointer(buffer), bufSize)
  ,
  readAsync: proc (s: InputStream, buffer: ptr byte, bufSize: int): Future[int]
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
    ChronosInputStream(s).transport.fsReadOnce(buffer, bufSize)
  ,
  closeSync: proc (s: InputStream)
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
    raiseFaststreamsError closingErrMsg:
      ChronosInputStream(s).transport.close()
  ,
  closeAsync: proc (s: InputStream, cb: CloseAsyncCallback): Future[void]
                   {.nimcall, gcsafe, raises: [IOError, Defect].} =
    ChronosInputStream(s).transport.fsCloseWait()
)

func chronosInput*(s: StreamTransport,
                   pageSize = output_stream.defaultPageSize,
                   allowWaitFor = false): InputStreamHandle =
  InputStreamHandle(s: ChronosInputStream(
    vtable: vtableAddr ChronosInputStreamVTable,
    pageSize: pageSize,
    allowWaitFor: allowWaitFor))

let ChronosOutputStreamVTable = OutputStreamVTable(
  writePageSync: proc (s: OutputStream, page: openarray[byte])
                      {.nimcall, gcsafe, raises: [IOError, Defect].} =
    var cs = ChronosOutputStream(s)
    doAssert cs.allowWaitFor
    let bytesWritten = raiseFaststreamsError writingErrMsg:
      waitFor cs.transport.write(unsafeAddr page[0], page.len)
    if bytesWritten != page.len:
      raise newException(IOError, writeIncompleteErrMsg)
  ,
  writePageAsync: proc (s: OutputStream, buf: pointer, bufLen: int): Future[void]
                       {.nimcall, gcsafe, raises: [IOError, Defect].} =
    var
      cs = ChronosOutputStream(s)
      retFuture = newFuture[void]("ChronosOutputStream.writePageAsync")
      writeFut: Future[int]

    proc continuation(udata: pointer) {.gcsafe.} =
      if writeFut.error != nil:
        retFuture.fail newException(IOError, writingErrMsg, writeFut.error)
      elif writeFut.read != bufLen:
        retFuture.fail newException(IOError, writeIncompleteErrMsg)
      else:
        retFuture.complete()

    var writeFut = cs.transport.write(unsafeAddr page[0], page.len)
    writeFut.addCallback(continuation, nil)

    retFuture.cancelCallback = proc (udata: pointer) {.gcsafe.} =
      writeFut.removeCallback(continuation, nil)

    return retFuture
  ,
  flushSync: proc (s: OutputStream)
                  {.nimcall, gcsafe, raises: [IOError, Defect].} =
    discard
  ,
  flushAsync: proc (s: OutputStream): Future[void]
                   {.nimcall, gcsafe, raises: [IOError, Defect].} =
    result = newFuture[void]("ChronosOutputStream.flushAsync")
    result.complete()
  ,
  closeSync: ChronosInputStreamVTable.closeSync,
  closeAsyncProc: ChronosInputStreamVTable.closeAsyncProc
)

func chronosOutput*(s: StreamTransport,
                    pageSize = output_stream.defaultPageSize,
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

