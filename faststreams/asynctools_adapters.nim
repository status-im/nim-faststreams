import
  asynctools/asyncpipe,
  inputs, outputs, buffers, multisync, async_backend

when (not fsAsyncSupport):
  {.fatal: "`-d:async_backend` has be to set".}

export
  inputs, outputs, asyncpipe, fsMultiSync

{.pragma: iocall, nimcall, gcsafe, raises: [IOError].}

type
  AsyncPipeInput* = ref object of InputStream
    pipe: AsyncPipe
    allowWaitFor: bool

  AsyncPipeOutput* = ref object of OutputStream
    pipe: AsyncPipe
    allowWaitFor: bool

const
  readingErrMsg = "Failed to read from AsyncPipe"
  writingErrMsg = "Failed to write to AsyncPipe"
  closingErrMsg = "Failed to close AsyncPipe"
  writeIncompleteErrMsg = "Failed to write all bytes to AsyncPipe"

proc closeAsyncPipe(pipe: AsyncPipe)
                   {.raises: [IOError].} =
  fsTranslateErrors closingErrMsg:
    close pipe

proc readOnce(s: AsyncPipeInput,
              dst: pointer, dstLen: Natural): Future[Natural] {.async.} =
  fsTranslateErrors readingErrMsg:
    return implementSingleRead(s.buffers, dst, dstLen, ReadFlags {},
                               readStartAddr, readLen):
      await s.pipe.readInto(readStartAddr, readLen)

proc write(s: AsyncPipeOutput, src: pointer, srcLen: Natural) {.async.} =
  fsTranslateErrors writeIncompleteErrMsg:
    implementWrites(s.buffers, src, srcLen, "AsyncPipe",
                    writeStartAddr, writeLen):
      await s.pipe.write(writeStartAddr, writeLen)

# TODO: Use the Raising type here
const asyncPipeInputVTable = InputStreamVTable(
  readSync: proc (s: InputStream, dst: pointer, dstLen: Natural): Natural
                 {.iocall.} =
    fsTranslateErrors "Unexpected exception from asyncdispatch":
      var cs = AsyncPipeInput(s)
      fsAssert cs.allowWaitFor
      return waitFor readOnce(cs, dst, dstLen)
  ,
  readAsync: proc (s: InputStream, dst: pointer, dstLen: Natural): Future[Natural]
                  {.iocall.} =
    fsTranslateErrors "Unexpected exception from merely forwarding a future":
      return readOnce(AsyncPipeInput s, dst, dstLen)
  ,
  closeSync: proc (s: InputStream)
                  {.iocall.} =
    closeAsyncPipe AsyncPipeInput(s).pipe
  ,
  closeAsync: proc (s: InputStream): Future[void]
                   {.iocall.} =
    closeAsyncPipe AsyncPipeInput(s).pipe
)

func asyncPipeInput*(pipe: AsyncPipe,
                     pageSize = defaultPageSize,
                     allowWaitFor = false): AsyncInputStream =
  AsyncInputStream AsyncPipeInput(
    vtable: vtableAddr asyncPipeInputVTable,
    buffers: initPageBuffers(pageSize),
    pipe: pipe,
    allowWaitFor: allowWaitFor)

const asyncPipeOutputVTable = OutputStreamVTable(
  writeSync: proc (s: OutputStream, src: pointer, srcLen: Natural)
                  {.iocall.} =
    fsTranslateErrors "Unexpected exception from asyncdispatch":
      var cs = AsyncPipeOutput(s)
      fsAssert cs.allowWaitFor
      waitFor write(cs, src, srcLen)
  ,
  writeAsync: proc (s: OutputStream, src: pointer, srcLen: Natural): Future[void]
                   {.iocall.} =
    fsTranslateErrors "Unexpected exception from merely forwarding a future":
      return write(AsyncPipeOutput s, src, srcLen)
  ,
  closeSync: proc (s: OutputStream)
                  {.iocall.} =
    closeAsyncPipe AsyncPipeOutput(s).pipe
  ,
  closeAsync: proc (s: OutputStream): Future[void]
                   {.iocall.} =
    closeAsyncPipe AsyncPipeOutput(s).pipe
)

func asyncPipeOutput*(pipe: AsyncPipe,
                      pageSize = defaultPageSize,
                      allowWaitFor = false): AsyncOutputStream =
  AsyncOutputStream AsyncPipeOutput(
    vtable: vtableAddr(asyncPipeOutputVTable),
    buffers: initPageBuffers(pageSize),
    pipe: pipe,
    allowWaitFor: allowWaitFor)

