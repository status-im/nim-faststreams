import
  async_backend

export
  async_backend

when fsAsyncSupport:
  import
    stew/shims/macros,
    "."/[inputs, outputs]

  macro fsMultiSync*(body: untyped) =
    # We will produce an identical copy of the annotated proc,
    # but taking async parameters and having the async pragma.
    var
      asyncProcBody = copy body
      asyncProcParams = asyncProcBody[3]

    asyncProcBody.addPragma(bindSym"async")

    # The return types becomes Future[T]
    if asyncProcParams[0].kind == nnkEmpty:
      asyncProcParams[0] = newTree(nnkBracketExpr, bindSym"Future", ident"void")
    else:
      asyncProcParams[0] = newTree(nnkBracketExpr, bindSym"Future", asyncProcParams[0])

    # We replace all stream inputs with their async counterparts
    for i in 1 ..< asyncProcParams.len:
      let paramsDef = asyncProcParams[i]
      let typ = paramsDef[^2]
      if eqIdent(typ, "InputStream"):
        paramsDef[^2] = bindSym "AsyncInputStream"
      elif eqIdent(typ, "OutputStream"):
        paramsDef[^2] = bindSym "AsyncOutputStream"

    result = newStmtList(body, asyncProcBody)
    when defined(debugSupportAsync):
      echo result.repr
else:
  macro fsMultiSync*(body: untyped) = body
