import postgres, asyncdispatch, strutils, macros, json
import byteswap, apg_array, apg_json

type
  apgConnection* = ref object of RootRef
    pgconn: PPGconn
  apgResult* = ref object of RootRef
    pgress: seq[PPGresult]

  Row* = seq[string]  ## a row of a dataset. NULL database values will be
                      ## converted to nil.

when defined(windows):
  const dllName = "libpq.dll"
elif defined(macosx):
  const dllName = "libpq.dylib"
else:
  const dllName = "libpq.so(.5|)"

proc pgEncodingToChar(encoding: int32): cstring 
     {.cdecl, dynlib: dllName, importc: "pg_encoding_to_char"}
proc pqlibVersion(): cint
     {.cdecl, dynlib: dllName, importc: "PQlibVersion".}

proc connect*(connection: string): Future[apgConnection] =
  var retFuture = newFuture[apgConnection]("asyncpg.connect")
  var conn = apgConnection()

  proc cb(fd: AsyncFD): bool {.closure,gcsafe.} =
    if not retFuture.finished:
      case pqconnectPoll(conn.pgconn)
      of PGRES_POLLING_READING:
        addRead(fd, cb)
      of PGRES_POLLING_WRITING:
        addWrite(fd, cb)
      of PGRES_POLLING_FAILED:
        retFuture.fail(newException(ValueError, $pqerrorMessage(conn.pgconn)))
      of PGRES_POLLING_OK:
        # set connection to non-blocking mode
        if pqsetnonblocking(conn.pgconn, 1) == -1:
          retFuture.fail(newException(ValueError, $pqerrorMessage(conn.pgconn)))
        # set maximum verbosity level for errors
        discard pqsetErrorVerbosity(conn.pgconn, PQERRORS_VERBOSE)
        retFuture.complete(conn)
      else:
        retFuture.fail(newException(ValueError,
                                    "Unsupported pqconnectPoll() result"))
    return true

  conn.pgconn = pqconnectStart(connection)
  if conn.pgconn == nil:
    retFuture.fail(newException(ValueError, $pqerrorMessage(conn.pgconn)))
  let socket = AsyncFD(pqsocket(conn.pgconn))
  register(socket)
  discard cb(socket)
  return retFuture

proc reset*(conn: apgConnection): Future[void] =
  var retFuture = newFuture[void]("asyncpg.reset")
  
  proc cb(fd: AsyncFD): bool {.closure,gcsafe.} =
    if not retFuture.finished:
      case pqresetPoll(conn.pgconn)
      of PGRES_POLLING_READING:
        addRead(fd, cb)
      of PGRES_POLLING_WRITING:
        addWrite(fd, cb)
      of PGRES_POLLING_FAILED:
        unregister(fd)
        retFuture.fail(newException(ValueError, $pqerrorMessage(conn.pgconn)))
      of PGRES_POLLING_OK:
        if pqsetnonblocking(conn.pgconn, 1) == -1:
          unregister(fd)
          retFuture.fail(newException(ValueError, $pqerrorMessage(conn.pgconn)))
        retFuture.complete()
      else:
        unregister(fd)
        retFuture.fail(newException(ValueError,
                                    "Unsupported pqresetPoll() result"))
    return true

  if conn.pgconn != nil:
    if pqresetStart(conn.pgconn) == 0:
      retFuture.fail(newException(ValueError, $pqerrorMessage(conn.pgconn)))
    let socket = AsyncFD(pqsocket(conn.pgconn))
    register(socket)
    discard cb(socket)
  else:
    retFuture.fail(newException(ValueError, "Connection is already closed"))
  return retFuture

proc close*(conn: apgConnection) =
  if conn.pgconn != nil:
    let fd = AsyncFD(pqsocket(conn.pgconn))
    unregister(fd)
    pqfinish(conn.pgconn)
    conn.pgconn = nil
  else:
    raise newException(ValueError, "Connection is already closed")

proc execAsync(conn: apgConnection, statement: string, pN: int32, pT: POid,
           pV: cstringArray, pL, pF: ptr int32, rF: int32): Future[apgResult] =
  var retFuture = newFuture[apgResult]("asyncpg.exec")
  var apgres = apgResult()
  apgres.pgress = newSeq[PPGresult]()
  var state = 0

  proc cb(fd: AsyncFD): bool {.closure,gcsafe.} =
    if not retFuture.finished:
      if state == 0:
        let res = pqflush(conn.pgconn)
        if res == 0:
          inc(state)
          addRead(fd, cb)
        elif res == 1:
          addWrite(fd, cb)
        else:
          retFuture.fail(newException(ValueError, $pqerrorMessage(conn.pgconn)))
        return true
      else:
        if pqconsumeInput(conn.pgconn) == 0:
          retFuture.fail(newException(ValueError, $pqerrorMessage(conn.pgconn)))
          return true
        else:
          # processing pending notifications
          while true:
            var nres = pqnotifies(conn.pgconn)
            if nres == nil:
              break
            else:
              discard
          # processing results
          while true:
            if pqisBusy(conn.pgconn) == 0:
              var res = pqgetResult(conn.pgconn)
              if res != nil:
                apgres.pgress.add(res)
              else:
                retFuture.complete(apgres)
                return true
            else:
              return false

  var query: cstring = statement
  if pqsendQueryParams(conn.pgconn, query, pN, pT, pV, pL, pF, rF) == 0:
    retFuture.fail(newException(ValueError, $pqerrorMessage(conn.pgconn)))
    return retFuture
  else:
    let fd = AsyncFD(pqsocket(conn.pgconn))
    discard cb(fd)
  return retFuture

proc len*(apgres: apgResult): int =
  result = len(apgres.pgress)

proc close*(apgres: apgResult) =
  for pgr in apgres.pgress:
    pqclear(pgr)
  apgres.pgress.setLen(0)

proc `[]`*(apgres: apgResult, index: int): PPGresult =
  result = apgres.pgress[index]

template setRow(pgres: PPGresult, r, line, cols) =
  for col in 0..<cols:
    let x = pqgetvalue(pgres, line.int32, col.int32)
    if x.isNil:
      r[col] = nil
    else:
      r[col] = $x

template setRowInline(pgres: PPGresult, r, line, cols) =
  for col in 0..<cols:
    setLen(r[col], 0)
    let x = pqgetvalue(pgres, line.int32, col.int32)
    if x.isNil:
      r[col] = nil
    else:
      add(r[col], x)

proc getValue*(pgres: PPGresult): string =
  var x = pqgetvalue(pgres, 0, 0)
  result = if isNil(x): "" else: $x

proc getRow*(pgres: PPGresult): Row =
  var L = pqnfields(pgres)
  result = newSeq[string](L)
  setRow(pgres, result, 0, L)
  pqclear(pgres)

proc getRows*(pgres: PPGresult, rows: int): seq[Row] = 
  var L = pqnfields(pgres)
  var R = pqntuples(pgres).int
  if (rows != -1) and (rows < R):
    R = rows
  result = newSeq[Row](R)
  for row in 0..<R:
    result[row] = newSeq[string](L)
    setRow(pgres, result[row], row, L)

template getAllRows*(pgres: PPGresult): seq[Row] =
  getRows(pgres, -1)

iterator rows*(pgres: PPGresult): Row =
  var L = pqnfields(pgres)
  var R = pqntuples(pgres)
  var result = newSeq[string](L)
  for i in 0..<R:
    setRowInline(pgres, result, i, L)
    yield result

proc getAffectedRows*(pgres: PPGresult): int64 =
  result = parseBiggestInt($pqcmdTuples(pgres))

proc setClientEncoding*(conn: apgConnection,
                        encoding: string): Future[bool] {.async.} =
  result = false
  var statement = "set client_encoding to '" & encoding & "'"
  var ares = await execAsync(conn, statement, 0, nil, nil, nil, nil, 0)
  if pqresultStatus(ares.pgress[0]) == PGRES_COMMAND_OK:
    result = true
  close(ares)

proc getClientEncoding*(conn: apgConnection): string =
  result = $pgEncodingToChar(pqclientEncoding(conn.pgconn))

proc getVersion*(): int =
  result = pqlibVersion().int

#
# exec macro
#

proc newVarArray(n: NimNode, b: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(n, newEmptyNode(), b)
  )

# var <n>: array[<c>, <t>]
# proc newVarArray(n: NimNode, t: string, c: int): NimNode {.compileTime.} =
#   result = newNimNode(nnkVarSection).add(
#     newNimNode(nnkIdentDefs).add(
#       n,
#       newNimNode(nnkBracketExpr).add(
#         newIdentNode(!"array"), newLit(c), newIdentNode(!t)
#       ), newEmptyNode()
#     )
#   )
# <n>[<i>] = <v>
# proc assignArray(n: NimNode, i: int, v: NimNode): NimNode {.compileTime.} =
#   result = newAssignment(
#     newNimNode(nnkBracketExpr).add(n, newLit(i)), v
#   )
# cast[pointer](addr <n>)
proc castPointer(n: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkCast).add(
    newIdentNode(!"pointer"),
    newNimNode(nnkCommand).add(newIdentNode(!"addr"), n)
  )
# cast[<v>](addr <n>[0])
proc castPointer0(n: NimNode, v: string): NimNode {.compileTime.} =
  result = newNimNode(nnkCast).add(
    newIdentNode(!v),
    newNimNode(nnkCommand).add(
      newIdentNode(!"addr"),
      newNimNode(nnkBracketExpr).add(n, newLit(0))
    )
  )
# <v>.Oid
proc castOid(v: int): NimNode {.compileTime.} =
  result = newDotExpr(newLit(v), newIdentNode(!"Oid"))
# len(<v>)
proc callLength(n: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkCall).add(newIdentNode(!"len"), n)
# size(<v>)
proc callSize(n: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkCall).add(newIdentNode(!"size"), n)
# raw(<v>)
proc callRaw(n: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkCall).add(newIdentNode(!"raw"), n)
# cast[<v>](n)
proc castSome(n: NimNode, v: string): NimNode {.compileTime.} =
  result = newNimNode(nnkCast).add(newIdentNode(!v), n)
# var <n> = prepare(<v>)
proc newVarInteger(n: NimNode, v: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(
      n, newEmptyNode(), newNimNode(nnkCall).add(newIdentNode(!"prepare"), v)
    )
  )
# var <n> = <v>
proc newVarSimple(n: NimNode, v: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(n, newEmptyNode(), v)
  )
# var <n> = cast[<i>](<v>)
proc newVarCast(n: NimNode, i: NimNode, v: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(
      n, newEmptyNode(), newNimNode(nnkCast).add(v, i)
    )
  )
#
proc newVarExec(n, c: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(n, newEmptyNode(), c)
  )

# var <n> = prepare(cast[<i>](<v>))
proc newVarFloat(n: NimNode, i: NimNode, v: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(
      n, newEmptyNode(),
      newNimNode(nnkCall).add(
        newIdentNode(!"prepare"),
        newNimNode(nnkCast).add(v, i)
      )
    )
  )
# var <n> = newPgArray((<v>))
proc newVarSeq(n: NimNode, v: NimNode, s: string): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(
      n, newEmptyNode(),
      newCall(
        newNimNode(nnkBracketExpr).add(
          newIdentNode(!"newPgArray"),
          newIdentNode(!s)
        ), v, newIdentNode(!"true")
      )
    )
  )
# addr(<n>[0])
proc newAddr0(n: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkCommand).add(
             newIdentNode(!"addr"),
             newNimNode(nnkBracketExpr).add(n, newLit(0))
           )

# this is copy of newLit(int) but makes int32
proc newLit(i: int32): NimNode {.compileTime.} =
  ## produces a new integer literal node.
  result = newNimNode(nnkInt32Lit)
  result.intVal = i
# echo(repr(<n>))
proc newEchoVar(n: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkCall).add(
    newIdentNode(!"echo"),
    newNimNode(nnkCall).add(newIdentNode(!"repr"), n)
  )

proc `$`(ntyType: NimTypeKind): string =
  var names = ["int", "int8", "int16", "int32", "int64", "float", "float32",
               "float64", "", "uint", "uint8", "uint16", "uint32", "uint64"]
  case ntyType
    of ntyBool:
      result = "bool"
    of ntyChar:
      result = "char"
    of ntyString:
      result = "string"
    of ntyInt..ntyFloat64, ntyUint..ntyUInt64:
      result = names[ntyType.int - ntyInt.int]
    else:
      result = ""

# This procedure converts Nim's array/sequence type to appropriate
# PostgreSQL Array Type Oid.
#
# Currently supported array/sequences:
# array/seq[bool] => ARRAY[bool] (1000)
# array/seq[char, uint8, int8] => BYTEA (17)
# array/seq[int16, uint16] => ARRAY[int2] (1005)
# array/seq[int32, uint32] => ARRAY[int4] (1007)
# array/seq[int64, uint64] => ARRAY[int8] (1016)
# array/seq[float32] => ARRAY[float4] (1021)
# array/seq[float64] => ARRAY[float8] (1022)
# array/seq[float] => ARRAY[float8] (1022)
# array/seq[int, uint] => ARRAY[int2, int4, int8] (1005, 1007, 1016)
# depends on platform's size of int
# array/seq[string] => ARRAY[text] (1009)
# array/seq[cstring] => ARRAY[text] (1009)
proc getOidArray(ntyType: NimTypeKind): int {.compileTime.} = 
  case ntyType
  of ntyBool:
    result = 1000
  of ntyInt16, ntyUInt16:
    result = 1005
  of ntyInt32, ntyUInt32:
    result = 1007
  of ntyInt64, ntyUInt64:
    result = 1016
  of ntyFloat32:
    result = 1021
  of ntyFloat64:
    result = 1022
  of ntyFloat:
    result = 1022
  of ntyInt, ntyUInt:
    when sizeof(int) == 8:
      result = 1016
    elif sizeof(int) == 4:
      result = 1007
    elif sizeof(int) == 2:
      result = 1005
    else:
      error("Unsupported int size!")
  of ntyString, ntyCString:
    result = 1009
  else:
    error("Unsupported array oid type!")

# This procedure converts Nim's type to appropriate
# PostgreSQL type Oid.
#
# Currently supported types:
# bool => bool(16)
# char => char(18)
# int8, uint8 => char(18)
# int16, uint16 => int2(21)
# int32, uint32 => int4(23)
# int64, uint64 => int8(20)
# float32 => float4(700)
# float64 => float8(701)
# float => float8(701), float type is always float64 in nim.
# int => int8(20), int4(23), int2(21), depends on platform's size of `int`
# uint => int8(20), int4(23), int2(21), depends on platform's size of `int`
proc getOidSimple(ntyType: NimTypeKind):
                  tuple[oid: int, size: int] {.compileTime.} =
  case ntyType
  of ntyBool:
    result = (oid: 16, size: 1)
  of ntyChar, ntyInt8, ntyUInt8:
    result = (oid: 18, size: 1)
  of ntyInt16, ntyUInt16:
    result = (oid: 21, size: 2)
  of ntyInt32, ntyUInt32:
    result = (oid: 23, size: 4)
  of ntyInt64, ntyUInt64:
    result = (oid: 20, size: 8)
  of ntyFloat32:
    result = (oid: 700, size: 4)
  of ntyFloat64:
    result = (oid: 701, size: 8)
  of ntyInt:
    when sizeof(int) == 8:
      result = (oid: 20, size: 8)
    elif sizeof(int) == 4:
      result = (oid: 23, size: 4)
    elif sizeof(int) == 2:
      result = (oid: 21, size: 2)
    else:
      result = (oid: -1, size: -1)
  of ntyUInt:
    when sizeof(int) == 8:
      result = (oid: 20, size: 8)
    elif sizeof(int) == 4:
      result = (oid: 23, size: 4)
    elif sizeof(int) == 2:
      result = (oid: 21, size: 2)
    else:
      result = (oid: -1, size: -1)
  of ntyFloat:
    result = (oid: 701, size: 8)
  else:
    result = (oid: -1, size: -1)

# This procedure process sequences and arrays
proc getSequence(ls, ps, op, ntp, np, pv, pl, pt, pf: NimNode) {.compileTime.} =
  var impType = ntp.typeKind
  case impType
  of ntyChar, ntyInt8, ntyUInt8:
    if op.kind == nnkSym:
      pv.add(castPointer0(op, "pointer"))
      pl.add(castSome(callLength(op), "int32"))
      pt.add(castOid(17)) # bytea
      pf.add(newLit(1'i32))
    else:
      ls.add(newVarSimple(np, op))
      pv.add(castPointer0(np, "pointer"))
      pl.add(castSome(callLength(np), "int32"))
      pt.add(castOid(17)) # bytea
      pf.add(newLit(1'i32))
  of ntyBool, ntyInt, ntyUInt, ntyInt16..ntyInt64, ntyUInt16..ntyUInt64,
     ntyFloat..ntyFloat64, ntyString:
    ls.add(newVarSeq(np, op, $impType))
    pv.add(callRaw(np))
    pl.add(castSome(callSize(np), "int32"))
    pt.add(castOid(getOidArray(impType)))
    pf.add(newLit(1'i32))
    ps.add(newCall(newIdentNode(!"free"), np))
  else:
    error("Argument's type `seq[" & $impType & "]`is not supported, " & lineinfo(op))

macro exec*(conn: apgConnection, statement: string,
            params: varargs[typed]): Future[apgResult] =
  # if there no params, we just generate call to execAsync
  if len(params) == 0:
    result = newTree(nnkStmtListExpr,
               newCall(bindSym"execAsync", conn, statement, newLit 0,
                       newNimNode(nnkNilLit), newNimNode(nnkNilLit),
                       newNimNode(nnkNilLit), newNimNode(nnkNilLit),
                       newLit(0))
             )
  else:
    result = newTree(nnkStmtListExpr)

    var postNodes = newNimNode(nnkStmtList)
    var execFuture = genSym(nskVar, "execFuture")

    var typesList = newNimNode(nnkBracket)
    var valuesList = newNimNode(nnkBracket)
    var lensList = newNimNode(nnkBracket)
    var formatsList = newNimNode(nnkBracket)

    var typesName = genSym(nskVar, "execTypes")
    var valuesName = genSym(nskVar, "execValues")
    var lensName = genSym(nskVar, "execLens")
    var formatsName = genSym(nskVar, "execFormats")

    var typesArray = newVarArray(typesName, typesList)
    var valuesArray = newVarArray(valuesName, valuesList)
    var lensArray = newVarArray(lensName, lensList)
    var formatsArray = newVarArray(formatsName, formatsList)
    var idx = 0

    for param in params:
      var np = genSym(nskVar, "param" & $idx)
      let kiType = getType(param).typeKind
      case kiType
        of ntyBool:
          let oid = getOidSimple(kiType)
          result.add(newVarCast(np, param, newIdentNode(!"int8")))
          valuesList.add(castPointer(np))
          lensList.add(newLit(oid.size.int32))
          typesList.add(castOid(oid.oid))
          formatsList.add(newLit(1.int32))
        of ntyChar, ntyInt8, ntyUInt8:
          let oid = getOidSimple(kiType)
          result.add(newVarSimple(np, param))
          valuesList.add(castPointer(np))
          lensList.add(newLit(oid.size.int32))
          typesList.add(castOid(oid.oid))
          formatsList.add(newLit(1.int32))
        of ntyInt, ntyUInt, ntyInt16..ntyInt64, ntyUInt16..ntyUInt64:
          let oid = getOidSimple(kiType)
          result.add(newVarInteger(np, param))
          valuesList.add(castPointer(np))
          lensList.add(newLit(oid.size.int32))
          typesList.add(castOid(oid.oid))
          formatsList.add(newLit(1.int32))
        of ntyFloat..ntyFloat64:
          let oid = getOidSimple(kiType)
          var nident: NimNode
          if kiType == ntyFloat or kiType == ntyFloat64:
            nident = newIdentNode(!"int64")
          else:
            nident = newIdentNode(!"int32")
          result.add(newVarFloat(np, param, nident))
          valuesList.add(castPointer(np))
          lensList.add(newLit(oid.size.int32))
          typesList.add(castOid(oid.oid))
          formatsList.add(newLit(1.int32))
        of ntyString, ntyCString:
          if param.kind == nnkStrLit:
            result.add(newVarSimple(np, param))
            valuesList.add(castPointer0(np, "pointer"))
            lensList.add(castSome(callLength(np), "int32"))
          else:
            valuesList.add(castPointer0(param, "pointer"))
            lensList.add(castSome(callLength(param), "int32"))
          typesList.add(castOid(25))
          formatsList.add(newLit(1.int32))
        of ntySequence:
          var imp = getTypeImpl(param)
          var ntp = getType(imp[1])
          getSequence(result, postNodes, param, ntp, np, valuesList,
                      lensList, typesList, formatsList)
        of ntyArrayConstr:
          var imp = getTypeImpl(param)
          var ntp = getType(imp[2])
          getSequence(result, postNodes, param, ntp, np, valuesList,
                      lensList, typesList, formatsList)
        of ntyObject:
          echo(getTypeInst(param).symbol)
        else:
          error("Argument's type `" & $kiType & "`is unsupported, " & lineinfo(param))
      inc(idx)

    result.add(
      valuesArray, typesArray, lensArray, formatsArray,
      # newEchoVar(valuesName), newEchoVar(typesName),
      # newEchoVar(lensName), newEchoVar(formatsName),
      newVarExec(execFuture,
        newCall(bindSym"execAsync", conn, statement,
                newLit len(params), castPointer0(typesName, "POid"),
                castPointer0(valuesName, "cstringArray"),
                newAddr0(lensName), newAddr0(formatsName), newLit(0)))
    )
    for child in postNodes.children:
      result.add(child)
    result.add(execFuture)
    #echo(toStrLit(result))
