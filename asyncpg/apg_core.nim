#
#
#                    AsyncPG
#        (c) Copyright 2016 Eugene Kabanov
#
#    See the file "LICENSE", included in this
#    distribution, for details about the copyright.
#

## This is core module

import postgres, asyncdispatch, strutils, macros, json, lists
import byteswap, apg_array, apg_json

type
  apgNotify* = object
    ## Object which represents PostgreSQL's asynchronous notify
    channel*: string
    payload*: string
    bepid: int

  apgNotifyCell = object
    notify: apgNotify
    future: Future[apgNotify]

  apgConnection* = ref object of RootRef
    ## Object which encapsulate connection to PostgreSQL database.
    pgconn: PPGconn
    notifies: DoublyLinkedList[apgNotifyCell]
    notices: seq[string]

  apgResult* = ref object of RootRef
    ## Object which encapsulate result of executed SQL query.
    pgress: seq[PPGresult]

  apgPool* = ref object of RootRef
    ## Object which encapsulates pool of connections to PostgreSQL database.
    connections: seq[apgConnection]
    futures: seq[Future[void]]
    notifies: DoublyLinkedList[apgNotifyCell]

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
proc pqserverVersion(conn: PPGconn): cint
     {.cdecl, dynlib: dllname, importc: "PQserverVersion".}

proc getFreeConnection(pool: apgPool): Future[int] =
  var retFuture = newFuture[int]("asyncpg.getFreeConnection")
  proc cb() =
    if not retFuture.finished:
      var index = 0
      while index < len(pool.futures):
        let fut = pool.futures[index]
        if fut == nil or fut.finished:
          var replaceFuture = newFuture[void]("asyncpg.pool." & $index)
          pool.futures[index] = replaceFuture
          retFuture.complete(index)
          break
        inc(index)
  cb()
  # this code will run only if there no available connections left
  if not retFuture.finished:
    var index = 0
    while index < len(pool.futures):
      pool.futures[index].callback = cb
      inc(index)
  return retFuture

# proc getIndexConnection(pool: apgPool, index: int): Future[int] =
#   var retFuture = newFuture[int]("asyncpg.getIndexConnection")
#   proc cb() =
#     if not retFuture.finished:
#       let fut = pool.futures[index]
#       if fut == nil or fut.finished:
#         var replaceFuture = newFuture[void]("asyncpg.pool." & $index)
#         pool.futures[index] = replaceFuture
#         retFuture.complete(index)
#   cb()
#   if not retFuture.finished:
#     pool.futures[index].callback = cb
#   return retFuture

proc newPool*(size = 10): apgPool =
  ## Creates new object ``apgPool`` with ``size`` connections inside.

  result = apgPool()
  result.connections = newSeq[apgConnection](size)
  result.futures = newSeq[Future[void]](size)

proc noticeReceiver*(arg: pointer, res: PPGresult) {.cdecl.} =
  var conn = cast[apgConnection](arg)
  conn.notices.add($pqresultErrorMessage(res))

proc connect*(connection: string): Future[apgConnection] =
  ## Establish connection to PostgreSQL database using ``connection`` string.

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
        # set notice receiver
        conn.notices = newSeq[string]()
        discard pqsetNoticeReceiver(conn.pgconn,
                                    cast[PQnoticeReceiver](noticeReceiver),
                                    cast[pointer](conn))
        # allocate sequence for notify objects
        conn.notifies = initDoublyLinkedList[apgNotifyCell]()
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
  ## Resets PostgreSQL database connection ``conn``.

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
  ## Closes connection to PostgreSQL server

  if conn.pgconn != nil:
    let fd = AsyncFD(pqsocket(conn.pgconn))
    unregister(fd)
    pqfinish(conn.pgconn)
    conn.pgconn = nil
  else:
    raise newException(ValueError, "Connection is already closed")

proc connect*(pool: apgPool, connection: string): Future[void] {.async.} =
  ## Establish pool's connections to PostgreSQL server using ``connection``
  ## statement.

  var i = 0
  while i < len(pool.connections):
    pool.connections[i] = await connect(connection)
    inc(i)

proc close*(pool: apgPool) =
  ## Closes pool's connections

  var i = 0
  while i < len(pool.connections):
    close(pool.connections[i])
    inc(i)

template processPendingNotifications(conn: apgConnection, notify: bool) =
  while true:
    var nres = pqnotifies(conn.pgconn)
    if nres != nil:
      var channel = $(nres.relname)
      if notify:
        for node in conn.notifies.nodes():
          if node.value.future != nil:
            if node.value.notify.channel == channel:
              node.value.notify.payload = $(nres.extra)
              node.value.notify.bepid = nres.be_pid
              node.value.future.complete(node.value.notify)
              conn.notifies.remove(node)
              break
      else:
        var cell = apgNotifyCell()
        cell.notify = apgNotify()
        cell.notify.channel = channel
        cell.notify.payload = $(nres.extra)
        cell.notify.bepid = nres.be_pid
        conn.notifies.append(cell)
      pqfreemem(cast[pointer](nres))
    else:
      break

proc copyTo*(conn: apgConnection, buffer: pointer,
            nbytes: int32): Future[apgResult] =
  ## Copy data from buffer ``buffer`` of size ``nbytes`` to PostgreSQL's
  ## stdin. 
  ## Be sure to execute ``COPY FROM`` statement before start sending data with
  ## this function. After all data have been sent, you need to finish sending
  ## process with call ``copyTo(conn, nil, 0)``.
  ## Function returns apgResult.
  var retFuture = newFuture[apgResult]("asyncpg.copyTo")
  var apgres = apgResult()
  apgres.pgress = newSeq[PPGresult]()
  var state = 0
  var notify = true

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
          processPendingNotifications(conn, notify)
          # processing results
          while true:
            if pqisBusy(conn.pgconn) == 0:
              var r = pqgetResult(conn.pgconn)
              if r != nil:
                let stres = pqresultStatus(r)
                case stres
                of PGRES_EMPTY_QUERY, PGRES_COMMAND_OK, PGRES_TUPLES_OK:
                  apgres.pgress.add(r)
                of PGRES_COPY_IN, PGRES_COPY_OUT:
                  apgres.pgress.add(r)
                  retFuture.complete(apgres)
                  return true
                else:
                  retFuture.fail(newException(ValueError,
                                              $pqerrorMessage(conn.pgconn)))
                  return true
              else:
                retFuture.complete(apgres)
                return true
            else:
              return false

  var res: int32 = 0
  if isNil(buffer) or nbytes == 0:
    res = pqputCopyEnd(conn.pgconn, nil)
  else:
    res = pqputCopyData(conn.pgconn, cast[cstring](buffer), nbytes)

  if res < 0:
    retFuture.fail(newException(ValueError, $pqerrorMessage(conn.pgconn)))
  elif res == 1:
    # processing pending notifications
    processPendingNotifications(conn, notify)
    # processing result
    while true:
      var r = pqgetResult(conn.pgconn)
      if r != nil:
        let stres = pqresultStatus(r)
        case stres
          of PGRES_EMPTY_QUERY, PGRES_COMMAND_OK, PGRES_TUPLES_OK:
            apgres.pgress.add(r)
          of PGRES_COPY_IN:
            apgres.pgress.add(r)
            retFuture.complete(apgres)
            break
          else:
            retFuture.fail(newException(ValueError,
                                        $pqerrorMessage(conn.pgconn)))
            break
      else:
        retFuture.complete(apgres)
        break
  else:
    let fd = AsyncFD(pqsocket(conn.pgconn))
    addWrite(fd, cb)

  return retFuture

proc copyFromInto*(conn: apgConnection, buffer: pointer,
                   nbytes: int): Future[int] =
  ## Copy data caused by sql `COPY TO` statement to buffer `buffer`.
  ## Because rows received one by one, size of buffer `nbytes` must be
  ## more or equal to text representation of single row.
  ## Returns `size` of received data.
  ## Be sure to execute `COPY TO` statement, before you call this
  ## function.

  var retFuture = newFuture[int]("asyncpg.copyFrom")
  var apgres = apgResult()
  apgres.pgress = newSeq[PPGresult]()
  var state = 0
  var notify = true
  var copyString: pointer = nil

  proc cb(fd: AsyncFD): bool {.closure,gcsafe.} =
    if not retFuture.finished:
      let res = pqgetCopyData(conn.pgconn, cast[cstringArray](addr copyString), 
                              1)
      if res > 0:
        doAssert(res.int <= nbytes)
        copyMem(cast[pointer](buffer), copyString, res)
        pqfreemem(copyString)
        retFuture.complete(res.int)
        return true
      elif res == 0:
        if state == 0:
          addRead(fd, cb)
          inc(state)
        else:
          return false
      elif res == -1:
        if copyString != nil: pqfreemem(copyString)
        # processing pending notifications
        processPendingNotifications(conn, notify)
        # processing result
        while true:
          var r = pqgetResult(conn.pgconn)
          if r != nil:
            let stres = pqresultStatus(r)
            pqclear(r)
            if stres != PGRES_COMMAND_OK:
              retFuture.fail(newException(ValueError,
                                          $pqerrorMessage(conn.pgconn)))
          else:
            retFuture.complete(0)
            break
        return true
      else:
        if copyString != nil: pqfreemem(copyString)
        retFuture.fail(newException(ValueError, $pqerrorMessage(conn.pgconn)))
        return true

  let fd = AsyncFD(pqsocket(conn.pgconn))
  discard cb(fd)
  return retFuture

template withConnection*(pool: apgPool, conn, body: untyped) =
  ## Retrieves first available connection from pool, assign it
  ## to variable with name `conn`. You can use this connection
  ## inside withConnection code block.
  mixin getFreeConnection
  var connFuture = getFreeConnection(pool)
  yield connFuture
  var index = connFuture.read
  block:
    var conn = pool.connections[index]
    body
  pool.futures[index].complete()

proc execAsync(conn: apgConnection, statement: string, pN: int32, pT: POid,
               pV: cstringArray, pL, pF: ptr int32,
               rF: int32, notify = true): Future[apgResult] =
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
          processPendingNotifications(conn, notify)
          # processing results
          while true:
            if pqisBusy(conn.pgconn) == 0:
              var res = pqgetResult(conn.pgconn)
              if res != nil:
                let stres = pqresultStatus(res)
                case stres
                of PGRES_EMPTY_QUERY, PGRES_COMMAND_OK, PGRES_TUPLES_OK:
                  apgres.pgress.add(res)
                of PGRES_COPY_IN, PGRES_COPY_OUT:
                  apgres.pgress.add(res)
                  retFuture.complete(apgres)
                  return true
                else:
                  retFuture.fail(newException(ValueError,
                                              $pqerrorMessage(conn.pgconn)))
                  return true
              else:
                retFuture.complete(apgres)
                return true
            else:
              return false

  var query: cstring = statement
  var qres: int32 = 0
  if pN == 0:
    qres = pqsendQuery(conn.pgconn, query)
  else:
    qres = pqsendQueryParams(conn.pgconn, query, pN, pT, pV, pL, pF, rF)

  if qres == 0:
    retFuture.fail(newException(ValueError, $pqerrorMessage(conn.pgconn)))
  else:
    let fd = AsyncFD(pqsocket(conn.pgconn))
    discard cb(fd)
  return retFuture

proc execPoolAsync(pool: apgPool, statement: string, pN: int32, pT: POid,
                   pV: cstringArray, pL, pF: ptr int32,
                   rF: int32): Future[apgResult] {.async.} =
  var index = await getFreeConnection(pool)
  result = await execAsync(pool.connections[index], statement, pN, pT,
                           pV, pL, pF, rF, false)
  # processing connection notifies
  for cnode in pool.connections[index].notifies.nodes():
    for pnode in pool.notifies.nodes():
      if pnode.value.future != nil:
        if pnode.value.notify.channel == cnode.value.notify.channel:
          pnode.value.future.complete(cnode.value.notify)
      pool.notifies.remove(pnode)
    pool.connections[index].notifies.remove(cnode)

  pool.futures[index].complete()

proc len*(apgres: apgResult): int =
  ## Returns number of query results stored inside ``apgres``.

  result = len(apgres.pgress)

proc close*(apgres: apgResult) =
  ## Closes query results stored inside ``apgres``.

  for pgr in apgres.pgress:
    pqclear(pgr)
  apgres.pgress.setLen(0)

proc `[]`*(apgres: apgResult, index: int): PPGresult =
  ## Returns query result from ``apgres`` by ``index``.

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
  ## Returns single value from result ``pgres``.

  var x = pqgetvalue(pgres, 0, 0)
  result = if isNil(x): "" else: $x


proc getRow*(pgres: PPGresult): Row =
  ## Returns ``Row`` value from result ``pgres``.

  var L = pqnfields(pgres)
  result = newSeq[string](L)
  setRow(pgres, result, 0, L)
  pqclear(pgres)

proc getRows*(pgres: PPGresult, rows: int): seq[Row] =
  ## Returns at least ``row`` number of rows from result ``pgres``.
  ## If ``row`` is -1, all rows will be returned.

  var L = pqnfields(pgres)
  var R = pqntuples(pgres).int
  if (rows != -1) and (rows < R):
    R = rows
  result = newSeq[Row](R)
  for row in 0..<R:
    result[row] = newSeq[string](L)
    setRow(pgres, result[row], row, L)

template getAllRows*(pgres: PPGresult): seq[Row] =
  ## Returns all rows from result ``pgres``.

  getRows(pgres, -1)

iterator rows*(pgres: PPGresult): Row =
  ## Iterates over ``pgres`` result rows

  var L = pqnfields(pgres)
  var R = pqntuples(pgres)
  var result = newSeq[string](L)
  for i in 0..<R:
    setRowInline(pgres, result, i, L)
    yield result

proc getAffectedRows*(pgres: PPGresult): int64 =
  ## Returns number of affected rows for ``pgres`` result.

  result = parseBiggestInt($pqcmdTuples(pgres))

proc setClientEncoding*(conn: apgConnection,
                        encoding: string): Future[void] {.async.} =
  ## Sets the client encoding.

  var statement = "set client_encoding to '" & encoding & "'"
  var ares = await execAsync(conn, statement, 0, nil, nil, nil, nil, 0)
  if pqresultStatus(ares.pgress[0]) != PGRES_COMMAND_OK:
    raise newException(ValueError, $pqerrorMessage(conn.pgconn))
  close(ares)

proc getClientEncoding*(conn: apgConnection): string =
  ## Returns the client encoding.

  result = $pgEncodingToChar(pqclientEncoding(conn.pgconn))

proc getVersion*(): int =
  ## Returns an integer representing the libpq version.

  result = pqlibVersion().int

proc getServerVersion*(conn: apgConnection): int =
  ## Returns an integer representing the backend version.

  result = pqserverVersion(conn.pgconn).int

proc getProtocolVersion*(conn: apgConnection): int =
  ## Interrogates the frontend/backend protocol being used.

  result = pqprotocolVersion(conn.pgconn).int

#
# exec macro
#

# shows error with linenumber and column for argument caused error
proc showError(s: string, n: NimNode = nil) {.compileTime.} =
  if isNil(n):
    error(s)
  else:
    error(s & ", at " & lineinfo(n))
# var <n> = <v>
proc newVarArray(n, v: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(n, newEmptyNode(), v)
  )
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
proc newVarInteger(n, v: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(
      n, newEmptyNode(), newNimNode(nnkCall).add(newIdentNode(!"prepare"), v)
    )
  )
# var <n> = <v>
proc newVarSimple(n, v: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(n, newEmptyNode(), v)
  )
# var <n> = cast[<i>](<v>)
proc newVarCast(n, i, v: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(
      n, newEmptyNode(), newNimNode(nnkCast).add(v, i)
    )
  )
# var <n> = <c>()
proc newVarExec(n, c: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(n, newEmptyNode(), c)
  )
# var <n> = $(<v>)
proc newVarStringify(n, v: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(n, newEmptyNode(),
      newNimNode(nnkPrefix).add(
        newIdentNode(!"$"),
        newNimNode(nnkPar).add(v)
      )
    )
  )

# var <n> = prepare(cast[<i>](<v>))
proc newVarFloat(n, i, v: NimNode): NimNode {.compileTime.} =
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
proc newVarSeq(n, v: NimNode, s: string): NimNode {.compileTime.} =
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
# var <n> = newPgJson((<v>))
proc newVarJson(n, v: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkVarSection).add(
    newNimNode(nnkIdentDefs).add(
      n, newEmptyNode(), newCall(newIdentNode(!"newPgJson"), v)
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
# proc newEchoVar(n: NimNode): NimNode {.compileTime.} =
#   result = newNimNode(nnkCall).add(
#     newIdentNode(!"echo"),
#     newNimNode(nnkCall).add(newIdentNode(!"repr"), n)
#   )
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
      showError("Unsupported int size!")
  of ntyString, ntyCString:
    result = 1009
  else:
    showError("Unsupported array oid type!")

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
    showError("Argument's type `seq[" & $impType & "]`is not supported", op)

macro exec*(conn: apgConnection|apgPool, statement: string,
            params: varargs[typed]): Future[apgResult] =
  ## Submits a command ``statement`` to the server connection
  ## or connection's pool. ``params`` is SQL parameters which will
  ## be passed with command.

  # if there no params, we just generate call to execAsync
  if len(params) == 0:
    if $getTypeInst(conn).symbol == "apgConnection":
      result = newTree(nnkStmtListExpr,
                 newCall(bindSym"execAsync", conn, statement, newLit 0,
                         newNimNode(nnkNilLit), newNimNode(nnkNilLit),
                         newNimNode(nnkNilLit), newNimNode(nnkNilLit),
                         newLit(0))
               )
    else:
      result = newTree(nnkStmtListExpr,
                 newCall(bindSym"execPoolAsync", conn, statement, newLit 0,
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
        of ntyArray, ntyArrayConstr:
          var imp = getTypeImpl(param)
          var ntp = getType(imp[2])
          getSequence(result, postNodes, param, ntp, np, valuesList,
                      lensList, typesList, formatsList)
        of ntyDistinct:
          var name = $getTypeInst(param).symbol
          case name
          of "JsonB":
            result.add(newVarJson(np, param[1]))
            valuesList.add(callRaw(np))
            lensList.add(castSome(callSize(np), "int32"))
            typesList.add(castOid(3802))
            formatsList.add(newLit(1'i32))
            postNodes.add(newCall(newIdentNode(!"free"), np))
          of "Json":
            result.add(newVarStringify(np, param[1]))
            valuesList.add(castPointer0(np, "pointer"))
            lensList.add(castSome(callLength(np), "int32"))
            typesList.add(castOid(114)) # json oid
            formatsList.add(newLit(1.int32))
          else:
            showError("Argument's type is not supported", param)
        of ntyRef:
          var name = $getTypeInst(param).symbol
          if name == "JsonNode":
            if param.kind == nnkPrefix:
              var jsonCompiled = genSym(nskLet, "jsonCompiled" & $idx)
              result.add(newLetStmt(jsonCompiled, param))
              result.add(newVarStringify(np, jsonCompiled))
            else:
              result.add(newVarStringify(np, param))
            valuesList.add(castPointer0(np, "pointer"))
            lensList.add(castSome(callLength(np), "int32"))
            typesList.add(castOid(114)) # json oid
            formatsList.add(newLit(1.int32))
          else:
            showError("Argument's type object of `" & name & "`is not supported",
                      param)
        else:
          showError("Argument's type is not supported", param)
      inc(idx)

    if $getTypeInst(conn).symbol == "apgConnection":
      result.add(
        valuesArray, typesArray, lensArray, formatsArray,
        #newEchoVar(valuesName), newEchoVar(typesName),
        #newEchoVar(lensName), newEchoVar(formatsName),
        newVarExec(execFuture,
          newCall(bindSym"execAsync", conn, statement,
                  newLit len(params), castPointer0(typesName, "POid"),
                  castPointer0(valuesName, "cstringArray"),
                  newAddr0(lensName), newAddr0(formatsName), newLit(0)))
      )
    else:
      result.add(
        valuesArray, typesArray, lensArray, formatsArray,
        #newEchoVar(valuesName), newEchoVar(typesName),
        #newEchoVar(lensName), newEchoVar(formatsName),
        newVarExec(execFuture,
          newCall(bindSym"execPoolAsync", conn, statement,
                  newLit len(params), castPointer0(typesName, "POid"),
                  castPointer0(valuesName, "cstringArray"),
                  newAddr0(lensName), newAddr0(formatsName), newLit(0)))
      )
    for child in postNodes.children:
      result.add(child)
    result.add(execFuture)
    #echo(toStrLit(result))

#
# Asynchronous notify
#

template checkResultCommand(res) =
  var sres = pqresultStatus(res.pgress[0])
  close(res)
  if sres != PGRES_COMMAND_OK:
    raise newException(ValueError, $pqerrorMessage(res.pgress[0].conn))

template checkResultTuple(res) =
  var sres = pqresultStatus(res.pgress[0])
  close(res)
  if sres != PGRES_TUPLES_OK:
    raise newException(ValueError, $pqerrorMessage(res.pgress[0].conn))

proc listenNotify*(conn: apgConnection,
                   channel: string): Future[void] {.async.} =
  ## Registers listening for asynchronous notifies on connection ``conn`` and
  ## channel ``channel``.

  var query = "LISTEN \"" & channel & "\";"
  var res = await exec(conn, query)
  checkResultCommand(res)

proc listenNotify*(pool: apgPool,
                   channel: string): Future[void] {.async.} =
  ## Registers listening for asynchronous notifies on pool ``pool`` and
  ## channel ``channel``.

  var query = "LISTEN \"" & channel & "\";"
  var res = await exec(pool, query)
  checkResultCommand(res)

proc unlistenNotify*(conn: apgConnection,
                     channel: string): Future[void] {.async.} =
  ## Unregisters listening for asynchronous notifies on connection ``conn`` and
  ## channel ``channel``.

  var query = "UNLISTEN \"" & channel & "\";"
  var res = await exec(conn, query)
  checkResultCommand(res)

proc unlistenNotify*(pool: apgPool,
                     channel: string): Future[void] {.async.} =
  ## Unregisters listening for asynchronous notifies on pool ``pool`` and
  ## channel ``channel``.

  var query = "UNLISTEN \"" & channel & "\";"
  var res = await exec(pool, query)
  checkResultCommand(res)

proc sendNotify*(conn: apgConnection, channel: string,
                 payload: string): Future[void] {.async.} =
  ## Send asynchronous notify to channel ``channel`` with data in ``payload``.
  ## Be sure size of ``payload`` must be less, than 8000 bytes.

  # https://www.postgresql.org/docs/9.0/static/sql-notify.html
  doAssert(len(payload) < 8000)

  var p1 = channel
  var p2 = payload
  var query = "SELECT pg_notify($1, $2);"
  var res = await exec(conn, query, p1, p2)
  checkResultTuple(res)

proc sendNotify*(pool: apgPool, channel: string,
                 payload: string): Future[void] {.async.} =
  ## Send asynchronous notify to channel ``channel`` with data in ``payload``.
  ## Be sure size of ``payload`` must be less, than 8000 bytes.

  # https://www.postgresql.org/docs/9.0/static/sql-notify.html
  doAssert(len(payload) < 8000)

  var p1 = channel
  var p2 = payload
  var query = "SELECT pg_notify($1, $2);"
  var res = await exec(pool, query, p1, p2)
  checkResultTuple(res)

proc notify*(conn: apgConnection|apgPool, channel: string): Future[apgNotify] =
  ## Returns future, which will receive `apgNotify`, when asynchronous notify
  ## on channel ``channel`` arrives.

  var cell = apgNotifyCell()
  cell.notify.channel = channel
  cell.future = newFuture[apgNotify]("asyncpg.notify")
  conn.notifies.append(cell)
  return cell.future

#
# Escape functions
#

proc pqescapeLiteral(conn: PPGconn, bintext: cstring, binlen: int): cstring
     {.cdecl, dynlib: dllName, importc: "PQescapeLiteral".}
proc pqescapeByteaConn(conn: PPGconn, bintext: cstring, binlen :int,
                       resultlen: ptr int): cstring
     {.cdecl, dynlib: dllName, importc: "PQescapeByteaConn".}

proc escapeString*(conn: apgConnection, str: string): string =
  ## Escapes a string for use within an SQL command according to rules
  ## of connection `conn`.
  var r = pqescapeLiteral(conn.pgconn, cstring(str), len(str))
  if isNil(r):
    raise newException(ValueError, $pqerrorMessage(conn.pgconn))
  else:
    result = $r
    pqfreemem(cast[pointer](r))

proc escapeBytea*(conn: apgConnection, buf: pointer, size: int): string =
  ## Converts binary data from pointer `buf` and length `size` to
  ## PostgreSQL's BYTEA string representation.
  var s = 0
  var r = pqescapeByteaConn(conn.pgconn, cast[cstring](buf), size, addr s)
  if isNil(r):
    raise newException(ValueError, $pqerrorMessage(conn.pgconn))
  else:
    result = $r
    pqfreemem(cast[pointer](r))

proc unescapeBytea*(str: string): seq[char] =
  ## Converts PostgreSQL's BYTEA string representation to seq[char].
  var n = 0
  var r = pqunescapeBytea(cstring(str), n)
  doAssert(r != nil)
  result = newSeq[char](n)
  copyMem(addr result[0], r, n)
  pqfreemem(cast[pointer](r))

