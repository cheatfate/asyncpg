import asyncdispatch, asyncpg, strutils

proc testEncoding(conn: apgConnection): Future[bool] {.async.} =
  result = false
  await setClientEncoding(conn, "WIN1252")
  var s = getClientEncoding(conn)
  if s == "WIN1252":
    result = true

proc testVersions(conn: apgConnection): bool =
  result = false
  if getVersion() > 0:
    if getServerVersion(conn) > 0:
      if getProtocolVersion(conn) > 0:
        result = true

proc testEscape(conn: apgConnection): bool =
  var r1 = false
  var r2 = false
  var r3 = false

  var a = "'\""
  if escapeString(conn, a) == "'''\"'":
    r1 = true
  var b = ['\x00', '\x01', '\x02', 'A', 'B']
  if escapeBytea(conn, addr(b[0]), len(b)) == "\\x0001024142":
    r2 = true
  var d = unescapeBytea("\\x0001024142")
  var i = 0
  r3 = true
  while i < len(b):
    if b[i] != d[i]:
      r3 = false
      break
    inc(i)
  result = r1 and r2 and r3

proc testCopy(conn: apgConnection): Future[bool] {.async.} =
  result = false
  var inString = newString(80)
  var outString = "8\l8\l8\l8\l8\l8\l8\l8\l8\l8\l"

  var dr = await exec(conn, "DROP TABLE IF EXISTS foo;")
  close(dr)

  var ar = await exec(conn, "CREATE TABLE foo(a int8);")
  close(ar)

  var sr1 = await exec(conn, "COPY foo FROM stdin;")
  close(sr1)

  var cr1 = await copyTo(conn, cast[pointer](addr outString[0]),
                         len(outString).int32)
  close(cr1)
  var cr2 = await copyTo(conn, nil, 0.int32)
  var rowsAffected = getAffectedRows(cr2[0])
  close(cr2)

  if rowsAffected == 10:
    var sr2 = await exec(conn, "COPY foo TO STDOUT;")
    close(sr2)
    var idx = 0
    while true:
      var cr3 = await copyFromInto(conn, cast[pointer](addr inString[idx]),
                                   len(inString))
      if cr3 == 0:
        inString.setLen(idx)
        break
      else:
        idx += cr3
  var br = await exec(conn, "DROP TABLE foo;")
  close(br)
  result = (inString == outString)

var connStr = "host=localhost port=5432 dbname=travis_ci_test user=postgres"
var conn = waitFor connect(connStr)

block: # set/get encoding test
  var res = waitFor(testEncoding(conn))
  doAssert(res)

block: # protocol/server/api versions test
  var res = testVersions(conn)
  doAssert(res)

block: # escaping test
  var res = testEscape(conn)
  doAssert(res)

block: # copy test
  var res = waitFor(testCopy(conn))
  doAssert(res)

close(conn)
