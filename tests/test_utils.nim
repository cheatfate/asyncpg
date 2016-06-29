import asyncdispatch, asyncpg

proc testEncoding(conn: apgConnection): Future[bool] {.async.} =
  result = false
  var r = await setClientEncoding(conn, "WIN1252")
  if r:
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

close(conn)
