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

var connStr = "host=localhost port=5432 dbname=travis_ci_test user=postgres"
var conn = waitFor connect(connStr)

block: # set/get encoding test
  var res = waitFor(testEncoding(conn))
  doAssert(res)

block: # protocol/server/api versions test
  var res = testVersions(conn)
  doAssert(res)

close(conn)
