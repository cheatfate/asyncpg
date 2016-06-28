import asyncdispatch, asyncpg

proc testEncoding(conn: apgConnection): Future[bool] {.async.} =
  result = false
  var r = await setClientEncoding(conn, "WIN1252")
  echo(r)
  if r:
    var s = getClientEncoding(conn)
    echo(s)
    if s == "WIN1252":
      result = true

var connStr = "host=localhost port=5432 dbname=travis_ci_test user=postgres"
var conn = waitFor connect(connStr)

block:
  var res = waitFor(testEncoding(conn))
  doAssert(res)

close(conn)
