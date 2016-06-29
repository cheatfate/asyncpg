import asyncdispatch, asyncpg

proc testRows(conn: apgConnection) {.async.} =
  block: # multiple statements in one query test
    var res = await asyncpg.exec(conn, "SELECT 3;SELECT 4;SELECT 5;")
    var length = len(res)
    doAssert(length == 3)
    var value1 = getValue(res[0])
    var value2 = getValue(res[1])
    var value3 = getValue(res[2])
    doAssert(value1 == "3" and value2 == "4" and value3 == "5")
    close(res)

var connStr = "host=localhost port=5432 dbname=travis_ci_test user=postgres"
var conn = waitFor connect(connStr)
waitFor testRows(conn)
close(conn)