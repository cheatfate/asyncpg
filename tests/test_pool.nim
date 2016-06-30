import asyncdispatch, asyncpg

const POOL_SIZE = 20

proc makeLongCall(pool: apgPool) {.async.} =
  var res = await exec(pool, "SELECT pg_sleep(1);")
  close(res)

proc makeShortCall(pool: apgPool) {.async.} =
  var res = await exec(pool, "SELECT pg_sleep(0.5);")
  close(res)

proc testPool(pool: apgPool): Future[bool] {.async.} =
  result = false
  var i = 0
  var futs = newSeq[Future[void]](POOL_SIZE)
  while i < POOL_SIZE:
    futs[i] = makeLongCall(pool)
    inc(i)
  await makeShortCall(pool)
  var k = 0
  i = 0
  while i < POOL_SIZE:
    if futs[i].finished:
      inc(k)
    inc(i)
  if k == POOL_SIZE:
    result = true

proc testWithConnection(pool: apgPool): Future[bool] {.async.} =
  result = true

  withConnection(pool, conn1) do:
    await setClientEncoding(conn1, "WIN1252")
  withConnection(pool, conn2) do:
    var b = getClientEncoding(conn2)
    if b != "WIN1252":
      result = false

var pool = newPool(POOL_SIZE)
var connStr = "host=localhost port=5432 dbname=travis_ci_test user=postgres"
waitFor pool.connect(connStr)
block:
  var res = waitFor testPool(pool)
  doAssert(res)

block:
  var res = waitFor testWithConnection(pool)
  doAssert(res)

close(pool)
