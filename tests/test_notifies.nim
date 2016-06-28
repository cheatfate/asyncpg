import asyncdispatch, asyncpg

proc testConnectionNotify(conn: apgConnection): Future[bool] {.async.} =
  var r1 = false
  var r2 = false
  await conn.listenNotify("testChannel")
  var notifyFuture1 = conn.notify("testChannel")
  await conn.sendNotify("testChannel", "payload")
  var sleepFuture1 = sleepAsync(100)
  await notifyFuture1 or sleepFuture1
  if notifyFuture1.finished:
    r1 = true
  await conn.unlistenNotify("testChannel")
  var notifyFuture2 = conn.notify("testChannel")
  await conn.sendNotify("testChannel", "payload")
  var sleepFuture2 = sleepAsync(100)
  await notifyFuture2 or sleepFuture2
  if not notifyFuture2.finished:
    r2 = true
  result = r1 and r2

proc testPoolNotify(pool: apgPool): Future[bool] {.async.} =
  var r1 = false
  var r2 = false
  await pool.listenNotify("testChannel")
  var notifyFuture = pool.notify("testChannel")
  await pool.sendNotify("testChannel", "payload")
  var sleepFuture = sleepAsync(100)
  await notifyFuture or sleepFuture
  if notifyFuture.finished:
    r1 = true
  else:
    r1 = false
  await pool.unlistenNotify("testChannel")
  var notifyFuture2 = pool.notify("testChannel")
  await pool.sendNotify("testChannel", "payload")
  var sleepFuture2 = sleepAsync(100)
  await notifyFuture2 or sleepFuture2
  if not notifyFuture2.finished:
    r2 = true
  result = r1 and r2

var connStr = "host=localhost port=5432 dbname=travis_ci_test user=postgres"
var conn = waitFor connect(connStr)
var pool = newPool(10)
waitFor pool.connect(connStr)

block: # asynchronous notify on connection test
  var r = waitFor testConnectionNotify(conn)
  doAssert(r)

block: # asynchronous notify on pool test
  var r = waitFor testPoolNotify(pool)
  doAssert(r)

close(pool)
close(conn)
