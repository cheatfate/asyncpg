import asyncdispatch, asyncpg

var connStr = "host=localhost port=5432 dbname=travis_ci_test user=postgres"
var conn = waitFor connect(connStr)
close(conn)
