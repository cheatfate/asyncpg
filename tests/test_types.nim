import asyncdispatch, asyncpg

proc testTypes(conn: apgConnection) {.async.} =
  block: # empty parameters test
    var res = await asyncpg.exec(conn, "SELECT 3;")
    var value = getValue(res[0])
    close(res)
    doAssert(value == "3")
    # expect: "3"

  block: # char type test
    var a = 0x30'u8
    var res = await exec(conn, "SELECT $1 || $2", a, 0x31'u8)
    var value = getValue(res[0])
    close(res)
    doAssert(value == "01")
    # expect: "01"

  block: # integer type test
    var a = 0x30'i16
    var res = await exec(conn, "SELECT $1 + $2", a, 0x31'i16)
    var value = getValue(res[0])
    close(res)
    doAssert(value == "97")
    # expect: "97"

  block: # boolean test
    var a = false
    var res = await exec(conn, "SELECT $1 OR $2", a, true)
    var value = getValue(res[0])
    close(res)
    doAssert(value == "t")
    # expect: "t"

  block: # string test
    var a = "Hello "
    var res = await exec(conn, "SELECT $1 || $2", a, "World!")
    var value = getValue(res[0])
    close(res)
    doAssert(value == "Hello World!")
    # expect: "Hello World!"

  block: # cstring test
    var a = cast[cstring]("Hello C ")
    var res = await exec(conn, "SELECT $1 || $2", a, "World!")
    var value = getValue(res[0])
    close(res)
    doAssert(value == "Hello C World!")
    # expect: "Hello C World!"

  block: # seq[bool] test
    var a = @[false, false]
    var res = await exec(conn, "SELECT $1 || $2", a, @[true, true])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{f,f,t,t}")
    # expect: "{f,f,t,t}"

  block: # array[bool] test
    var a = [false, false]
    var res = await exec(conn, "SELECT $1 || $2", a, [true, true])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{f,f,t,t}")
    # expect: "{f,f,t,t}"

  block: # seq[char, int8, uint8] test
    var a = @['0', '1']
    var b = @[0x32'i8, 0x33'i8]
    var c = @[0x34'u8, 0x35'u8]
    var res = await exec(conn, "SELECT $1 || $2 || $3 || $4 || $5 || $6",
                         a, b, c, @['6', '7'], @[0x38'i8, 0x39'i8],
                         @[0x30'u8, 0x30'u8])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "\\x303132333435363738393030")
    # expect: "\x303132333435363738393030"

  block: # array[char, int8, uint8] test
    var a = ['0', '1']
    var b = [0x32'i8, 0x33'i8]
    var c = [0x34'u8, 0x35'u8]
    var res = await exec(conn, "SELECT $1 || $2 || $3 || $4 || $5 || $6",
                         a, b, c, ['6', '7'], [0x38'i8, 0x39'i8],
                         [0x30'u8, 0x30'u8])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "\\x303132333435363738393030")
    # expect: "\x303132333435363738393030"

  block: # seq[int16, uint16] test
    var a = @[30000'i16, -30000'i16]
    var b = @[0xFFFF'u16, 0xFFFF'u16]
    var res = await exec(conn, "SELECT $1 || $2 || $3 || $4",
                         a, b, @[30000'i16, -30000'i16],
                         @[0xFFFF'u16, 0xFFFF'u16])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{30000,-30000,-1,-1,30000,-30000,-1,-1}")
    # expect: "{30000,-30000,-1,-1,30000,-30000,-1,-1}"

  block: # array[int16, uint16] test
    var a = [30000'i16, -30000'i16]
    var b = [0xFFFF'u16, 0xFFFF'u16]
    var res = await exec(conn, "SELECT $1 || $2 || $3 || $4",
                         a, b, [30000'i16, -30000'i16],
                         [0xFFFF'u16, 0xFFFF'u16])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{30000,-30000,-1,-1,30000,-30000,-1,-1}")
    # expect: "{30000,-30000,-1,-1,30000,-30000,-1,-1}"

  block: # seq[int32, uint32] test
    var a = @[100000'i32, -100000'i32]
    var b = @[0xFFFFFFFE'u32, 0xFFFFFFFE'u32]
    var res = await exec(conn, "SELECT $1 || $2 || $3 || $4",
                         a, b, @[100000'i32, -100000'i32],
                         @[0xFFFFFFFE'u32, 0xFFFFFFFE'u32])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{100000,-100000,-2,-2,100000,-100000,-2,-2}")
    # expect: "{100000,-100000,-2,-2,100000,-100000,-2,-2}"

  block: # seq[int32, uint32] test
    var a = [100000'i32, -100000'i32]
    var b = [0xFFFFFFFE'u32, 0xFFFFFFFE'u32]
    var res = await exec(conn, "SELECT $1 || $2 || $3 || $4",
                         a, b, [100000'i32, -100000'i32],
                         [0xFFFFFFFE'u32, 0xFFFFFFFE'u32])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{100000,-100000,-2,-2,100000,-100000,-2,-2}")
    # expect: "{100000,-100000,-2,-2,100000,-100000,-2,-2}"

  block: # seq[int64, uint64] test
    var a = @[2_000_000_000'i64, -2_000_000_000'i64]
    var b = @[0xFFFFFFFFFFFFFFFE'u64, 0xFFFFFFFFFFFFFFFFE'u64]
    var res = await exec(conn, "SELECT $1 || $2 || $3 || $4",
                         a, b, @[2_000_000_000'i64, -2_000_000_000'i64],
                         @[0xFFFFFFFFFFFFFFFE'u64, 0xFFFFFFFFFFFFFFFFE'u64])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{2000000000,-2000000000,-2,-2,2000000000,-2000000000,-2,-2}")
    # expect: "{2000000000,-2000000000,-2,-2,2000000000,-2000000000,-2,-2}"

  block: # array[int64, uint64] test
    var a = [2_000_000_000'i64, -2_000_000_000'i64]
    var b = [0xFFFFFFFFFFFFFFFE'u64, 0xFFFFFFFFFFFFFFFFE'u64]
    var res = await exec(conn, "SELECT $1 || $2 || $3 || $4",
                         a, b, [2_000_000_000'i64, -2_000_000_000'i64],
                         [0xFFFFFFFFFFFFFFFE'u64, 0xFFFFFFFFFFFFFFFFE'u64])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{2000000000,-2000000000,-2,-2,2000000000,-2000000000,-2,-2}")
    # expect: "{2000000000,-2000000000,-2,-2,2000000000,-2000000000,-2,-2}"

  block: # seq[float32] test
    var a = @[1.2'f32, 3.4'f32]
    var b = @[5.6'f32, 7.8'f32]
    var res = await exec(conn, "SELECT $1 || $2 || $3 || $4",
                         a, b, @[9.10'f32, 11.12'f32],
                         @[13.14'f32, 15.16'f32])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{1.2,3.4,5.6,7.8,9.1,11.12,13.14,15.16}")
    # expect: "{1.2,3.4,5.6,7.8,9.1,11.12,13.14,15.16}"

  block: # array[float32] test
    var a = [1.2'f32, 3.4'f32]
    var b = [5.6'f32, 7.8'f32]
    var res = await exec(conn, "SELECT $1 || $2 || $3 || $4",
                         a, b, [9.10'f32, 11.12'f32],
                         [13.14'f32, 15.16'f32])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{1.2,3.4,5.6,7.8,9.1,11.12,13.14,15.16}")
    # expect: "{1.2,3.4,5.6,7.8,9.1,11.12,13.14,15.16}"

  block: # seq[float] test
    var a = @[1.01, 2.02]
    var b = @[3.03, 4.04]
    var res = await exec(conn, "SELECT $1 || $2 || $3 || $4",
                         a, b, @[5.05, 6.06],
                         @[7.07, 8.08])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{1.01,2.02,3.03,4.04,5.05,6.06,7.07,8.08}")
    # expect: "{1.01,2.02,3.03,4.04,5.05,6.06,7.07,8.08}"

  block: # array[float] test
    var a = [1.01, 2.02]
    var b = [3.03, 4.04]
    var res = await exec(conn, "SELECT $1 || $2 || $3 || $4",
                         a, b, [5.05, 6.06],
                         [7.07, 8.08])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{1.01,2.02,3.03,4.04,5.05,6.06,7.07,8.08}")
    # expect: "{1.01,2.02,3.03,4.04,5.05,6.06,7.07,8.08}"

  block: # seq[string]
    var a = @["January", "February", "March", "April"]
    var res = await exec(conn, "SELECT $1 || $2",
                         a, @["May", "June", "July", "August"])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{January,February,March,April,May,June,July,August}")
    # expect "{January,February,March,April,May,June,July,August}"

  block: # array[string]
    var a = ["January", "February", "March", "April"]
    var res = await exec(conn, "SELECT $1 || $2",
                         a, ["May", "June", "July", "August"])
    var value = getValue(res[0])
    close(res)
    doAssert(value == "{January,February,March,April,May,June,July,August}")
    # expect "{January,February,March,April,May,June,July,August}"

var connStr = "host=localhost port=5432 dbname=travis_ci_test user=postgres"
var conn = waitFor connect(connStr)
waitFor testTypes(conn)
close(conn)
