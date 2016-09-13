#
#
#                    AsyncPG
#        (c) Copyright 2016 Eugene Kabanov
#
#    See the file "LICENSE", included in this
#    distribution, for details about the copyright.
#

## This module implements PostgreSQL ARRAY binary helper

import strutils, byteswap

when defined(windows) and defined(gcc):
  {.passC:"-mno-ms-bitfields".}

type
  pgUncArray {.unchecked.}[T] = array[0..100_000_000, T]

  pgArray*[T] = object
    p: pointer
    size: int
    swap: bool

  pgArrayElement* {.packed.} [T] = object
    size: int32
    value: T

proc `+`(p: pointer, i: uint|int): pointer {.inline.} =
  cast[pointer](cast[uint](p) + i.uint)

proc newPgArray*[T](x: openarray[T], swap = false): pgArray[T] =
  var oid: int32 = 0
  var sw = swap
  when (T is int16) or (T is uint16):
    oid = 21
  elif (T is int32) or (T is uint32):
    oid = 23
  elif (T is int64) or (T is uint64):
    oid = 20
  elif (T is int) or (T is uint):
    when sizeof(int) == 8:
      oid = 20
    elif sizeof(int) == 4:
      oid = 23
    elif sizeof(int) == 2:
      oid = 21
    else:
      raise newException(ValueError, "Unsupported int size!")
  elif T is float32:
    oid = 700
  elif T is float64:
    oid = 701
  elif T is bool:
    oid = 16
    sw = false
  elif T is string:
    oid = 25
    sw = false
  else:
    raise newException(ValueError, "Unsupported array type!")

  when T is string:
    var size = 0
    var i = 0

    while i < len(x):
      size += len(x[i])
      inc(i)
    size += len(x) * sizeof(int32) + 20

    var p = alloc(size)
    var h = cast[ptr pgUncArray[int32]](p)
    var d = cast[pointer](p + 20)

    h[0] = prepare(1'i32) # NDIM
    h[1] = 0 # FLAGS
    h[2] = prepare(oid) # OID
    h[3] = prepare(len(x).int32) # LEN DIM[0]
    h[4] = prepare(1'i32) # LBOUND DIM[0]

    i = 0
    while i < len(x):
      var ss = x[i]
      var ms = cast[ptr int32](d)
      var ml = len(x[i])
      if ml >= high(int32):
        raise newException(ValueError, "String with index `" & $i & "` is too big!")
      var ps = prepare(ml.int32)
      ms[] = ps.int32
      d = d + 4
      copyMem(d, addr ss[0], ml)
      d = d + ml
      inc(i)
    result = pgArray[T](p: p, size: size, swap: swap)
  else:
    var size = sizeof(pgArrayElement[T]) * len(x) + 20
    var p = alloc(size)
    var h = cast[ptr pgUncArray[int32]](p)
    var d = cast[ptr pgUncArray[pgArrayElement[T]]](p + 20)

    h[0] = prepare(1'i32) # NDIM
    h[1] = 0 # FLAGS
    h[2] = prepare(oid) # OID
    h[3] = prepare(len(x).int32) # LEN DIM[0]
    h[4] = prepare(1'i32) # LBOUND DIM[0]

    if sw:
      var idx = 0
      while idx < len(x):
        d[idx].size = prepare(sizeof(T).int32)
        d[idx].value = prepare(x[idx])
        inc(idx)
    else:
      var idx = 0
      while idx < len(x):
        d[idx].size = prepare(sizeof(T).int32)
        d[idx].value = x[idx]
        inc(idx)
    result = pgArray[T](p: p, size: size, swap: swap)

proc raw*[T](pga: pgArray[T]): pointer =
  result = pga.p

proc `$`*[T](pga: pgArray[T]): string =
  var h = cast[ptr pgUncArray[int32]](pga.p)
  result = "PGARRAY at 0x" & toHex(cast[int](pga.p), sizeof(int) * 2) & "\n"
  result &= "header = [ndim = " & $prepare(h[0]) & ", "
  result &= "flags = " & $prepare(h[1]) & ", "
  result &= "oid = " & $prepare(h[2]) & ", "
  result &= "len(dim0) = " & $prepare(h[3]) & ", "
  result &= "lbound(dim0) = " & $prepare(h[4]) & ", "
  result &= "byteswap = " & $pga.swap & "]\n"

  when T is string:
    var d = cast[pointer](pga.p + 20)
    var i = 0
    var length = prepare(h[3]).int
    var r = ""
    while i < length:
      var hs = cast[ptr int32](d)
      var ds = cast[pointer](d + 4)
      var s = prepare(hs[].int32)
      var cs = newString(s)
      copyMem(addr cs[0], ds, s)
      d = d + 4 + s.int
      if i != length - 1:
        r = r & "'" & cs & "'/" & $s & ", "
      else:
        r = r & "'" & cs & "'/" & $s
      inc(i)
  else:
    var d = cast[ptr pgUncArray[pgArrayElement[T]]](pga.p + 20)
    var i = 0
    var length = prepare(h[3]).int
    var r = ""
    while i < length:
      if i != length - 1:
        if pga.swap:
          r = r & $prepare(d[i].value) & "/" & $prepare(d[i].size) & ", "
        else:
          r = r & $d[i].value & "/" & $prepare(d[i].size) & ", "
      else:
        if pga.swap:
          r = r & $prepare(d[i].value) & "/" & $prepare(d[i].size)
        else:
          r = r & $d[i].value & "/" & $prepare(d[i].size)
      inc(i)
  result &= "data = [" & $r & "]\n"
  result &= "total length in bytes = " & $pga.size

proc size*[T](pga: pgArray[T]): int =
  result = pga.size

proc len*[T](pga: pgArray[T]): int =
  var h = cast[ptr pgUncArray[int32]](pga.p)
  result = h[3].int

proc free*[T](pga: pgArray[T]) =
  dealloc(pga.p)
