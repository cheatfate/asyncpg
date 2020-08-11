#
#
#                    AsyncPG
#        (c) Copyright 2016 Eugene Kabanov
#
#    See the file "LICENSE", included in this
#    distribution, for details about the copyright.
#

## This module implements PostgreSQL JSONB binary helper

import json, strutils, byteswap

type
  JsonB* = distinct JsonNode
  Json* = distinct JsonNode

  pgJson = object
    p: pointer
    size: int

proc `+`(p: pointer, i: uint|int): pointer {.inline.} =
  cast[pointer](cast[uint](p) + i.uint)

proc newPgJson*(n: JsonNode): pgJson =
  var s = $n
  var size = len(s) + 1
  var p = alloc(size)
  var h = cast[ptr UncheckedArray[int8]](p)
  var d = cast[pointer](p + 1)
  h[0] = 1'i8
  copyMem(d, addr(s[0]), len(s))
  result = pgJson(p: p, size: size)

proc raw*(pgj: pgJson): pointer =
  result = pgj.p

proc `$`*(pgj: pgJson): string =
  var h = cast[ptr UncheckedArray[int32]](pgj.p)
  result = "PGJSON at 0x" & toHex(cast[int](pgj.p), sizeof(int) * 2) & "\n"
  result &= "header = [version = " & $prepare(h[0]) & "]\n"
  var d = cast[pointer](pgj.p + 1)
  var r = newString(pgj.size)
  copyMem(addr(r[0]), d, pgj.size - 1)
  result &= "data = [" & $r & "]\n"
  result &= "total length in bytes = " & $pgj.size

proc size*(pgj: pgJson):int =
  result = pgj.size

proc free*(pgj: pgJson) =
  dealloc(pgj.p)

when isMainModule:
  var j = %* [{"name": "John", "age": 30}, {"name": "Susan", "age": 31}]
  var c = newPgJson(j)
  echo $c
  var d = newPgJson(%* [{"name": "John", "age": 30}, {"name": "Susan", "age": 31}])
  echo $d
