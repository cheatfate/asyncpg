#
#
#                    AsyncPG
#        (c) Copyright 2016 Eugene Kabanov
#
#    See the file "LICENSE", included in this
#    distribution, for details about the copyright.
#

## This module implements functions for network byteorder
## conversation for different platforms.
## (Windows, MacOS, FreeBSD, OpenBSD, NetBSD, Linux, Solaris)

when cpuEndian == bigEndian:
  template prepare*(x: int16): int16 = x
  template prepare*(x: uint16): uint16 = x
  template prepare*(x: int32): int32 = x
  template prepare*(x: uint32): uint32 = x
  template prepare*(x: int): int = x
  template prepare*(x: uint): uint = x
  template prepare*(x: float32): float32 = x
  template prepare*(x: float64): float64 = x
  template prepare*(x: float): float = x
  template prepare*(x: bool): bool = x
  template prepare*(x: int64): int64 = x
  template prepare*(x: uint64): uint64 = x
else:
  template prepareLong*(x: int64): int64 =
    ((cast[int64](x) shl 56) or
     ((cast[int64](x) shl 40) and 0xFF000000000000'i64) or
     ((cast[int64](x) shl 24) and 0xFF0000000000'i64) or
     ((cast[int64](x) shl 8) and 0xFF00000000'i64) or
     ((cast[int64](x) shr 8) and 0xFF000000'i64) or
     ((cast[int64](x) shr 24) and 0xFF0000'i64) or
     ((cast[int64](x) shr 40) and 0xFF00'i64) or
     (cast[int64](x) shr 56))

  template prepareLong*(x: uint64): uint64 =
    cast[uint64](prepareLong(cast[int64](x)))

  when defined(windows):
    when defined(vcc):
      proc prepare*(x: uint16): uint16
           {.importc: "_byteswap_ushort", header: "<intrin.h>".}
      template prepare*(x: int16): int16 =
        cast[int16](prepare(x.uint16))
      proc prepare*(x: uint32): uint32
           {.importc: "_byteswap_ulong", header: "<intrin.h>".}
      template prepare*(x: int32): int32 =
        cast[int32](prepare(x.uint32))
      when sizeof(int) == 8:
        proc prepare*(x: uint64): uint64
             {.importc: "_byteswap_uint64", header: "<intrin.h>".}
        template prepare*(x: int64): int64 =
          cast[int64](prepare(x.uint64))
      else:
        proc prepare*(x: uint64): uint64 =
          result = prepareLong(x)
        proc prepare*(x: int64): int64 =
          result = prepareLong(x)
    elif defined(gcc):
      template prepare*(x: uint16): uint16 =
               (x shr 8'u16) or (x shl 8'u16)
      template prepare*(x: int16): int16 =
        (cast[int16](prepare(x.uint16)))
      proc prepare*(x: uint32): uint32
           {.importc: "__bswapd", header: "<x86intrin.h>".}
      template prepare*(x: int32): int32 =
        (cast[int32](prepare(x.uint32)))
      when sizeof(int) == 8:
        proc prepare*(x: uint64): uint64
             {.importc: "__bswapq", header: "<x86intrin.h>".}
        template prepare*(x: int64): int64 =
          (cast[int64](prepare(x.uint64)))
      else:
        proc prepare*(x: uint64): uint64 =
          result = prepareLong(x)
        proc prepare*(x: int64): int64 =
          result = prepareLong(x)

  elif defined(macosx):
    template prepare*(x: uint16): uint16 =
      (x shr 8'u16) or (x shl 8'u16)
    template prepare*(x: int16): int16 =
      (cast[int16](prepare(x.uint16)))
    proc prepare*(x: uint32): uint32
         {.importc: "__builtin_bswap32", header: "<sys/_endian.h>".}
    template prepare*(x: int32): int32 =
      (cast[int32](prepare(x.uint32)))
    when sizeof(int) == 8:
      proc prepare*(x: uint64): uint64
           {.importc: "__builtin_bswap64", header: "<sys/_endian.h>".}
      template prepare*(x: int64): int64 =
        (cast[int64](prepare(x.uint64)))
    else:
      proc prepare*(x: uint64): uint64 =
        result = prepareLong(x)
      proc prepare*(x: int64): int64 =
        result = prepareLong(x)

  elif defined(linux):
    proc prepare*(x: uint16): uint16
         {.importc: "bswap_16", header: "<byteswap.h>".}
    template prepare*(x: int16): int16 =
      (cast[int16](prepare(x.uint16)))
    proc prepare*(x: uint32): uint32
         {.importc: "bswap_32", header: "<byteswap.h>".}
    template prepare*(x: int32): int32 =
      (cast[int32](prepare(x.uint32)))
    when sizeof(int) == 8:
      proc prepare*(x: uint64): uint64
        {.importc: "bswap_64", header: "<byteswap.h>".}
      template prepare*(x: int64): int64 =
        (cast[int64](prepare(x.uint64)))
    else:
      proc prepare*(x: uint64): uint64 =
        result = prepareLong(x)
      proc prepare*(x: int64): int64 =
        result = prepareLong(x)

  elif defined(freebsd) or defined(netbsd):
    proc prepare*(x: uint16): uint16
         {.importc: "bswap16", header: "<sys/endian.h>".}
    template prepare*(x: int16): int16 =
      (cast[int16](prepare(x.uint16)))
    proc prepare*(x: uint32): uint32
         {.importc: "bswap32", header: "<sys/endian.h>".}
    template prepare*(x: int32): int32 =
      (cast[int32](prepare(x.uint32)))
    when sizeof(int) == 8:
      proc prepare*(x: uint64): uint64
           {.importc: "bswap64", header: "<sys/endian.h>".}
      template prepare*(x: int64): int64 =
        (cast[int64](prepare(x.uint64)))
    else:
      proc prepare*(x: uint64): uint64 =
        result = prepareLong(x)
      proc prepare*(x: int64): int64 =
        result = prepareLong(x)

  elif defined(openbsd):
    proc prepare*(x: uint16): uint16
         {.importc: "__swap16", header: "<sys/_endian.h>".}
    template prepare*(x: int16): int16 =
      (cast[int16](prepare(x.uint16)))
    proc prepare*(x: uint32): uint32
         {.importc: "__swap32", header: "<sys/_endian.h>".}
    template prepare*(x: int32): int32 =
      (cast[int32](prepare(x.uint32)))
    when sizeof(int) == 8:
      proc prepare*(x: uint64): uint64
           {.importc: "__swap64", header: "<sys/_endian.h>".}
      template prepare*(x: int64): int64 =
        (cast[int64](prepare(x.uint64)))
    else:
      proc prepare*(x: uint64): uint64 =
        result = prepareLong(x)
      proc prepare*(x: int64): int64 =
        result = prepareLong(x)
  elif defined(solaris):
    proc prepare*(x: uint16): uint16
         {.importc: "htons", header: "<asm/byteorder.h>".}
    template prepare*(x: int16): int16 =
      (cast[int16](prepare(x.uint16)))
    proc prepare*(x: uint32): uint32
         {.importc: "htonl", header: "<asm/byteorder.h>".}
    template prepare*(x: int32): int32 =
      (cast[int32](prepare(x.uint32)))
    when sizeof(int) == 8:
      proc prepare*(x: uint64): uint64
           {.importc: "htonll", header: "<asm/byteorder.h>".}
      template prepare*(x: int64): int64 =
        (cast[int64](prepare(x.uint64)))
    else:
      proc prepare*(x: uint64): uint64 =
        result = prepareLong(x)
      proc prepare*(x: int64): int64 =
        result = prepareLong(x)

  when sizeof(int) == 8:
    template prepare*(x: int): int =
      (cast[int](prepare(x.uint64)))
    template prepare*(x: uint): uint =
      (cast[uint](prepare(x.uint64)))
  elif sizeof(int) == 4:
    template prepare*(x: int): int =
      (cast[int](prepare(x.uint32)))
    template prepare*(x: uint): uint =
      (cast[uint](prepare(x.uint32)))
  elif sizeof(int) == 2:
    template prepare*(x: int): int =
      (cast[int](prepare(x.uint16)))
    template prepare*(x: uint): uint =
      (cast[uint](prepare(x.uint32)))

  template prepare*(x: float32): float32 =
    (cast[float32](prepare(cast[int32](x))))
  template prepare*(x: float64): float64 =
    (cast[float64](prepare(cast[int64](x))))
  template prepare*(x: float): float =
    (cast[float](prepare(cast[int64](x))))

  template prepare*(x: bool): bool = x
