# asyncPG [![Build Status](https://travis-ci.org/cheatfate/asyncpg.svg?branch=master)](https://travis-ci.org/cheatfate/asyncpg)

Asynchronous PostgreSQL driver for Nim Language [http://www.nim-lang.org](http://nim-lang.org/)

## Main features

- Pure asynchronous implementation, based on LibPQ and Nim's async core
- Support for many SQL statements/results in one request
- Ability to send parameters separately from query, thus avoiding the need for tedious and error-prone quoting and escaping.
- Automatic type conversion for basic Nim types
- Sending and receiving COPY data
- ARRAYS and JSON/JSONB support
- Asynchronous Notification

## Installation

You can use Nim official package manager `nimble` to install `asyncpg`. The most recent version of the library can be installed via:  

```
$ nimble install asyncpg
```
or directly from Github repository
```
$ nimble install https://github.com/cheatfate/asyncpg.git
```

## Minimal requirements

- PostgreSQL 9.x (if you want to have support for JSONB types, you need PostgreSQL 9.4 at least)
- Nim language compiler 0.14.2

## Documentation

You can find documentation in [[Wiki]].