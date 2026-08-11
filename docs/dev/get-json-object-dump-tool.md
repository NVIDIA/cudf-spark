---
layout: page
title: Dump tool for get_json_object
nav_order: 12
parent: Developer Overview
---

# Dump tool for get_json_object

## Overview
In order to help debug the issues with the `get_json_object` function, the NVIDIA cuDF plugin for
Apache Spark provides a dump tool to save debug information to try and reproduce the issues. Note,
the dumped data will be masked to protect the customer data.

## How to enable
This assumes that the cuDF plugin has already been enabled.

The `get_json_object` expression may be off by default so enable it first
```
'spark.rapids.sql.expression.GetJsonObject': 'true'
```

To enable debugging just set the path to dump the data to. Note that this
path is interpreted using the Hadoop FileSystem APIs. This means that
a path with no schema will go to the default file system.

```
'spark.rapids.sql.expression.GetJsonObject.debugPath': '/tmp/DEBUG_JSON_DUMP/'
```

This path should be a directory or someplace that we can create a directory to
store files in. Multiple files may be written out. Note that each instance of
`get_json_object` will mask the data in different ways, but the same
instance should mask the data in the same way.

You may also set the max number of rows for each file/batch. Each time a new
batch of data comes into the `get_json_object` expression a new file is written
and this controls the maximum number of rows that may be written out. 
```
'spark.rapids.sql.test.get_json_object.saveRows': '1024'
```
This config can be skipped, because default value works.

## Masking
Please note that this cannot currently be disabled.
This tool should not dump the original input data.
The goal is to find out what types of issues are showing up, and ideally 
give the cuDF team enough information to reproduce it.

Digits `[0-9]` will be remapped to `[0-9]`, the mapping is chosen
randomly for each instance of the expression. This is done to preserve
the format of the numbers, even if they are not 100% the same.

The characters `[a-zA-Z]` are also randomly remapped to `[a-zA-Z]` similar
to the numbers. But many of these are preserved because they are part of
special cases.

The letters that are preserved are `a, b, c, d, e, f, l, n, r, s, t, u, A, B, C, D, E, F`

These are preserved because they could be used as a part of 
  * special keywords like `true`, `false`, or `null`
  * number formatting like `1.0E-3`
  * escape characters defined in the JSON standard `\b\f\n\r\t\u`
  * or hexadecimal numbers that are a part of the `\u` escape sequence

All other characters are mapped to the letter `s` unless they are one of the following.

  * ASCII `[0 to 31]` are considered to be control characters in the JSON spec and in some cases are not allowed
  * `-` for negative numbers
  * `{ } [ ] , : " '` are part of the structure of JSON, or at least are considered that way
  * `\` for escape sequences
  * `$ [ ] . * '` which are part of JSON paths
  * `?` which Spark has as a special case for JSON path, but no one else does.

## Stored Data
The dumped data is stored in a CSV file, that should be compatible with Spark,
and most other CSV readers.  CSV is a format that is not great at storing complex
data, like JSON in it, so there are likely to be some small compatibility issues.
The following shows you how to read the stored data using Spark with Scala.

Spark wants the data to be stored with no line separators, but JSON can have this.
So we replace `\r` and `\n` with a character sequences that is not likely to show up
in practice. JSON data can also conflict with CSV escape handling, especially if the
input data is not valid JSON. As such we also replace double quotes and commas just in
case.

```scala
// Replace this with the actual path to read from 
val readPath = "/data/tmp/DEBUG_JSON_DUMP"

val df = spark.read.
  schema("isLegacy boolean, path string, originalInput string, cpuOutput string, gpuOutput string").
  csv(readPath)

val strUnescape = Seq("isLegacy") ++ Seq("path", "originalInput", "cpuOutput", "gpuOutput").
  map(c => s"""replace(replace(replace(replace($c, '**CR**', '\r'), '**LF**', '\n'), '**QT**', '"'), '**COMMA**', ',') as $c""")

val data = df.selectExpr(strUnescape : _*)
```
