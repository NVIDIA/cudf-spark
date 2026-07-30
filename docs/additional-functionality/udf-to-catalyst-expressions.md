---
layout: page
title: UDF to Catalyst Expressions
parent: Additional Functionality
nav_order: 4
---
# UDF to Catalyst Expressions

To speedup the processing of user defined functions (UDFs), the NVIDIA cuDF plugin for Apache Spark
introduces a UDF compiler extension to translate UDFs to Catalyst expressions.

To enable this operation on the GPU, set
[`spark.rapids.sql.udfCompiler.enabled`](../configs.md#sql.udfCompiler.enabled) to `true`.

Be aware Spark may produce different results for a compiled UDF vs. the non-compiled. For example: a
UDF of `x/y` where `y` happens to be `0`, the compiled catalyst expressions will return `NULL` while
the original UDF would fail the entire job with a `java.lang.ArithmeticException: / by zero`

When translating UDFs to Catalyst expressions, the supported UDF functions are limited:

| Operand type             | Operation                                                |
| -------------------------| ---------------------------------------------------------|
| Arithmetic Unary         | +x                                                       |
|                          | -x                                                       |
| Arithmetic Binary        | lhs + rhs                                                |
|                          | lhs - rhs                                                |
|                          | lhs * rhs                                                |
|                          | lhs / rhs                                                |
|                          | lhs % rhs                                                |
| Logical                  | lhs && rhs                                               |
|                          | lhs &#124;&#124; rhs                                     |
|                          | !x                                                       |
| Equality and Relational  | lhs == rhs                                               |
|                          | lhs < rhs                                                |
|                          | lhs <= rhs                                               |
|                          | lhs > rhs                                                |
|                          | lhs >= rhs                                               |
| Bitwise                  | lhs & rhs                                                |
|                          | lhs &#124; rhs                                           |
|                          | lhs ^ rhs                                                |
|                          | ~x                                                       |
|                          | lhs << rhs                                               |
|                          | lhs >> rhs                                               |
|                          | lhs >>> rhs                                              |
| Conditional              | if                                                       |
|                          | case                                                     |
| Math                     | abs(x)                                                   |
|                          | cos(x)                                                   |
|                          | acos(x)                                                  |
|                          | asin(x)                                                  |
|                          | tan(x)                                                   |
|                          | atan(x)                                                  |
|                          | tanh(x)                                                  |
|                          | cosh(x)                                                  |
|                          | ceil(x)                                                  |
|                          | floor(x)                                                 |
|                          | exp(x)                                                   |
|                          | log(x)                                                   |
|                          | log10(x)                                                 |
|                          | sqrt(x)                                                  |
|                          | x.isNaN                                                  |
| Type Cast                | *                                                        |
| String                   | lhs + rhs                                                |
|                          | lhs.equalsIgnoreCase(String rhs)                         |
|                          | x.toUpperCase()                                          |
|                          | x.trim()                                                 |
|                          | x.substring(int begin)                                   |
|                          | x.substring(int begin, int end)                          |
|                          | x.replace(char oldChar, char newChar)                    |
|                          | x.replace(CharSequence target, CharSequence replacement) |
|                          | x.startsWith(String prefix)                              |
|                          | lhs.equals(Object rhs)                                   |
|                          | x.toLowerCase()                                          |
|                          | x.length()                                               |
|                          | x.endsWith(String suffix)                                |
|                          | lhs.concat(String rhs)                                   |
|                          | x.isEmpty()                                              |
|                          | String.valueOf(boolean b)                                |
|                          | String.valueOf(char c)                                   |
|                          | String.valueOf(double d)                                 |
|                          | String.valueOf(float f)                                  |
|                          | String.valueOf(int i)                                    |
|                          | String.valueOf(long l)                                   |
|                          | x.contains(CharSequence s)                               |
|                          | x.indexOf(String str)                                    |
|                          | x.indexOf(String str, int fromIndex)                     |
|                          | x.replaceAll(String regex, String replacement)           |
|                          | x.split(String regex)                                    |
|                          | x.split(String regex, int limit)                         |
|                          | x.getBytes()                                             |
|                          | x.getBytes(String charsetName)                           |
| Date and Time            | LocalDateTime.parse(x, DateTimeFormatter.ofPattern(pattern)).getYear       |
|                          | LocalDateTime.parse(x, DateTimeFormatter.ofPattern(pattern)).getMonthValue |
|                          | LocalDateTime.parse(x, DateTimeFormatter.ofPattern(pattern)).getDayOfMonth |
|                          | LocalDateTime.parse(x, DateTimeFormatter.ofPattern(pattern)).getHour       |
|                          | LocalDateTime.parse(x, DateTimeFormatter.ofPattern(pattern)).getMinute     |
|                          | LocalDateTime.parse(x, DateTimeFormatter.ofPattern(pattern)).getSecond     |
| Empty array creation     | Array.empty[Boolean]                                     |
|                          | Array.empty[Byte]                                        |
|                          | Array.empty[Short]                                       |
|                          | Array.empty[Int]                                         |
|                          | Array.empty[Long]                                        |
|                          | Array.empty[Float]                                       |
|                          | Array.empty[Double]                                      |
|                          | Array.empty[String]                                      |
| Arraybuffer              | new ArrayBuffer()                                        |
|                          | x.distinct                                               |
|                          | x.toArray                                                |
|                          | lhs += rhs                                               |
|                          | lhs :+ rhs                                               |
| Method call              | Only if the method being called 1. Consists of operations supported by the UDF compiler, and 2. is one of the folllowing: a final method, a method in a final class, or a method in a final object |
| Captured variables       | Only primitive type variables captured from a method |
| Throwing exception       | Only if the exception thrown is a SparkException.  The exception is then convered to a RuntimeException at runtime |

All other expressions, including but not limited to `try` and `catch`, are unsupported and UDFs
with such expressions cannot be compiled.
