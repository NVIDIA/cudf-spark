# Big Data Generation

In order to do scale testing we need a way to generate lots of data in a
deterministic way that gives us control over the number of unique values
in a column, the skew of the values in a column, and the correlation of
data between tables for joins. To accomplish this we wrote
`org.apache.spark.sql.tests.datagen`.

## Setup Environment

To get started with big data generation the first thing you need to do is
to include the appropriate jar on the classpath for your version of Apache Spark.
Note that this does not run on the GPU, but it does use parts of the shim framework
that the RAPIDS Accelerator does. The jar is specific to the version of Spark you
are using and is not pushed to Maven Central. Because of this you will have to
build it from source yourself.

```shell
cd datagen
mvn clean package -Dbuildver=$SPARK_VERSION
```

Where `$SPARK_VERSION` is a compressed version number, like 330 for Spark 3.3.0.

After this the jar should be at
`target/datagen_2.12-$PLUGIN_VERSION-spark$SPARK_VERSION.jar`
for example a Spark 3.3.0 jar for the 26.08.0 release would be
`target/datagen_2.12-26.08.0-spark330.jar`

To get a spark shell with this you can run
```shell
spark-shell --jars target/datagen_2.12-26.08.0-spark330.jar
```

After that you should be good to go.

## Generate Some Data

The first thing to do is to import the classes

```scala
import org.apache.spark.sql.tests.datagen._
```

After this the main entry point is `DBGen`. `DBGen` provides a way to generate
multiple tables, but we are going to start off with just one. To do this we can
call `addTable` on it.

```scala
val dataTable = DBGen().addTable("data", "a string, b byte", 5)
dataTable.toDF(spark).show()
+----------+----+
|         a|   b|
+----------+----+
|t=qIHOf:O)|  47|
|yXT-j<zLO^| -23|
|^tn*C#C=e/| -38|
|d=K0(!|8mQ|  38|
|m7oI7P\Vu5|-112|
+----------+----+
```

The parameters to `addTable` are first a name, then a schema to generate. This
can be a DDL like in the example or Spark SQL types. Finally, the number of
rows to be generated. Note that not all types are currently supported. It is not
too hard to add more, but it is a starting point.

### Nulls

By default, nulls are not generated. This is just to keep things simple. If you
want to include nulls you can do so by setting the null probability for
a column, or sub-column.

```scala
val dataTable = DBGen().addTable("data", "a string, b byte", 5)
dataTable("a").setNullProbability(0.5)
dataTable.toDF(spark).show()
+----------+----+
|         a|   b|
+----------+----+
|      null|  47|
|yXT-j<zLO^| -23|
|      null| -38|
|d=K0(!|8mQ|  38|
|      null|-112|
+----------+----+
```

You can provide your own code to control nulls too, but that is described in
the [advanced control section](#advanced-control).

## Controlling Unique Count

Generating nearly random data that fits a schema is great, but we want
to process this data in interesting ways, like doing a hash aggregate to see
how well it scales. To do that we really need to have a way to configure
the number of unique values that appear in a column. The
[Internal Details](#internal-details) section describes how this works
but here we will control this by setting a seed range. Let's start off by creating
10 million rows of data to be processed.

```scala
val dataTable = DBGen().addTable("data", "a string, b long", 10000000)
dataTable.toDF(spark).selectExpr("COUNT(DISTINCT a)", "COUNT(b)").show
+-----------------+--------+
|count(DISTINCT a)|count(b)|
+-----------------+--------+
|         10000000|10000000|
+-----------------+--------+
```

We have 10 million unique keys (column a). Which is fine if we want to test what
happens if nothing in the aggregate is combined. To reduce the number of unique
values you can give the column an inclusive min and max seed.

```scala
dataTable("a").setSeedRange(0, 100)
dataTable.toDF(spark).selectExpr("COUNT(DISTINCT a)", "COUNT(b)").show
+-----------------+--------+
|count(DISTINCT a)|count(b)|
+-----------------+--------+
|              101|10000000|
+-----------------+--------+
```

A seed is passed to a data generation function that uses it to deterministically
generate values. The seed range limits the seeds that are passed to the data
generation function to the provided inclusive range. This limits the number of
unique values that could be generated.

### Value Ranges

We also support value ranges for some, but not all data types. This can be
used to reduce the unique count of values for a data type, or restrict the
range of values produced to match a particular query you want to work with.
This is separate from the seed range. If you combine the two you might end
up with some values in the value range that are not produced because the
seed range is too small to have found a mapping to a particular value.

## Controlling Distribution

Generating values with a desired unique count is great, but we also want
to be able to generate values with various different distributions of keys.
By default, the generated values have a `FlatDistribution`, meaning that
every seed has approximately the same probability of being generated as any
other seed.

```scala
val dataTable = DBGen().addTable("data", "a string", 10000000)
dataTable("a").setSeedRange(0, 5)
dataTable.toDF(spark).groupBy("a").count.show
+----------+-------+
|         a|  count|
+----------+-------+
|P(m9]loe1s|1665529|
|h1uU#C[.Qc|1666387|
|p,mtx?CXMd|1668519|
|7c5vDh-|yd|1665820|
|Az[M`Q.'mn|1666036|
|FsZ!/*!){O|1667709|
+----------+-------+
```

Please note that for nested types, like structs and arrays, the seeds passed into
the parent type has little to no impact on the resulting output, except for nulls.
It is the child columns that have their own distributions that need to be taken
into account. See [Multi-Column Keys](#multi-column-keys) for info on how to work
around this.

### DistinctDistribution

The `DistinctDistribution` generates seeds from min seed to max seed, but
walks them in a non-sequential order. The data generation function also maps
the data in a pseudo-random way so the unique keys will not show this very
clearly.

```scala
val dataTable = DBGen().addTable("data", "a byte", 10)
dataTable("a").setSeedMapping(DistinctDistribution())
dataTable.toDF(spark).show
+---+
|  a|
+---+
| 97|
|121|
|-64|
| 51|
|-66|
| 83|
| 90|
| 76|
| 48|
| 80|
+---+
```

Be careful with this. If you set the seed range too small to support all
the rows, or if the cardinality of the data itself is too small, like a byte,
you will get repeated values.

But a `DistinctDistribution` is really helpful when wanting to generate a
fact table for a join where you want the primary key to be distinct.

Please note that this does not work well with structs or other nested types
because the seeds passed into the parents do not control what the children
generate. See [Multi-Column Keys](#multi-column-keys) for info on how to work
around this.

### NormalDistribution

Often data is distributed in a normal or Gaussian like distribution.
`NormalDistribution` takes a mean and a standard deviation to provide a way to
insert some basic skew into your data. Please note that this will clamp
the produced values to the configured seed range, so if seed range is not large
enough to hold several standard deviations of values from the mean the
actual distribution will be off.

```scala
val dataTable = DBGen().addTable("data", "a string", 100000)
dataTable("a").setSeedMapping(NormalDistribution(50, 1.0)).setSeedRange(0, 100)
dataTable.toDF(spark).groupBy("a").count().orderBy(desc("count")).show()
+----------+-----+
|         a|count|
+----------+-----+
|,J~pA-KqBn|38286|
| 0~{5)Uek>|24279|
|9=o`YbIDy{|24122|
|F&1rC%ge3P| 6147|
|#"IlU%=azD| 5977|
|xqW\HOC.;L|  591|
|bK%|@|fs9a|  547|
|(Z=9IR8h83|   26|
|T<oMow6W_L|   24|
|Eb9fEBb-]B|    1|
+----------+-----+
```

### ExponentialDistribution

`ExponentialDistribution` takes a target seed value and a standard deviation to provide a way
to insert an exponential skew. The target seed is the seed that is most likely to show up and
the standard deviation is `1/rate`. The median should be one standard deviation below the
target.

```scala
val dataTable = DBGen().addTable("data", "a string", 100000)
dataTable("a").setSeedMapping(ExponentialDistribution(50, 1.0)).setSeedRange(0, 100)
dataTable.toDF(spark).groupBy("a").count().orderBy(desc("count")).show()
+----------+-----+
|         a|count|
+----------+-----+
|,J~pA-KqBn|63428|
| 0~{5)Uek>|23026|
|#"IlU%=azD| 8602|
|xqW\HOC.;L| 3141|
|(Z=9IR8h83| 1164|
|Eb9fEBb-]B|  412|
|do)6AwiT_T|  129|
||i2l\J)u8I|   62|
|VZav:oU#g[|   23|
|kFR]RZ9pu||    8|
| aMZ({x5#1|    5|
+----------+-----+
```

### MultiDistribution

There are times when you might want to combine more than one distribution. Like
having a `NormalDistribution` along with a `FlatDistribution` so that the data
is skewed, but there is still nearly full coverage of the seed range. Of you could
combine two `NormalDistribution` instances to have two different sized bumps at
different key ranges. `MultiDistribution` allows you to do this. It takes a
`Seq` of weight/`LocationToSeedMapping` pairs. The weights are relative to
each other and determine how often on mapping will be used vs another. If
you wanted a `NormalDistribution` to be used 10 times as often as a
`FlatDistribution` you would give the normal a weight of 10 and the flat a
weight of 1.


```scala
val dataTable = DBGen().addTable("data", "a string", 100000)
dataTable("a").setSeedMapping(MultiDistribution(Seq(
  (10.0, NormalDistribution(50, 1.0)),
  (1.0, FlatDistribution())))).setSeedRange(0, 100)
dataTable.toDF(spark).groupBy("a").count().orderBy(desc("count")).show()
+----------+-----+
|         a|count|
+----------+-----+
|,J~pA-KqBn|33532|
| 0~{5)Uek>|23093|
|9=o`YbIDy{|22131|
|F&1rC%ge3P| 5711|
|#"IlU%=azD| 5646|
|xqW\HOC.;L|  659|
|bK%|@|fs9a|  615|
|(Z=9IR8h83|  120|
|n&]AosAQJf|  111|
||H6h"R!7CH|  110|
|-bVd8htg"^|  108|
|u2^.x?oJBb|  107|
| aMZ({x5#1|  107|
|Qb#XoQx[{Z|  107|
|5C&<?S31Kp|  106|
|T<oMow6W_L|  106|
|)Wf2']8yFm|  105|
|_qo)|Ti2}n|  105|
|S1Jdbn_hda|  104|
|\SANbeK.?`|  103|
+----------+-----+
only showing top 20 rows
```

## Multi-Column Keys

With the basic tools provided we can now replicate a lot of processing. We can do
complicated things like a join with a fact table followed by an aggregation.

```scala
val dbgen = DBGen()
val factTable = dbgen.addTable("facts", "join_key long, value int", 1000L)
factTable("join_key").setSeedRange(0, 999).setSeedMapping(DistinctDistribution())
val dataTable = dbgen.addTable("data", "join_key long, agg_key long", 100000000L)
dataTable("join_key").setSeedRange(0, 999)
dataTable("agg_key").setSeedRange(0, 9)
val fdf = factTable.toDF(spark)
val ddf = dataTable.toDF(spark)
spark.time(fdf.join(ddf).groupBy("agg_key").agg(min("value"),
  max("value"), sum("value")).orderBy("agg_key").show())
+--------------------+-----------+----------+-------------------+
|             agg_key| min(value)|max(value)|         sum(value)|
+--------------------+-----------+----------+-------------------+
|-5395130417820713410|-2146262091|2137753315|-790927616093345360|
|-2676343845813715061|-2146262091|2137753315|-790805185346631680|
|-2519888240660072622|-2146262091|2137753315|-790643842502125280|
| -423375629809417248|-2146262091|2137753315|-790620036523597620|
| 2201036318770978408|-2146262091|2137753315|-790864265299987700|
| 2236810499658231116|-2146262091|2137753315|-791190430932705540|
| 5462872210444757997|-2146262091|2137753315|-791094653391187280|
| 5615780013263136643|-2146262091|2137753315|-790580412619137960|
| 6937819975370740711|-2146262091|2137753315|-791081287243774740|
| 8365571621216757512|-2146262091|2137753315|-791155236047506840|
+--------------------+-----------+----------+-------------------+

Time taken: 890163 ms
```

Or you could run it with the RAPIDS Accelerator where the data generations is still
done on the CPU and cut the time down to just 75,696 ms

But what if we wanted to join on multiple columns, or do an aggregation with
multiple columns as the keys. Or what about nested columns where there are structs
or arrays in the key set. This gets to be much more difficult. To help with this
we provide key groups. A key group is set on a table using the `configureKeyGroup`
API. There are currently two types of key groups supported.

### CorrelatedKeyGroup

A correlated key group is a group of columns and child columns where the code to
generate the seed is normalized so that for each row the same seed is passed into
all the generator functions. (This is not 100% correct for arrays and maps, but it
is close enough). This results in the generated data being correlated with each
other so that if you set a seed range of 1 to 200, you will get 200 unique values
in each column, and 200 unique values for any combination of the keys in that
key group.

This should work with any distribution and any type you want. The key to making
this work is that you need to configure the value ranges the same for both sets
of corresponding keys. In most cases you want the types to be the same as well,
but Spark supports equi-joins where the left and right keys are different types.
The default generators for integral types should produce the same values for the
same input keys if the value range is the same for both. This is not true for
floating point (no one should ever join on floating point values), or decimal
types, unless the scale is the same for both sides.

```scala
val dataTable = DBGen().addTable("data", "a byte, b long", 3)
dataTable.toDF(spark).show()
+----+--------------------+
|   a|                   b|
+----+--------------------+
|-120| 3448826316550928693|
|  26|-1617384775774370579|
| -43|-2697761218773508623|
+----+--------------------+
dataTable.configureKeyGroup(Seq("a", "b"), CorrelatedKeyGroup(1, 0, 1), FlatDistribution())
dataTable.toDF(spark).show()
+---+-------------------+
|  a|                  b|
+---+-------------------+
| 31|2236810499658231116|
| 31|2236810499658231116|
| 30|2201036318770978408|
+---+-------------------+
dataTable("b").setValueRange(Byte.MinValue, Byte.MaxValue)
dataTable.toDF(spark).show()
+---+---+
|  a|  b|
+---+---+
| 31| 31|
| 31| 31|
| 30| 30|
+---+---+
```

This can also be used to create a column for a struct or an array that has the
desired properties as if it were a single column.

Here the `CorrelatedKeyGroup` takes an ID for this grouping, should be the same for
both sides of a join, followed by the seed range for the group of values. The
seed range is consistent between groups with the same ID. This allows you to setup
ranges that only partially overlap if desired.

### CombinatorialKeyGroup

A combinatorial key group really is just a key group that tries to automatically
set the seed ranges for the columns involved so that the number of possible
combinations is close to, but not over, the desired number of unique values.
It does not currently work with nested types, and really only the
FlatDistribution will produce the results you want. In theory this
could be extended to support structs, but it does not currently do so.
It also does not take into account value ranges when computing these. But
it does remove the need for correlation between the various values in the
columns, and is likely closer to many real world data sets.

## Internal Details

Internally there are a few different user pluggable levels of deterministic
mappings. These mappings go from a row number generated by a `spark.range`
command to the final data in a column.

### LocationToSeedMapping

The first level maps the current location of a data item
(table, column, row + sub-row) to a single 64-bit seed. The
`LocationToSeedMapping` class handles this. That mapping should produce a
seed that corresponds to the user provided seed range. But it has full
control over how it wants to do that. It could favor some seed more than
others, or simply go off of the row itself.

You can manually set this for columns or sub-columns through the
`configureKeyGroup` API in a `TableGen`. Or you can call
`setSeedMapping` on a column or sub-column. Be careful not to mix the two
because they can conflict with each other and there are no guard rails.

### NullGeneratorFunction

The next level decides if a null should be returned on not. This is an optional
level. If the user does not configure nulls, or if the type is not nullable
this never runs.

This can be set on any column or sub-column by calling either `setNullProbability`
which will install a `NullProbabilityGenerationFunction` or by calling the
`setNullGen` API on that item.

### LengthGeneratorFunction

For variable length types, like strings and arrays, a pluggable length generator
function is used to produce those lengths. Fixed length generator is preferred
to avoid data skew in the resulting column. This is because the naive way to generate a length
where all possible lengths have an equal probability produces skew in the
resulting values. A length of 0 has one and only one possible value in it.
So if we restrict the length to 0 or 1, then half of all values generated will be
zero length strings, which is not ideal.

If you want to set the length of a String or Array you can navigate to the
column or sub-column you want and call `setLength(fixedLen)` on it. This will install
an updated `FixedLengthGeneratorFunction`. You may set a range of lengths using
setLength(minLen, maxLen), but this may introduce skew in the resulting data.

```scala
val dataTable = DBGen().addTable("data", "a string, b array<string>, c string", 3)
dataTable("a").setLength(1)
dataTable("b").setLength(2)
dataTable("b")("data").setLength(3)
dataTable("c").setLength(1,5)
dataTable.toDF(spark).show(false)
+---+----------+----+
|a  |b         |c   |
+---+----------+----+
|t  |[X]6, /<E]|_,sA|
|y  |[[d", uu=]|H:  |
|^  |[uH[, wjX]|ooa>|
+---+----------+----+
```

You can also set a `LengthGeneratorFunction` instance for any column or sub-column
using the `setLengthGen` API.

### GeneratorFunction

The thing that actually produces data is a `GeneratorFunction`. It maps the key to
a value in the desired value range if that range is supported. For nested
types like structs or arrays parts of this can be delegated to child
GeneratorFunctions.

You can set the `GeneratorFunction` for a column or sub-column with the
`setValueGen` API.

## Advanced Control

The reality is that nearly everything is pluggable and the `GeneratorFunction` has
close to ultimate control over what is actually generated. The `GeneratorFunction`
for a column can be replaced by a user supplied implementation. It also has
control to decide how the location information is mapped to the values. By
convention, it should honor things like the `LocationToSeedMapping`,
but it is under no requirement to do so.

This is similar for the `LocationToSeedMapping` and the `NullGeneratorFunction`.
If you have a requirement to generate null values from row 1024 to row 9999999,
you can write a `NullGeneratorFunction` to do that and install it on a column

```scala
case class MyNullGen(minRow: Long, maxRow: Long,
    gen: GeneratorFunction = null) extends NullGeneratorFunction {

  override def withWrapped(gen: GeneratorFunction): MyNullGen =
    MyNullGen(minRow, maxRow, gen)

  override def withLocationToSeedMapping(
      mapping: LocationToSeedMapping): MyNullGen =
    this

  override def apply(rowLoc: RowLocation): Any = {
    val rowNum = rowLoc.rowNum
    if (rowNum <= maxRow && rowNum >= minRow) {
      null
    } else {
      gen(rowLoc)
    }
  }
}

...

dataTable("a").setNullGen(MyNullGen(1024, 9999999L))
```

Similarly, if you have a requirement to generate JSON formatted strings that
follow a given pattern you can do that. Or provide a distribution where a very
specific seed shows up 99% of the time, and the rest of the time it falls back
to the regular `FlatDistribution`, you can also do that. It is designed to be very
flexible.

# Scale Test Data Generation Entry
In order to generate large scale dataset to test the query engine, we use the data
generation library above to create a test suite. For more details like the data schema,
how to use the test suite etc, please refer to [ScaleTest.md](./ScaleTest.md).
