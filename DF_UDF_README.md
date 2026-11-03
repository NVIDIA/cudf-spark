# Scala / Java UDFS implemented using data frame

User Defined Functions (UDFs) are used for a number of reasons in Apache Spark. Much of the time it is to implement 
logic that is either very difficult or impossible to implement using existing SQL/Dataframe APIs directly. But they
are also used as a way to standardize processing logic across an organization or for code reused.

But UDFs come with some downsides. The biggest one is visibility into the processing being done. SQL is a language that
can be highly optimized. But a UDF in most cases is a black box, that the SQL optimizer cannot do anything about.
This can result in less than ideal query planning. Additionally, accelerated execution environments, like the
NVIDIA cuDF plugin for Apache Spark have no easy way to replace UDFs with accelerated versions, which can result in
slow performance.

This attempts to add visibility to the code reuse use case by providing a way to implement a UDF in terms of dataframe
commands. 

## Setup

The dataframe UDF plugin is packaged in the same jar as the cuDF plugin. This jar will need to
be added as a compile time dependency for code that wants to use this feature as well as adding the jar to your Spark
classpath just like you would do for GPU acceleration.

If you plan to not use the GPU accelerated processing, but still want dataframe UDF support on CPU applications then
add `com.nvidia.spark.DFUDFPlugin` to the `spark.sql.extensions` config. If you do use GPU accelerated processing
the cuDF plugin will enable this automatically. You don't need to set the `spark.sql.extensions` config, but it
won't hurt anything if you do add it. Now you can implement a UDF in terms of Dataframe operations.

## Usage

```scala
import com.nvidia.spark.functions._

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

val sum_array = df_udf((longArray: Column) => 
  aggregate(longArray,
    lit(0L),
    (a, b) => coalesce(a, lit(0L)) + coalesce(b, lit(0L)),
    a => a))
spark.udf.register("sum_array", sum_array)
```

You can then use `sum_array` however you would have used any other UDF. This allows you to provide a drop in replacement
implementation of an existing UDF. 

```scala
Seq(Array(1L, 2L, 3L)).toDF("data").selectExpr("sum_array(data) as result").show()

+------+
|result|
+------+
|     6|
+------+
```

Java APIs are also supported and should work the same as Spark's UDFs

```java
import com.nvidia.spark.functions.df_udf

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;


UserDefinedFunction myAdd = df_udf((Column lhs, Column rhs) -> lhs + rhs)
spark.udf().register("myadd", myAdd)

spark.sql("SELECT myadd(1, 1) as r").show();
// +--+
// | r|
// +--+
// | 2|
// +--+

```

## Type Checks

DataFrame APIs do not provide type safety when writing the code and that is the same here. There are no builtin type
checks for inputs yet. Also, because of how types are resolved in Spark there is no way to adjust the query based on
the types passed in. Type checks are handled by the SQL planner/optimizer after the UDF has been replaced. This means
that the final SQL will not violate any type safety, but it also means that the errors might be confusing. For example,
if I passed in an `ARRAY<DOUBLE>` to `sum_array` instead of an `ARRAY<LONG>` I would get an error like

```scala
Seq(Array(1.0, 2.0, 3.0)).toDF("data").selectExpr("sum_array(data) as result").show()
org.apache.spark.sql.AnalysisException: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "aggregate(data, 0, lambdafunction((coalesce(namedlambdavariable(), 0) + coalesce(namedlambdavariable(), 0)), namedlambdavariable(), namedlambdavariable()), lambdafunction(namedlambdavariable(), namedlambdavariable()))" due to data type mismatch: Parameter 3 requires the "BIGINT" type, however "lambdafunction((coalesce(namedlambdavariable(), 0) + coalesce(namedlambdavariable(), 0)), namedlambdavariable(), namedlambdavariable())" has the type "DOUBLE".; line 1 pos 0;
Project [aggregate(data#46, 0, lambdafunction((cast(coalesce(lambda x_9#49L, 0) as double) + coalesce(lambda y_10#50, cast(0 as double))), lambda x_9#49L, lambda y_10#50, false), lambdafunction(lambda x_11#51L, lambda x_11#51L, false)) AS result#48L]
+- Project [value#43 AS data#46]
   +- LocalRelation [value#43]

  at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.dataTypeMismatch(package.scala:73)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis0$5(CheckAnalysis.scala:269)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis0$5$adapted(CheckAnalysis.scala:256)
```

Which is not as simple to understand as a normal UDF.

```scala
val sum_array = udf((a: Array[Long]) => a.sum)

spark.udf.register("sum_array", sum_array)

Seq(Array(1.0, 2.0, 3.0)).toDF("data").selectExpr("sum_array(data) as result").show()
org.apache.spark.sql.AnalysisException: [CANNOT_UP_CAST_DATATYPE] Cannot up cast array element from "DOUBLE" to "BIGINT".
  The type path of the target object is:
- array element class: "long"
- root class: "[J"
You can either add an explicit cast to the input data or choose a higher precision type of the field in the target object
at org.apache.spark.sql.errors.QueryCompilationErrors$.upCastFailureError(QueryCompilationErrors.scala:285)
at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveUpCast$.org$apache$spark$sql$catalyst$analysis$Analyzer$ResolveUpCast$$fail(Analyzer.scala:3646)
at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveUpCast$$anonfun$apply$57$$anonfun$applyOrElse$234.applyOrElse(Analyzer.scala:3677)
at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveUpCast$$anonfun$apply$57$$anonfun$applyOrElse$234.applyOrElse(Analyzer.scala:3654)
```

We hope to add optional type checks in the future.
