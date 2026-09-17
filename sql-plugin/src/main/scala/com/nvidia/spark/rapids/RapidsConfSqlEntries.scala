/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.rapids

import ai.rapids.cudf.ParquetWriterOptions

import org.apache.spark.network.util.ByteUnit

private[rapids] trait RapidsConfSqlEntries extends RapidsConfResourceEntries {

  // ENABLE/DISABLE PROCESSING

  val SQL_ENABLED = conf("spark.rapids.sql.enabled")
    .doc("Enable (true) or disable (false) sql operations on the GPU")
    .commonlyUsed()
    .booleanConf
    .createWithDefault(true)

  val SQL_MODE = conf("spark.rapids.sql.mode")
    .doc("Set the mode for the cuDF plugin. The supported modes are explainOnly and " +
         "executeOnGPU. This config can not be changed at runtime, you must restart the " +
         "application for it to take affect. The default mode is executeOnGPU, which means " +
         "the cuDF plugin converts Spark operations and executes them on the " +
         "GPU when possible. The explainOnly mode allows running queries on the CPU and the " +
         "cuDF plugin will evaluate the queries as if it was going to run on the GPU. " +
         "The explanations of what would have run on the GPU and why are output in log " +
         "messages. When using explainOnly mode, the default explain output is ALL, this can " +
         "be changed by setting spark.rapids.sql.explain. See that config for more details.")
    .startupOnly()
    .stringConf
    .transform(_.toLowerCase(java.util.Locale.ROOT))
    .checkValues(Set("explainonly", "executeongpu"))
    .createWithDefault("executeongpu")

  val UDF_COMPILER_ENABLED = conf("spark.rapids.sql.udfCompiler.enabled")
    .doc("When set to true, Scala UDFs will be considered for compilation as Catalyst expressions")
    .commonlyUsed()
    .booleanConf
    .createWithDefault(false)

  val DFUDF_ENABLED = conf("spark.rapids.sql.dfudf.enabled")
    .doc("When set to false, the DataFrame UDF plugin is disabled. True enables it.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val INCOMPATIBLE_OPS = conf("spark.rapids.sql.incompatibleOps.enabled")
    .doc("For operations that work, but are not 100% compatible with the Spark equivalent " +
      "set if they should be enabled by default or disabled by default.")
    .booleanConf
    .createWithDefault(true)

  val INCOMPATIBLE_DATE_FORMATS = conf("spark.rapids.sql.incompatibleDateFormats.enabled")
    .doc("When parsing strings as dates and timestamps in functions like unix_timestamp, some " +
         "formats are fully supported on the GPU and some are unsupported and will fall back to " +
         "the CPU.  Some formats behave differently on the GPU than the CPU.  Spark on the CPU " +
         "interprets date formats with unsupported trailing characters as nulls, while Spark on " +
         "the GPU will parse the date with invalid trailing characters. More detail can be found " +
         "at [parsing strings as dates or timestamps]" +
         "(../compatibility.md#parsing-strings-as-dates-or-timestamps).")
      .booleanConf
      .createWithDefault(false)

  val IMPROVED_FLOAT_OPS = conf("spark.rapids.sql.improvedFloatOps.enabled")
    .doc("For some floating point operations spark uses one way to compute the value " +
      "and the underlying cudf implementation can use an improved algorithm. " +
      "In some cases this can result in cudf producing an answer when spark overflows.")
    .booleanConf
    .createWithDefault(true)

  val NEED_DECIMAL_OVERFLOW_GUARANTEES = conf("spark.rapids.sql.decimalOverflowGuarantees")
      .doc("FOR TESTING ONLY. DO NOT USE IN PRODUCTION. Please see the decimal section of " +
          "the compatibility documents for more information on this config.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_WINDOW_COLLECT_LIST = conf("spark.rapids.sql.window.collectList.enabled")
      .doc("The output size of collect list for a window operation is proportional to " +
          "the window size squared. The current GPU implementation does not handle this well " +
          "and is disabled by default. If you know that your window size is very small you " +
          "can try to enable it.")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_WINDOW_COLLECT_SET = conf("spark.rapids.sql.window.collectSet.enabled")
      .doc("The output size of collect set for a window operation can be proportional to " +
          "the window size squared. The current GPU implementation does not handle this well " +
          "and is disabled by default. If you know that your window size is very small you " +
          "can try to enable it.")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_WINDOW_UNBOUNDED_AGG = conf("spark.rapids.sql.window.unboundedAgg.enabled")
      .doc("This is a temporary internal config to turn on an unbounded to unbounded " +
          "window optimization that is still a work in progress. It should eventually replace " +
          "the double pass window exec.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val ENABLE_WINDOW_GROUP_LIMIT_OPT = conf("spark.rapids.sql.window.groupLimit.opt.enabled")
      .doc("When enabled, the plugin will skip redundant Final WindowGroupLimit operators " +
          "when they are followed by a WindowExec and FilterExec that perform the same " +
          "rank computation and filtering. This avoids computing the rank twice and " +
          "improves performance for queries with rank-based window limits.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val ENABLE_FLOAT_AGG = conf("spark.rapids.sql.variableFloatAgg.enabled")
    .doc("Spark assumes that all operations produce the exact same result each time. " +
      "This is not true for some floating point aggregations, which can produce slightly " +
      "different results on the GPU as the aggregation is done in parallel.  This can enable " +
      "those operations if you know the query is only computing it once.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_REPLACE_SORTMERGEJOIN = conf("spark.rapids.sql.replaceSortMergeJoin.enabled")
    .doc("Allow replacing sortMergeJoin with HashJoin")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_HASH_OPTIMIZE_SORT = conf("spark.rapids.sql.hashOptimizeSort.enabled")
    .doc("Whether sorts should be inserted after some hashed operations to improve " +
      "output ordering. This can improve output file sizes when saving to columnar formats.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_FOLD_LOCAL_AGGREGATE = conf("spark.rapids.sql.foldLocalAggregate.enabled")
    .doc("Whether to fold two-stages local aggregate into one-shot Complete aggregate.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_CAST_FLOAT_TO_DECIMAL = conf("spark.rapids.sql.castFloatToDecimal.enabled")
    .doc("Casting from floating point types to decimal on the GPU returns results that have " +
      "tiny difference compared to results returned from CPU.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_CAST_FLOAT_TO_STRING = conf("spark.rapids.sql.castFloatToString.enabled")
    .doc("Casting from floating point types to string on the GPU returns results that have " +
      "a different precision than the default results of Spark.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_FLOAT_FORMAT_NUMBER = conf("spark.rapids.sql.formatNumberFloat.enabled")
    .doc("format_number with floating point types on the GPU returns results that have " +
      "a different precision than the default results of Spark.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES =
    conf("spark.rapids.sql.castFloatToIntegralTypes.enabled")
      .doc("Casting from floating point types to integral types on the GPU supports a " +
          "slightly different range of values when using Spark 3.1.0 or later. Refer to the CAST " +
          "documentation for more details.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_CAST_DECIMAL_TO_FLOAT = conf("spark.rapids.sql.castDecimalToFloat.enabled")
      .doc("Casting from decimal to floating point types on the GPU returns results that have " +
          "tiny difference compared to results returned from CPU.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_CAST_STRING_TO_FLOAT = conf("spark.rapids.sql.castStringToFloat.enabled")
    .doc("When set to true, enables casting from strings to float types (float, double) " +
      "on the GPU. Currently hex values aren't supported on the GPU. Also note that casting from " +
      "string to float types on the GPU returns incorrect results when the string represents any " +
      "number \"1.7976931348623158E308\" <= x < \"1.7976931348623159E308\" " +
      "and \"-1.7976931348623158E308\" >= x > \"-1.7976931348623159E308\" in both these cases " +
      "the GPU returns Double.MaxValue while CPU returns \"+Infinity\" and \"-Infinity\" " +
      "respectively")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_CAST_STRING_TO_TIMESTAMP = conf("spark.rapids.sql.castStringToTimestamp.enabled")
    .doc("When set to false, this disables casting from string to timestamp on the GPU.")
    .booleanConf
    .createWithDefault(true)

  val HAS_EXTENDED_YEAR_VALUES = conf("spark.rapids.sql.hasExtendedYearValues")
      .doc("Spark 3.2.0+ extended parsing of years in dates and " +
          "timestamps to support the full range of possible values. Prior " +
          "to this it was limited to a positive 4 digit year. The Accelerator does not " +
          "support the extended range yet. This config indicates if your data includes " +
          "this extended range or not, or if you don't care about getting the correct " +
          "values on values with the extended range.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_INNER_JOIN = conf("spark.rapids.sql.join.inner.enabled")
      .doc("When set to true inner joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_CROSS_JOIN = conf("spark.rapids.sql.join.cross.enabled")
      .doc("When set to true cross joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_LEFT_OUTER_JOIN = conf("spark.rapids.sql.join.leftOuter.enabled")
      .doc("When set to true left outer joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_RIGHT_OUTER_JOIN = conf("spark.rapids.sql.join.rightOuter.enabled")
      .doc("When set to true right outer joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_FULL_OUTER_JOIN = conf("spark.rapids.sql.join.fullOuter.enabled")
      .doc("When set to true full outer joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_LEFT_SEMI_JOIN = conf("spark.rapids.sql.join.leftSemi.enabled")
      .doc("When set to true left semi joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_LEFT_ANTI_JOIN = conf("spark.rapids.sql.join.leftAnti.enabled")
      .doc("When set to true left anti joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_EXISTENCE_JOIN = conf("spark.rapids.sql.join.existence.enabled")
      .doc("When set to true existence joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_PROJECT_AST = conf("spark.rapids.sql.projectAstEnabled")
      .doc("Enable project operations to use cudf AST expressions when possible.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val ENABLE_TIERED_PROJECT = conf("spark.rapids.sql.tiered.project.enabled")
      .doc("Enable tiered projections.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val ENABLE_COMBINED_EXPR_PREFIX = "spark.rapids.sql.expression.combined."

  val ENABLE_COMBINED_EXPRESSIONS = conf("spark.rapids.sql.combined.expressions.enabled")
    .doc("For some expressions it can be much more efficient to combine multiple " +
      "expressions together into a single kernel call. This enables or disables that " +
      s"feature. Note that this requires that $ENABLE_TIERED_PROJECT is turned on or " +
      "else there is no performance improvement. You can also disable this feature for " +
      "expressions that support it. Each config is expression specific and starts with " +
      s"$ENABLE_COMBINED_EXPR_PREFIX followed by the name of the GPU expression class " +
      s"similar to what we do for enabling converting individual expressions to the GPU.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_RLIKE_REGEX_REWRITE = conf("spark.rapids.sql.rLikeRegexRewrite.enabled")
      .doc("Enable the optimization to rewrite rlike regex to contains in some cases.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  // FILE FORMATS
  val MULTITHREAD_READ_NUM_THREADS = conf("spark.rapids.sql.multiThreadedRead.numThreads")
      .doc("The maximum number of threads on each executor to use for reading small " +
        "files in parallel. This can not be changed at runtime after the executor has " +
        "started. Used with COALESCING and MULTITHREADED readers, see " +
        "spark.rapids.sql.format.parquet.reader.type, " +
        "spark.rapids.sql.format.orc.reader.type, or " +
        "spark.rapids.sql.format.avro.reader.type for a discussion of reader types. " +
        "If it is not set explicitly and spark.executor.cores is set, it will be tried to " +
        "assign value of `max(MULTITHREAD_READ_NUM_THREADS_DEFAULT, spark.executor.cores)`, " +
        s"where MULTITHREAD_READ_NUM_THREADS_DEFAULT = $MULTITHREAD_READ_NUM_THREADS_DEFAULT" +
        ".")
      .startupOnly()
      .commonlyUsed()
      .integerConf
      .checkValue(v => v > 0, "The thread count must be greater than zero.")
      .createWithDefault(MULTITHREAD_READ_NUM_THREADS_DEFAULT)

  // Memory-bounded multithreaded reading
  val MULTITHREAD_READ_MEMORY_LIMIT_ENABLED = {
    // TODO: This conf should be adjusted during the runtime
    conf("spark.rapids.sql.multiThreadedRead.memoryLimit.enabled")
        .doc("Enable memory-bounded multi-threaded reading. When enabled, the concurrency of " +
            "multi-threaded reading will be dynamically controlled based on the available memory " +
            s"budget per Spark executor.")
        .startupOnly()
        .internal()
        .booleanConf
        .createWithDefault(false)
  }

  val MULTITHREAD_READ_MEMORY_LIMIT_SIZE = {
    // TODO: This conf should be adjusted during the runtime
    conf("spark.rapids.sql.multiThreadedRead.memoryLimit.size")
        .doc("The maximum memory capacity in bytes to use for reading files in parallel. " +
            "This can not be changed at runtime after the executor has started. And if 0, it " +
            "will be set with 90% of executor's off-heap memory. This config only takes effect " +
            s"when ${MULTITHREAD_READ_MEMORY_LIMIT_ENABLED.key} is enabled.")
        .startupOnly()
        .internal()
        .bytesConf(ByteUnit.BYTE)
        .checkValue(v => v >= 0, s"The memory capacity must be greatThanOrEqual zero")
        .createWithDefault(0)
  }

  val MULTITHREAD_READ_MEMORY_LIMIT_ACQUISITION_TIMEOUT  = {
    conf("spark.rapids.sql.multiThreadedRead.memoryLimit.acquisitionTimeout")
        .doc("The maximum time in milliseconds to wait for a task to acquire required resource " +
            "before giving up and put off the run, by re-appending the task back into the queue " +
            "with a priority penality.")
        .startupOnly()
        .internal()
        .longConf
        .checkValue(v => v >= 0, "The timeout must be greater than zero")
        .createWithDefault(30 * 1000L)
  } // 30 seconds

  val MULTITHREAD_READ_MEMORY_LIMIT_TEST_PER_STAGE_POOL = {
    conf("spark.rapids.sql.multiThreadedRead.memoryLimit.tests.perStagePool")
        .doc("Enable test mode for the multi-threaded read. This will create different " +
            "threadpools for each stage, so as to verify threadpools with different configs " +
            "as independent test cases which can be run in parallel.")
        .startupOnly()
        .internal()
        .booleanConf
        .createWithDefault(false)
  }

  val ENABLE_PARQUET = conf("spark.rapids.sql.format.parquet.enabled")
    .doc("When set to false disables all parquet input and output acceleration")
    .booleanConf
    .createWithDefault(true)

  val ACCELERATED_COLUMNAR_TO_ROW_ENABLED =
    conf("spark.rapids.sql.acceleratedColumnarToRow.enabled")
      .doc("When set to true (default) the GPU columnar-to-row transition uses the GPU " +
        "transpose kernel (AcceleratedColumnarToRowIterator) for wide fixed-width / STRING " +
        "schemas. Setting it to false forces the slower per-row CPU iterator " +
        "(ColumnarToRowIterator). Mainly useful for troubleshooting and performance " +
        "comparisons; production workloads should leave this on.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_PARQUET_INT96_WRITE = conf("spark.rapids.sql.format.parquet.writer.int96.enabled")
    .doc("When set to false, disables accelerated parquet write if the " +
      "spark.sql.parquet.outputTimestampType is set to INT96")
    .booleanConf
    .createWithDefault(true)

  val PARQUET_WRITER_ROW_GROUP_SIZE_ROWS =
    conf("spark.rapids.sql.format.parquet.writer.rowGroupSizeRows")
      .doc("Maximum number of rows in a Parquet row group written by the GPU writer. " +
        "This is a best-effort limit because the cuDF writer does not split row groups " +
        "below its page fragment granularity. If not set, the cuDF writer default is used.")
      .internal()
      .integerConf
      .checkValue(v => v > 0, "The Parquet writer row group size rows must be positive.")
      .createOptional

  val PARQUET_WRITER_ROW_GROUP_SIZE_BYTES =
    conf("spark.rapids.sql.format.parquet.writer.rowGroupSizeBytes")
      .doc("Maximum size in bytes of a Parquet row group written by the GPU writer. " +
        "This is a best-effort limit because the cuDF writer does not split row groups " +
        "below its page fragment granularity. If not set, the cuDF writer default is used.")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v >= 1024L,
        "The Parquet writer row group size bytes must be at least 1024.")
      .createOptional

  val PARQUET_WRITER_DICTIONARY_POLICY =
    conf("spark.rapids.sql.format.parquet.writer.dictionaryPolicy")
      .doc("Dictionary policy for the GPU Parquet writer. NEVER disables dictionary " +
        "encoding for every column. ADAPTIVE (cuDF default) uses dictionary encoding " +
        "unless the per-column-chunk dictionary would exceed " +
        "spark.rapids.sql.format.parquet.writer.maxDictionarySize. ALWAYS allows " +
        "dictionary encoding even when it would prevent compression of a column chunk. " +
        "If not set, the cuDF writer default (ADAPTIVE) is used.")
      .internal()
      .stringConf
      .transform(_.toUpperCase(java.util.Locale.ROOT))
      .checkValues(ParquetWriterOptions.DictionaryPolicy.values.map(_.toString).toSet)
      .createOptional

  val PARQUET_WRITER_MAX_DICTIONARY_SIZE =
    conf("spark.rapids.sql.format.parquet.writer.maxDictionarySize")
      .doc("Maximum size in bytes of a per-column-chunk dictionary in a Parquet row " +
        "group written by the GPU writer. Only consulted when the dictionary policy is " +
        "ADAPTIVE (see spark.rapids.sql.format.parquet.writer.dictionaryPolicy); column " +
        "chunks whose dictionary payload would exceed this limit fall back to PLAIN " +
        "encoding. Raising this can reduce GPU-written file size for high-cardinality " +
        "columns at the cost of larger dictionary pages. If not set, the cuDF writer " +
        "default (1 MiB) is used.")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v >= 0L && v <= Integer.MAX_VALUE.toLong,
        s"The Parquet writer max dictionary size must be in [0, ${Integer.MAX_VALUE}] " +
          "(cuDF requires the dictionary size to fit in an int32).")
      .createOptional

  val PARQUET_READER_FOOTER_TYPE =
    conf("spark.rapids.sql.format.parquet.reader.footer.type")
      .doc("In some cases reading the footer of the file is very expensive. Typically this " +
          "happens when there are a large number of columns and relatively few " +
          "of them are being read on a large number of files. " +
          "This provides the ability to use a different path to parse and filter the footer. " +
          "AUTO is the default and decides which path to take using a heuristic. JAVA " +
          "follows closely with what Apache Spark does. NATIVE will parse and " +
          "filter the footer using C++.")
      .stringConf
      .transform(_.toUpperCase(java.util.Locale.ROOT))
      .checkValues(Set("JAVA", "NATIVE", "AUTO"))
      .createWithDefault("AUTO")

  // Deprecated legacy row-based UDF path. CPU bridge is the preferred replacement when it can
  // bridge the expression safely.
  val ENABLE_CPU_BASED_UDF = conf("spark.rapids.sql.rowBasedUDF.enabled")
    .doc("Deprecated. When set to true, enables the legacy row-based UDF path in a GPU " +
      "operation by transferring only the data it needs between GPU and CPU inside a query " +
      "operation, instead of falling this operation back to CPU. CPU bridge is the preferred " +
      "replacement when it can safely bridge the expression, and this config might be removed " +
      "in the future.")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_READER_TYPE = conf("spark.rapids.sql.format.parquet.reader.type")
    .doc("Sets the Parquet reader type. We support different types that are optimized for " +
      "different environments. The original Spark style reader can be selected by setting this " +
      "to PERFILE which individually reads and copies files to the GPU. Loading many small files " +
      "individually has high overhead, and using either COALESCING or MULTITHREADED is " +
      "recommended instead. The COALESCING reader is good when using a local file system where " +
      "the executors are on the same nodes or close to the nodes the data is being read on. " +
      "This reader coalesces all the files assigned to a task into a single host buffer before " +
      "sending it down to the GPU. It copies blocks from a single file into a host buffer in " +
      s"separate threads in parallel, see $MULTITHREAD_READ_NUM_THREADS. " +
      "MULTITHREADED is good for cloud environments where you are reading from a blobstore " +
      "that is totally separate and likely has a higher I/O read cost. Many times the cloud " +
      "environments also get better throughput when you have multiple readers in parallel. " +
      "This reader uses multiple threads to read each file in parallel and each file is sent " +
      "to the GPU separately. This allows the CPU to keep reading while GPU is also doing work. " +
      s"See $MULTITHREAD_READ_NUM_THREADS and " +
      "spark.rapids.sql.format.parquet.multiThreadedRead.maxNumFilesParallel to control " +
      "the number of threads and amount of memory used. " +
      "By default this is set to AUTO so we select the reader we think is best. This will " +
      "either be the COALESCING or the MULTITHREADED based on whether we think the file is " +
      "in the cloud. See spark.rapids.cloudSchemes.")
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(RapidsReaderType.values.map(_.toString))
    .createWithDefault(RapidsReaderType.AUTO.toString)

  val PARQUET_DECOMPRESS_CPU =
    conf("spark.rapids.sql.format.parquet.decompressCpu")
      .doc("If true then the CPU is eligible to decompress Parquet data rather than the GPU. " +
          s"See other spark.rapids.sql.format.parquet.decompressCpu.* configuration settings " +
          "to control this for specific compression codecs.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val PARQUET_DECOMPRESS_CPU_SNAPPY =
    conf("spark.rapids.sql.format.parquet.decompressCpu.snappy")
      .doc(s"If true and $PARQUET_DECOMPRESS_CPU is true then the CPU decompresses " +
          "Parquet Snappy data rather than the GPU")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val PARQUET_DECOMPRESS_CPU_ZSTD =
    conf("spark.rapids.sql.format.parquet.decompressCpu.zstd")
      .doc(s"If true and $PARQUET_DECOMPRESS_CPU is true then the CPU decompresses " +
          "Parquet Zstandard data rather than the GPU")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val READER_MULTITHREADED_COMBINE_THRESHOLD =
    conf("spark.rapids.sql.reader.multithreaded.combine.sizeBytes")
      .doc("The target size in bytes to combine multiple small files together when using the " +
        "MULTITHREADED parquet or orc reader. With combine disabled, the MULTITHREADED reader " +
        "reads the files in parallel and sends individual files down to the GPU, but that can " +
        "be inefficient for small files. When combine is enabled, files that are ready within " +
        "spark.rapids.sql.reader.multithreaded.combine.waitTime together, up to this " +
        "threshold size, are combined before sending down to GPU. This can be disabled by " +
        "setting it to 0. Note that combine also will not go over the " +
        "spark.rapids.sql.reader.batchSizeRows or spark.rapids.sql.reader.batchSizeBytes limits.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(64 * 1024 * 1024) // 64M

  val READER_MULTITHREADED_COMBINE_WAIT_TIME =
    conf("spark.rapids.sql.reader.multithreaded.combine.waitTime")
      .doc("When using the multithreaded parquet or orc reader with combine mode, how long " +
        "to wait, in milliseconds, for more files to finish if haven't met the size threshold. " +
        "Note that this will wait this amount of time from when the last file was available, " +
        "so total wait time could be larger then this.")
      .integerConf
      .createWithDefault(200) // ms

  val READER_MULTITHREADED_READ_KEEP_ORDER =
    conf("spark.rapids.sql.reader.multithreaded.read.keepOrder")
      .doc("When using the MULTITHREADED reader, if this is set to true we read " +
        "the files in the same order Spark does, otherwise the order may not be the same. " +
        "Now it is supported only for parquet and orc.")
      .booleanConf
      .createWithDefault(true)

  val PARQUET_MULTITHREADED_COMBINE_THRESHOLD =
    conf("spark.rapids.sql.format.parquet.multithreaded.combine.sizeBytes")
      .doc("The target size in bytes to combine multiple small files together when using the " +
        "MULTITHREADED parquet reader. With combine disabled, the MULTITHREADED reader reads the " +
        "files in parallel and sends individual files down to the GPU, but that can be " +
        "inefficient for small files. When combine is enabled, files that are ready within " +
        "spark.rapids.sql.format.parquet.multithreaded.combine.waitTime together, up to this " +
        "threshold size, are combined before sending down to GPU. This can be disabled by " +
        "setting it to 0. Note that combine also will not go over the " +
        "spark.rapids.sql.reader.batchSizeRows or spark.rapids.sql.reader.batchSizeBytes " +
        s"limits. DEPRECATED: use $READER_MULTITHREADED_COMBINE_THRESHOLD instead.")
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val PARQUET_MULTITHREADED_COMBINE_WAIT_TIME =
    conf("spark.rapids.sql.format.parquet.multithreaded.combine.waitTime")
      .doc("When using the multithreaded parquet reader with combine mode, how long " +
        "to wait, in milliseconds, for more files to finish if haven't met the size threshold. " +
        "Note that this will wait this amount of time from when the last file was available, " +
        "so total wait time could be larger then this. " +
        s"DEPRECATED: use $READER_MULTITHREADED_COMBINE_WAIT_TIME instead.")
      .integerConf
      .createOptional

  val PARQUET_MULTITHREADED_READ_KEEP_ORDER =
    conf("spark.rapids.sql.format.parquet.multithreaded.read.keepOrder")
      .doc("When using the MULTITHREADED reader, if this is set to true we read " +
        "the files in the same order Spark does, otherwise the order may not be the same. " +
        s"DEPRECATED: use $READER_MULTITHREADED_READ_KEEP_ORDER instead.")
      .booleanConf
      .createOptional

  /** List of schemes that are always considered cloud storage schemes */
  private[rapids] lazy val DEFAULT_CLOUD_SCHEMES =
    Seq("abfs", "abfss", "dbfs", "gs", "s3", "s3a", "s3n", "wasbs", "cosn")

  val CLOUD_SCHEMES = conf("spark.rapids.cloudSchemes")
    .doc("Comma separated list of additional URI schemes that are to be considered cloud based " +
      s"filesystems. Schemes already included: ${DEFAULT_CLOUD_SCHEMES.mkString(", ")}. Cloud " +
      "based stores generally would be total separate from the executors and likely have a " +
      "higher I/O read cost. Many times the cloud filesystems also get better throughput when " +
      "you have multiple readers in parallel. This is used with " +
      "spark.rapids.sql.format.parquet.reader.type")
    .commonlyUsed()
    .stringConf
    .toSequence
    .createOptional

  val PARQUET_MULTITHREAD_READ_NUM_THREADS =
    conf("spark.rapids.sql.format.parquet.multiThreadedRead.numThreads")
      .doc("The maximum number of threads, on the executor, to use for reading small " +
        "Parquet files in parallel. This can not be changed at runtime after the executor has " +
        "started. Used with COALESCING and MULTITHREADED reader, see " +
        s"$PARQUET_READER_TYPE. DEPRECATED: use $MULTITHREAD_READ_NUM_THREADS")
      .startupOnly()
      .integerConf
      .createOptional

  val PARQUET_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL =
    conf("spark.rapids.sql.format.parquet.multiThreadedRead.maxNumFilesParallel")
      .doc("A limit on the maximum number of files per task processed in parallel on the CPU " +
        "side before the file is sent to the GPU. This affects the amount of host memory used " +
        "when reading the files in parallel. Used with MULTITHREADED reader, see " +
        s"$PARQUET_READER_TYPE.")
      .integerConf
      .checkValue(v => v > 0, "The maximum number of files must be greater than 0.")
      .createWithDefault(Integer.MAX_VALUE)

  val ENABLE_PARQUET_READ = conf("spark.rapids.sql.format.parquet.read.enabled")
    .doc("When set to false disables parquet input acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_PARQUET_WRITE = conf("spark.rapids.sql.format.parquet.write.enabled")
    .doc("When set to false disables parquet output acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC = conf("spark.rapids.sql.format.orc.enabled")
    .doc("When set to false disables all orc input and output acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC_READ = conf("spark.rapids.sql.format.orc.read.enabled")
    .doc("When set to false disables orc input acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC_WRITE = conf("spark.rapids.sql.format.orc.write.enabled")
    .doc("When set to false disables orc output acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC_BOOL = conf("spark.rapids.sql.format.orc.write.boolType.enabled")
    .doc("When set to false disables boolean columns for ORC writes. " +
      "Set to true if you want to experiment. " +
      "See https://github.com/NVIDIA/spark-rapids/issues/11736.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val TEST_ORC_STRIPE_SIZE_ROWS = conf("spark.rapids.sql.test.orc.write.stripeSizeRows")
    .doc("Only to be used in tests. When set to a valid value(no less than 512, as specified " +
      "in cudf), the ORC writer will use this value as the maximum number of rows per stripe. " +
      "Additionally, the ORC writer rounds the number of elements in each stripe " +
      "(except the last) to a multiple of 8 to efficiently encode the validity masks. " +
      "If not set, the ORC writer will use the default value 1,000,000.")
    .internal()
    .integerConf
    .checkValue(v => v >= 512, "The stripe size rows must be no less than 512.")
    .createOptional

  val ENABLE_EXPAND_PREPROJECT = conf("spark.rapids.sql.expandPreproject.enabled")
    .doc("When set to false disables the pre-projection for GPU Expand. " +
      "Pre-projection leverages the tiered projection to evaluate expressions that " +
      "semantically equal across Expand projection lists before expanding, to avoid " +
      s"duplicate evaluations. '${ENABLE_TIERED_PROJECT.key}' should also set to true " +
      "to enable this.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COALESCE_AFTER_EXPAND = conf("spark.rapids.sql.coalesceAfterExpand.enabled")
    .doc("When set to false disables the coalesce after GPU Expand. ")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC_FLOAT_TYPES_TO_STRING =
    conf("spark.rapids.sql.format.orc.floatTypesToString.enable")
    .doc("When reading an ORC file, the source data schemas(schemas of ORC file) may differ " +
      "from the target schemas (schemas of the reader), we need to handle the castings from " +
      "source type to target type. Since float/double numbers in GPU have different precision " +
      "with CPU, when casting float/double to string, the result of GPU is different from " +
      "result of CPU spark. Its default value is `true` (this means the strings result will " +
      "differ from result of CPU). If it's set `false` explicitly and there exists casting " +
      "from float/double to string in the job, then such behavior will cause an exception, " +
      "and the job will fail.")
    .booleanConf
    .createWithDefault(true)

  val ORC_READER_TYPE = conf("spark.rapids.sql.format.orc.reader.type")
    .doc("Sets the ORC reader type. We support different types that are optimized for " +
      "different environments. The original Spark style reader can be selected by setting this " +
      "to PERFILE which individually reads and copies files to the GPU. Loading many small files " +
      "individually has high overhead, and using either COALESCING or MULTITHREADED is " +
      "recommended instead. The COALESCING reader is good when using a local file system where " +
      "the executors are on the same nodes or close to the nodes the data is being read on. " +
      "This reader coalesces all the files assigned to a task into a single host buffer before " +
      "sending it down to the GPU. It copies blocks from a single file into a host buffer in " +
      s"separate threads in parallel, see $MULTITHREAD_READ_NUM_THREADS. " +
      "MULTITHREADED is good for cloud environments where you are reading from a blobstore " +
      "that is totally separate and likely has a higher I/O read cost. Many times the cloud " +
      "environments also get better throughput when you have multiple readers in parallel. " +
      "This reader uses multiple threads to read each file in parallel and each file is sent " +
      "to the GPU separately. This allows the CPU to keep reading while GPU is also doing work. " +
      s"See $MULTITHREAD_READ_NUM_THREADS and " +
      "spark.rapids.sql.format.orc.multiThreadedRead.maxNumFilesParallel to control " +
      "the number of threads and amount of memory used. " +
      "By default this is set to AUTO so we select the reader we think is best. This will " +
      "either be the COALESCING or the MULTITHREADED based on whether we think the file is " +
      "in the cloud. See spark.rapids.cloudSchemes.")
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(RapidsReaderType.values.map(_.toString))
    .createWithDefault(RapidsReaderType.AUTO.toString)

  val ORC_MULTITHREAD_READ_NUM_THREADS =
    conf("spark.rapids.sql.format.orc.multiThreadedRead.numThreads")
      .doc("The maximum number of threads, on the executor, to use for reading small " +
        "ORC files in parallel. This can not be changed at runtime after the executor has " +
        "started. Used with MULTITHREADED reader, see " +
        s"$ORC_READER_TYPE. DEPRECATED: use $MULTITHREAD_READ_NUM_THREADS")
      .startupOnly()
      .integerConf
      .createOptional

  val ORC_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL =
    conf("spark.rapids.sql.format.orc.multiThreadedRead.maxNumFilesParallel")
      .doc("A limit on the maximum number of files per task processed in parallel on the CPU " +
        "side before the file is sent to the GPU. This affects the amount of host memory used " +
        "when reading the files in parallel. Used with MULTITHREADED reader, see " +
        s"$ORC_READER_TYPE.")
      .integerConf
      .checkValue(v => v > 0, "The maximum number of files must be greater than 0.")
      .createWithDefault(Integer.MAX_VALUE)

  val ENABLE_CSV = conf("spark.rapids.sql.format.csv.enabled")
    .doc("When set to false disables all csv input and output acceleration. " +
      "(only input is currently supported anyways)")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_CSV_READ = conf("spark.rapids.sql.format.csv.read.enabled")
    .doc("When set to false disables csv input acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_CSV_FLOATS = conf("spark.rapids.sql.csv.read.float.enabled")
    .doc("CSV reading is not 100% compatible when reading floats.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_CSV_DOUBLES = conf("spark.rapids.sql.csv.read.double.enabled")
    .doc("CSV reading is not 100% compatible when reading doubles.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_CSV_DECIMALS = conf("spark.rapids.sql.csv.read.decimal.enabled")
    .doc("CSV reading is not 100% compatible when reading decimals.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_JSON = conf("spark.rapids.sql.format.json.enabled")
    .doc("When set to true enables all json input and output acceleration. " +
      "(only input is currently supported anyways)")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_JSON_READ = conf("spark.rapids.sql.format.json.read.enabled")
    .doc("When set to true enables json input acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_JSON_FLOATS = conf("spark.rapids.sql.json.read.float.enabled")
    .doc("JSON reading is not 100% compatible when reading floats.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_JSON_DOUBLES = conf("spark.rapids.sql.json.read.double.enabled")
    .doc("JSON reading is not 100% compatible when reading doubles.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_JSON_DECIMALS = conf("spark.rapids.sql.json.read.decimal.enabled")
    .doc("When reading a quoted string as a decimal Spark supports reading non-ascii " +
        "unicode digits, and the cuDF plugin does not.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_JSON_DATE_TIME = conf("spark.rapids.sql.json.read.datetime.enabled")
    .doc("JSON reading is not 100% compatible when reading dates and timestamps.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_AVRO = conf("spark.rapids.sql.format.avro.enabled")
    .doc("When set to true enables all avro input and output acceleration. " +
      "(only input is currently supported anyways)")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_AVRO_READ = conf("spark.rapids.sql.format.avro.read.enabled")
    .doc("When set to true enables avro input acceleration")
    .booleanConf
    .createWithDefault(false)

  val AVRO_READER_TYPE = conf("spark.rapids.sql.format.avro.reader.type")
    .doc("Sets the Avro reader type. We support different types that are optimized for " +
      "different environments. The original Spark style reader can be selected by setting this " +
      "to PERFILE which individually reads and copies files to the GPU. Loading many small files " +
      "individually has high overhead, and using either COALESCING or MULTITHREADED is " +
      "recommended instead. The COALESCING reader is good when using a local file system where " +
      "the executors are on the same nodes or close to the nodes the data is being read on. " +
      "This reader coalesces all the files assigned to a task into a single host buffer before " +
      "sending it down to the GPU. It copies blocks from a single file into a host buffer in " +
      s"separate threads in parallel, see $MULTITHREAD_READ_NUM_THREADS. " +
      "MULTITHREADED is good for cloud environments where you are reading from a blobstore " +
      "that is totally separate and likely has a higher I/O read cost. Many times the cloud " +
      "environments also get better throughput when you have multiple readers in parallel. " +
      "This reader uses multiple threads to read each file in parallel and each file is sent " +
      "to the GPU separately. This allows the CPU to keep reading while GPU is also doing work. " +
      s"See $MULTITHREAD_READ_NUM_THREADS and " +
      "spark.rapids.sql.format.avro.multiThreadedRead.maxNumFilesParallel to control " +
      "the number of threads and amount of memory used. " +
      "By default this is set to AUTO so we select the reader we think is best. This will " +
      "either be the COALESCING or the MULTITHREADED based on whether we think the file is " +
      "in the cloud. See spark.rapids.cloudSchemes.")
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(RapidsReaderType.values.map(_.toString))
    .createWithDefault(RapidsReaderType.AUTO.toString)

  val AVRO_MULTITHREAD_READ_NUM_THREADS =
    conf("spark.rapids.sql.format.avro.multiThreadedRead.numThreads")
      .doc("The maximum number of threads, on one executor, to use for reading small " +
        "Avro files in parallel. This can not be changed at runtime after the executor has " +
        "started. Used with MULTITHREADED reader, see " +
        s"$AVRO_READER_TYPE. DEPRECATED: use $MULTITHREAD_READ_NUM_THREADS")
      .startupOnly()
      .integerConf
      .createOptional

  val AVRO_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL =
    conf("spark.rapids.sql.format.avro.multiThreadedRead.maxNumFilesParallel")
      .doc("A limit on the maximum number of files per task processed in parallel on the CPU " +
        "side before the file is sent to the GPU. This affects the amount of host memory used " +
        "when reading the files in parallel. Used with MULTITHREADED reader, see " +
        s"$AVRO_READER_TYPE.")
      .integerConf
      .checkValue(v => v > 0, "The maximum number of files must be greater than 0.")
      .createWithDefault(Integer.MAX_VALUE)

  val ENABLE_DELTA_WRITE = conf("spark.rapids.sql.format.delta.write.enabled")
      .doc("When set to false disables Delta Lake output acceleration.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_ICEBERG = conf("spark.rapids.sql.format.iceberg.enabled")
    .doc("When set to false disables all Iceberg acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ICEBERG_READ = conf("spark.rapids.sql.format.iceberg.read.enabled")
    .doc("When set to false disables Iceberg input acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ICEBERG_V3 = conf("spark.rapids.sql.format.iceberg.v3.enabled")
    .doc("Internal temporary configuration to allow Iceberg table format versions greater " +
      "than 2 to use GPU paths while v3 support is under development. This configuration " +
      "will be removed after most Iceberg v3 features are supported.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val VALIDATE_ICEBERG_DELETION_VECTOR_CRC =
    conf("spark.rapids.sql.format.iceberg.deletionVector.crcCheck.enabled")
      .doc("When set to false, skips validation of the CRC-32 checksum of each Iceberg " +
        "deletion vector. Disabling validation avoids an additional CPU pass over the " +
        "deletion-vector data, but may allow corrupted deletion vectors to be read.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_ICEBERG_WRITE = conf("spark.rapids.sql.format.iceberg.write.enabled")
    .doc("When set to false disables Iceberg write acceleration")
    .booleanConf
    .createWithDefault(true)

  val ICEBERG_S3_ASYNC_MAX_CONCURRENCY =
    conf("spark.rapids.iceberg.s3.async.max-concurrency")
      .doc("Max concurrent connections for the AwsCrtAsyncHttpClient used by the " +
        "cuDF plugin Iceberg S3 byte-range reader. Used only when the Iceberg " +
        "FileIO property `s3.crt.max-concurrency` is not set.")
      .startupOnly()
      .integerConf
      .createWithDefault(200)

  val ICEBERG_S3_ASYNC_CONNECTION_MAX_IDLE_MS =
    conf("spark.rapids.iceberg.s3.async.connection-max-idle-time-ms")
      .doc("Connection-max-idle-time (ms) for the AwsCrtAsyncHttpClient used by the " +
        "cuDF plugin Iceberg S3 byte-range reader. No equivalent Iceberg property.")
      .startupOnly()
      .longConf
      .createWithDefault(5L * 60 * 1000)

  val ICEBERG_S3_ASYNC_TCP_KEEPALIVE_INTERVAL_MS =
    conf("spark.rapids.iceberg.s3.async.tcp-keepalive-interval-ms")
      .doc("TCP keep-alive probe interval (ms) for the AwsCrtAsyncHttpClient used by " +
        "the cuDF plugin Iceberg S3 byte-range reader. No equivalent Iceberg property.")
      .startupOnly()
      .longConf
      .createWithDefault(60L * 1000)

  val ICEBERG_S3_ASYNC_TCP_KEEPALIVE_TIMEOUT_MS =
    conf("spark.rapids.iceberg.s3.async.tcp-keepalive-timeout-ms")
      .doc("TCP keep-alive probe timeout (ms) for the AwsCrtAsyncHttpClient used by " +
        "the cuDF plugin Iceberg S3 byte-range reader. No equivalent Iceberg property.")
      .startupOnly()
      .longConf
      .createWithDefault(30L * 1000)

  val ENABLE_HIVE_TEXT: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.format.hive.text.enabled")
      .doc("When set to false disables Hive text table acceleration. " +
           "Array/Struct/Map columns are unsupported for acceleration.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_HIVE_TEXT_READ: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.format.hive.text.read.enabled")
      .doc("When set to false disables Hive text table read acceleration. " +
           "Array/Struct/Map columns are unsupported for read acceleration.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_HIVE_TEXT_WRITE: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.format.hive.text.write.enabled")
      .doc("When set to false disables Hive text table write acceleration. " +
           "Array/Struct/Map columns are unsupported for write acceleration.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_READ_HIVE_FLOATS = conf("spark.rapids.sql.format.hive.text.read.float.enabled")
      .doc("Hive text file reading is not 100% compatible when reading floats.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_READ_HIVE_DOUBLES = conf("spark.rapids.sql.format.hive.text.read.double.enabled")
      .doc("Hive text file reading is not 100% compatible when reading doubles.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_READ_HIVE_DECIMALS = conf("spark.rapids.sql.format.hive.text.read.decimal.enabled")
      .doc("Hive text file reading is not 100% compatible when reading decimals. Hive has " +
          "more limitations on what is valid compared to the GPU implementation in some corner " +
          "cases. See https://github.com/NVIDIA/cudf-spark/issues/7246")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_RANGE_WINDOW_BYTES = conf("spark.rapids.sql.window.range.byte.enabled")
    .doc("When the order-by column of a range based window is byte type and " +
      "the range boundary calculated for a value has overflow, CPU and GPU will get " +
      "the different results. When set to false disables the range window acceleration for the " +
      "byte type order-by column")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_RANGE_WINDOW_SHORT = conf("spark.rapids.sql.window.range.short.enabled")
    .doc("When the order-by column of a range based window is short type and " +
      "the range boundary calculated for a value has overflow, CPU and GPU will get " +
      "the different results. When set to false disables the range window acceleration for the " +
      "short type order-by column")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_RANGE_WINDOW_INT = conf("spark.rapids.sql.window.range.int.enabled")
    .doc("When the order-by column of a range based window is int type and " +
      "the range boundary calculated for a value has overflow, CPU and GPU will get " +
      "the different results. When set to false disables the range window acceleration for the " +
      "int type order-by column")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_RANGE_WINDOW_LONG = conf("spark.rapids.sql.window.range.long.enabled")
    .doc("When the order-by column of a range based window is long type and " +
      "the range boundary calculated for a value has overflow, CPU and GPU will get " +
      "the different results. When set to false disables the range window acceleration for the " +
      "long type order-by column")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_RANGE_WINDOW_FLOAT: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.window.range.float.enabled")
      .doc("When set to false, this disables the range window acceleration for the " +
        "FLOAT type order-by column")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_RANGE_WINDOW_DOUBLE: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.window.range.double.enabled")
      .doc("When set to false, this disables the range window acceleration for the " +
        "double type order-by column")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_RANGE_WINDOW_DECIMAL: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.window.range.decimal.enabled")
    .doc("When set to false, this disables the range window acceleration for the " +
      "DECIMAL type order-by column")
    .booleanConf
    .createWithDefault(true)

  val BATCHED_BOUNDED_ROW_WINDOW_MAX: ConfEntryWithDefault[Integer] =
    conf("spark.rapids.sql.window.batched.bounded.row.max")
      .doc("Max value for bounded row window preceding/following extents " +
      "permissible for the window to be evaluated in batched mode. This value affects " +
      "both the preceding and following bounds, potentially doubling the window size " +
      "permitted for batched execution")
      .integerConf
      .createWithDefault(value = 100)

  val ENABLE_SINGLE_PASS_PARTIAL_SORT_AGG: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.agg.singlePassPartialSortEnabled")
    .doc("Enable or disable a single pass partial sort optimization where if a heuristic " +
        "indicates it would be good we pre-sort the data before a partial agg and then " +
        "do the agg in a single pass with no merge, so there is no spilling")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val SKIP_AGG_PASS_REDUCTION_RATIO = conf("spark.rapids.sql.agg.skipAggPassReductionRatio")
    .doc("In non-final aggregation stages, if the previous pass has a row retention ratio " +
        "greater than this value, the next aggregation pass will be skipped." +
        "Setting this to 1 essentially disables this feature. For Historical reasons it is " +
        "called skipAggPassReductionRatio, actually skipAggPassRetentionRatio would have " +
        "been better")
    .internal()
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The ratio value must be in [0, 1].")
    .createWithDefault(0.85)

  val FORCE_SINGLE_PASS_PARTIAL_SORT_AGG: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.agg.forceSinglePassPartialSort")
    .doc("Force a single pass partial sort agg to happen in all cases that it could, " +
        "no matter what the heuristic says. This is really just for testing.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val ENABLE_REGEXP = conf("spark.rapids.sql.regexp.enabled")
    .doc("Specifies whether supported regular expressions will be evaluated on the GPU. " +
      "Unsupported expressions will fall back to CPU. However, there are some known edge cases " +
      "that will still execute on GPU and produce incorrect results and these are documented in " +
      "the compatibility guide. Setting this config to false will make all regular expressions " +
      "run on the CPU instead.")
    .booleanConf
    .createWithDefault(true)

}
