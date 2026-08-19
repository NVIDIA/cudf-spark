/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.utils

// Import all Rapids test suites to avoid merge conflicts when multiple people add new suites
import org.apache.spark.sql.rapids.suites._

// Some settings' line length exceeds 100
// scalastyle:off line.size.limit

class RapidsTestSettings extends BackendTestSettings {

  enableSuite[RapidsApproximatePercentileQuerySuite]
    .exclude("percentile_approx(col, ...), input rows contains null, with group by", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14634"))
    .exclude("SPARK-32908: maximum target error in percentile_approx", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14635"))
  enableSuite[RapidsDataFrameReaderWriterSuite]
  enableSuite[RapidsInsertSuite]
    .exclude("Throw exceptions on inserting out-of-range int value with ANSI casting policy", ADJUST_UT("Replaced by a testRapids version that verifies the GPU ANSI overflow failure family and no partial write."))
    .exclude("Throw exceptions on inserting out-of-range long value with ANSI casting policy", ADJUST_UT("Replaced by a testRapids version that verifies the GPU ANSI overflow failure family and no partial write."))
    .exclude("Stop task set if FileAlreadyExistsException was thrown", ADJUST_UT("Replaced by a testRapids version that verifies both fast-fail settings preserve the FileAlreadyExistsException failure family."))
  enableSuite[RapidsBucketedReadWithoutHiveSupportSuite]
    .exclude("read partitioning bucketed tables with bucket pruning filters", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: add GPU scan-aware bucket pruning assertions; P1"))
    .exclude("read non-partitioning bucketed tables with bucket pruning filters", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: add GPU scan-aware bucket pruning assertions; P1"))
    .exclude("read partitioning bucketed tables having null in bucketing key", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: add GPU scan-aware bucket pruning assertions; P1"))
    .exclude("bucket pruning support IsNaN", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: add GPU scan-aware bucket pruning assertions; P1"))
    .exclude("read partitioning bucketed tables having composite filters", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: add GPU scan-aware bucket pruning assertions; P1"))
    .exclude("read bucketed table without filters", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: add GPU scan-aware bucketing assertions; P1"))
    .exclude("error if there exists any malformed bucket files", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: accept the equivalent GPU malformed-bucket exception family; P1"))
    .exclude("disable bucketing when the output doesn't contain all bucketing columns", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: add GPU scan-aware bucketing assertions; P1"))
    .exclude("SPARK-29655 Read bucketed tables obeys spark.sql.shuffle.partitions", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: replace CPU SortMergeJoinExec assertion with a GPU join contract; P1"))
    .exclude("SPARK-32767 Bucket join should work if SHUFFLE_PARTITIONS larger than bucket number", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: replace CPU SortMergeJoinExec assertion with a GPU join contract; P1"))
    .exclude("bucket coalescing eliminates shuffle", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: replace CPU SortMergeJoinExec assertion with a GPU join contract; P1"))
    .exclude("bucket coalescing is applied when join expressions match with partitioning expressions", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: add GPU scan-aware coalesced bucket assertions; P1"))
  enableSuite[RapidsInMemoryColumnarQuerySuite]
    .exclude("primitive type with nullability:true", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: adapt CPU CachedBatch serializer assertions for RAPIDS cache; P1"))
    .exclude("primitive type with nullability:false", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: adapt CPU CachedBatch serializer assertions for RAPIDS cache; P1"))
    .exclude("non-primitive type with nullability:true", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: adapt CPU CachedBatch serializer assertions for RAPIDS cache; P1"))
    .exclude("non-primitive type with nullability:false", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: adapt CPU CachedBatch serializer assertions for RAPIDS cache; P1"))
    .exclude("simple columnar query", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: adapt CPU CachedBatch serializer assertions for RAPIDS cache; P1"))
    .exclude("projection", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: adapt CPU CachedBatch serializer assertions for RAPIDS cache; P1"))
    .exclude("SPARK-1436 regression: in-memory columns must be able to be accessed multiple times", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: adapt CPU CachedBatch serializer assertions for RAPIDS cache; P1"))
    .exclude("test different data types", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: adapt CPU CachedBatch serializer assertions for RAPIDS cache; P1"))
    .exclude("SPARK-17549: cached table size should be correctly calculated", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: define RAPIDS cache size-stat contract; P1"))
    .exclude("cached row count should be calculated", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: load the RAPIDS cache serializer before validating row-count stats; P1"))
    .exclude("SPARK-22348: table cache should do partition batch pruning (whole-stage-codegen off)", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: add RAPIDS cache batch-pruning metrics assertion; P1"))
    .exclude("SPARK-22348: table cache should do partition batch pruning (whole-stage-codegen on)", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: add RAPIDS cache batch-pruning metrics assertion; P1"))
    .exclude("SPARK-22673: InMemoryRelation should utilize existing stats of the plan to be cached", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15452; Recovery: define RAPIDS cache statistics expectation; P1"))
  enableSuite[RapidsPathFilterSuite]
  enableSuite[RapidsDataSourceV2Suite]
    .exclude("partitioning reporting", ADJUST_UT("Replaced by a testRapids version that checks ShuffleExchangeLike for CPU and GPU plans."))
  enableSuite[RapidsJsonParsingOptionsSuite]
  enableSuite[RapidsPartitionedWriteSuite]
  enableSuite[RapidsV1WriteFallbackSuite]
  enableSuite[RapidsGlobalTempViewSuite]
  enableSuite[RapidsDataFrameJoinSuite]
    .exclude("SPARK-24690 enables star schema detection even if CBO disabled", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14653"))
  enableSuite[RapidsBroadcastJoinSuite]
    .exclude("unsafe broadcast hash join updates peak execution memory", WONT_FIX_ISSUE("CPU memory metric test; not applicable to GPU broadcast hash join execution."))
    .exclude("unsafe broadcast hash outer join updates peak execution memory", WONT_FIX_ISSUE("CPU memory metric test; not applicable to GPU broadcast hash join execution."))
    .exclude("unsafe broadcast left semi join updates peak execution memory", WONT_FIX_ISSUE("CPU memory metric test; not applicable to GPU broadcast hash join execution."))
    .exclude("SPARK-23192: broadcast hint should be retained after using the cached data", ADJUST_UT("Replaced by testRapids version that checks GpuBroadcastHashJoinExec."))
    .exclude("SPARK-23214: cached data should not carry extra hint info", ADJUST_UT("Replaced by testRapids version that checks GpuInMemoryTableScanExec and no GpuBroadcastHashJoinExec."))
    .exclude("Shouldn't change broadcast join buildSide if user clearly specified", ADJUST_UT("Replaced by testRapids version that checks GpuBroadcastHashJoinExec and supported GpuBroadcastNestedLoopJoinExec build-side choices."))
    .exclude("Shouldn't bias towards build right if user didn't specify", ADJUST_UT("Replaced by testRapids version that checks no-hint GpuBroadcastHashJoinExec and supported GpuBroadcastNestedLoopJoinExec build-side choices."))
    .exclude("broadcast join where streamed side's output partitioning is HashPartitioning", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15141"))
    .exclude("broadcast join where streamed side's output partitioning is PartitioningCollection", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15141"))
    .exclude("BroadcastHashJoinExec output partitioning scenarios for inner join", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15141"))
    .exclude("BroadcastHashJoinExec output partitioning size should be limited with a config", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15141"))
    .exclude("SPARK-37742: join planning shouldn't read invalid InMemoryRelation stats", ADJUST_UT("Replaced by testRapids version that checks GpuInMemoryTableScanExec and no GpuBroadcastHashJoinExec."))
  enableSuite[RapidsBroadcastJoinSuiteAE]
    .exclude("unsafe broadcast hash join updates peak execution memory", WONT_FIX_ISSUE("CPU memory metric test; not applicable to GPU broadcast hash join execution."))
    .exclude("unsafe broadcast hash outer join updates peak execution memory", WONT_FIX_ISSUE("CPU memory metric test; not applicable to GPU broadcast hash join execution."))
    .exclude("unsafe broadcast left semi join updates peak execution memory", WONT_FIX_ISSUE("CPU memory metric test; not applicable to GPU broadcast hash join execution."))
    .exclude("SPARK-23192: broadcast hint should be retained after using the cached data", ADJUST_UT("Replaced by testRapids version that checks GpuBroadcastHashJoinExec."))
    .exclude("SPARK-23214: cached data should not carry extra hint info", ADJUST_UT("Replaced by testRapids version that checks GpuInMemoryTableScanExec and no GpuBroadcastHashJoinExec."))
    .exclude("Shouldn't change broadcast join buildSide if user clearly specified", ADJUST_UT("Replaced by testRapids version that checks GpuBroadcastHashJoinExec and supported GpuBroadcastNestedLoopJoinExec build-side choices."))
    .exclude("Shouldn't bias towards build right if user didn't specify", ADJUST_UT("Replaced by testRapids version that checks no-hint GpuBroadcastHashJoinExec and supported GpuBroadcastNestedLoopJoinExec build-side choices."))
    .exclude("broadcast join where streamed side's output partitioning is HashPartitioning", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15141"))
    .exclude("broadcast join where streamed side's output partitioning is PartitioningCollection", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15141"))
    .exclude("BroadcastHashJoinExec output partitioning scenarios for inner join", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15141"))
    .exclude("BroadcastHashJoinExec output partitioning size should be limited with a config", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15141"))
    .exclude("SPARK-37742: join planning shouldn't read invalid InMemoryRelation stats", ADJUST_UT("Replaced by testRapids version that checks GpuInMemoryTableScanExec and no GpuBroadcastHashJoinExec."))
  enableSuite[RapidsDynamicPartitionPruningV1SuiteAEOff]
    .exclude("Make sure dynamic pruning works on uncorrelated queries", ADJUST_UT("Replaced by testRapids version that checks GpuSubqueryBroadcastExec"))
    .exclude("static scan metrics", ADJUST_UT("Replaced by testRapids version that checks GpuFileSourceScanExec metrics"))
    .exclude("SPARK-32659: Fix the data issue when pruning DPP on non-atomic type", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14836"))
  enableSuite[RapidsDynamicPartitionPruningV1SuiteAEOn]
    .exclude("Make sure dynamic pruning works on uncorrelated queries", ADJUST_UT("Replaced by testRapids version that checks GpuSubqueryBroadcastExec"))
    .exclude("SPARK-32659: Fix the data issue when pruning DPP on non-atomic type", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14836"))
  enableSuite[RapidsDataFrameSelfJoinSuite]
  enableSuite[RapidsDataFrameWindowFramesSuite]
  enableSuite[RapidsDataFrameTimeWindowingSuite]
  enableSuite[RapidsDataFrameSessionWindowingSuite]
  enableSuite[RapidsDataFrameStatSuite]
  enableSuite[RapidsTypedImperativeAggregateSuite]
  enableSuite[RapidsDatasetAggregatorSuite]
  enableSuite[RapidsInjectRuntimeFilterSuite]
  enableSuite[RapidsArithmeticExpressionSuite]
  enableSuite[RapidsBitwiseExpressionsSuite]
  enableSuite[RapidsBloomFilterAggregateQuerySuite]
  enableSuite[RapidsComplexTypeSuite]
  enableSuite[RapidsConditionalExpressionSuite]
  enableSuite[RapidsCountMinSketchAggQuerySuite]
  enableSuite[RapidsHashExpressionsSuite]
  enableSuite[RapidsIntervalExpressionsSuite]
  enableSuite[RapidsNullExpressionsSuite]
  enableSuite[RapidsPredicateSuite]
  enableSuite[RapidsSubexpressionEliminationSuite]
  enableSuite[RapidsTimeWindowSuite]
  enableSuite[RapidsCastSuite]
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone", WONT_FIX_ISSUE("https://issues.apache.org/jira/browse/SPARK-40851"))
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone", WONT_FIX_ISSUE("https://issues.apache.org/jira/browse/SPARK-40851"))
    .exclude("SPARK-35112: Cast string to day-time interval", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10980"))
    .exclude("SPARK-35735: Take into account day-time interval fields in cast", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10980"))
    .exclude("casting to fixed-precision decimals", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/11250"))
    .exclude("SPARK-32828: cast from a derived user-defined type to a base type", WONT_FIX_ISSUE("User-defined types are not supported"))
    .exclude("cast string to timestamp", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/blob/main/docs/compatibility.md#string-to-timestamp"))
    .exclude("cast string to date", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10771"))
  enableSuite[RapidsUnwrapCastInComparisonEndToEndSuite]
  enableSuite[RapidsCollectionExpressionsSuite]
    .exclude("Array Intersect", ADJUST_UT("Replaced by testRapids version that doesn't check the order of the elements in the result array. See https://github.com/NVIDIA/spark-rapids/issues/13696 for more details."))
    .exclude("Shuffle", ADJUST_UT("Replaced by testRapids version that adjusts the expected results to match the running by --master local[2]."))
  enableSuite[RapidsColumnExpressionSuite]
    .exclude("input_file_name, input_file_block_start, input_file_block_length - HadoopRDD", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14153"))
    .exclude("input_file_name, input_file_block_start, input_file_block_length - NewHadoopRDD", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14153"))
  enableSuite[RapidsDataFrameFunctionsSuite]
    .exclude("array_intersect functions", ADJUST_UT("Replaced by testRapids version that doesn't check the order of the elements in the result array. See https://github.com/NVIDIA/spark-rapids/issues/13696 for more details."))
  enableSuite[RapidsDataFrameAsOfJoinSuite]
  enableSuite[RapidsJoinSuite]
    .exclude("test SortMergeJoin (with spill)", WONT_FIX_ISSUE("The case is to test spill in SortMergeJoin, which is not applicable for GPU."))
    .exclude("SPARK-32649: Optimize BHJ/SHJ inner/semi join with empty hashed relation", WONT_FIX_ISSUE("The case is to test the codegen behavior for BHJ/SHJ inner/semi join, which is not applicable for GPU."))
    .exclude("SPARK-36794: Ignore duplicated key when building relation for semi/anti hash join", ADJUST_UT("Replaced by testRapids version that checks GPU or CPU join operators"))
    .exclude("SPARK-28323: PythonUDF should be able to use in join condition", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14258"))
    .exclude("SPARK-28345: PythonUDF predicate should be able to pushdown to join", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14258"))
  enableSuite[RapidsSubquerySuite]
    .exclude("SPARK-26893: Allow pushdown of partition pruning subquery filters to file source", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14172 - partition pruning with subquery not working on GPU"))
    .exclude("SPARK-27279: Reuse Subquery", ADJUST_UT("Replaced by testRapids version for GPU execution"))
    .exclude("SPARK-36280: Remove redundant aliases after RewritePredicateSubquery", ADJUST_UT("Replaced by testRapids version that checks GPU or CPU shuffle exchange"))
    .exclude("SPARK-15832: Test embedded existential predicate sub-queries", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14221 - Causes deadlock with AQE when executing deeply nested subqueries (3+ levels)"))
    .exclude("SPARK-28441: COUNT bug in WHERE clause (Filter) with PythonUDF", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14258"))
    .exclude("SPARK-28441: COUNT bug in SELECT clause (Project) with PythonUDF", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14258"))
    .exclude("SPARK-28441: COUNT bug in HAVING clause (Filter) with PythonUDF", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14258"))
    .exclude("SPARK-28441: COUNT bug in Aggregate with PythonUDF", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14258"))
    .exclude("SPARK-28441: COUNT bug negative examples with PythonUDF", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14258"))
    .exclude("SPARK-28441: COUNT bug in nested subquery with PythonUDF", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14258"))
    .exclude("SPARK-28441: COUNT bug with nasty predicate expr with PythonUDF", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14258"))
    .exclude("SPARK-28441: COUNT bug with attribute ref in subquery input and output with PythonUDF", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14258"))
  enableSuite[RapidsSQLViewSuite]
  enableSuite[RapidsDataFrameSuite]
    .exclude("reuse exchange", ADJUST_UT("Replaced by testRapids version that uses GPU class name"))
    .exclude("SPARK-22520: support code generation for large CaseWhen", WONT_FIX_ISSUE("It's a codegen related test, not applicable for GPU"))
    .exclude("Uuid expressions should produce same results at retries in the same DataFrame", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14149"))
    .exclude("SPARK-27439: Explain result should match collected result after view change", ADJUST_UT("Replaced by testRapids version that uses GPU class name"))
  enableSuite[RapidsDatasetSuite]
    .exclude("groupBy single field class, count", ADJUST_UT("Replaced by testRapids version to sort the results for consistent ordering"))
    .exclude("SPARK-34806: observation on datasets", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14152"))
    .exclude("SPARK-37203: Fix NotSerializableException when observe with TypedImperativeAggregate", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14152"))
    .exclude("dropDuplicates", ADJUST_UT("Replaced by testRapids version to sort the results for consistent ordering"))
    .exclude("dropDuplicates: columns with same column name", ADJUST_UT("Replaced by testRapids version to sort the results for consistent ordering"))
    .exclude("SPARK-24762: typed agg on Option[Product] type", ADJUST_UT("Replaced by testRapids version to sort the results for consistent ordering"))
    .exclude("Check RelationalGroupedDataset toString: Single data", ADJUST_UT("Replaced by testRapids version because the RelationalGroupedDataset.toString method returns an empty string for the type on JDK 11+"))
    .exclude("Check RelationalGroupedDataset toString: over length schema ", ADJUST_UT("Replaced by testRapids version because the RelationalGroupedDataset.toString method returns an empty string for the type on JDK 11+"))
  enableSuite[RapidsDataFrameAggregateSuite]
    .exclude("collect functions", ADJUST_UT("order of elements in the array is non-deterministic in collect"))
    .exclude("collect functions structs", ADJUST_UT("order of elements in the array is non-deterministic in collect"))
    .exclude("collect functions should be able to cast to array type with no null values", ADJUST_UT("order of elements in the array is non-deterministic in collect"))
    .exclude("SPARK-17641: collect functions should not collect null values", ADJUST_UT("order of elements in the array is non-deterministic in collect"))
    .exclude("SPARK-19471: AggregationIterator does not initialize the generated result projection before using it", WONT_FIX_ISSUE("Codegen related UT, not applicable for GPU"))
    .exclude("SPARK-24788: RelationalGroupedDataset.toString with unresolved exprs should not fail", ADJUST_UT("Replaced by testRapids version that considers the difference of JDK version"))
  enableSuite[RapidsDataFrameComplexTypeSuite]
  enableSuite[RapidsDataFrameNaFunctionsSuite]
  enableSuite[RapidsDataFramePivotSuite]
  enableSuite[RapidsDataFrameSetOperationsSuite]
    .exclude("SPARK-37371: UnionExec should support columnar if all children support columnar", ADJUST_UT("CPU test uses CPU-specific node checks (InMemoryTableScanExec, UnionExec); GPU version implemented as testRapids() in RapidsDataFrameSetOperationsSuite"))
  enableSuite[RapidsDataFrameRangeSuite]
  enableSuite[RapidsFileBasedDataSourceSuite]
    .exclude("Enabling/disabling ignoreMissingFiles using orc", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15100"))
    .exclude("SPARK-25237 compute correct input metrics in FileScanRDD", ADJUST_UT("Replaced by testRapids version that checks GpuFileSourceScanExec file metrics and populated Spark input metrics."))
    .exclude("Option recursiveFileLookup: disable partition inferring", ADJUST_UT("Replaced by testRapids version that uses testFile() to expand Spark test resources from the tests jar before reading with binaryFile."))
    .exclude("SPARK-22790,SPARK-27668: spark.sql.sources.compressionFactor takes effect", ADJUST_UT("Replaced by testRapids version that checks file-compression statistics with GpuBroadcastHashJoinExec and GpuShuffledSymmetricHashJoinExec."))
    .exclude("File source v2: support partition pruning", ADJUST_UT("Replaced by testRapids version that checks GpuBatchScanExec file filters and selected partitions."))
    .exclude("File source v2: support passing data filters to FileScan without partitionFilters", ADJUST_UT("Replaced by testRapids version that checks GpuBatchScanExec file filters and selected partitions."))
  enableSuite[RapidsCachedTableSuite]
    .exclude("InMemoryRelation statistics", ADJUST_UT("Replaced by testRapids version that checks cache statistics with RAPIDS cache serializer and GpuInMemoryTableScanExec."))
    .exclude("SPARK-19993 subquery with cached underlying relation", ADJUST_UT("Replaced by testRapids version that checks cached subquery reuse with recursive GpuInMemoryTableScanExec nodes."))
    .exclude("SPARK-36120: Support cache/uncache table with TimestampNTZ type", ADJUST_UT("Replaced by testRapids version that checks TimestampNTZ cache correctness with RAPIDS cache stats."))
  enableSuite[RapidsDatasetCacheSuite]
    .exclude("SPARK-24613 Cache with UDF could not be matched with subsequent dependent caches", ADJUST_UT("Replaced by testRapids version that checks dependent cache matching with RAPIDS cache scans."))
    .exclude("SPARK-24596 Non-cascading Cache Invalidation - verify cached data reuse", ADJUST_UT("Replaced by testRapids version that checks non-cascading cache invalidation reuses loaded cached data without re-evaluating the UDF."))
    .exclude("SPARK-26708 Cache data and cached plan should stay consistent", ADJUST_UT("Replaced by testRapids version that checks loaded and unloaded dependent caches keep consistent cached plans under RAPIDS."))
  enableSuite[RapidsDatasetPrimitiveSuite]
  enableSuite[RapidsBasicCharVarcharTestSuite]
  enableSuite[RapidsFileSourceCharVarcharTestSuite]
  enableSuite[RapidsDSV2CharVarcharTestSuite]
  enableSuite[RapidsDataFrameTungstenSuite]
  enableSuite[RapidsDataFrameImplicitsSuite]
  enableSuite[RapidsDatasetOptimizationSuite]
  enableSuite[RapidsDataFrameWriterV2Suite]
  enableSuite[RapidsUserDefinedTypeSuite]
  enableSuite[RapidsDeprecatedAPISuite]
  enableSuite[RapidsDeprecatedDatasetAggregatorSuite]
  enableSuite[RapidsStatisticsCollectionSuite]
  enableSuite[RapidsDataFrameCallbackSuite]
    .exclude("get numRows metrics by callback",
      ADJUST_UT("Replaced by a testRapids version that locates numOutputRows across the GPU " +
        "plan instead of assuming a CPU WholeStageCodegen root. Original contract: " +
        "https://github.com/apache/spark/blob/v3.3.0/sql/core/src/test/scala/org/apache/" +
        "spark/sql/util/DataFrameCallbackSuite.scala#L98-L130; P1."))
    .exclude("execute callback functions for DataFrameWriter",
      ADJUST_UT("Replaced by a testRapids version that identifies callback commands by type " +
        "instead of fixed positions. Original contract: https://github.com/apache/spark/blob/" +
        "v3.3.0/sql/core/src/test/scala/org/apache/spark/sql/util/" +
        "DataFrameCallbackSuite.scala#L181-L238; P1."))
    .exclude("get observable metrics by callback",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/14152. Recovery trigger: " +
        "CollectMetricsExec aggregation expressions no longer fail GPU override tagging; P0."))
    .exclude("SPARK-35296: observe should work even if a task contains multiple partitions",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/14152. Recovery trigger: " +
        "CollectMetricsExec supports multi-partition observation under RAPIDS; P0."))
    .exclude("SPARK-35695: get observable metrics with persist by callback",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/14152. Recovery trigger: " +
        "CollectMetricsExec supports persisted observations under RAPIDS; P0."))
    .exclude("SPARK-35695: get observable metrics with adaptive execution by callback",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/14152. Recovery trigger: " +
        "CollectMetricsExec supports adaptive observations under RAPIDS; P0."))
  enableSuite[RapidsInMemoryTableMetricSuite]
  enableSuite[RapidsOptimizeMetadataOnlyQuerySuite]
  enableSuite[RapidsQueryExecutionErrorsSuite]
    .exclude("INCONSISTENT_BEHAVIOR_CROSS_VERSION: " +
      "compatibility with Spark 2.4/3.2 in reading/writing dates",
      ADJUST_UT("Replaced by a testRapids version that expands the Spark test resource with " +
        "testFile() before reading it. Original contract: https://github.com/apache/spark/blob/" +
        "v3.3.0/sql/core/src/test/scala/org/apache/spark/sql/errors/" +
        "QueryExecutionErrorsSuite.scala#L171-L232; P1."))
  enableSuite[RapidsWriteDistributionAndOrderingSuite]
  enableSuite[RapidsOrcSourceV1Suite]
    .exclude("Propagate Hadoop configs from orc options to underlying file system",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/11602. " +
        "Recovery trigger: GPU ORC writes propagate data-source options; P1."))
    .exclude("Write Spark version into ORC file metadata",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15468. " +
        "Recovery trigger: GPU ORC files include Spark version metadata; P1."))
    .exclude("SPARK-31238: compatibility with Spark 2.4 in reading dates",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15470. " +
        "Recovery trigger: GPU ORC legacy date rebasing matches Spark CPU; P0."))
    .exclude("SPARK-31284: compatibility with Spark 2.4 in reading timestamps",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15471. " +
        "Recovery trigger: GPU ORC legacy timestamp reads match Spark CPU; P0."))
    .exclude("SPARK-31284, SPARK-31423: rebasing timestamps in write",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15473. " +
        "Recovery trigger: GPU ORC legacy timestamp round trips match Spark CPU; P0."))
    .exclude("Enforce direct encoding column-wise selectively",
      WONT_FIX_ISSUE("GPU ORC uses valid libcudf-selected encodings rather than the Spark ORC " +
        "selective direct-encoding option. See https://github.com/NVIDIA/cudf-spark/issues/15469. " +
        "Recovery trigger: the GPU writer supports this tuning contract or falls back; P2."))
    .exclude("SPARK-36663: OrcUtils.toCatalystSchema should correctly handle " +
      "a column name which consists of only numbers",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15472. " +
        "Recovery trigger: GPU ORC reader handles numeric-only field names; P0."))
  enableSuite[RapidsOrcSourceV2Suite]
    .exclude("Propagate Hadoop configs from orc options to underlying file system",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/11602. " +
        "Recovery trigger: GPU ORC writes propagate data-source options; P1."))
    .exclude("Write Spark version into ORC file metadata",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15468. " +
        "Recovery trigger: GPU ORC files include Spark version metadata; P1."))
    .exclude("SPARK-31238: compatibility with Spark 2.4 in reading dates",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15470. " +
        "Recovery trigger: GPU ORC legacy date rebasing matches Spark CPU; P0."))
    .exclude("SPARK-31284: compatibility with Spark 2.4 in reading timestamps",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15471. " +
        "Recovery trigger: GPU ORC legacy timestamp reads match Spark CPU; P0."))
    .exclude("SPARK-31284, SPARK-31423: rebasing timestamps in write",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15473. " +
        "Recovery trigger: GPU ORC legacy timestamp round trips match Spark CPU; P0."))
    .exclude("Enforce direct encoding column-wise selectively",
      WONT_FIX_ISSUE("GPU ORC uses valid libcudf-selected encodings rather than the Spark ORC " +
        "selective direct-encoding option. See https://github.com/NVIDIA/cudf-spark/issues/15469. " +
        "Recovery trigger: the GPU writer supports this tuning contract or falls back; P2."))
    .exclude("SPARK-36663: OrcUtils.toCatalystSchema should correctly handle " +
      "a column name which consists of only numbers",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15472. " +
        "Recovery trigger: GPU ORC reader handles numeric-only field names; P0."))
  enableSuite[RapidsKeyGroupedPartitioningSuite]
  enableSuite[RapidsDataSourceV2DataFrameSuite]
  enableSuite[RapidsBucketedWriteWithoutHiveSupportSuite]
  enableSuite[RapidsCreateTableAsSelectSuite]
  enableSuite[RapidsFileDataSourceV2FallBackSuite]
    .exclude("Fallback Parquet V2 to V1",
      ADJUST_UT("Replaced by a testRapids version that preserves V1 fallback assertions and " +
        "checks GpuFileSourceScanExec. See https://github.com/NVIDIA/cudf-spark/issues/15465. " +
        "Recovery trigger: the Spark test accepts the GPU V1 scan node; P2."))
  enableSuite[RapidsV1ReadFallbackWithDataFrameReaderSuite]
  enableSuite[RapidsV1ReadFallbackWithCatalogSuite]
  enableSuite[RapidsFileSourceCharVarcharDDLTestSuite]
    .exclude("SPARK-33901: ctas should should not change table's schema",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15549. " +
        "Recovery trigger: GPU V1 CTAS preserves raw CHAR/VARCHAR schema metadata; P0."))
    .exclude("SPARK-37160: CREATE TABLE AS SELECT with CHAR_AS_VARCHAR",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15549. " +
        "Recovery trigger: GPU V1 CTAS applies CHAR_AS_VARCHAR to raw schema metadata; P0."))
  enableSuite[RapidsDSV2CharVarcharDDLTestSuite]
  enableSuite[RapidsParquetCodecSuite]
    .exclude("write and read - file source parquet - codec: lz4",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15550. " +
        "Recovery trigger: GPU Parquet supports Hadoop LZ4 or safely falls back; P1."))
  enableSuite[RapidsOrcCodecSuite]
    .exclude("write and read - file source orc - codec: lzo",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15551. " +
        "Recovery trigger: GPU ORC supports LZO or safely falls back; P1."))
  enableSuite[RapidsSaveLoadSuite]
  enableSuite[RapidsTableScanSuite]
  enableSuite[RapidsDataSourceV2DataFrameSessionCatalogSuite]
  enableSuite[RapidsTextV1Suite]
  enableSuite[RapidsTextV2Suite]
  enableSuite[RapidsBinaryFileFormatSuite]
    .exclude("BinaryFileFormat methods",
      ADJUST_UT("https://github.com/NVIDIA/cudf-spark/issues/15552. " +
        "Recovery trigger: add equivalent query-level RAPIDS coverage or a GPU-owned hook; P2."))
    .exclude("createFilterFunction",
      ADJUST_UT("https://github.com/NVIDIA/cudf-spark/issues/15552. " +
        "Recovery trigger: add equivalent query-level RAPIDS coverage or a GPU-owned hook; P2."))
    .exclude("buildReader",
      ADJUST_UT("https://github.com/NVIDIA/cudf-spark/issues/15552. " +
        "Recovery trigger: add equivalent query-level RAPIDS coverage or a GPU-owned hook; P2."))
    .exclude("column pruning",
      ADJUST_UT("https://github.com/NVIDIA/cudf-spark/issues/15552. " +
        "Recovery trigger: add equivalent query-level RAPIDS coverage or a GPU-owned hook; P2."))
    .exclude("column pruning - non-readable file",
      ADJUST_UT("https://github.com/NVIDIA/cudf-spark/issues/15552. " +
        "Recovery trigger: replace the chmod-based unreadability setup with filesystem-independent " +
        "fault injection while preserving query-level count coverage; P2."))
  enableSuite[RapidsFileMetadataStructSuite]
  enableSuite[RapidsDisableUnnecessaryBucketedScanWithoutHiveSupportSuite]
    .exclude("SPARK-32859: disable unnecessary bucketed table scan - basic test",
      ADJUST_UT("Replaced by GPU-aware testRapids coverage. See " +
        "https://github.com/NVIDIA/cudf-spark/issues/15512. Recovery trigger: upstream Spark " +
        "plan assertions become implementation agnostic; P2."))
    .exclude("SPARK-32859: disable unnecessary bucketed table scan - multiple joins test",
      ADJUST_UT("Replaced by GPU-aware testRapids coverage. See " +
        "https://github.com/NVIDIA/cudf-spark/issues/15512. Recovery trigger: upstream Spark " +
        "plan assertions become implementation agnostic; P2."))
    .exclude(
      "SPARK-32859: disable unnecessary bucketed table scan - multiple bucketed columns test",
      ADJUST_UT("Replaced by GPU-aware testRapids coverage. See " +
        "https://github.com/NVIDIA/cudf-spark/issues/15512. Recovery trigger: upstream Spark " +
        "plan assertions become implementation agnostic; P2."))
    .exclude("SPARK-32859: disable unnecessary bucketed table scan - other operators test",
      ADJUST_UT("Replaced by GPU-aware testRapids coverage. See " +
        "https://github.com/NVIDIA/cudf-spark/issues/15512. Recovery trigger: upstream Spark " +
        "plan assertions become implementation agnostic; P2."))
    .exclude("SPARK-33075: not disable bucketed table scan for cached query",
      ADJUST_UT("Replaced by testRapids coverage that checks CPU and GPU shuffle nodes. " +
        "See https://github.com/NVIDIA/cudf-spark/issues/15512. Recovery trigger: upstream " +
        "Spark plan assertions become implementation agnostic; P2."))
    .exclude("Aggregates with no groupby over tables having 1 BUCKET, return multiple rows",
      ADJUST_UT("Replaced by GPU-aware testRapids coverage. See " +
        "https://github.com/NVIDIA/cudf-spark/issues/15512. Recovery trigger: upstream Spark " +
        "plan assertions become implementation agnostic; P2."))
  enableSuite[RapidsDisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE]
  enableSuite[RapidsOrcPartitionDiscoverySuite]
  enableSuite[RapidsOrcV1PartitionDiscoverySuite]
  enableSuite[RapidsFileFormatWriterSuite]
  enableSuite[RapidsFileSourceSQLInsertTestSuite]
  enableSuite[RapidsDSV2SQLInsertTestSuite]
  enableSuite[RapidsMetadataCacheV1Suite]
    .exclude("SPARK-16336,SPARK-27961 Suggest fixing FileNotFoundException",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15511. " +
        "Recovery trigger: GPU ORC V1 missing-file errors include Spark-equivalent recreate " +
        "guidance; P1."))
    .exclude("SPARK-16337 temporary view refresh",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15511. " +
        "Recovery trigger: GPU ORC V1 missing-file errors include Spark-equivalent REFRESH " +
        "and recreate guidance; P1."))
  enableSuite[RapidsMetadataCacheV2Suite]
    .exclude("SPARK-16336,SPARK-27961 Suggest fixing FileNotFoundException",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15511. " +
        "Recovery trigger: GPU ORC V2 missing-file errors include Spark-equivalent recreate " +
        "guidance; P1."))
  enableSuite[RapidsFileSourceStrategySuite]
    .exclude("partitioned table - after scan filters", ADJUST_UT("Replaced by testRapids version that checks GpuFilterExec residual filters."))
    .exclude("[SPARK-16818] partition pruned file scans implement sameResult correctly", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15161"))
  enableSuite[RapidsFileScanSuite]
  enableSuite[RapidsPruneFileSourcePartitionsSuite]
  enableSuite[RapidsDataFrameWindowFunctionsSuite]
    .exclude("Window spill with more than the inMemoryThreshold and spillThreshold", WONT_FIX_ISSUE("GPU implementation doesn't respect the inMemoryThreshold and spillThreshold"))
    .exclude("SPARK-21258: complex object in combination with spilling", WONT_FIX_ISSUE("GPU implementation doesn't respect the inMemoryThreshold and spillThreshold"))
    .exclude("SPARK-38237: require all cluster keys for child required distribution for window query", ADJUST_UT("Replaced by testRapids version for GPU execution"))
  enableSuite[RapidsDateExpressionsSuite]
    .exclude("SPARK-34761,SPARK-35889: add a day-time interval to a timestamp", ADJUST_UT("Replaced by modified version without intercept[Exception] part"))
    .exclude("DATE_FROM_UNIX_DATE", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/14955"))
  enableSuite[RapidsDateFunctionsSuite]
    .exclude("function to_date", WONT_FIX_ISSUE("CPU expects SparkException for invalid date format, but GPU with incompatibleDateFormats.enabled=true has different behavior. This is a known difference documented in compatibility.md"))
    .exclude("unix_timestamp", WONT_FIX_ISSUE("GPU with incompatibleDateFormats.enabled=true parses dates differently than CPU for invalid formats - returns values instead of null. This is documented behavior."))
    .exclude("to_unix_timestamp", WONT_FIX_ISSUE("GPU with incompatibleDateFormats.enabled=true parses dates differently than CPU for invalid formats - returns values instead of null. This is documented behavior."))
  enableSuite[RapidsDecimalExpressionSuite]
  enableSuite[RapidsIntervalFunctionsSuite]
  enableSuite[RapidsJsonExpressionsSuite]
    .exclude("from_json - invalid data", ADJUST_UT("Replaced by testRapids version that expects a SparkException instead of TestFailedException"))
  enableSuite[RapidsJsonFunctionsSuite]
    .exclude("SPARK-33134: return partial results only for root JSON objects",
      ADJUST_UT("Inherited test also exercises unsupported root ArrayType and " +
        "MapType[String, StructType]; supported root StructType cases use testRapids"))
  enableSuite[RapidsJsonSuite]
  enableSuite[RapidsMathExpressionsSuite]
    .exclude("round/bround/floor/ceil", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/13747"))
  enableSuite[RapidsMathFunctionsSuite]
  enableSuite[RapidsMiscFunctionsSuite]
  enableSuite[RapidsParquetAvroCompatibilitySuite]
  enableSuite[RapidsParquetV1FilterSuite]
    .exclude("filter pushdown - binary", ADJUST_UT("Original Spark test verifies parquet-mr FilterPredicate construction and record-level filtering for BinaryType; GPU Parquet scan does not use parquet-mr record filters, so the replacement checks final output correctness with a GPU scan."))
    .exclude("filter pushdown - date", ADJUST_UT("Original Spark test verifies parquet-mr FilterPredicate construction and record-level filtering for DateType across CORRECTED/LEGACY rebase modes; GPU Parquet scan does not use parquet-mr predicates, so the replacement checks final output correctness for the supported GPU scan path."))
    .exclude("filter pushdown - timestamp", ADJUST_UT("Original Spark test verifies parquet-mr FilterPredicate construction and record-level filtering for TIMESTAMP_MILLIS/TIMESTAMP_MICROS, plus no INT96 pushdown, across CORRECTED/LEGACY rebase modes; GPU Parquet scan does not use parquet-mr predicates, so the replacement checks final output correctness for the supported GPU scan path."))
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level", WONT_FIX_ISSUE("CPU Parquet row-group pruning accumulator test; it asserts Spark parquet-mr reader counters instead of GPU scan behavior."))
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names", WONT_FIX_ISSUE("CPU Parquet-mr reader counter assertion; GPU scan metrics do not expose the same reader counter."))
    .exclude("Filters should be pushed down for Parquet readers at row group level", WONT_FIX_ISSUE("CPU Parquet row-group pruning accumulator test; it asserts Spark parquet-mr reader counters instead of GPU scan behavior."))
    .exclude("filter pushdown - StringStartsWith", WONT_FIX_ISSUE("CPU Parquet row-group pruning accumulator test; it asserts Spark parquet-mr reader counters instead of GPU scan behavior."))
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down", WONT_FIX_ISSUE("CPU Parquet row-group pruning accumulator test; it asserts Spark parquet-mr reader counters instead of GPU scan behavior."))
    .exclude("Support Parquet column index", WONT_FIX_ISSUE("CPU Parquet column-index pruning accumulator test; GPU scan metrics do not expose the same reader counter."))
    .exclude("SPARK-34562: Bloom filter push down", WONT_FIX_ISSUE("CPU Parquet bloom-filter pruning accumulator test; GPU scan metrics do not expose the same reader counter."))
    .exclude("SPARK-36866: filter pushdown - year-month interval", WONT_FIX_ISSUE("Original Spark test verifies parquet-mr FilterPredicate construction and exact record-level filtering for YearMonthIntervalType after stripping Spark filters; RAPIDS uses GPU Parquet scan, and this shim does not support year-month interval comparison predicates as GPU residual filters, so the inherited check can observe extra row-group rows."))
    .exclude("SPARK-36866: filter pushdown - day-time interval", WONT_FIX_ISSUE("Original Spark test verifies parquet-mr FilterPredicate construction and exact record-level filtering for DayTimeIntervalType after stripping Spark filters; RAPIDS uses GPU Parquet scan rather than parquet-mr record filtering, so the inherited check can observe extra row-group rows."))
  enableSuite[RapidsParquetV2FilterSuite]
    .exclude("filter pushdown - binary", ADJUST_UT("Original Spark test verifies parquet-mr FilterPredicate construction and record-level filtering for BinaryType; GPU Parquet scan does not use parquet-mr record filters, so the replacement checks final output correctness with a GPU scan."))
    .exclude("filter pushdown - date", ADJUST_UT("Original Spark test verifies parquet-mr FilterPredicate construction and record-level filtering for DateType across CORRECTED/LEGACY rebase modes; GPU Parquet scan does not use parquet-mr predicates, so the replacement checks final output correctness for the supported GPU scan path."))
    .exclude("filter pushdown - timestamp", ADJUST_UT("Original Spark test verifies parquet-mr FilterPredicate construction and record-level filtering for TIMESTAMP_MILLIS/TIMESTAMP_MICROS, plus no INT96 pushdown, across CORRECTED/LEGACY rebase modes; GPU Parquet scan does not use parquet-mr predicates, so the replacement checks final output correctness for the supported GPU scan path."))
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level", WONT_FIX_ISSUE("CPU Parquet row-group pruning accumulator test; it asserts Spark parquet-mr reader counters instead of GPU scan behavior."))
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names", WONT_FIX_ISSUE("CPU Parquet-mr reader counter assertion; GPU scan metrics do not expose the same reader counter."))
    .exclude("Filters should be pushed down for Parquet readers at row group level", WONT_FIX_ISSUE("CPU Parquet row-group pruning accumulator test; it asserts Spark parquet-mr reader counters instead of GPU scan behavior."))
    .exclude("filter pushdown - StringStartsWith", WONT_FIX_ISSUE("CPU Parquet row-group pruning accumulator test; it asserts Spark parquet-mr reader counters instead of GPU scan behavior."))
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down", WONT_FIX_ISSUE("CPU Parquet row-group pruning accumulator test; it asserts Spark parquet-mr reader counters instead of GPU scan behavior."))
    .exclude("Support Parquet column index", WONT_FIX_ISSUE("CPU Parquet column-index pruning accumulator test; GPU scan metrics do not expose the same reader counter."))
    .exclude("SPARK-34562: Bloom filter push down", WONT_FIX_ISSUE("CPU Parquet bloom-filter pruning accumulator test; GPU scan metrics do not expose the same reader counter."))
    .exclude("SPARK-36866: filter pushdown - year-month interval", WONT_FIX_ISSUE("Original Spark test verifies parquet-mr FilterPredicate construction and exact record-level filtering for YearMonthIntervalType after stripping Spark filters; RAPIDS uses GPU Parquet scan, and this shim does not support year-month interval comparison predicates as GPU residual filters, so the inherited check can observe extra row-group rows."))
    .exclude("SPARK-36866: filter pushdown - day-time interval", WONT_FIX_ISSUE("Original Spark test verifies parquet-mr FilterPredicate construction and exact record-level filtering for DayTimeIntervalType after stripping Spark filters; RAPIDS uses GPU Parquet scan rather than parquet-mr record filtering, so the inherited check can observe extra row-group rows."))
  enableSuite[RapidsParquetIOSuite]
    .exclude("vectorized reader: missing all struct fields", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15178"))
    .exclude("compression codec", WONT_FIX_ISSUE("Inherited test asserts CPU Parquet writer codec metadata; GPU writer can emit a different valid set of codecs."))
    .exclude("SPARK-35640: int as long should throw schema incompatible error", WONT_FIX_ISSUE("GPU Parquet reader accepts the widening read where the CPU vectorized reader throws this schema mismatch exception."))
    .exclude("SPARK-11044 Parquet writer version fixed as version1 ", WONT_FIX_ISSUE("Inherited test asserts CPU parquet-mr dictionary encoding names; GPU writer uses different valid encodings."))
  enableSuite[RapidsParquetV1AggregatePushDownSuite]
  enableSuite[RapidsParquetV2AggregatePushDownSuite]
  enableSuite[RapidsParquetColumnIndexSuite]
  enableSuite[RapidsParquetCompressionCodecPrecedenceSuite]
    .exclude("Create parquet table with compression", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/11416"))
  enableSuite[RapidsParquetDeltaByteArrayEncodingSuite]
  enableSuite[RapidsParquetDeltaEncodingInteger]
  enableSuite[RapidsParquetDeltaEncodingLong]
  enableSuite[RapidsParquetDeltaLengthByteArrayEncodingSuite]
  enableSuite[RapidsParquetEncodingSuite]
    .exclude("Read row group containing both dictionary and plain encoded pages", ADJUST_UT("Test uses CPU VectorizedParquetRecordReader directly, not a GPU path. GPU-native mixed encoding test added in PR #13982. See https://github.com/NVIDIA/spark-rapids/issues/13739"))
    .exclude("parquet v2 pages - delta encoding", ADJUST_UT("Replaced by testRapids version that drops encoding-format assertion. GPU's libcudf parquet writer chooses different but valid Parquet v2 encodings (PLAIN/PLAIN_DICTIONARY) than CPU's DELTA_BINARY_PACKED/DELTA_BYTE_ARRAY; encoding choice is internal optimization that doesn't affect data correctness. See https://github.com/NVIDIA/spark-rapids/issues/13745"))
    .exclude("parquet v2 pages - rle encoding for boolean value columns", ADJUST_UT("Replaced by testRapids version that drops encoding-format assertion. GPU's libcudf parquet writer uses PLAIN encoding for booleans rather than CPU's RLE; encoding choice is internal optimization that doesn't affect data correctness. See https://github.com/NVIDIA/spark-rapids/issues/13746"))
  enableSuite[RapidsParquetFileFormatSuite]
    .excludeByPrefix("Propagate Hadoop configs from", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/11602"))
  enableSuite[RapidsParquetFieldIdIOSuite]
  enableSuite[RapidsParquetFieldIdSchemaSuite]
  enableSuite[RapidsParquetInteroperabilitySuite]
    .exclude("parquet timestamp conversion", ADJUST_UT("replaced by testRapids version which copies the impala_timestamp file from the resources directory"))
  enableSuite[RapidsParquetPartitionDiscoverySuite]
    .exclude("Various partition value types", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/11583"))
  enableSuite[RapidsParquetProtobufCompatibilitySuite]
  enableSuite[RapidsParquetQuerySuite]
    .exclude("SPARK-26677: negated null-safe equality comparison should not filter matched row groups", ADJUST_UT("fetches the CPU version of Execution Plan instead of the GPU version."))
    .exclude("SPARK-34212 Parquet should read decimals correctly", ADJUST_UT("Vectorized Parquet reader throws an exception when scale is narrowed in Apache Spark where as the spark-rapids plugin does not."))
  enableSuite[RapidsParquetRebaseDatetimeSuite]
    .exclude("SPARK-31159, SPARK-37705: compatibility with Spark 2.4/3.2 in reading dates/timestamps", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/11599"))
    .exclude("SPARK-31159, SPARK-37705: rebasing timestamps in write", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/11593"))
    .exclude("SPARK-31159: rebasing dates in write", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/11480"))
    .exclude("SPARK-35427: datetime rebasing in the EXCEPTION mode", ADJUST_UT("original test case inherited from Spark cannot find the needed local resources"))
  enableSuite[RapidsParquetSchemaPruningSuite]
  enableSuite[RapidsParquetSchemaSuite]
    .exclude("schema mismatch failure error message for parquet reader", WONT_FIX_ISSUE("GPU uses a unified parquet reader path; the non-vectorized CPU error variant rooted in ParquetDecodingException is not reachable by design. See https://github.com/NVIDIA/spark-rapids/issues/11434"))
  enableSuite[RapidsParquetThriftCompatibilitySuite]
    .exclude("Read Parquet file generated by parquet-thrift", ADJUST_UT("https://github.com/NVIDIA/spark-rapids/pull/11591"))
  enableSuite[RapidsParquetVectorizedSuite]
  enableSuite[RapidsOrcFilterSuite]
  enableSuite[RapidsOrcV1QuerySuite]
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core", WONT_FIX_ISSUE("Inherited Spark check toggles between CPU native/hive ORC implementations; RAPIDS uses the GPU ORC path instead."))
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column", WONT_FIX_ISSUE("CPU ORC vectorized-reader assertion; GPU uses GpuOrcScan rather than Spark CPU vectorized reader flag."))
    .exclude("SPARK-37728: Reading nested columns with ORC vectorized reader should not cause ArrayIndexOutOfBoundsException", WONT_FIX_ISSUE("CPU ORC vectorized-reader assertion; GPU uses GpuOrcScan rather than Spark CPU vectorized reader flag."))
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields", WONT_FIX_ISSUE("CPU ORC vectorized-reader limit assertion; GPU does not expose Spark CPU ORC vectorized-reader flag."))
  enableSuite[RapidsOrcV2QuerySuite]
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core", WONT_FIX_ISSUE("Inherited Spark check toggles between CPU native/hive ORC implementations; RAPIDS uses the GPU ORC path instead."))
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column", WONT_FIX_ISSUE("CPU ORC vectorized-reader assertion; GPU uses GpuOrcScan rather than Spark CPU vectorized reader flag."))
    .exclude("SPARK-37728: Reading nested columns with ORC vectorized reader should not cause ArrayIndexOutOfBoundsException", WONT_FIX_ISSUE("CPU ORC vectorized-reader assertion; GPU uses GpuOrcScan rather than Spark CPU vectorized reader flag."))
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields", WONT_FIX_ISSUE("CPU ORC vectorized-reader limit assertion; GPU does not expose Spark CPU ORC vectorized-reader flag."))
  enableSuite[RapidsOrcV1AggregatePushDownSuite]
  enableSuite[RapidsOrcV2AggregatePushDownSuite]
    .exclude("nested column: Count(top level column) push down", KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15186"))

  private val orcSchemaPruningModes = Seq(
    "Spark vectorized reader - without partition data column",
    "Spark vectorized reader - with partition data column",
    "Non-vectorized reader - without partition data column",
    "Non-vectorized reader - with partition data column")

  private val orcComplexReaderFailureReason =
    KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/15179")

  private val orcV1ComplexReaderFailureCases = Seq(
    "select a single complex field",
    "select a single complex field and the partition column",
    "partial schema intersection - select missing subfield",
    "empty schema intersection",
    "select one deep nested complex field after join",
    "select one deep nested complex field after outer join")

  private val orcV1SchemaPruning = enableSuite[RapidsOrcV1SchemaPruningSuite]
  orcV1ComplexReaderFailureCases.foreach { testName =>
    orcSchemaPruningModes.foreach { mode =>
      orcV1SchemaPruning.exclude(s"$mode - $testName", orcComplexReaderFailureReason)
    }
  }

  private val orcV2ComplexReaderFailureCases = Seq(
    "select a single complex field",
    "select a single complex field and the partition column",
    "partial schema intersection - select missing subfield",
    "empty schema intersection",
    "select one deep nested complex field after join",
    "select one deep nested complex field after outer join")

  private val orcV2SchemaPruning = enableSuite[RapidsOrcV2SchemaPruningSuite]
  orcV2ComplexReaderFailureCases.foreach { testName =>
    orcSchemaPruningModes.foreach { mode =>
      orcV2SchemaPruning.exclude(s"$mode - $testName", orcComplexReaderFailureReason)
    }
  }
  enableSuite[RapidsRandomSuite]
    .exclude("random", ADJUST_UT("Replaced by testRapids version that considers partitionIndex offset"))
    .exclude("SPARK-9127 codegen with long seed", ADJUST_UT("Replaced by testRapids version that considers partitionIndex offset"))
  enableSuite[RapidsReplaceNullWithFalseInPredicateEndToEndSuite]
  enableSuite[RapidsRegexpExpressionsSuite]
  enableSuite[RapidsSQLWindowFunctionSuite]
    .exclude("test with low buffer spill threshold", WONT_FIX_ISSUE("GPU window implementation doesn't respect Spark WindowExec spill thresholds."))
  enableSuite[RapidsSortSuite]
    .exclude("basic sorting using ExternalSort", ADJUST_UT("Replaced by testRapids query-level version that checks GpuSortExec."))
    .exclude("sorting all nulls", WONT_FIX_ISSUE("Direct CPU SortExec SparkPlanTest, not a GPU execution path."))
    .exclude("sort followed by limit", ADJUST_UT("Replaced by testRapids query-level version that checks GpuTopN."))
    .exclude("sorting does not crash for large inputs", WONT_FIX_ISSUE("Direct CPU SortExec spill-path SparkPlanTest, not a GPU execution path."))
    .exclude("sorting updates peak execution memory", WONT_FIX_ISSUE("CPU memory metric test; not applicable to GPU sort execution."))
    .exclude("SPARK-33260: sort order is a Stream", WONT_FIX_ISSUE("Direct CPU SortExec SparkPlanTest, not a GPU execution path."))
    .excludeByPrefix("sorting on ", WONT_FIX_ISSUE("Direct CPU SortExec SparkPlanTest, not a GPU execution path."))
  enableSuite[RapidsTakeOrderedAndProjectSuite]
    .exclude("TakeOrderedAndProject.doExecute without project", ADJUST_UT("Replaced by testRapids query-level version that checks GpuTopN."))
    .exclude("TakeOrderedAndProject.doExecute with project", ADJUST_UT("Replaced by testRapids query-level version that checks GpuTopN."))
  enableSuite[RapidsStringExpressionsSuite]
    .exclude("SPARK-22550: Elt should not generate codes beyond 64KB", WONT_FIX_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("SPARK-22603: FormatString should not generate codes beyond 64KB", WONT_FIX_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
  enableSuite[RapidsStringFunctionsSuite]
  enableSuite[RapidsProductAggSuite]
  enableSuite[RapidsComplexTypesSuite]
  enableSuite[RapidsCSVSuite]
    .exclude("nullable fields with user defined null value of \"null\"", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/13893"))
    .exclude("empty fields with user defined empty values", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/13894"))
    .exclude("save csv with empty fields with user defined empty values", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/13895"))
    .exclude("SPARK-24329: skip lines with comments, and one or multiple whitespaces", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/13896"))
    .exclude("SPARK-23786: warning should be printed if CSV header doesn't conform to schema", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/13897"))
    .exclude("SPARK-33566: configure UnescapedQuoteHandling to parse unescaped quotes and unescaped delimiter data correctly", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/13901"))
  enableSuite[RapidsCsvExpressionsSuite]
    .exclude("unsupported mode", ADJUST_UT("Replaced by a testRapids case which changed the expectation of SparkException instead of TestFailedException"))
  enableSuite[RapidsCsvFunctionsSuite]
  enableSuite[RapidsCSVReadSchemaSuite]
  enableSuite[RapidsHeaderCSVReadSchemaSuite]
  enableSuite[RapidsJsonReadSchemaSuite]
  enableSuite[RapidsOrcReadSchemaSuite]
  enableSuite[RapidsVectorizedOrcReadSchemaSuite]
  enableSuite[RapidsMergedOrcReadSchemaSuite]
  enableSuite[RapidsParquetReadSchemaSuite]
  enableSuite[RapidsVectorizedParquetReadSchemaSuite]
  enableSuite[RapidsMergedParquetReadSchemaSuite]
  enableSuite[RapidsGeneratorFunctionSuite]
  enableSuite[RapidsSQLQuerySuite]
    .exclude("aggregation with codegen updates peak execution memory", WONT_FIX_ISSUE("Codegen and memory metrics not applicable for GPU"))
    .exclude("external sorting updates peak execution memory", WONT_FIX_ISSUE("Memory metrics implementation differs on GPU"))
    .exclude("run sql directly on files", ADJUST_UT("Replaced by testRapids version that expects \"Path does not exist\" instead of \"Hive built-in ORC data source must be used with Hive support\" because there's a spark-hive jar in the CLASSPATH in our UT running"))
    .exclude("Common subexpression elimination", WONT_FIX_ISSUE("CPU test asserts per-row Scala UDF invocation counts via a LongAccumulator to verify Catalyst's subexpression elimination. GPU's columnar batch execution does not have equivalent per-row UDF invocation semantics, so the accumulator counts diverge from CPU by design even though the output values match. See https://github.com/NVIDIA/spark-rapids/issues/14106"))
    .exclude("SPARK-31594: Do not display the seed of rand/randn with no argument in output schema", ADJUST_UT("Replaced by testRapids version with a correct regex expression to match the projectExplainOutput, randn isn't supported now. See https://github.com/NVIDIA/spark-rapids/issues/11613"))
    .exclude("SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar", ADJUST_UT("Inherited Spark test overfits exact AnalysisException wrapping for invalid LIKE escape patterns; the bridge behavior is compatible but can surface different exception wrapping. See https://github.com/NVIDIA/spark-rapids/issues/14953"))
    .exclude("SPARK-33084: Add jar support Ivy URI in SQL", WONT_FIX_ISSUE("Spark ADD JAR Ivy dependency resolution/listJars infrastructure test, not a RAPIDS GPU execution path. Excluded to avoid transient external Maven/Ivy download failures. See https://github.com/NVIDIA/spark-rapids/issues/14777. Recovery trigger: make the dependency resolution hermetic or guarantee CI repository availability; P2."))
    .exclude("SPARK-33084: Add jar support Ivy URI in SQL -- jar contains udf class", ADJUST_UT("Replaced by testRapids version that uses testFile() to access Spark test resources instead of getContextClassLoader"))
    .exclude("SPARK-33482: Fix FileScan canonicalization", ADJUST_UT("Replaced by testRapids version using V1 sources with AQE and broadcast disabled to assert ReusedExchangeExec directly"))
    .exclude("SPARK-36093: RemoveRedundantAliases should not change expression's name", ADJUST_UT("Replaced by testRapids version that checks the partition column name of the GpuInsertIntoHadoopFsRelationCommand"))
  enableSuite[RapidsCTEInlineSuiteAEOff]
  enableSuite[RapidsCTEInlineSuiteAEOn]
  enableSuite[RapidsFilteredScanSuite]
    .excludeByPrefix(
      "PushDown Returns ",
      KNOWN_ISSUE(
        "The Spark helper directly executes the CPU RowDataSourceScanExec instead of the " +
          "query-level plan. See https://github.com/NVIDIA/cudf-spark/issues/15566. " +
          "Recovery trigger: add query-level RAPIDS coverage for equivalent pushdown and " +
          "column-pruning assertions; P2."))
  enableSuite[RapidsPrunedScanSuite]
    .excludeByPrefix(
      "Columns output ",
      KNOWN_ISSUE(
        "The Spark helper directly executes the CPU RowDataSourceScanExec instead of the " +
          "query-level plan. See https://github.com/NVIDIA/cudf-spark/issues/15567. " +
          "Recovery trigger: add query-level RAPIDS coverage for equivalent column-pruning " +
          "assertions; P2."))
  enableSuite[RapidsSupportsCatalogOptionsSuite]
  enableSuite[RapidsLocalTempViewTestSuite]
  enableSuite[RapidsGlobalTempViewTestSuite]
  enableSuite[RapidsPersistedViewTestSuite]
  enableSuite[RapidsRowDataSourceStrategySuite]
  enableSuite[RapidsConfigBehaviorSuite]
}
// scalastyle:on line.size.limit
