# Change log
Generated on 2026-06-14

## Release 26.06

### Features
|||
|:---|:---|
|[#14601](https://github.com/NVIDIA/spark-rapids/issues/14601)|[FEA] Delta Lake DB-17.3: Enable Delta Lake tests in CI|
|[#14598](https://github.com/NVIDIA/spark-rapids/issues/14598)|[FEA] Delta Lake DB-17.3: Enable GPU MERGE INTO|
|[#14054](https://github.com/NVIDIA/spark-rapids/issues/14054)|[FEA] splitTargetSizeInHalfGpu should split the sequence by elements if splitting by byte size is not possible|
|[#14597](https://github.com/NVIDIA/spark-rapids/issues/14597)|[FEA] Delta Lake DB-17.3: Enable GPU DELETE + UPDATE|
|[#14600](https://github.com/NVIDIA/spark-rapids/issues/14600)|[FEA] Enable GPU-accelerated Deletion Vector (DV) reads for Delta Lake on Databricks 17.3.|
|[#14561](https://github.com/NVIDIA/spark-rapids/issues/14561)|[FEA] Support `replace( strCol, searchCol, replCol )`|
|[#14596](https://github.com/NVIDIA/spark-rapids/issues/14596)|[FEA] Delta Lake DB-17.3: Build system setup and write path|
|[#14461](https://github.com/NVIDIA/spark-rapids/issues/14461)|[FEA] Add support for Delta Lake 4.1.x|
|[#14539](https://github.com/NVIDIA/spark-rapids/issues/14539)|[FEA] Support `contains( strCol, expr )`|
|[#14613](https://github.com/NVIDIA/spark-rapids/issues/14613)|[FEA] Support binary type in higher-order functions|
|[#12550](https://github.com/NVIDIA/spark-rapids/issues/12550)|[FEA] Support `org.apache.spark.sql.catalyst.expressions.Hex`|

### Performance
|||
|:---|:---|
|[#14283](https://github.com/NVIDIA/spark-rapids/issues/14283)|[FEA] Support join condition which has "cast to bigint"|
|[#14068](https://github.com/NVIDIA/spark-rapids/issues/14068)|[FEA] Iceberg planning overhead is larger than parquet planning.|
|[#14591](https://github.com/NVIDIA/spark-rapids/issues/14591)|[FEA] Support perf io in iceberg.|
|[#14064](https://github.com/NVIDIA/spark-rapids/issues/14064)|[FEA] Iceberg parquet reader should use file cache for parquet footers.|
|[#14063](https://github.com/NVIDIA/spark-rapids/issues/14063)|[FEA] Iceberg parquet reader should not blindly disable small combination.|

### Bugs Fixed
|||
|:---|:---|
|[#14864](https://github.com/NVIDIA/spark-rapids/issues/14864)|[BUG] test_parquet_interleaved_file_splits_partition_value_alignment fails on Dataproc Serverless: os.walk cannot see GCS-backed spark_tmp_path|
|[#14981](https://github.com/NVIDIA/spark-rapids/issues/14981)|[BUG] DBR 14.3 CPU fallback MERGE with NOT MATCHED BY SOURCE can fail with GpuUnionExec|
|[#14986](https://github.com/NVIDIA/spark-rapids/issues/14986)|[BUG] Delta DELETE on Databricks 14.3 returns num_affected_rows = -1 for metadata-only (partition/whole-table) deletes|
|[#14976](https://github.com/NVIDIA/spark-rapids/issues/14976)|[BUG] Delta DV predicate pushdown crashes when DV predicate filter remains CPU FilterExec|
|[#14949](https://github.com/NVIDIA/spark-rapids/issues/14949)|[BUG] Delta DV zero-column PERFILE scans ignore deleted rows and return incorrect counts|
|[#14944](https://github.com/NVIDIA/spark-rapids/issues/14944)|[BUG] [v26.06.0-SNAPSHOT][DB 17.3] Delta OPTIMIZE DV fallback fails with NPE in GpuOverrides.isDeltaLakeMetadataQuery|
|[#14807](https://github.com/NVIDIA/spark-rapids/issues/14807)|[BUG] Iceberg _pos is task-local on split data files, causing silent data corruption on MoR reads with positional deletes|
|[#14726](https://github.com/NVIDIA/spark-rapids/issues/14726)|[BUG] Iceberg 1.10.1 SparkWrite class loader issue when jars in $SPARK_HOME/jars|
|[#14895](https://github.com/NVIDIA/spark-rapids/issues/14895)|[BUG] DBR 17.3 Delta no-DV read fails with DELTA_SKIP_ROW_COLUMN_NOT_FILLED on GPU|
|[#14861](https://github.com/NVIDIA/spark-rapids/issues/14861)|[BUG] test_comprehensive_from_utc_timestamp fails on Databricks 14.3 for timezone SystemV/EST5EDT (1-hour GPU/CPU diff) intermittently|
|[#14813](https://github.com/NVIDIA/spark-rapids/issues/14813)|GpuJsonToStructs fails with token count assertion on multiple malformed open-brace rows|
|[#14689](https://github.com/NVIDIA/spark-rapids/issues/14689)|[BUG] Replace cuDF regex chain in GpuToTimestamp with a fused JNI kernel|
|[#14815](https://github.com/NVIDIA/spark-rapids/issues/14815)|[BUG] Dataproc Serverless 2.2 IT: string_test.py Spark job exceeded 3600s timeout, batch FAILED|
|[#14831](https://github.com/NVIDIA/spark-rapids/issues/14831)|[BUG] Parquet COALESCING reader can return invalid results from partitioned tables|
|[#11653](https://github.com/NVIDIA/spark-rapids/issues/11653)|[BUG] Spark UT framework: select explode of nested field of array of struct: Encountered an exception applying GPU overrides|
|[#14790](https://github.com/NVIDIA/spark-rapids/issues/14790)|[BUG] MetricsEventLogValidationSuite parquet write operator time ratio test fails near 10% threshold on Spark 4.1.1 / Scala 2.13|
|[#14696](https://github.com/NVIDIA/spark-rapids/issues/14696)|[BUG] Queries against Delta tables with deletion vectors may not reuse plan parts that should be reusable|
|[#14800](https://github.com/NVIDIA/spark-rapids/issues/14800)|[BUG] iceberg parquet shim class-name collision in dist jar: cache-aware 1.10.x shim silently dropped|
|[#14763](https://github.com/NVIDIA/spark-rapids/issues/14763)|[BUG] [CI] Spark Connect smoke test fails to start server on 127.0.0.1:15002 for multiple jobs|
|[#14767](https://github.com/NVIDIA/spark-rapids/issues/14767)|[BUG] GitHub mvn verify docgen check no longer validates Spark 330 generated docs|
|[#14681](https://github.com/NVIDIA/spark-rapids/issues/14681)|[BUG] test_std_variance fails with GPU nan vs CPU inf on Double data with small batchSizeBytes intermittently|
|[#14758](https://github.com/NVIDIA/spark-rapids/issues/14758)|[BUG] Unsafe close of HostColumnVectors in `GpuColumnVector::extractHostColumns()`|
|[#14765](https://github.com/NVIDIA/spark-rapids/issues/14765)|[BUG] Nightly IT matrix: delta-core 2.1.1 Ivy resolution fails on Spark 3.3.4 and Spark Connect server fails to start on Spark 3.5.8|
|[#14766](https://github.com/NVIDIA/spark-rapids/issues/14766)|[BUG] Iceberg S3Tables IT fails: ivy unresolved dependencies (iceberg/AWS SDK v2/netty) -> JAVA_GATEWAY_EXITED, no tests ran|
|[#14755](https://github.com/NVIDIA/spark-rapids/issues/14755)|[BUG] Nightly dependency-check fails: Non-resolvable parent POM for rapids-4-spark-parent SNAPSHOT|
|[#14712](https://github.com/NVIDIA/spark-rapids/issues/14712)|[BUG] spark.rapids.sql.optimizer.enabled=true throws NoClassDefFoundError: com/nvidia/spark/rapids/Optimizer|
|[#14630](https://github.com/NVIDIA/spark-rapids/issues/14630)|[BUG] Fatal cudaErrorIllegalAddress error occurred in CI test job|
|[#14701](https://github.com/NVIDIA/spark-rapids/issues/14701)|[BUG] Spark 411 unit test fails with NoSuchMethodError RowDeltaUtils.REINSERT_OPERATION in RapidsShuffleIntegrationSuite|
|[#14705](https://github.com/NVIDIA/spark-rapids/issues/14705)|[BUG] FileCache metrics is missing for iceberg.|
|[#14699](https://github.com/NVIDIA/spark-rapids/issues/14699)|[BUG] cudf_udf nightly fails: `No module named pip` in newly created conda env (python=3.12, cudf=26.06)|
|[#14567](https://github.com/NVIDIA/spark-rapids/issues/14567)|[BUG] hash_aggregate_test.py::test_hash_grpby_pivot failed java.lang.ArithmeticException: BigInteger out of long range in DB 17.3 runtime intermittently|
|[#14614](https://github.com/NVIDIA/spark-rapids/issues/14614)|[BUG] Build failure: `object sketch is not a member of package org.apache.spark.util` in GpuBloomFilterAggregate on Databricks runtimes|
|[#13816](https://github.com/NVIDIA/spark-rapids/issues/13816)|[AutoSparkUT]MakeDecimal test failed in DecimalExpressSuite from Spark UT|
|[#14532](https://github.com/NVIDIA/spark-rapids/issues/14532)|[BUG] GPU JSON reader incorrectly returns null/drops rows for non-timestamp values after isTimestamp validation change in incompatible date formats path|
|[#14581](https://github.com/NVIDIA/spark-rapids/issues/14581)|[BUG] JVM crash (SIGSEGV) in native cuDF code during RapidsDataFrameFunctionsSuite array_repeat (Spark 3.3.0, cuda12)|
|[#11416](https://github.com/NVIDIA/spark-rapids/issues/11416)|[BUG] Create parquet table with compression|
|[#14592](https://github.com/NVIDIA/spark-rapids/issues/14592)|arrays_zip crashes with 'Range is out of bounds' when input batch has 0 rows|
|[#13759](https://github.com/NVIDIA/spark-rapids/issues/13759)|[AutoSparkUT] GetTimestamp Parses Invalid Format Instead of Returning Null|
|[#14109](https://github.com/NVIDIA/spark-rapids/issues/14109)|[AutoSparkUT]"SPARK-17515: CollectLimit.execute() should perform per-partition limits" in SQLQuerySuite failed|
|[#12452](https://github.com/NVIDIA/spark-rapids/issues/12452)|[BUG] hyper_log_log_plus_plus_test.test_hllpp_precisions_groupby[0.3] failed in mismatch cpu and gpu result|
|[#14122](https://github.com/NVIDIA/spark-rapids/issues/14122)|[AutoSparkUT]"SPARK-33482: Fix FileScan canonicalization" in SQLQuerySuite failed|

### PRs
|||
|:---|:---|
|[#15079](https://github.com/NVIDIA/spark-rapids/pull/15079)|Update dependency version private to 26.06.1|
|[#15024](https://github.com/NVIDIA/spark-rapids/pull/15024)|Fall back when Iceberg S3 PerfIO is unsupported|
|[#15066](https://github.com/NVIDIA/spark-rapids/pull/15066)|Use current repository name in GitHub workflows [skip ci]|
|[#14943](https://github.com/NVIDIA/spark-rapids/pull/14943)|Update changelog for the v26.06.0 release|
|[#14941](https://github.com/NVIDIA/spark-rapids/pull/14941)|Update dependency version JNI, private, hybrid to 26.06.0|
|[#14998](https://github.com/NVIDIA/spark-rapids/pull/14998)|Add the missing case for GpuShuffleCoalesceExec for the broadcast hash join on DBR 14.3 [Databricks]|
|[#14990](https://github.com/NVIDIA/spark-rapids/pull/14990)|[BUG] Keep DBR 14.3 local union source on CPU|
|[#14987](https://github.com/NVIDIA/spark-rapids/pull/14987)|Fix Delta DELETE num_affected_rows on DBR 13.3 and 14.3 for metadata-only deletes|
|[#14988](https://github.com/NVIDIA/spark-rapids/pull/14988)|Fix Delta DV predicate pushdown with CPU FilterExec|
|[#14952](https://github.com/NVIDIA/spark-rapids/pull/14952)|Fix Delta DV zero-column scan row counts|
|[#14945](https://github.com/NVIDIA/spark-rapids/pull/14945)|Fix Delta OPTIMIZE DV fallback NPE on DBR 17.3|
|[#14808](https://github.com/NVIDIA/spark-rapids/pull/14808)|Fix Iceberg _pos to be file-global instead of task-local on split files|
|[#14937](https://github.com/NVIDIA/spark-rapids/pull/14937)|Iceberg integration tests: trim redundant coverage matrices|
|[#14922](https://github.com/NVIDIA/spark-rapids/pull/14922)|[DOC] update download page for 26.06 release [skip ci]|
|[#14920](https://github.com/NVIDIA/spark-rapids/pull/14920)|Add regression tests for to_timestamp bug fixes|
|[#14866](https://github.com/NVIDIA/spark-rapids/pull/14866)|Fix Iceberg package-private access after shim isolation|
|[#14903](https://github.com/NVIDIA/spark-rapids/pull/14903)|[CHERRYPICK] Shuffle bytes double count metric fix + tests to cover shuffle removal at unregister|
|[#14914](https://github.com/NVIDIA/spark-rapids/pull/14914)|Keep row transition for final AQE exchanges|
|[#14847](https://github.com/NVIDIA/spark-rapids/pull/14847)|Add DBR 17.3 Delta OPTIMIZE and auto compaction support|
|[#14904](https://github.com/NVIDIA/spark-rapids/pull/14904)|Fix for delta skip row  exception|
|[#14880](https://github.com/NVIDIA/spark-rapids/pull/14880)|[BUG FIX] Iceberg: fix Missing required field for newly-added nested MAP/LIST|
|[#14820](https://github.com/NVIDIA/spark-rapids/pull/14820)|[DeltaLake] Enable GPU Delta MERGE on DBR 17.3|
|[#14804](https://github.com/NVIDIA/spark-rapids/pull/14804)|ci: declare workflow-level `contents: read` on 4 workflows [skip ci]|
|[#14859](https://github.com/NVIDIA/spark-rapids/pull/14859)|Fall back to CPU for Iceberg partition transforms sourcing nested fields|
|[#14839](https://github.com/NVIDIA/spark-rapids/pull/14839)|[AutoSparkUT] CSV: support decimal grouping separator parsing (Locale.US)|
|[#14706](https://github.com/NVIDIA/spark-rapids/pull/14706)|Replace cuDF regex chain in GpuToTimestamp with fused JNI parser|
|[#14865](https://github.com/NVIDIA/spark-rapids/pull/14865)|Fix async output write with pipe-backed cloud streams|
|[#14850](https://github.com/NVIDIA/spark-rapids/pull/14850)|Revert "Skip GBK decode test on Dataproc Serverless (#14816)"|
|[#14851](https://github.com/NVIDIA/spark-rapids/pull/14851)|Fix concurrent writer fallback with empty caches|
|[#14845](https://github.com/NVIDIA/spark-rapids/pull/14845)|Optimize null-restore sequence in cast struct to json|
|[#14835](https://github.com/NVIDIA/spark-rapids/pull/14835)|[AutoSparkUT] Fix invalid numSplits 0 in single-column explode (#11653)|
|[#14849](https://github.com/NVIDIA/spark-rapids/pull/14849)|[AutoSparkUT] Fix regex complexity estimator overflow|
|[#14852](https://github.com/NVIDIA/spark-rapids/pull/14852)|Relax parquet write operator time lower bound|
|[#14841](https://github.com/NVIDIA/spark-rapids/pull/14841)|Fix parquet coalescing reader file grouping alignment|
|[#14843](https://github.com/NVIDIA/spark-rapids/pull/14843)|Use fused replaceNulls to compute per row repetition in explode|
|[#14842](https://github.com/NVIDIA/spark-rapids/pull/14842)|Use null-propagating stringConcatenate in cast complex-type-to-string|
|[#14684](https://github.com/NVIDIA/spark-rapids/pull/14684)|splitTargetSizeInHalfGpu by data size if not target size|
|[#14844](https://github.com/NVIDIA/spark-rapids/pull/14844)|Port Delta DV predicate pruning fix to DBR 17.3|
|[#14825](https://github.com/NVIDIA/spark-rapids/pull/14825)|TimeoutSparkListener: dump executor threads in addition to driver threads|
|[#14830](https://github.com/NVIDIA/spark-rapids/pull/14830)|Use fused mergeAndSetValidity kernel in hypot|
|[#14817](https://github.com/NVIDIA/spark-rapids/pull/14817)|Use fused replaceNulls for non-nested types in GpuNvl|
|[#14818](https://github.com/NVIDIA/spark-rapids/pull/14818)|Use fused mergeAndSetValidity kernel in mergeNulls|
|[#14819](https://github.com/NVIDIA/spark-rapids/pull/14819)|Use scalar extractListElement index instead of column in substring where length is fixed|
|[#14647](https://github.com/NVIDIA/spark-rapids/pull/14647)|Use scalar extractListElement index instead of column in regex extract|
|[#14826](https://github.com/NVIDIA/spark-rapids/pull/14826)|Move former shim sources to conventional source code roots|
|[#14810](https://github.com/NVIDIA/spark-rapids/pull/14810)|Enable Delta DELETE and UPDATE for DBR 17.3|
|[#14824](https://github.com/NVIDIA/spark-rapids/pull/14824)|[AutoSparkUT] Fix CSV maxCharsPerColumn fallback|
|[#14770](https://github.com/NVIDIA/spark-rapids/pull/14770)|[DOC] update download page for 26.04.2 hot release [skip ci]|
|[#14761](https://github.com/NVIDIA/spark-rapids/pull/14761)|Remove deletion vector predicate from dataFilters of scan|
|[#14787](https://github.com/NVIDIA/spark-rapids/pull/14787)|Enable native Delta DV reads for DBR 17.3|
|[#14793](https://github.com/NVIDIA/spark-rapids/pull/14793)|Support join condition which has cast|
|[#14809](https://github.com/NVIDIA/spark-rapids/pull/14809)|[Perf] [bugfix] Fix a Iceberg class collision between Iceberg versions to improve perf|
|[#14795](https://github.com/NVIDIA/spark-rapids/pull/14795)|Fix flaky parquet write operator time validation|
|[#14792](https://github.com/NVIDIA/spark-rapids/pull/14792)|[AutoSparkUT] Preserve non-deterministic expression values across coalesce/union (#14156)|
|[#14778](https://github.com/NVIDIA/spark-rapids/pull/14778)|[AutoSparkUT] URI-decode JSON/CSV file path in GpuTextBasedPartitionReader (#11158, #13898)|
|[#14754](https://github.com/NVIDIA/spark-rapids/pull/14754)|Add per-table session-level Iceberg scan-option overrides|
|[#14783](https://github.com/NVIDIA/spark-rapids/pull/14783)|Expose cuDF Parquet writer row group size configs|
|[#14816](https://github.com/NVIDIA/spark-rapids/pull/14816)|Skip GBK decode test on Dataproc Serverless|
|[#14814](https://github.com/NVIDIA/spark-rapids/pull/14814)|[integration tests]: extend spark connect startup wait [skip ci]|
|[#14545](https://github.com/NVIDIA/spark-rapids/pull/14545)|Support StringDecode for GBK encoding|
|[#14803](https://github.com/NVIDIA/spark-rapids/pull/14803)|Use Spark330 for generated docs [skip ci]|
|[#14652](https://github.com/NVIDIA/spark-rapids/pull/14652)|Add GPU ArrayAggregate for SUM/PRODUCT/MAX/MIN/ALL/ANY|
|[#14801](https://github.com/NVIDIA/spark-rapids/pull/14801)|[integration tests]: pass ivy settings to spark connect smoke test [skip ci]|
|[#14799](https://github.com/NVIDIA/spark-rapids/pull/14799)|[AutoSparkUT] Exclude flaky SPARK-33084 Add jar Ivy URI SQL test (#14777)|
|[#14772](https://github.com/NVIDIA/spark-rapids/pull/14772)|Avoid to_json fallback for JSON without timestamps on unsupported timezones|
|[#14796](https://github.com/NVIDIA/spark-rapids/pull/14796)|[AutoSparkUT] Re-enable parquet vectorized schema mismatch test|
|[#14660](https://github.com/NVIDIA/spark-rapids/pull/14660)|[AutoSparkUT] Add RapidsInjectRuntimeFilterSuite|
|[#14762](https://github.com/NVIDIA/spark-rapids/pull/14762)|[AutoSparkUT] Fix std variance floating overflow coverage|
|[#14789](https://github.com/NVIDIA/spark-rapids/pull/14789)|Update premerge CI m2 cache restore [skip ci]|
|[#14674](https://github.com/NVIDIA/spark-rapids/pull/14674)|optimize iceberg read|
|[#14798](https://github.com/NVIDIA/spark-rapids/pull/14798)|Fix docs [skip ci]|
|[#14791](https://github.com/NVIDIA/spark-rapids/pull/14791)|[DeltaLake] Address Delta 4.x follow-up nits and add type widening tests|
|[#14637](https://github.com/NVIDIA/spark-rapids/pull/14637)|[AutoSparkUT] Fix SPARK-39175 Cast ANSI error query context (#14123)|
|[#14779](https://github.com/NVIDIA/spark-rapids/pull/14779)|[AutoSparkUT] Reclassify #14106 (Common subexpression elimination) as WONT_FIX_ISSUE|
|[#14694](https://github.com/NVIDIA/spark-rapids/pull/14694)|[AutoSparkUT] Recover ParquetEncodingSuite v2 tests via ADJUST_UT testRapids (#13745, #13746)|
|[#14611](https://github.com/NVIDIA/spark-rapids/pull/14611)|Support Iceberg nested and binary GPU writes|
|[#14759](https://github.com/NVIDIA/spark-rapids/pull/14759)|Fix unsafe close of HostColumnVectors in `GpuColumnVector::extractHostColumns()`|
|[#14623](https://github.com/NVIDIA/spark-rapids/pull/14623)|Support `replace(col, targetExpr, replExpr)` for strings. (Include  for testing.)|
|[#14692](https://github.com/NVIDIA/spark-rapids/pull/14692)|[AutoSparkUT] Fix #14172: relax dynamicallySelectedPartitions visibility + recover SPARK-26893 subquery pushdown test|
|[#14610](https://github.com/NVIDIA/spark-rapids/pull/14610)|[AutoSparkUT] Re-enable Flatten test after cuDF fix (rapidsai/cudf#22147)|
|[#14724](https://github.com/NVIDIA/spark-rapids/pull/14724)|Add split-and-retry path to GpuProjectExec|
|[#14716](https://github.com/NVIDIA/spark-rapids/pull/14716)|Add initial Delta lake write support for Databricks-17.3|
|[#14586](https://github.com/NVIDIA/spark-rapids/pull/14586)|Optimize format number implementation|
|[#14646](https://github.com/NVIDIA/spark-rapids/pull/14646)|Support Delta Lake 4.1 on Spark 4.1|
|[#14774](https://github.com/NVIDIA/spark-rapids/pull/14774)|Use ivysettings for spark packages resolution [skip ci]|
|[#14612](https://github.com/NVIDIA/spark-rapids/pull/14612)|[AutoSparkUT] Fix null struct entry handling in GpuMapFromEntries (issue #14128)|
|[#14764](https://github.com/NVIDIA/spark-rapids/pull/14764)|Run dependency checks without Jenkins Maven settings [skip ci]|
|[#14693](https://github.com/NVIDIA/spark-rapids/pull/14693)|[AutoSparkUT] Fix GpuCast decimal-overflow error to match CPU's CheckOverflow message (#14143)|
|[#14691](https://github.com/NVIDIA/spark-rapids/pull/14691)|[AutoSparkUT] Reclassify #11434 as WONT_FIX_ISSUE: parquet non-vectorized error path is unreachable on GPU|
|[#14654](https://github.com/NVIDIA/spark-rapids/pull/14654)|[AutoSparkUT] Add RapidsDataFrameJoinSuite + RapidsBloomFilterAggregateQuerySuite|
|[#14632](https://github.com/NVIDIA/spark-rapids/pull/14632)|[AutoSparkUT] Fix SPARK-39177 map ANSI error query context (#14123)|
|[#14752](https://github.com/NVIDIA/spark-rapids/pull/14752)|Quick fix of the cannot find settings file issue [skip ci]|
|[#14702](https://github.com/NVIDIA/spark-rapids/pull/14702)|[AutoSparkUT] Fix binary host columnar copy for SPARK-33593|
|[#14688](https://github.com/NVIDIA/spark-rapids/pull/14688)|Remove non-Kudo test from integration test|
|[#14727](https://github.com/NVIDIA/spark-rapids/pull/14727)|Use mirror for internal usage to avoid 429 from maven central [skip ci]|
|[#14713](https://github.com/NVIDIA/spark-rapids/pull/14713)|Publish Optimizer trait at JAR root to fix NoClassDefFoundError|
|[#14690](https://github.com/NVIDIA/spark-rapids/pull/14690)|[AutoSparkUT] Fix GpuParquetScan schema-mismatch error message format (#11446)|
|[#14669](https://github.com/NVIDIA/spark-rapids/pull/14669)|[AutoSparkUT] Fix GpuCreateMap empty-map eval; RowToColumnar NullType (#14140, #14108)|
|[#14678](https://github.com/NVIDIA/spark-rapids/pull/14678)|Rename URM helpers and credentials to Artifactory naming in CI and build config|
|[#14538](https://github.com/NVIDIA/spark-rapids/pull/14538)|Support `contains(col, expr)` for strings|
|[#14723](https://github.com/NVIDIA/spark-rapids/pull/14723)|Fix async profiler output copy to s3 [skip ci]|
|[#14719](https://github.com/NVIDIA/spark-rapids/pull/14719)|[DOC] update download page for 26.04.1 hot release [skip ci]|
|[#14717](https://github.com/NVIDIA/spark-rapids/pull/14717)|Update POM files to include Iceberg artifact properties|
|[#14722](https://github.com/NVIDIA/spark-rapids/pull/14722)|Xfail quoted get_json_object test on Dataproc Serverless|
|[#14718](https://github.com/NVIDIA/spark-rapids/pull/14718)|Use GpuShuffleBlockResolverBase.wrapped in unregisterShuffle|
|[#14708](https://github.com/NVIDIA/spark-rapids/pull/14708)|Add file cache metrics for iceberg|
|[#14707](https://github.com/NVIDIA/spark-rapids/pull/14707)|Temporarily xfail std variance edge case|
|[#14687](https://github.com/NVIDIA/spark-rapids/pull/14687)|[CHERRY-PICK] Fix unregister/remove path for wrapped shuffle resolver|
|[#14700](https://github.com/NVIDIA/spark-rapids/pull/14700)|Explicitly install pip for cudf_udf cases [skip ci]|
|[#14645](https://github.com/NVIDIA/spark-rapids/pull/14645)|Add footer cache for iceberg|
|[#14520](https://github.com/NVIDIA/spark-rapids/pull/14520)|Reduce pom bloat for easier shim management|
|[#14639](https://github.com/NVIDIA/spark-rapids/pull/14639)|Add FLOAT/DECIMAL coverage for asinh/atanh/cbrt math functions (#14638)|
|[#14642](https://github.com/NVIDIA/spark-rapids/pull/14642)|Add DECIMAL value coverage for transform_values (#14641)|
|[#14672](https://github.com/NVIDIA/spark-rapids/pull/14672)|[AutoSparkUT] Add 7 all-pass Spark suites (batch: SelfJoin / WindowFrames / TimeWindow / SessionWindow / Stat / TypedImperativeAgg / DatasetAggregator)|
|[#14633](https://github.com/NVIDIA/spark-rapids/pull/14633)|Add FP corner-case coverage for stddev/variance aggregates (#14631)|
|[#14676](https://github.com/NVIDIA/spark-rapids/pull/14676)|Increase Databricks cluster create wait from 60 to 150 iterations [skip ci]|
|[#14640](https://github.com/NVIDIA/spark-rapids/pull/14640)|Fix buffer leak in KudoGpuTableOperator.concat under OOM|
|[#14593](https://github.com/NVIDIA/spark-rapids/pull/14593)|Allow combining of small files in iceberg parquet reader.|
|[#14636](https://github.com/NVIDIA/spark-rapids/pull/14636)|[AutoSparkUT] Add RapidsApproximatePercentileQuerySuite|
|[#14605](https://github.com/NVIDIA/spark-rapids/pull/14605)|Fix GpuArrayRemove to fallback for unsupported element types|
|[#14570](https://github.com/NVIDIA/spark-rapids/pull/14570)|Add aggregate reduction path coverage tests|
|[#14625](https://github.com/NVIDIA/spark-rapids/pull/14625)|Drop _V2 suffix from URM/Artifactory symbols [skip ci]|
|[#14580](https://github.com/NVIDIA/spark-rapids/pull/14580)|Add named accumulators to track PerfIO S3 backend usage per executor|
|[#14618](https://github.com/NVIDIA/spark-rapids/pull/14618)|Support binary type in higher-order functions|
|[#14617](https://github.com/NVIDIA/spark-rapids/pull/14617)|Fix BloomFilterAggregate buffer conversion on DB runtimes|
|[#14526](https://github.com/NVIDIA/spark-rapids/pull/14526)|[AutoSparkUT] Fix GpuMakeDecimal bitcast crash for low-precision decimals|
|[#14575](https://github.com/NVIDIA/spark-rapids/pull/14575)|Support Hex expression|
|[#14573](https://github.com/NVIDIA/spark-rapids/pull/14573)|Fix BloomFilterAggregate buffer conversion across CPU/GPU stages|
|[#14604](https://github.com/NVIDIA/spark-rapids/pull/14604)|Enable array_repeat test case|
|[#14528](https://github.com/NVIDIA/spark-rapids/pull/14528)|[AutoSparkUT] Fix legacy 2-level Parquet LIST schema evolution crash (issue #11454)|
|[#14527](https://github.com/NVIDIA/spark-rapids/pull/14527)|[AutoSparkUT] Add testRapids for parquet compression codec test (issue #11416)|
|[#14594](https://github.com/NVIDIA/spark-rapids/pull/14594)|Fix "Range is out of bounds" crash from GpuArraysZip when receiving a 0-row batch|
|[#14590](https://github.com/NVIDIA/spark-rapids/pull/14590)|Fix auto merge conflict 14589 [skip ci]|
|[#14587](https://github.com/NVIDIA/spark-rapids/pull/14587)|Append rishic3 to blossom-ci allowlist [skip ci]|
|[#14584](https://github.com/NVIDIA/spark-rapids/pull/14584)|Exclude array_repeat from auto unit tests while debugging|
|[#14458](https://github.com/NVIDIA/spark-rapids/pull/14458)|Add AI code review configuration and enhanced PR template [skip ci]|
|[#14550](https://github.com/NVIDIA/spark-rapids/pull/14550)|Explicit check boxes for non-applicable PR checklist items [skip ci]|
|[#14552](https://github.com/NVIDIA/spark-rapids/pull/14552)|Map snapshots-repo through URM for mirror profile|
|[#14507](https://github.com/NVIDIA/spark-rapids/pull/14507)|Fix MT read memory limit defaulting to wrong size when off-heap limit is disabled|
|[#14531](https://github.com/NVIDIA/spark-rapids/pull/14531)|Append patilkishorv to authorized user to blossom-ci whitelist[skip ci]|
|[#14517](https://github.com/NVIDIA/spark-rapids/pull/14517)|[CI] Use Artifactory v2 for URM and expand Maven settings|
|[#14529](https://github.com/NVIDIA/spark-rapids/pull/14529)|Fix batched window passthrough for mixed ROWS windows|
|[#14502](https://github.com/NVIDIA/spark-rapids/pull/14502)|[AutoSparkUT] Add yyyy-MM-dd HH:mm:ss.SSS to CORRECTED_COMPATIBLE_FORMATS (issue #13759)|
|[#14392](https://github.com/NVIDIA/spark-rapids/pull/14392)|[AutoSparkUT] Fix GpuCollectLimitExec per-partition row-level limits (issue #14109)|
|[#14440](https://github.com/NVIDIA/spark-rapids/pull/14440)|[AutoSparkUT] Propagate SQL query context to GPU arithmetic overflow exceptions (issue #14123)|
|[#14500](https://github.com/NVIDIA/spark-rapids/pull/14500)|Remove deprecated GpuTimeZoneDB cache overload usage|
|[#14496](https://github.com/NVIDIA/spark-rapids/pull/14496)|Update dependency version JNI, private, hybrid to 26.06.0-SNAPSHOT|
|[#14493](https://github.com/NVIDIA/spark-rapids/pull/14493)|Update shared actions to Node 24 for GitHub Actions Node 20 deprecation [skip ci]|
|[#14479](https://github.com/NVIDIA/spark-rapids/pull/14479)|[AutoSparkUT] Fix AM-PM timestamp parsing when hour field is missing (issue #13758)|
|[#14463](https://github.com/NVIDIA/spark-rapids/pull/14463)|Delay Parquet reader resource collection until close.|
|[#14453](https://github.com/NVIDIA/spark-rapids/pull/14453)|Add IcebergProvider$ to dist/unshimmed-from-each-spark3xx.txt to fix classloader issue.|
|[#14478](https://github.com/NVIDIA/spark-rapids/pull/14478)|Bump up version to 26.06|

## Release 26.04

### Features
|||
|:---|:---|
|[#14571](https://github.com/NVIDIA/spark-rapids/issues/14571)|[FEA] Drop support for the Databricks 12.2 runtime|
|[#12395](https://github.com/NVIDIA/spark-rapids/issues/12395)|[FEA] Update Delta Lake support to 3.3.0+|
|[#14429](https://github.com/NVIDIA/spark-rapids/issues/14429)|[FEA] Add iceberg 1.10.1 for spark 4.0|
|[#12071](https://github.com/NVIDIA/spark-rapids/issues/12071)|[FEA] Deletion vector read support with splits|
|[#13566](https://github.com/NVIDIA/spark-rapids/issues/13566)|[FEA] Enable Predicate Pushdown for MULTITHREADED reader in Delta Lake when dealing with Deletion Vectors|
|[#12576](https://github.com/NVIDIA/spark-rapids/issues/12576)|[FEA][DV] Implement Parquet+DV Scan using MULTIFILE* Parquet Reader|
|[#14238](https://github.com/NVIDIA/spark-rapids/issues/14238)|[FEA] Support read nested data type for iceberg.|
|[#13591](https://github.com/NVIDIA/spark-rapids/issues/13591)|[FEA] Support iceberg 1.10|
|[#14378](https://github.com/NVIDIA/spark-rapids/issues/14378)|[FEA] Support iceberg 1.9.2 in scala 2.12|
|[#14159](https://github.com/NVIDIA/spark-rapids/issues/14159)|[FEA] Add support for Apache Spark 3.5.8|
|[#14321](https://github.com/NVIDIA/spark-rapids/issues/14321)|[FEA] Support Spark 4.0.2|
|[#14276](https://github.com/NVIDIA/spark-rapids/issues/14276)|[FEA] Support BinaryType in GetArrayStructFields|

### Performance
|||
|:---|:---|
|[#14418](https://github.com/NVIDIA/spark-rapids/issues/14418)|Investigate performance of the DV-aware multi-threaded reader|
|[#14259](https://github.com/NVIDIA/spark-rapids/issues/14259)|Redundant filter handling for deletion vectors|

### Bugs Fixed
|||
|:---|:---|
|[#14704](https://github.com/NVIDIA/spark-rapids/issues/14704)|[BUG] `SpillablePartialHandle` should not increment the spilledToDisk counter if write is just shuffle write|
|[#14683](https://github.com/NVIDIA/spark-rapids/issues/14683)|[BUG] NDS-H SF1K single-node benchmark fails with java.io.IOException: No space left on device|
|[#14682](https://github.com/NVIDIA/spark-rapids/issues/14682)|[BUG] [DB-17.3 IT agaist v26.04.0] NoSuchMethodError on CatalogTable.copy in GpuCreateDataSourceTableAsSelectCommand|
|[#14607](https://github.com/NVIDIA/spark-rapids/issues/14607)|[BUG] Unity Catalog table scan fails with "Path must be absolute" on Databricks 17.3|
|[#14564](https://github.com/NVIDIA/spark-rapids/issues/14564)|[BUG] cuDF-based Delta DV reader returns incorrect results when NATIVE footer reader is used with split Parquet files|
|[#14168](https://github.com/NVIDIA/spark-rapids/issues/14168)|[BUG] `.agg(F.first()` brings inconsistency when using the CPU and GPU engines|
|[#14395](https://github.com/NVIDIA/spark-rapids/issues/14395)|[BUG] IllegalArgumentException: EmptyRelationExec not columnar in LeftAnti broadcast nested loop join with AQE|
|[#14511](https://github.com/NVIDIA/spark-rapids/issues/14511)|[BUG] test_delta_scan_split_with_DV_enabled_with_DVs failed assert 2 == 1|
|[#14368](https://github.com/NVIDIA/spark-rapids/issues/14368)|[BUG] Per-row retry in GpuRowToColumnarExec (#13842) may introduce noticeable overhead|
|[#14292](https://github.com/NVIDIA/spark-rapids/issues/14292)|[BUG] multithreaded read limit defaults to wrong size, ignores non-pinned off heap memory|
|[#14407](https://github.com/NVIDIA/spark-rapids/issues/14407)|[BUG] Iceberg integration tests fail with executor OOM (exit code 137) during GPU write operations|
|[#14512](https://github.com/NVIDIA/spark-rapids/issues/14512)|[BUG] UnsupportedClassVersionError for IcebergSparkSessionExtensions in spark-3.5.0 rocky9 cuda13 integration tests|
|[#14380](https://github.com/NVIDIA/spark-rapids/issues/14380)|[BUG] Intermittent failure on NDS trying to allocate host memory for shuffle|
|[#14536](https://github.com/NVIDIA/spark-rapids/issues/14536)|[BUG] CloseableTableSeqWithTargetSize.toString causes Java heap OOM during shuffle coalesce retry|
|[#14508](https://github.com/NVIDIA/spark-rapids/issues/14508)|[BUG] DB-17.3 Integration test pipeline hangs with grpc polling error|
|[#14487](https://github.com/NVIDIA/spark-rapids/issues/14487)|[BUG] Iceberg version detection may fail if a vendor build does not include properties behind IcebergBuild|
|[#13758](https://github.com/NVIDIA/spark-rapids/issues/13758)|[AutoSparkUT] AM-PM Timestamp Parsing Returns Wrong Hour When Hour Field is Missing|
|[#14452](https://github.com/NVIDIA/spark-rapids/issues/14452)|[BUG] spark 3.5.3 + iceberg 1.6.1 failed.|
|[#14472](https://github.com/NVIDIA/spark-rapids/issues/14472)|[BUG] Smoke test fails on Databricks-17.3|
|[#14474](https://github.com/NVIDIA/spark-rapids/issues/14474)|[BUG] Build failure: rapids-4-spark-iceberg-stub IcebergProviderImpl.scala references missing Spark classes in Databricks shim build|
|[#14462](https://github.com/NVIDIA/spark-rapids/issues/14462)|[BUG] Scala compilation fails: BloomFilter.create() deprecated method treated as fatal error in GpuBloomFilterAggregate|
|[#14110](https://github.com/NVIDIA/spark-rapids/issues/14110)|[AutoSparkUT]"SPARK-19650: An action on a Command should not trigger a Spark job" in SQLQuerySuite failed|
|[#14308](https://github.com/NVIDIA/spark-rapids/issues/14308)|[BUG][DBR 17.3] Integration test failures in join_test.py|
|[#14313](https://github.com/NVIDIA/spark-rapids/issues/14313)|[BUG][DBR 17.3] Integration test failures in parquet_test.py|
|[#14314](https://github.com/NVIDIA/spark-rapids/issues/14314)|[BUG][DBR 17.3] Integration test failures in parquet_write_test.py|
|[#14301](https://github.com/NVIDIA/spark-rapids/issues/14301)|[BUG] [DBR 17.3] Integration test failures in date_time_test.py|
|[#14309](https://github.com/NVIDIA/spark-rapids/issues/14309)|[BUG][DBR 17.3] Integration test failures in json_matrix_test.py|
|[#14310](https://github.com/NVIDIA/spark-rapids/issues/14310)|[BUG][DBR 17.3] Integration test failures in json_test.py|
|[#14312](https://github.com/NVIDIA/spark-rapids/issues/14312)|[BUG][DBR 17.3] Integration test failures in orc_write_test.py|
|[#14098](https://github.com/NVIDIA/spark-rapids/issues/14098)|[AutoSparkUT]"SPARK-6743: no columns from cache" in SQLQuerySuite failed|
|[#14116](https://github.com/NVIDIA/spark-rapids/issues/14116)|[AutoSparkUT]"normalize special floating numbers in subquery" in SQLQuerySuite failed|
|[#14286](https://github.com/NVIDIA/spark-rapids/issues/14286)|[BUG] Gap between C2R and R2C stream time|
|[#14442](https://github.com/NVIDIA/spark-rapids/issues/14442)|[BUG] java.lang.RuntimeException: Couldn't find _metadata thrown while running a delete command for Delta Lake tables|
|[#14449](https://github.com/NVIDIA/spark-rapids/issues/14449)|UCX shuffle mode broken on Spark 4.x: shuffle heartbeat manager not initialized due to SparkEnv.get returning null during DriverPlugin.init|
|[#14067](https://github.com/NVIDIA/spark-rapids/issues/14067)|[FEA] Agg push down not working for iceberg.|
|[#13757](https://github.com/NVIDIA/spark-rapids/issues/13757)|[AutoSparkUT] Trunc/Date_Trunc - Invalid Format String Handling|
|[#14425](https://github.com/NVIDIA/spark-rapids/issues/14425)|[BUG] Error: Shim 3xx listed in dyn.shim.excluded.releases in pom.xml not present in releases|
|[#14318](https://github.com/NVIDIA/spark-rapids/issues/14318)|[BUG][DBR 17.3] Integration test failures in udf_test.py|
|[#14304](https://github.com/NVIDIA/spark-rapids/issues/14304)|[BUG] [DBR 17.3] Integration test failures in explain_test.py|
|[#14307](https://github.com/NVIDIA/spark-rapids/issues/14307)|[BUG][DBR 17.3] Integration test failures in hive_delimited_text_test.py|
|[#14315](https://github.com/NVIDIA/spark-rapids/issues/14315)|[BUG][DBR 17.3] Integration test failures in prune_partition_column_test.py|
|[#14316](https://github.com/NVIDIA/spark-rapids/issues/14316)|[BUG][DBR 17.3] Integration test failures in schema_evolution_test.py|
|[#14300](https://github.com/NVIDIA/spark-rapids/issues/14300)|[BUG] [DBR 17.3] Integration test failures in csv_test.py|
|[#14117](https://github.com/NVIDIA/spark-rapids/issues/14117)|[AutoSparkUT]"SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar" in SQLQuerySuite failed|
|[#14433](https://github.com/NVIDIA/spark-rapids/issues/14433)|[BUG] Multi-threaded reader for Delta Integration tests failing in premerge-CI|
|[#14317](https://github.com/NVIDIA/spark-rapids/issues/14317)|[BUG][DBR 17.3] Integration test failures in string_type_test.py|
|[#14302](https://github.com/NVIDIA/spark-rapids/issues/14302)|[BUG] [DBR 17.3] Integration test failures in dpp_test.py|
|[#14408](https://github.com/NVIDIA/spark-rapids/issues/14408)|[BUG] Iceberg integration tests fail with NoSuchMethodError: ContentFile.location() incompatible with Iceberg 1.6.1|
|[#14422](https://github.com/NVIDIA/spark-rapids/issues/14422)|[BUG] Fix FileScan canonicalization case FAILED: computeStats called before pushdown on DSv2 relation|
|[#14306](https://github.com/NVIDIA/spark-rapids/issues/14306)|[BUG][DBR 17.3] Integration test failures in hash_aggregate_test.py|
|[#14410](https://github.com/NVIDIA/spark-rapids/issues/14410)|[BUG] spark-rapids build is broken after iceberg 1.9.2 support change|
|[#14311](https://github.com/NVIDIA/spark-rapids/issues/14311)|[BUG][DBR 17.3] Integration test failures in map_test.py|
|[#14305](https://github.com/NVIDIA/spark-rapids/issues/14305)|[BUG][DBR 17.3] Integration test failures in generate_expr_test.py|
|[#14299](https://github.com/NVIDIA/spark-rapids/issues/14299)|[BUG][DBR 17.3] Integration test failures in array_test.py|
|[#14413](https://github.com/NVIDIA/spark-rapids/issues/14413)|[BUG] 17.3 runtime (scala2.13) deploy failed due to incorrect artifacts path|
|[#14365](https://github.com/NVIDIA/spark-rapids/issues/14365)|[BUG] Iceberg append test fails with IllegalArgumentException: Part of the plan is not columnar ShuffleExchangeExec on bucket-partitioned table|
|[#14361](https://github.com/NVIDIA/spark-rapids/issues/14361)|[BUG] many cases of sort_test and join_test fails unexpected GPU plans in spark 400+ w/ AQE enabled|
|[#14155](https://github.com/NVIDIA/spark-rapids/issues/14155)|[AutoSparkUT]"SPARK-34868: divide year-month interval by numeric" in ColumnExpressionSuite failed|
|[#14158](https://github.com/NVIDIA/spark-rapids/issues/14158)|[AutoSparkUT]"runtime error when the number of rows is greater than 1" in SubquerySuite failed|
|[#14154](https://github.com/NVIDIA/spark-rapids/issues/14154)|[AutoSparkUT]"assert_true" in ColumnExpressionSuite failed|
|[#14253](https://github.com/NVIDIA/spark-rapids/issues/14253)|[BUG] FileCache layer should also use RapidFileIO to access remote storage.|
|[#14355](https://github.com/NVIDIA/spark-rapids/issues/14355)|[BUG] cudf_udf tests test_group_agg/test_sql_group/test_window fail with IllegalArgumentException: Part of the plan is not columnar ShuffleExchangeExec (Spark 3.5.0)|
|[#14225](https://github.com/NVIDIA/spark-rapids/issues/14225)|[BUG] Some integration tests FAILED when AQE is on|
|[#14194](https://github.com/NVIDIA/spark-rapids/issues/14194)|[BUG] StackOverflowError MultiFileCloudPartitionReaderBase.readBuffersToBatch|

### PRs
|||
|:---|:---|
|[#14649](https://github.com/NVIDIA/spark-rapids/pull/14649)|[DOC] add missing release notes to 26.04 download page [skip ci]|
|[#14768](https://github.com/NVIDIA/spark-rapids/pull/14768)|Update changelog for the v26.04.2 release [skip ci]|
|[#14769](https://github.com/NVIDIA/spark-rapids/pull/14769)|[DOC] update download page for 26.04.2 [skip ci]|
|[#14714](https://github.com/NVIDIA/spark-rapids/pull/14714)|Shuffle bytes double count metric fix + tests to cover shuffle removal at unregister|
|[#14697](https://github.com/NVIDIA/spark-rapids/pull/14697)|Update changelog for the v26.04.1 release [skip ci]|
|[#14686](https://github.com/NVIDIA/spark-rapids/pull/14686)|[BUG] Fix unregister/remove path for wrapped shuffle resolver|
|[#14650](https://github.com/NVIDIA/spark-rapids/pull/14650)|[DOC] Fix configs.md example: drop -SNAPSHOT jar name and swap conGPUTask conf [skip ci]|
|[#14629](https://github.com/NVIDIA/spark-rapids/pull/14629)|[DOC] call out know issue 14574 in download page [skip ci]|
|[#14621](https://github.com/NVIDIA/spark-rapids/pull/14621)|Update changelog for the v26.04 release [skip ci]|
|[#14602](https://github.com/NVIDIA/spark-rapids/pull/14602)|Backport settings artifactory [skip ci]|
|[#14590](https://github.com/NVIDIA/spark-rapids/pull/14590)|Fix auto merge conflict 14589 [skip ci]|
|[#14569](https://github.com/NVIDIA/spark-rapids/pull/14569)|Update changelog for the v26.04 release [skip ci]|
|[#14585](https://github.com/NVIDIA/spark-rapids/pull/14585)|Update dependency version JNI, private, hybrid to 26.04.0|
|[#14565](https://github.com/NVIDIA/spark-rapids/pull/14565)|[DOC] Update download and archive pages for v26.04.0 release [skip ci]|
|[#14555](https://github.com/NVIDIA/spark-rapids/pull/14555)|Fix GPU first reduction state tracking|
|[#14562](https://github.com/NVIDIA/spark-rapids/pull/14562)|Disable AQE for Delta merge tests and delete test with Spark 4.0+|
|[#14549](https://github.com/NVIDIA/spark-rapids/pull/14549)|Fix splitability tests for the Delta scan with deletion vectors|
|[#14428](https://github.com/NVIDIA/spark-rapids/pull/14428)|Use per-batch retry block in R2C with lightweight OOM recovery|
|[#14476](https://github.com/NVIDIA/spark-rapids/pull/14476)|[DOC] update cuda13 related jars in download page [skip ci]|
|[#14537](https://github.com/NVIDIA/spark-rapids/pull/14537)|fix bounded retry logging for shuffle coalesce|
|[#14525](https://github.com/NVIDIA/spark-rapids/pull/14525)|Fix oom by correctly setting fanout|
|[#14518](https://github.com/NVIDIA/spark-rapids/pull/14518)|Enabled Databricks 17.3 tests and removed support for Databricks 12.2|
|[#14533](https://github.com/NVIDIA/spark-rapids/pull/14533)|Fix batched window passthrough for mixed ROWS windows|
|[#14509](https://github.com/NVIDIA/spark-rapids/pull/14509)|Fix integration test process hang on DB-17.3|
|[#14513](https://github.com/NVIDIA/spark-rapids/pull/14513)|Fix thread-safety issue in RapidsShuffleInternalManagerBase.unregisterShuffle|
|[#14488](https://github.com/NVIDIA/spark-rapids/pull/14488)|Enhance IcebergProbeImpl with jar source logging and version fallback|
|[#14515](https://github.com/NVIDIA/spark-rapids/pull/14515)|Fix ci problem caused by iceberg detect version.|
|[#14459](https://github.com/NVIDIA/spark-rapids/pull/14459)|Add support for iceberg 1.10.1 + spark 4.0.2|
|[#14406](https://github.com/NVIDIA/spark-rapids/pull/14406)|BloomFilter v2 support|
|[#14490](https://github.com/NVIDIA/spark-rapids/pull/14490)|Delay Parquet reader resource collection until close.|
|[#14495](https://github.com/NVIDIA/spark-rapids/pull/14495)|[AutoSparkUT] Fix AM-PM timestamp parsing when hour field is missing (issue #13758) [backport release/26.04]|
|[#14494](https://github.com/NVIDIA/spark-rapids/pull/14494)|Add IcebergProvider$ to dist/unshimmed-from-each-spark3xx.txt to fix classloader issue.|
|[#14489](https://github.com/NVIDIA/spark-rapids/pull/14489)|Skip delta lake tests for DB-17.3|
|[#14492](https://github.com/NVIDIA/spark-rapids/pull/14492)|Databricks run-tests: rsync test reports from scala2.13 for Spark 4.x|
|[#14464](https://github.com/NVIDIA/spark-rapids/pull/14464)|Fix oom of iceberg ci|
|[#14475](https://github.com/NVIDIA/spark-rapids/pull/14475)|Fix databricks build break caused by `NoIcebergProviderImpl`|
|[#14466](https://github.com/NVIDIA/spark-rapids/pull/14466)|Use long instead of int to track kudo buffer sizes and offsets in GpuShuffleCoalesce|
|[#14467](https://github.com/NVIDIA/spark-rapids/pull/14467)|Update doc for deletionVectors.predicatePushdown.enabled to note useMetadataRowIndex dependency [skip ci]|
|[#14468](https://github.com/NVIDIA/spark-rapids/pull/14468)|Fix compile break from BloomFilter.create deprecation|
|[#14460](https://github.com/NVIDIA/spark-rapids/pull/14460)|Enable the new DV-aware Delta reader by default|
|[#14398](https://github.com/NVIDIA/spark-rapids/pull/14398)|[AutoSparkUT] Fix CollectLimitExec GPU replacement for Command results (SPARK-19650)|
|[#14456](https://github.com/NVIDIA/spark-rapids/pull/14456)|Fix DB-17.3 integration tests in join_test.py|
|[#14435](https://github.com/NVIDIA/spark-rapids/pull/14435)|Add support for Coalesce reader for Delta Lake using cuDF deletion vector APIs|
|[#14270](https://github.com/NVIDIA/spark-rapids/pull/14270)|Add read support for nested data types in iceberg.|
|[#14400](https://github.com/NVIDIA/spark-rapids/pull/14400)|[AutoSparkUT] Re-enable 'normalize special floating numbers in subquery' test (issue #14116)|
|[#14450](https://github.com/NVIDIA/spark-rapids/pull/14450)|Fix `_metadata` column incorrectly pruned when nested fields are referenced|
|[#14448](https://github.com/NVIDIA/spark-rapids/pull/14448)|check rapids shuffle configured instead of available before heartbeat init|
|[#14436](https://github.com/NVIDIA/spark-rapids/pull/14436)|[AutoSparkUT] Fix trunc/date_trunc to return null for invalid format strings|
|[#14437](https://github.com/NVIDIA/spark-rapids/pull/14437)|Improve pytest-xdist load balancing by leveraging pytest-xdist worksteal mode|
|[#14426](https://github.com/NVIDIA/spark-rapids/pull/14426)|Fix dyn.shim.excluded.releases validation for scala2.13 POM|
|[#14427](https://github.com/NVIDIA/spark-rapids/pull/14427)|Fix DB-17.3 integration tests in udf_test.py|
|[#14421](https://github.com/NVIDIA/spark-rapids/pull/14421)|Fix integration tests caused due to file format detection and DataSource lookup for DB 17.3|
|[#14388](https://github.com/NVIDIA/spark-rapids/pull/14388)|[AutoSparkUT] Fix LIKE with invalid escape pattern to match CPU behavior|
|[#14434](https://github.com/NVIDIA/spark-rapids/pull/14434)|Skip test_delta_deletion_vector_multithreaded_combine_count_star on Databricks|
|[#14417](https://github.com/NVIDIA/spark-rapids/pull/14417)|Fix DB-17.3 integration test failure in string_type_test|
|[#14431](https://github.com/NVIDIA/spark-rapids/pull/14431)|Fix DPP test failures on DBR 17.3|
|[#14382](https://github.com/NVIDIA/spark-rapids/pull/14382)|Add support for multi-threaded reader for Delta using cuDF deletion vector APIs|
|[#14424](https://github.com/NVIDIA/spark-rapids/pull/14424)|[AutoSparkUT] Fix SPARK-33482 testRapids: use V1 source to avoid DSv2 computeStats guard|
|[#14401](https://github.com/NVIDIA/spark-rapids/pull/14401)|Support iceberg 1.10.1|
|[#14367](https://github.com/NVIDIA/spark-rapids/pull/14367)|[AutoSparkUT] Recover TIMESTAMP_MICROS test (issue #13760)|
|[#14376](https://github.com/NVIDIA/spark-rapids/pull/14376)|[AutoSparkUT] Add testRapids for SPARK-33482 FileScan canonicalization|
|[#14416](https://github.com/NVIDIA/spark-rapids/pull/14416)|Databricks shims for both Scala 2.12 and 2.13|
|[#14411](https://github.com/NVIDIA/spark-rapids/pull/14411)|Update pom and docs for jdk 17 switch|
|[#14399](https://github.com/NVIDIA/spark-rapids/pull/14399)|[AutoSparkUT] Reclassify ParquetEncodingSuite mixed encoding exclusion as ADJUST_UT|
|[#14414](https://github.com/NVIDIA/spark-rapids/pull/14414)|Deploy the Databricks 17.3 artifacts in the scala2.13/ directory [skip ci]|
|[#14360](https://github.com/NVIDIA/spark-rapids/pull/14360)|Add Databricks-17.3 support|
|[#14402](https://github.com/NVIDIA/spark-rapids/pull/14402)|[doc] Revert "Update index.md to change the name to Apache Spark Accelerated With cuDF"[skip ci]|
|[#14390](https://github.com/NVIDIA/spark-rapids/pull/14390)|Support iceberg 1.9.2 for scala 2.12|
|[#14396](https://github.com/NVIDIA/spark-rapids/pull/14396)|Add driver-compatible-relink-step for pre-merge CI|
|[#14393](https://github.com/NVIDIA/spark-rapids/pull/14393)|[doc]Update index.md to change the name to "Apache Spark Accelerated With cuDF"[skip ci]|
|[#14391](https://github.com/NVIDIA/spark-rapids/pull/14391)|Increase shallow clone depth of pre-merge CI|
|[#14379](https://github.com/NVIDIA/spark-rapids/pull/14379)|[AutoSparkUT] Forward GPU memory allocation properties to forked test JVM|
|[#14387](https://github.com/NVIDIA/spark-rapids/pull/14387)|Update docs for the v26.02.1 release [skip ci]|
|[#14349](https://github.com/NVIDIA/spark-rapids/pull/14349)|[AutoSparkUT] spark330: add SPARK-34212 Parquet decimal test and align ADJUST_UT test name keys|
|[#14366](https://github.com/NVIDIA/spark-rapids/pull/14366)|Disable AQE for more join and sort tests as they are failing with spark 400+|
|[#14369](https://github.com/NVIDIA/spark-rapids/pull/14369)|[AutoSparkUT] Make GPU memory allocation configurable for parallel UT execution|
|[#14374](https://github.com/NVIDIA/spark-rapids/pull/14374)|Add missing shim for Spark 358 for GpuParquetUtilsShims|
|[#14363](https://github.com/NVIDIA/spark-rapids/pull/14363)|[AutoSparkUT] Fix GpuDivideYMInterval to throw correct divide-by-zero exception|
|[#14347](https://github.com/NVIDIA/spark-rapids/pull/14347)|Add support for PERFILE reader using cuDF deletion vector APIs|
|[#14359](https://github.com/NVIDIA/spark-rapids/pull/14359)|Add support for Spark-3.5.8|
|[#14356](https://github.com/NVIDIA/spark-rapids/pull/14356)|[AutoSparkUT] Fix GpuScalarSubquery to throw IllegalStateException matching Spark|
|[#14364](https://github.com/NVIDIA/spark-rapids/pull/14364)|[AutoSparkUT] Fix GpuIf null predicate handling for side-effect expressions|
|[#14342](https://github.com/NVIDIA/spark-rapids/pull/14342)|Use `RapidsFileIO` for FileCache.|
|[#14357](https://github.com/NVIDIA/spark-rapids/pull/14357)|Temporarily disable AQE for some udf tests|
|[#14282](https://github.com/NVIDIA/spark-rapids/pull/14282)|Fix wrong config name in shuffle checksum fallback check|
|[#14345](https://github.com/NVIDIA/spark-rapids/pull/14345)|[DOC] Update shims.md to refer to Spark 3.3.0+ and 4.x[skip ci]|
|[#14344](https://github.com/NVIDIA/spark-rapids/pull/14344)|[Doc]Put design and shuffle-metrics docs under Developer Overview in nav in main branch[skip ci]|
|[#14323](https://github.com/NVIDIA/spark-rapids/pull/14323)|Enable AQE on majority of integration test cases|
|[#14332](https://github.com/NVIDIA/spark-rapids/pull/14332)|Support `Iterator[pandas.DataFrame]` in `GpuGroupedMapPandasUDF`|
|[#14334](https://github.com/NVIDIA/spark-rapids/pull/14334)|Add support for Spark-4.0.2|
|[#14329](https://github.com/NVIDIA/spark-rapids/pull/14329)|Refactor JSON path depth handling and update hash stack depth references|
|[#14330](https://github.com/NVIDIA/spark-rapids/pull/14330)|Remove sources for desupported shims 320, 321cdh, 322, 323, 324, 330cdh, 332cdh|
|[#14288](https://github.com/NVIDIA/spark-rapids/pull/14288)|Use dynamic shared buffer size in KudoSerializedBatchIterator to reduce memory waste|
|[#14298](https://github.com/NVIDIA/spark-rapids/pull/14298)|xfail test_get_json_object_quoted_question on Dataproc|
|[#14260](https://github.com/NVIDIA/spark-rapids/pull/14260)|Predicate pushdown for deletion vectors|
|[#14287](https://github.com/NVIDIA/spark-rapids/pull/14287)|Exclude PythonUDF cases in JoinSuite|
|[#14281](https://github.com/NVIDIA/spark-rapids/pull/14281)|Filter out blank lines when reading CSV|
|[#14284](https://github.com/NVIDIA/spark-rapids/pull/14284)|Remove the cycle between next() and readBuffersToBatch() in MultiFileCloudPartitionReaderBase|
|[#14280](https://github.com/NVIDIA/spark-rapids/pull/14280)|Exclude pythonUDF cases in SubquerySuite|
|[#14277](https://github.com/NVIDIA/spark-rapids/pull/14277)|Support BinaryType in GetArrayStructFields|
|[#14266](https://github.com/NVIDIA/spark-rapids/pull/14266)|Fetch the PRs for each commit in parallel to generate the CHANGELOG [skip ci]|
|[#14275](https://github.com/NVIDIA/spark-rapids/pull/14275)|Fix auto merge conflict 14274 [skip ci]|
|[#14237](https://github.com/NVIDIA/spark-rapids/pull/14237)|Shrink size of integration images and provide dedicated environment for cudf-udf cases [skip ci]|
|[#14173](https://github.com/NVIDIA/spark-rapids/pull/14173)|[AutoSparkUT] Add 3 Spark UT suites|
|[#14220](https://github.com/NVIDIA/spark-rapids/pull/14220)|resubmit shuffle manager v2 phase 2|
|[#14219](https://github.com/NVIDIA/spark-rapids/pull/14219)|Revert "Rapids shuffle manager V2 phase 2" [skip ci]|
|[#14090](https://github.com/NVIDIA/spark-rapids/pull/14090)|Rapids shuffle manager V2 phase 2 : skips the partial file merge if possible|
|[#14205](https://github.com/NVIDIA/spark-rapids/pull/14205)|[follow-up][test]add test for BroadcastExchangeExec should not reset metrics|
|[#14147](https://github.com/NVIDIA/spark-rapids/pull/14147)|Change calls to cuDF partitioning apis to account for the numRows offset|
|[#14162](https://github.com/NVIDIA/spark-rapids/pull/14162)|Avoid doing a redundant Final WindowGroupLimit|
|[#14200](https://github.com/NVIDIA/spark-rapids/pull/14200)|Update dependency version JNI, private, hybrid to 26.04.0-SNAPSHOT|
|[#14199](https://github.com/NVIDIA/spark-rapids/pull/14199)|Bump up version to 26.04 [skip ci]|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
