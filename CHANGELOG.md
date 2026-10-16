# Change log
Generated on 2026-10-09

## Release 26.10

### Features
|||
|:---|:---|
|[#15664](https://github.com/NVIDIA/cudf-spark/issues/15664)|[FEA] Delta 4.2 support|
|[#8415](https://github.com/NVIDIA/cudf-spark/issues/8415)|[FEA] Support GpuMergeIntoCommand notMatchedBySourceClauses on GPU for Databricks|
|[#14757](https://github.com/NVIDIA/cudf-spark/issues/14757)|[FEA] Support Delta Lake 4.2 release|
|[#15442](https://github.com/NVIDIA/cudf-spark/issues/15442)|[FEA] Iceberg v3: write and merge Puffin deletion vectors|
|[#12577](https://github.com/NVIDIA/cudf-spark/issues/12577)|[FEA][DV] Accelerate DML writes to DV-enabled tables|
|[#11169](https://github.com/NVIDIA/cudf-spark/issues/11169)|Delta Lake compaction writes can fallback due to WriteIntoDeltaCommand|
|[#15665](https://github.com/NVIDIA/cudf-spark/issues/15665)|Add Delta 4.2 provider and runtime|
|[#13930](https://github.com/NVIDIA/cudf-spark/issues/13930)|[FEA] Support AutoOptimizedShuffle for Databricks|
|[#15367](https://github.com/NVIDIA/cudf-spark/issues/15367)|[FEA] Support Delta CDF read|
|[#15438](https://github.com/NVIDIA/cudf-spark/issues/15438)|[FEA] Iceberg v3: support row-lineage reads on GPU|
|[#15440](https://github.com/NVIDIA/cudf-spark/issues/15440)|[FEA] Iceberg v3: read Puffin deletion vectors on GPU|
|[#13074](https://github.com/NVIDIA/cudf-spark/issues/13074)|[FEA] Support noop format|
|[#14521](https://github.com/NVIDIA/cudf-spark/issues/14521)|[FEA] Add support for Delta REORG TABLE command|
|[#14947](https://github.com/NVIDIA/cudf-spark/issues/14947)|[FEA] Support inline flag groups `(?i)` for case-insensitive matches in `regexp_like` / `regexp_extract` / `regexp_replace`|
|[#15305](https://github.com/NVIDIA/cudf-spark/issues/15305)|[FEA] Support InMemoryTableScanExec with nested NullType output|
|[#8626](https://github.com/NVIDIA/cudf-spark/issues/8626)|[FEA] Support ntile|
|[#15349](https://github.com/NVIDIA/cudf-spark/issues/15349)|[AI-AUDIT][SPARK-52689][SPARK-53891][SQL] Pass DSv2 write summaries from GPU row-level DML commits|
|[#15306](https://github.com/NVIDIA/cudf-spark/issues/15306)|[FEA] Support quantified \D in regexp_extract|

### Performance
|||
|:---|:---|
|[#10413](https://github.com/NVIDIA/cudf-spark/issues/10413)|Cache broadcast hash tables|
|[#12327](https://github.com/NVIDIA/cudf-spark/issues/12327)|[FEA] Reuse cuDF generated hash map for the build side keys when doing join|
|[#15444](https://github.com/NVIDIA/cudf-spark/issues/15444)|Slow Delta write with auto optimize|
|[#15798](https://github.com/NVIDIA/cudf-spark/issues/15798)|[FEA] Reuse sorted GroupPartitionsExec input in GPU external merge|
|[#15602](https://github.com/NVIDIA/cudf-spark/issues/15602)|[FEA] Support GroupPartitionsExec on GPU|
|[#15681](https://github.com/NVIDIA/cudf-spark/issues/15681)|[FEA] Consider defaulting spark.rapids.memory.pinnedPool.parallelInit.threads to "all"|
|[#14055](https://github.com/NVIDIA/cudf-spark/issues/14055)|[FEA] Upgrade to UCX 1.22|

### Bugs Fixed
|||
|:---|:---|
|[#16166](https://github.com/NVIDIA/cudf-spark/issues/16166)|[BUG] Delta replaceWhere saveAsTable test fails on CPU with Spark 3.5.x|
|[#16208](https://github.com/NVIDIA/cudf-spark/issues/16208)|[BUG] try_to_timestamp throws on invalid input on GPU with ANSI mode enabled|
|[#16164](https://github.com/NVIDIA/cudf-spark/issues/16164)|[BUG] ParquetCachedBatchSerializer overflows Int sizes and segfaults when a cached partition exceeds 2 GiB|
|[#15430](https://github.com/NVIDIA/cudf-spark/issues/15430)|[BUG] DB14.3 IT: NoClassDefFoundError com.databricks.sql.SupportsLineage in GpuInsertIntoHiveTableMeta.convertToGpu|
|[#16142](https://github.com/NVIDIA/cudf-spark/issues/16142)|[BUG] UCX shuffle under the ASYNC pool reserves 8 MiB for 256 MiB of bounce buffers allocated outside RMM|
|[#16179](https://github.com/NVIDIA/cudf-spark/issues/16179)|[BUG] RapidsDatasetSuite dropDuplicates tests are flaky|
|[#16128](https://github.com/NVIDIA/cudf-spark/issues/16128)|[BUG] Native Parquet footer reader drops empty key-value metadata values|
|[#16158](https://github.com/NVIDIA/cudf-spark/issues/16158)|[BUG] CPU-to-GPU transitions fail for Spark VariantType columns|
|[#16116](https://github.com/NVIDIA/cudf-spark/issues/16116)|[BUG] Delta Lake MERGE/DELETE GPU plan failures across Spark versions|
|[#16074](https://github.com/NVIDIA/cudf-spark/issues/16074)|[P0][BUG] Scope failed caching-writer cleanup to the failed map output|
|[#16138](https://github.com/NVIDIA/cudf-spark/issues/16138)|[BUG] UCX shuffle configs managementServerHost and managementConnectionTimeout are never read|
|[#16026](https://github.com/NVIDIA/cudf-spark/issues/16026)|[BUG] Rows-only batches crash GpuAnd/GpuOr, GpuIf and GpuCaseWhen|
|[#16070](https://github.com/NVIDIA/cudf-spark/issues/16070)|[BUG] GpuColumnVector.filter depends on an undocumented invalid mask-sum payload|
|[#16018](https://github.com/NVIDIA/cudf-spark/issues/16018)|[BUG] Skip-merge batch fetch can return partial data during cleanup|
|[#16069](https://github.com/NVIDIA/cudf-spark/issues/16069)|[BUG] withResource nesting audit overwrites a source file|
|[#14734](https://github.com/NVIDIA/cudf-spark/issues/14734)|[BUG] containsNewline misses hex/octal newline escapes; line-anchor check is bypassed for `\\x0A$` and friends|
|[#16067](https://github.com/NVIDIA/cudf-spark/issues/16067)|[BUG] Conditional right outer join fails with non-boolean AST|
|[#16100](https://github.com/NVIDIA/cudf-spark/issues/16100)|[BUG] SparkRapidsBuildInfoEvent is not root-safe across runtime families|
|[#16058](https://github.com/NVIDIA/cudf-spark/issues/16058)|[BUG] Delta Lake IT test_delta_replace_where_save_as_table_preserves_partitioning fails: GpuAppendDataExecV1 was not executed (Spark 3.3.0)|
|[#16025](https://github.com/NVIDIA/cudf-spark/issues/16025)|[BUG] Rows-only batches crash RmmRapidsRetryIterator.splitSpillableInHalfByRows|
|[#16038](https://github.com/NVIDIA/cudf-spark/issues/16038)|[BUG] Non-UTC integration tests fail: GpuCpuBridge disallows VariantGet for try_variant_get (variant_test.py, TZ=Asia/Shanghai)|
|[#8403](https://github.com/NVIDIA/cudf-spark/issues/8403)|[BUG] distributed CI failed in `test_read_case_col_name`|
|[#15981](https://github.com/NVIDIA/cudf-spark/issues/15981)|[BUG] UCX shuffle sender can terminate after GPU OOM while materializing spilled buffers|
|[#14221](https://github.com/NVIDIA/cudf-spark/issues/14221)|Causes deadlock when executing nested subqueries with AQE|
|[#15926](https://github.com/NVIDIA/cudf-spark/issues/15926)|[BUG] GPU Delta replaceWhere via saveAsTable drops partition values for untouched rows|
|[#15808](https://github.com/NVIDIA/cudf-spark/issues/15808)|[BUG] Fail closed when positional aggregate result type mapping is incomplete|
|[#16002](https://github.com/NVIDIA/cudf-spark/issues/16002)|[BUG] Iceberg 1.10.1 row-lineage DELETE test fails on Spark 3.5.9 across all catalogs|
|[#15951](https://github.com/NVIDIA/cudf-spark/issues/15951)|[BUG] Non-UTC IT (Spark 3.4.0, Asia/Shanghai): ORC timestamp write tests fail - WriteFilesExec not replaced by GpuWriteFilesExec|
|[#15903](https://github.com/NVIDIA/cudf-spark/issues/15903)|[BUG] GPU range-boundary sampling scans full wide rows instead of only range keys|
|[#16013](https://github.com/NVIDIA/cudf-spark/issues/16013)|[BUG] Nightly distribution build fails: missing Databricks spark350db143 artifact rapids-4-spark-sql-plugin-columnar in Artifactory|
|[#16004](https://github.com/NVIDIA/cudf-spark/issues/16004)|[BUG] Premerge Delta compilation fails with 26.10 JNI snapshot missing batched deletion-vector API|
|[#15975](https://github.com/NVIDIA/cudf-spark/issues/15975)|[BUG] GPU range-partition boundary sampling crashes on rows-only (zero-column) batches|
|[#15953](https://github.com/NVIDIA/cudf-spark/issues/15953)|[BUG] zero-rows (rows-only) aggregates can cause an error or corruption|
|[#15970](https://github.com/NVIDIA/cudf-spark/issues/15970)|[BUG] Iceberg REST catalog IT: test_iceberg_v3_deletion_vector fails with S3 'Timeout waiting for connection from pool'|
|[#15680](https://github.com/NVIDIA/cudf-spark/issues/15680)|[BUG] Iceberg V3 copy-on-write DELETE fallback test fails on CPU baseline with PLAN_VALIDATION_FAILED_RULE_IN_BATCH (Spark 3.5.8 + Iceberg 1.10.1)|
|[#15950](https://github.com/NVIDIA/cudf-spark/issues/15950)|[BUG] Iceberg V3 row-lineage copy-on-write DELETE fails Spark plan validation (PLAN_VALIDATION_FAILED_RULE_IN_BATCH) on Spark 3.5.7 + Iceberg 1.10.1|
|[#10641](https://github.com/NVIDIA/cudf-spark/issues/10641)|[BUG] test_regexp_choice failed|
|[#15952](https://github.com/NVIDIA/cudf-spark/issues/15952)|[BUG] Spark 4 variant_test.py fallback tests fail: GPU Parquet reader cannot read VariantType columns|
|[#15839](https://github.com/NVIDIA/cudf-spark/issues/15839)|[BUG] SPJ: filteredPartitions rejects valid partitions [SPARK-58783]|
|[#8042](https://github.com/NVIDIA/cudf-spark/issues/8042)|[BUG] Filter not on GPU because of <IncrementMetric> true|
|[#14928](https://github.com/NVIDIA/cudf-spark/issues/14928)|[BUG] Mortgage test fails in proprietary Spark distribution PullUpUnion AQE rule|
|[#140](https://github.com/NVIDIA/cudf-spark/issues/140)|[BUG] Orc writer wrong for timestamps prior to 1970|
|[#15910](https://github.com/NVIDIA/cudf-spark/issues/15910)|[BUG] Spark master IT (spark500): spark-shell smoke test fails with NoClassDefFoundError org/apache/spark/shuffle/RapidsShuffleManagerBase|
|[#15919](https://github.com/NVIDIA/cudf-spark/issues/15919)|[BUG] Rare Iceberg S3 reads can stall for about 60 seconds without retrying|
|[#11453](https://github.com/NVIDIA/cudf-spark/issues/11453)|[FEA][AUDIT][SPARK-48949][SQL] SPJ: Runtime partition filtering|
|[#15747](https://github.com/NVIDIA/cudf-spark/issues/15747)|[BUG] Nightly dependency-check fails with HTTP 429 (Too Many Requests) downloading h2-2.1.210.pom from Maven Central|
|[#15909](https://github.com/NVIDIA/cudf-spark/issues/15909)|[BUG] Spark master build fails: value displayName is not a member of KeyReducer in GpuGroupPartitionsExec (spark500 shim)|
|[#15816](https://github.com/NVIDIA/cudf-spark/issues/15816)|[BUG] Spark 4 DataSourceRDD overwrites input bytes reported by GPU multithreaded scans|
|[#15611](https://github.com/NVIDIA/cudf-spark/issues/15611)|[BUG] Multithreaded shuffle writer can hang when compression failure strands bytes-in-flight quota|
|[#15812](https://github.com/NVIDIA/cudf-spark/issues/15812)|[BUG] Spark500 shim UT fails in BridgeHostColumnProjectionSuite as unmatched references|
|[#15687](https://github.com/NVIDIA/cudf-spark/issues/15687)|[BUG] Iceberg NDS2 weekly: query9 reports CompletedWithTaskFailures causing check_query_result.sh to fail the build|
|[#15804](https://github.com/NVIDIA/cudf-spark/issues/15804)|[BUG] ClassInitializationSuite 'SparkShimImpl and GpuOverrides can be initialized concurrently' times out on shim 344 and 351 (nightly UT)|
|[#15828](https://github.com/NVIDIA/cudf-spark/issues/15828)|[BUG] V2 write recognition fails with Iceberg on extraClassPath|
|[#12886](https://github.com/NVIDIA/cudf-spark/issues/12886)|[BUG] test_exact_percentile_groupby failed GPU and CPU float values are different intermittently|
|[#15550](https://github.com/NVIDIA/cudf-spark/issues/15550)|[AutoSparkUT] ParquetCodecSuite LZ4 read - GPU Execution Issue|
|[#10181](https://github.com/NVIDIA/cudf-spark/issues/10181)|[BUG] PartitionReaderWithBytesRead is probably over-incrementing task bytes read|
|[#15791](https://github.com/NVIDIA/cudf-spark/issues/15791)|[BUG] PERFILE reader repeatedly adds cumulative filesystem bytes to metrics|
|[#14829](https://github.com/NVIDIA/cudf-spark/issues/14829)|Check shade warnings for duplicate internal RAPIDS classes|
|[#15705](https://github.com/NVIDIA/cudf-spark/issues/15705)|[BUG][Iceberg] PerfIO S3 reads do not update Spark input byte metrics|
|[#14786](https://github.com/NVIDIA/cudf-spark/issues/14786)|[BUG] Reduce noisy unit-test output from println/debug messages|
|[#15782](https://github.com/NVIDIA/cudf-spark/issues/15782)|[BUG] Spark master build fails because VariantTypeShims is missing from the spark500 shim|
|[#15684](https://github.com/NVIDIA/cudf-spark/issues/15684)|[BUG] AQE can construct invalid PartitioningCollection for cached GPU symmetric hash join|
|[#15744](https://github.com/NVIDIA/cudf-spark/issues/15744)|[BUG] Iceberg GPU scan fails when planned from a background thread|
|[#10027](https://github.com/NVIDIA/cudf-spark/issues/10027)|[BUG] test_date[add/sub]_with_date_overflow fail on DATAGEN_SEED=1702342238 TZ=Asia/Shanghai|
|[#15549](https://github.com/NVIDIA/cudf-spark/issues/15549)|[AutoSparkUT] FileSource Char/Varchar CTAS metadata - GPU Execution Issue|
|[#15772](https://github.com/NVIDIA/cudf-spark/issues/15772)|[BUG] Case-insensitive \p{Lower} / \p{Upper} GPU results don't match JDK 8/11|
|[#15670](https://github.com/NVIDIA/cudf-spark/issues/15670)|[AUDIT] [BUG]  Retain input aggregate-buffer attributes in GPU partial/final aggregate references|
|[#15742](https://github.com/NVIDIA/cudf-spark/issues/15742)|[BUG] Typed imperative partial aggregate can expose the wrong shuffle buffer schema|
|[#15770](https://github.com/NVIDIA/cudf-spark/issues/15770)|[BUG] ClassInitializationSuite child-process output handling can fail with Stream closed|
|[#15669](https://github.com/NVIDIA/cudf-spark/issues/15669)|[AUDIT] [BUG]  Preserve per-write option precedence in Spark 4.2 GPU file writes|
|[#15768](https://github.com/NVIDIA/cudf-spark/issues/15768)|[BUG] Nightly build fails: create-parallel-world cannot resolve sql-plugin-format spark359 jar; flaky ClassInitializationSuite 'Stream closed'|
|[#15721](https://github.com/NVIDIA/cudf-spark/issues/15721)|[BUG] GpuBatchScanExec ignores spjParams.reducers, silently losing join rows when allowCompatibleTransforms is enabled|
|[#15702](https://github.com/NVIDIA/cudf-spark/issues/15702)|[BUG] CPU bridge leaves captured outer attributes unbound in bridged higher-order functions|
|[#15566](https://github.com/NVIDIA/cudf-spark/issues/15566)|[AutoSparkUT] [FilteredScanSuite PushDown Returns tests] - GPU Execution Issue|
|[#15567](https://github.com/NVIDIA/cudf-spark/issues/15567)|[AutoSparkUT] [PrunedScanSuite Columns output tests] - GPU Execution Issue|
|[#15596](https://github.com/NVIDIA/cudf-spark/issues/15596)|[BUG] EXCEPTION timeParserPolicy does not detect corrected/legacy disagreement in GPU datetime expressions|
|[#13272](https://github.com/NVIDIA/cudf-spark/issues/13272)|[BUG] GPU generates a wrong file when writing timestamp < 1970 year.|
|[#15572](https://github.com/NVIDIA/cudf-spark/issues/15572)|[BUG] cudf Java tests fail with UnsatisfiedLinkError on native methods (ColumnView.host*, Rmm, nvcomp) causing mass NoClassDefFoundError on cuda12/cuda13|
|[#15738](https://github.com/NVIDIA/cudf-spark/issues/15738)|[BUG] Executor-wide class-initialization deadlock between GpuOverrides and SparkShimImpl|
|[#15018](https://github.com/NVIDIA/cudf-spark/issues/15018)|[BUG] SkipMerge shuffle closes catalog buffers during active reads|
|[#15719](https://github.com/NVIDIA/cudf-spark/issues/15719)|[BUG] Delta Lake REORG TABLE tests fail on Spark 4.0.4 with NoSuchMethodError for ParquetToSparkSchemaConverter constructor |
|[#11305](https://github.com/NVIDIA/cudf-spark/issues/11305)|[BUG] row count only tests can fail with 'int' object is not iterable|
|[#15707](https://github.com/NVIDIA/cudf-spark/issues/15707)|[BUG] Default 8 MiB Hadoop vectored-read buffer regresses local Parquet scans|
|[#9767](https://github.com/NVIDIA/cudf-spark/issues/9767)|[BUG] `fastparquet` test fails with `DATAGEN_SEED=1700171382` on Databricks (Spark 3.4.1)|
|[#15469](https://github.com/NVIDIA/cudf-spark/issues/15469)|[AutoSparkUT] [Enforce direct encoding column-wise selectively] - GPU Execution Issue|
|[#15237](https://github.com/NVIDIA/cudf-spark/issues/15237)|[BUG] cache_test.py carries inert InMemoryTableScanExec allowances that assert nothing|
|[#15470](https://github.com/NVIDIA/cudf-spark/issues/15470)|[AutoSparkUT] [SPARK-31238: compatibility with Spark 2.4 in reading dates] - GPU Execution Issue|
|[#15701](https://github.com/NVIDIA/cudf-spark/issues/15701)|[BUG] Scala 2.13 tools build makes generated docs build-order dependent|
|[#15653](https://github.com/NVIDIA/cudf-spark/issues/15653)|[BUG] Integration test test_group_partitions_partial_clustering_distinct fails on Spark 3.5.9, Spark 4.0.4 and 4.1.3 (shuffle inserted through DISTINCT)|
|[#15675](https://github.com/NVIDIA/cudf-spark/issues/15675)|[BUG] Iceberg S3Tables IT (Spark 4.1.1) fails: Netty NoSuchMethodError SingleThreadEventLoop during SparkContext init|
|[#15677](https://github.com/NVIDIA/cudf-spark/issues/15677)|[BUG] DeltaLakeQuerySuiteSpark411 fails: delta provider resolves to NoDeltaProvider on Spark 412 unit test (scala2.13, cuda12)|
|[#10485](https://github.com/NVIDIA/cudf-spark/issues/10485)|[BUG] setting timestampFormat/dateFormat for JsonToStructs appears to fall back too often and also not often enough|
|[#15582](https://github.com/NVIDIA/cudf-spark/issues/15582)|[BUG] Databricks 14.3 (Spark 3.5.0) nightly IT: cascading 'Cannot call methods on a stopped SparkContext' causes 7520 failures after java.lang.NoClassDefFoundError: com/databricks/sql/SupportsLineage|
|[#15659](https://github.com/NVIDIA/cudf-spark/issues/15659)|[BUG] Classify shuffle-only issues as Features in generated changelogs|
|[#15656](https://github.com/NVIDIA/cudf-spark/issues/15656)|[BUG] Iceberg V3 fallback tests fail on REST catalog: BadRequestException 'Invalid format version specified in table_properties: 3'|
|[#15634](https://github.com/NVIDIA/cudf-spark/issues/15634)|[BUG] test_from_json_invalid_float_ansi fails on Spark 4.0.0 non-UTC: GpuCpuBridge disallows CPU expression JsonToStructs|
|[#14609](https://github.com/NVIDIA/cudf-spark/issues/14609)|[BUG] hash_aggregate_test.py::test_hash_grpby_sum failed by shuffle timeout 120s|
|[#15608](https://github.com/NVIDIA/cudf-spark/issues/15608)|[AUDIT] Fix collation-aware PIVOT in Spark|
|[#15647](https://github.com/NVIDIA/cudf-spark/issues/15647)|[BUG][DOC] Broken RAPIDS installation link in integration_tests/README.md|
|[#15545](https://github.com/NVIDIA/cudf-spark/issues/15545)|[BUG] Delta Lake integration tests fail on Spark 4.1.3 (scala2.13) due to Delta 4.1.0 binary incompatibility|
|[#15609](https://github.com/NVIDIA/cudf-spark/issues/15609)|[BUG] Databricks pre-merge test-selection logic misses version-suffixed shims and Delta Lake changes|
|[#14088](https://github.com/NVIDIA/cudf-spark/issues/14088)|[SparkUT] SPARK-33134: return partial results only for root JSON objects failed in JsonFunctionsSuite|
|[#15472](https://github.com/NVIDIA/cudf-spark/issues/15472)|[AutoSparkUT] [SPARK-36663: OrcUtils.toCatalystSchema should correctly handle a column name which consists of only numbers] - GPU Execution Issue|
|[#15495](https://github.com/NVIDIA/cudf-spark/issues/15495)|[AutoSparkUT] [regexp_replace oversized quantifier] - GPU Execution Issue|
|[#15468](https://github.com/NVIDIA/cudf-spark/issues/15468)|[AutoSparkUT] [Write Spark version into ORC file metadata] - GPU Execution Issue|
|[#15598](https://github.com/NVIDIA/cudf-spark/issues/15598)|[BUG] DVPredicatePushdown.mergeIdenticalProjects drops an alias-producing GpuProjectExec (DBR 17.3 and OSS Delta 3.3-4.1), causing "Couldn't find <attr>" bind failure at execution|
|[#15594](https://github.com/NVIDIA/cudf-spark/issues/15594)|[BUG] Preserve coalesced hash partition boundaries in GPU shuffle reader|
|[#15383](https://github.com/NVIDIA/cudf-spark/issues/15383)|[AI-AUDIT][SPARK-57507][SQL] Clamp truncated trailing UTF-8 in GPU reverse|
|[#15575](https://github.com/NVIDIA/cudf-spark/issues/15575)|[BUG] Iceberg 1.9.2 tests fail: IllegalAccessError GpuStructInternalRow cannot access superclass StructInternalRow (classloader mismatch)|
|[#15373](https://github.com/NVIDIA/cudf-spark/issues/15373)|[BUG] Dataproc 2.2 image 2.2.84 breaks skewed BHJ optimizer: NoSuchMethodError on ShufflePartitionsUtil.createSkewPartitionSpecs|
|[#15326](https://github.com/NVIDIA/cudf-spark/issues/15326)|[BUG] Delta CDF read of a table with deletion vector fails|
|[#15382](https://github.com/NVIDIA/cudf-spark/issues/15382)|[AI-AUDIT][SPARK-56663][SQL] Match date_trunc overflow semantics at Long.MinValue|
|[#15405](https://github.com/NVIDIA/cudf-spark/issues/15405)|[BUG] test_orc_gpu_write_cpu_read_timestamp_in_non_utc_timezone fails after PR 13437|
|[#13552](https://github.com/NVIDIA/cudf-spark/issues/13552)|[BUG] Low shuffle merge test failures with change data feed on databricks|

### PRs
|||
|:---|:---|
|[#16194](https://github.com/NVIDIA/cudf-spark/pull/16194)|Fix Maven 3.10 distribution builds on release/26.10|
|[#16132](https://github.com/NVIDIA/cudf-spark/pull/16132)|Add S3-backed Parquet canary support to integration tests|
|[#16161](https://github.com/NVIDIA/cudf-spark/pull/16161)|Fix CPU-to-GPU transitions for Spark Variant columns|
|[#16129](https://github.com/NVIDIA/cudf-spark/pull/16129)|Fix OSS Delta 2.4 DML test expectations|
|[#16082](https://github.com/NVIDIA/cudf-spark/pull/16082)|[SkipRecovery] Fix Spark 4 Hive SimpleUDF evaluation|
|[#16125](https://github.com/NVIDIA/cudf-spark/pull/16125)|Remove Gluten from the NOTICE file [skip-ci]|
|[#16124](https://github.com/NVIDIA/cudf-spark/pull/16124)|Fix OSS Delta 4.0 DV MERGE test|
|[#16110](https://github.com/NVIDIA/cudf-spark/pull/16110)|Remove unused plugin and shim code|
|[#16078](https://github.com/NVIDIA/cudf-spark/pull/16078)|Filter rows-only batches without a cudf Table in conditional expressions [reduced-it]|
|[#16108](https://github.com/NVIDIA/cudf-spark/pull/16108)|Report missing data when cleanup races a skip-merge batch lookup [reduced-it]|
|[#16021](https://github.com/NVIDIA/cudf-spark/pull/16021)|Support Boolean, Float and Double Variant targets|
|[#16117](https://github.com/NVIDIA/cudf-spark/pull/16117)|Fix withResource nesting audit overwriting source files [skip ci]|
|[#15284](https://github.com/NVIDIA/cudf-spark/pull/15284)|[BUG] Detect escaped line terminators near regex anchors|
|[#16109](https://github.com/NVIDIA/cudf-spark/pull/16109)|Fix conditional outer join root predicates|
|[#16055](https://github.com/NVIDIA/cudf-spark/pull/16055)|Move stable scalar helpers to root-safe modules|
|[#15992](https://github.com/NVIDIA/cudf-spark/pull/15992)|Add Delta 4.3 provider and runtime with basic features|
|[#16103](https://github.com/NVIDIA/cudf-spark/pull/16103)|Make build info event root-safe across runtimes|
|[#15080](https://github.com/NVIDIA/cudf-spark/pull/15080)|Fix byte position accounting in cached batch output [reduced-it]|
|[#15717](https://github.com/NVIDIA/cudf-spark/pull/15717)|Support Iceberg v3 deletion vector writes on GPU [reduced-it]|
|[#16057](https://github.com/NVIDIA/cudf-spark/pull/16057)|Document RapidsConf entry placement for agents [skip ci]|
|[#16060](https://github.com/NVIDIA/cudf-spark/pull/16060)|Shorten Variant integer-cast resource lifetimes|
|[#16061](https://github.com/NVIDIA/cudf-spark/pull/16061)|Fix Delta RTAS V1 plan assertion for Spark 3.3/3.4|
|[#16080](https://github.com/NVIDIA/cudf-spark/pull/16080)|Fix premerge debug bundle credentials and exception handling  [reduced-it]|
|[#16076](https://github.com/NVIDIA/cudf-spark/pull/16076)|Resolve GitHub Actions deprecation warnings [skip ci]|
|[#16064](https://github.com/NVIDIA/cudf-spark/pull/16064)|Fix Spark master GroupPartitionsExec after expected key API change [reduced-it]|
|[#15869](https://github.com/NVIDIA/cudf-spark/pull/15869)|Adding support for DML writes to DV enabled tables|
|[#16020](https://github.com/NVIDIA/cudf-spark/pull/16020)|Support Variant array-index paths|
|[#16044](https://github.com/NVIDIA/cudf-spark/pull/16044)|[DOC] clarify Iceberg 1.11 Spark support [skip ci]|
|[#16017](https://github.com/NVIDIA/cudf-spark/pull/16017)|Move stable runtime bridges to root-safe modules|
|[#16054](https://github.com/NVIDIA/cudf-spark/pull/16054)|Split rows-only batches by row count in the retry split policy [reduced-it]|
|[#16040](https://github.com/NVIDIA/cudf-spark/pull/16040)|Make rows-only batch conversion fail legibly in GpuColumnVector.from [reduced-it]|
|[#16050](https://github.com/NVIDIA/cudf-spark/pull/16050)|Fall back floating-point exact percentile on Spark 5[reduced-it]|
|[#16045](https://github.com/NVIDIA/cudf-spark/pull/16045)|[BUG] Fix Variant extraction for non-UTC and EMR|
|[#16000](https://github.com/NVIDIA/cudf-spark/pull/16000)|[SkipRecovery] Re-enable JSON column-name tests|
|[#15245](https://github.com/NVIDIA/cudf-spark/pull/15245)|[DOC] add config since version metadata|
|[#16048](https://github.com/NVIDIA/cudf-spark/pull/16048)|Fix Spark 5 planning fallbacks for array_sort and stateful expressions[reduced-it]|
|[#16043](https://github.com/NVIDIA/cudf-spark/pull/16043)|Fix complement selection in range-partition sampling|
|[#15194](https://github.com/NVIDIA/cudf-spark/pull/15194)|Avoid busy spin in skip-merge FileRegion transfer|
|[#15982](https://github.com/NVIDIA/cudf-spark/pull/15982)|[BUG] Retry UCX shuffle sends after GPU OOM [reduced-it]|
|[#16022](https://github.com/NVIDIA/cudf-spark/pull/16022)|Support Delta 4.2 OSS Unity Catalog managed tables|
|[#15969](https://github.com/NVIDIA/cudf-spark/pull/15969)|[SkipRecovery] Re-enable CORRECTED Parquet timestamp checks|
|[#15645](https://github.com/NVIDIA/cudf-spark/pull/15645)|Enable Variant extraction for Databricks 17.3|
|[#16034](https://github.com/NVIDIA/cudf-spark/pull/16034)|Fix nested subquery GPU broadcast deadlock|
|[#15998](https://github.com/NVIDIA/cudf-spark/pull/15998)|[SkipRecovery] Re-enable Databricks ROW_NUMBER QA cases [reduced-it]|
|[#16012](https://github.com/NVIDIA/cudf-spark/pull/16012)|Add Spark 4.2.0 NAAJ BuildLeft fallback coverage[reduced-it]|
|[#15927](https://github.com/NVIDIA/cudf-spark/pull/15927)|Fix Delta v1 writer detection for spark 4.0+|
|[#15979](https://github.com/NVIDIA/cudf-spark/pull/15979)|[BUG] Fail closed on incomplete aggregate result mapping|
|[#15714](https://github.com/NVIDIA/cudf-spark/pull/15714)|[BUG] Pre-decode schema estimate is producing underfull batches|
|[#15943](https://github.com/NVIDIA/cudf-spark/pull/15943)|Document PerfIO S3 client configuration [reduced-it]|
|[#16009](https://github.com/NVIDIA/cudf-spark/pull/16009)|Fix broadcast-to-row output partitioning[reduced-it]|
|[#16005](https://github.com/NVIDIA/cudf-spark/pull/16005)|Consolidate Iceberg internal row shims|
|[#16003](https://github.com/NVIDIA/cudf-spark/pull/16003)|Exempt withResource baseline from CI/CD code ownership [skip ci]|
|[#16019](https://github.com/NVIDIA/cudf-spark/pull/16019)|Fix skip-merge buffer handoff during shuffle cleanup  [reduced-it]|
|[#15948](https://github.com/NVIDIA/cudf-spark/pull/15948)|from_protobuf: Add reusable protobuf test data helpers  [reduced-it]|
|[#16011](https://github.com/NVIDIA/cudf-spark/pull/16011)|Fix non-UTC ORC write test assertion|
|[#16015](https://github.com/NVIDIA/cudf-spark/pull/16015)|Move standalone utilities to root-safe modules|
|[#15925](https://github.com/NVIDIA/cudf-spark/pull/15925)|Liquid-clustering boundary sampling|
|[#15942](https://github.com/NVIDIA/cudf-spark/pull/15942)|Support native Delta OPTIMIZE writes on GPU for DBR 14.3 and 17.3|
|[#16010](https://github.com/NVIDIA/cudf-spark/pull/16010)|Limit Iceberg spark.testing override to Spark 3.5 [reduced-it]|
|[#16001](https://github.com/NVIDIA/cudf-spark/pull/16001)|Shorten resource scopes in Iceberg delete processing|
|[#15735](https://github.com/NVIDIA/cudf-spark/pull/15735)|Split RapidsConf declarations for faster incremental compilation|
|[#16014](https://github.com/NVIDIA/cudf-spark/pull/16014)|Publish Databricks helper JARs required by distribution builds [skip ci]|
|[#15984](https://github.com/NVIDIA/cudf-spark/pull/15984)|[DOC] update tested GPUs and driver requirements [skip ci]|
|[#15989](https://github.com/NVIDIA/cudf-spark/pull/15989)|[SkipRecovery] Re-enable special-case join tests|
|[#15994](https://github.com/NVIDIA/cudf-spark/pull/15994)|Guard GPU range-partition boundary sampling against rows-only batches [reduced-it]|
|[#14680](https://github.com/NVIDIA/cudf-spark/pull/14680)|Cache and reuse build-side hash tables during broadcast hash joins|
|[#14846](https://github.com/NVIDIA/cudf-spark/pull/14846)|Use merge and set validity in cast struct to struct|
|[#15986](https://github.com/NVIDIA/cudf-spark/pull/15986)|Fix Spark 5 multi-file reader type visibility [reduced-it]|
|[#15879](https://github.com/NVIDIA/cudf-spark/pull/15879)|Run and report withResource nesting lint with Jython [reduced-it]|
|[#15964](https://github.com/NVIDIA/cudf-spark/pull/15964)|[SkipRecovery] Re-enable mixed-delete tests on Iceberg 1.10.2+ [reduced-it]|
|[#15978](https://github.com/NVIDIA/cudf-spark/pull/15978)|Switch from deprecated inner-join call|
|[#15958](https://github.com/NVIDIA/cudf-spark/pull/15958)|Move runtime helpers to root-safe module|
|[#15954](https://github.com/NVIDIA/cudf-spark/pull/15954)|Fix 0-col case in concatenateBatchesWithRetry + canUsePartialSortAgg fix|
|[#15722](https://github.com/NVIDIA/cudf-spark/pull/15722)|Use cuDF to count the total deleted row count for cuDF based Delta reader for deletion vectors|
|[#15974](https://github.com/NVIDIA/cudf-spark/pull/15974)|Skip Iceberg 1.9.2 REST deletion vector tests [reduced-it]|
|[#15985](https://github.com/NVIDIA/cudf-spark/pull/15985)|Fix root-safe provider test runtime stub [reduced-it]|
|[#15966](https://github.com/NVIDIA/cudf-spark/pull/15966)|Disable Spark testing mode for Iceberg tests [reduced-it]|
|[#15983](https://github.com/NVIDIA/cudf-spark/pull/15983)|Disable protobuf tests by default without explicit jar enablement  [reduced-it]|
|[#15944](https://github.com/NVIDIA/cudf-spark/pull/15944)|Finalize Iceberg root and shim class placement [reduced-it]|
|[#15938](https://github.com/NVIDIA/cudf-spark/pull/15938)|[SkipRecovery][FEA] Support count on ANSI interval columns|
|[#15949](https://github.com/NVIDIA/cudf-spark/pull/15949)|[SkipRecovery] Restore empty partitioned Parquet write tests [reduced-it]|
|[#15899](https://github.com/NVIDIA/cudf-spark/pull/15899)|Refactor regex quantifiers into base and mode [reduced-it]|
|[#15965](https://github.com/NVIDIA/cudf-spark/pull/15965)|[SkipRecovery] Restore randomized regex choice coverage [reduced-it]|
|[#15850](https://github.com/NVIDIA/cudf-spark/pull/15850)|[SkipRecovery] Fix get_json_object runtime path semantics|
|[#15967](https://github.com/NVIDIA/cudf-spark/pull/15967)|Fix Variant Parquet scans on Spark 4.1+|
|[#15957](https://github.com/NVIDIA/cudf-spark/pull/15957)|Support explicit Iceberg audit runtime paths [reduced-it]|
|[#15941](https://github.com/NVIDIA/cudf-spark/pull/15941)|Avoid eager input materialization for GPU range shuffle|
|[#15924](https://github.com/NVIDIA/cudf-spark/pull/15924)|Fix SPJ runtime partition validation [reduced-it]|
|[#15929](https://github.com/NVIDIA/cudf-spark/pull/15929)|[FEA] Consume renamed cudf-spark-jni artifacts|
|[#15960](https://github.com/NVIDIA/cudf-spark/pull/15960)|Budget parallel unit tests from available GPU memory [reduced-it]|
|[#15932](https://github.com/NVIDIA/cudf-spark/pull/15932)|Fix Spark master GpuBatchScanExec compile after replanWithRuntimeFilters API change[reduced-it]|
|[#15936](https://github.com/NVIDIA/cudf-spark/pull/15936)|Fix PCBS binary column decoding [serial-ut]|
|[#15841](https://github.com/NVIDIA/cudf-spark/pull/15841)|Move independent Iceberg helpers to root-safe module [reduced-it]|
|[#15933](https://github.com/NVIDIA/cudf-spark/pull/15933)|Exercise the ORC empty-child STRUCT guard|
|[#15796](https://github.com/NVIDIA/cudf-spark/pull/15796)|Add Delta Lake 4.2 support with basic features|
|[#15644](https://github.com/NVIDIA/cudf-spark/pull/15644)|Enable Variant extraction for Apache Spark 4|
|[#15818](https://github.com/NVIDIA/cudf-spark/pull/15818)|Add AutoOptimizedShuffle regression coverage|
|[#15814](https://github.com/NVIDIA/cudf-spark/pull/15814)|from_protobuf: Re-enable protobuf integration tests on OSS Spark and Databricks  [reduced-it]|
|[#15931](https://github.com/NVIDIA/cudf-spark/pull/15931)|Move host helpers to root-safe columnar module [reduced-it]|
|[#15907](https://github.com/NVIDIA/cudf-spark/pull/15907)|Add row tracking preservation tests for MERGE, UPDATE and DELETE on OSS Delta|
|[#15884](https://github.com/NVIDIA/cudf-spark/pull/15884)|Support NOT MATCHED BY SOURCE in the GPU MERGE command on Databricks 17.3|
|[#15908](https://github.com/NVIDIA/cudf-spark/pull/15908)|Run the Databricks IncrementMetric expressions on the GPU for Databricks 17.3|
|[#15922](https://github.com/NVIDIA/cudf-spark/pull/15922)|Fix mortgage test on EMR with Spark testing enabled|
|[#15918](https://github.com/NVIDIA/cudf-spark/pull/15918)|[SkipRecovery] Narrow ORC negative timestamp xfail|
|[#15835](https://github.com/NVIDIA/cudf-spark/pull/15835)|[AutoSparkUT] Fix GPU missing-file recovery guidance [reduced-it]|
|[#15915](https://github.com/NVIDIA/cudf-spark/pull/15915)|Add Delta column-mapping predicate coverage with deletion vectors|
|[#15917](https://github.com/NVIDIA/cudf-spark/pull/15917)|[BUG] Unshim Spark 5 RapidsShuffleManagerBase for spark-shell[reduced-it]|
|[#15905](https://github.com/NVIDIA/cudf-spark/pull/15905)|Make Iceberg package-private access root-safe|
|[#15833](https://github.com/NVIDIA/cudf-spark/pull/15833)|[DOC] warn against explain ALL in production|
|[#15904](https://github.com/NVIDIA/cudf-spark/pull/15904)|Restore Blackwell integration tests after cuDF scan workaround|
|[#15842](https://github.com/NVIDIA/cudf-spark/pull/15842)|Port SPARK-48949 scan-side partition filter to GpuBatchScanExec [fast-ut][reduced-it]|
|[#15920](https://github.com/NVIDIA/cudf-spark/pull/15920)|Enable Iceberg V3 REST catalog tests [reduced-it]|
|[#15921](https://github.com/NVIDIA/cudf-spark/pull/15921)|[dependency-check]: isolate warmup dependencies to avoid maven central 429 [skip ci]|
|[#15880](https://github.com/NVIDIA/cudf-spark/pull/15880)|Work around Dataproc Py4J weak container references [reduced-it]|
|[#15914](https://github.com/NVIDIA/cudf-spark/pull/15914)|[AutoSparkUT] Spark 3.3 data sources: recover 52 ignored tests|
|[#15912](https://github.com/NVIDIA/cudf-spark/pull/15912)|Fix Spark master GpuGroupPartitionsExec compile after KeyReducer API change[reduced-it]|
|[#15584](https://github.com/NVIDIA/cudf-spark/pull/15584)|[FEA] Add decoded GPU batch bytes metric to GPU scans|
|[#15885](https://github.com/NVIDIA/cudf-spark/pull/15885)|Cover Spark 4.2 Arrow Python UDF columnar input [fast-ut] [reduced-it]|
|[#15852](https://github.com/NVIDIA/cudf-spark/pull/15852)|[SkipRecovery] Re-enable Parquet V2 encoding fixtures [fast-ut]|
|[#15898](https://github.com/NVIDIA/cudf-spark/pull/15898)|[AutoSparkUT] Recover ORC legacy date read coverage [fast-ut]|
|[#15877](https://github.com/NVIDIA/cudf-spark/pull/15877)|Use returned cuDF Parquet footer for Iceberg writes [fast-ut]|
|[#15881](https://github.com/NVIDIA/cudf-spark/pull/15881)|Update dependencies for renamed cudf-spark-private artifacts|
|[#15900](https://github.com/NVIDIA/cudf-spark/pull/15900)|[AutoSparkUT] Fix map_zip_with lambda result nullability [fast-ut]|
|[#15901](https://github.com/NVIDIA/cudf-spark/pull/15901)|Fix Delta CDF tests  on Spark 3.4|
|[#15824](https://github.com/NVIDIA/cudf-spark/pull/15824)|Extract Iceberg schema accessors to root-safe module [reduced-it]|
|[#15851](https://github.com/NVIDIA/cudf-spark/pull/15851)|Enable parallel unit tests by default in premerge [reduced-it] [serial-ut]|
|[#15849](https://github.com/NVIDIA/cudf-spark/pull/15849)|Remove hybrid execution support [fast-ut] [reduced-it]|
|[#15811](https://github.com/NVIDIA/cudf-spark/pull/15811)|[AUDIT][SPARK-54223] Track GPU Arrow Python output metrics[fast-ut][reduced-it]|
|[#15894](https://github.com/NVIDIA/cudf-spark/pull/15894)|[BUG] Mock ParquetTableWriter in CachedBatchWriterSuite [reduced-it]|
|[#15870](https://github.com/NVIDIA/cudf-spark/pull/15870)|Fix nested date scalar normalization|
|[#15883](https://github.com/NVIDIA/cudf-spark/pull/15883)|[DOC] apply rename follow-ups on main [skip ci]|
|[#15825](https://github.com/NVIDIA/cudf-spark/pull/15825)|Support column patterns in LIKE expressions|
|[#15819](https://github.com/NVIDIA/cudf-spark/pull/15819)|[BUG] Preserve input bytes in Spark 4 data source scans [fast-ut]|
|[#15854](https://github.com/NVIDIA/cudf-spark/pull/15854)|Remove dedicated Parquet test scheduling [fast-ut] [reduced-it]|
|[#15478](https://github.com/NVIDIA/cudf-spark/pull/15478)|[BUG] Fix bounded reluctant regex quantifiers [reduced-it]  [fast-ut]|
|[#15629](https://github.com/NVIDIA/cudf-spark/pull/15629)|[BUG] Release bytes-in-flight quota on compression task failure or cancellation|
|[#15790](https://github.com/NVIDIA/cudf-spark/pull/15790)|Support Delta CDF read|
|[#15844](https://github.com/NVIDIA/cudf-spark/pull/15844)|Fix bridge lambda reference test across Spark versions[fast-ut][reduced-it]|
|[#15767](https://github.com/NVIDIA/cudf-spark/pull/15767)|[DOC] Cherry-pick v26.08.1 download update to main [skip ci]|
|[#15845](https://github.com/NVIDIA/cudf-spark/pull/15845)|Fix ConsoleOutput visibility in standalone integration test builds [fast-ut] [reduced-it]|
|[#15848](https://github.com/NVIDIA/cudf-spark/pull/15848)|Replace deprecated cuDF retention mask calls and skip Blackwell row-conversion failures [fast-ut]|
|[#15729](https://github.com/NVIDIA/cudf-spark/pull/15729)|Install Git in the Rocky Linux integration test image|
|[#15836](https://github.com/NVIDIA/cudf-spark/pull/15836)|Speed up Parquet write UT for the sorted partitioned write case [fast-ut] [reduced-it]|
|[#15837](https://github.com/NVIDIA/cudf-spark/pull/15837)|Speed up Parquet write UT for cases write with max records per file [fast-ut]  [reduced-it]|
|[#15579](https://github.com/NVIDIA/cudf-spark/pull/15579)|Support Iceberg v3 row lineage metadata reads [fast-ut] [reduced-it]|
|[#15522](https://github.com/NVIDIA/cudf-spark/pull/15522)|Reduce redundant ORC premerge cases [fast-ut] [reduced-it]|
|[#15810](https://github.com/NVIDIA/cudf-spark/pull/15810)|Reuse sorted GroupPartitionsExec input in GPU external merge [fast-ut][reduced-it]|
|[#15822](https://github.com/NVIDIA/cudf-spark/pull/15822)|Move Iceberg helpers to root-safe module [fast-ut][reduced-it]|
|[#15799](https://github.com/NVIDIA/cudf-spark/pull/15799)|[DOC] Document pre-merge CI title tags [skip ci]|
|[#15834](https://github.com/NVIDIA/cudf-spark/pull/15834)|Fix ClassInitializationSuite JaCoCo deadlock [fast-ut] [reduced-it]|
|[#15829](https://github.com/NVIDIA/cudf-spark/pull/15829)|Move Iceberg write access bridge to root-safe module [fast-ut][reduced-it]|
|[#15479](https://github.com/NVIDIA/cudf-spark/pull/15479)|[fast-ut] [reduced-it] [SkipRecovery] Restore Delta liquid-clustering RTAS coverage|
|[#15781](https://github.com/NVIDIA/cudf-spark/pull/15781)|[fast-ut] [reduced-it] Add ANSI interval expression type coverage|
|[#15813](https://github.com/NVIDIA/cudf-spark/pull/15813)|Work around Spark signed-zero percentile bug in exact percentile tests|
|[#15785](https://github.com/NVIDIA/cudf-spark/pull/15785)|[fast-ut] [reduced-it] Add collection expression corner-case coverage|
|[#15802](https://github.com/NVIDIA/cudf-spark/pull/15802)|[AutoSparkUT] Fix legacy Hadoop LZ4 Parquet reads|
|[#15762](https://github.com/NVIDIA/cudf-spark/pull/15762)|Move Iceberg common helpers to root-safe module [fast-ut][reduced-it]|
|[#15780](https://github.com/NVIDIA/cudf-spark/pull/15780)|Add withResource nesting lint and shorten window resource lifetimes|
|[#15803](https://github.com/NVIDIA/cudf-spark/pull/15803)|[BUG] Fix input bytes accounting for GPU file scans [fast-ut]|
|[#15797](https://github.com/NVIDIA/cudf-spark/pull/15797)|Add unit tests for the dynamic GPU task concurrency estimator [fast-ut] [reduced-it]|
|[#15643](https://github.com/NVIDIA/cudf-spark/pull/15643)|Add GPU Variant extraction implementation|
|[#14320](https://github.com/NVIDIA/cudf-spark/pull/14320)|Add in support for array_compact with tests|
|[#15761](https://github.com/NVIDIA/cudf-spark/pull/15761)|Move columnar base helpers to columnar module [fast-ut]|
|[#15779](https://github.com/NVIDIA/cudf-spark/pull/15779)|Fail on conflicting duplicate aggregator classes|
|[#15800](https://github.com/NVIDIA/cudf-spark/pull/15800)|[DOC] Document GPU expression registration [skip ci]|
|[#15760](https://github.com/NVIDIA/cudf-spark/pull/15760)|Move Hadoop file I/O helpers to fileio module|
|[#15637](https://github.com/NVIDIA/cudf-spark/pull/15637)|fix: inconsistent indentation (11 spaces) on writer.option line|
|[#15713](https://github.com/NVIDIA/cudf-spark/pull/15713)|Include spjParams in GpuBatchScanExec.hashCode|
|[#15788](https://github.com/NVIDIA/cudf-spark/pull/15788)|Update Iceberg S3 input byte metrics|
|[#15638](https://github.com/NVIDIA/cudf-spark/pull/15638)|fix: stray string literal after statements is dead code, not a docstring|
|[#15639](https://github.com/NVIDIA/cudf-spark/pull/15639)|Remove unused data_type locals in test_and and test_or|
|[#15128](https://github.com/NVIDIA/cudf-spark/pull/15128)|Reduce noisy RAPIDS test output|
|[#15704](https://github.com/NVIDIA/cudf-spark/pull/15704)|Support sorted-merge GroupPartitionsExec on GPU [fast-ut][reduced-it]|
|[#15801](https://github.com/NVIDIA/cudf-spark/pull/15801)|Fix Spark master VariantType shim build[fast-ut][reduced-it]|
|[#15699](https://github.com/NVIDIA/cudf-spark/pull/15699)|[BUG] Preserve cached partition counts under AQE|
|[#15745](https://github.com/NVIDIA/cudf-spark/pull/15745)|Fix Iceberg scan planning from background threads|
|[#15726](https://github.com/NVIDIA/cudf-spark/pull/15726)|[SkipRecovery] Restore random-seed coverage for date add/sub overflow tests [fast-ut] [reduced-it]|
|[#15763](https://github.com/NVIDIA/cudf-spark/pull/15763)|[AutoSparkUT] Preserve CHAR/VARCHAR metadata for GPU CTAS|
|[#15727](https://github.com/NVIDIA/cudf-spark/pull/15727)|[DOC] Require AI agents to follow repository policies [skip ci]|
|[#15773](https://github.com/NVIDIA/cudf-spark/pull/15773)|[BUG] Fall back to CPU for case-insensitive \p{Lower} / \p{Upper} predefined classes [fast-ut][reduced-it]|
|[#15758](https://github.com/NVIDIA/cudf-spark/pull/15758)|Fix GPU aggregate produced attributes [reduced-it][fast-ut]|
|[#15743](https://github.com/NVIDIA/cudf-spark/pull/15743)|Fix typed aggregate partial result buffer types|
|[#15789](https://github.com/NVIDIA/cudf-spark/pull/15789)|Fix pre-1000 DateGen lead lag defaults|
|[#15734](https://github.com/NVIDIA/cudf-spark/pull/15734)|Split GpuOverrides registries for faster incremental compilation|
|[#15774](https://github.com/NVIDIA/cudf-spark/pull/15774)|Fix flaky ClassInitializationSuite|
|[#15751](https://github.com/NVIDIA/cudf-spark/pull/15751)|Preserve per-write Parquet option precedence [reduced-it][fast-ut]|
|[#15765](https://github.com/NVIDIA/cudf-spark/pull/15765)|[fast-ut] [reduced-it] Add missing numeric expression type coverage|
|[#15633](https://github.com/NVIDIA/cudf-spark/pull/15633)|Support Iceberg v3 deletion vectors on GPU|
|[#15771](https://github.com/NVIDIA/cudf-spark/pull/15771)|[BUG] Copy root-safe module artifacts into nightly Maven repo [skip ci]|
|[#15746](https://github.com/NVIDIA/cudf-spark/pull/15746)|Upgrade scala-maven-plugin to 4.9.10 without build regressions|
|[#15736](https://github.com/NVIDIA/cudf-spark/pull/15736)|Fix wrong results in SPJ with reduced partition transforms|
|[#15711](https://github.com/NVIDIA/cudf-spark/pull/15711)|Fix CPU bridge binding of captured lambda attributes|
|[#15750](https://github.com/NVIDIA/cudf-spark/pull/15750)|[AutoSparkUT] Recover filtered and pruned scan coverage [fast-ut] [reduced-it]|
|[#15739](https://github.com/NVIDIA/cudf-spark/pull/15739)|Move FlatBuffers format classes to helper module|
|[#15651](https://github.com/NVIDIA/cudf-spark/pull/15651)|Handle EXCEPTION time parser policy disagreement  [fast-ut] [reduced-it]|
|[#15710](https://github.com/NVIDIA/cudf-spark/pull/15710)|[FEA] Support dynamic expressions in IN [fast-ut]|
|[#15718](https://github.com/NVIDIA/cudf-spark/pull/15718)|Enable pre-epoch ORC timestamp tests [fast-ut] [reduced-it]|
|[#15740](https://github.com/NVIDIA/cudf-spark/pull/15740)|Fix class-initialization deadlock between GpuOverrides and SparkShimImpl|
|[#14983](https://github.com/NVIDIA/cudf-spark/pull/14983)|Replace deprecated JSON read overload|
|[#15064](https://github.com/NVIDIA/cudf-spark/pull/15064)|Fix skip-merge shuffle handle lifetime|
|[#15732](https://github.com/NVIDIA/cudf-spark/pull/15732)|Optimize dist packaging hot paths|
|[#15733](https://github.com/NVIDIA/cudf-spark/pull/15733)|Reuse unpacked native dependencies in dist builds|
|[#15731](https://github.com/NVIDIA/cudf-spark/pull/15731)|Avoid redundant Shimplify work in Maven builds|
|[#15748](https://github.com/NVIDIA/cudf-spark/pull/15748)|Warm up transitive dependencies for dependency checks [skip ci]|
|[#15737](https://github.com/NVIDIA/cudf-spark/pull/15737)|Xfail Delta REORG tests for Delta 4.0.1 and 4.1.0|
|[#15109](https://github.com/NVIDIA/cudf-spark/pull/15109)|Add regression test for assert_equal row-count mismatch|
|[#15519](https://github.com/NVIDIA/cudf-spark/pull/15519)|Add SQL plugin helper module wiring|
|[#13314](https://github.com/NVIDIA/cudf-spark/pull/13314)|Support write executors for noop format DataFrame writes|
|[#15696](https://github.com/NVIDIA/cudf-spark/pull/15696)|Avoid O(n^2) array copies in string translate|
|[#15642](https://github.com/NVIDIA/cudf-spark/pull/15642)|Add Spark Variant type infrastructure|
|[#15720](https://github.com/NVIDIA/cudf-spark/pull/15720)|Fix Delta Lake REORG test failures on Spark 3.5.9 and 4.0.4|
|[#15709](https://github.com/NVIDIA/cudf-spark/pull/15709)|Avoid oversized Hadoop vectored-read buffer allocations|
|[#15697](https://github.com/NVIDIA/cudf-spark/pull/15697)|fix: remove dead null branch in BasePad pad string construction|
|[#15698](https://github.com/NVIDIA/cudf-spark/pull/15698)|fix: remove redundant cast in ContainsAny AST fold|
|[#15712](https://github.com/NVIDIA/cudf-spark/pull/15712)|[DOC] Clarify PR submission and draft PR guidelines [skip ci]|
|[#15189](https://github.com/NVIDIA/cudf-spark/pull/15189)|[SkipRecovery] Preserve NaN in fastparquet reference reads [fast-ut]|
|[#15517](https://github.com/NVIDIA/cudf-spark/pull/15517)|Add Spark master shim support[reduced-it][fast-ut]|
|[#15703](https://github.com/NVIDIA/cudf-spark/pull/15703)|[fast-ut] [reduced-it] Remove dead regex line-anchor backref state|
|[#15506](https://github.com/NVIDIA/cudf-spark/pull/15506)|[AutoSparkUT] Fall back for unsupported GPU ORC encoding options|
|[#15708](https://github.com/NVIDIA/cudf-spark/pull/15708)|[fast-ut] [reduced-it] [SkipRecovery] Narrow JSON array overflow xfails|
|[#15500](https://github.com/NVIDIA/cudf-spark/pull/15500)|Delta Lake REORG TABLE APPLY (PURGE) Support|
|[#15706](https://github.com/NVIDIA/cudf-spark/pull/15706)|Update default pinned pool init threads to 'all'|
|[#15682](https://github.com/NVIDIA/cudf-spark/pull/15682)|Remove inert InMemoryTableScanExec allowances in cache_test.py|
|[#15693](https://github.com/NVIDIA/cudf-spark/pull/15693)|[fast-ut] [reduced-it] Xfail Iceberg V3 COW DELETE on Spark 3.5|
|[#15496](https://github.com/NVIDIA/cudf-spark/pull/15496)|Fix legacy ORC date rebasing|
|[#14806](https://github.com/NVIDIA/cudf-spark/pull/14806)|[BUILD] Avoid scala213 doc generation overwriting shared outputs[reduced-it][fast-ut]|
|[#15603](https://github.com/NVIDIA/cudf-spark/pull/15603)|Fix historical ORC integer timestamp conversion [fast-ut] [reduced-it]|
|[#15689](https://github.com/NVIDIA/cudf-spark/pull/15689)|[BUG] Correct partial-clustering DISTINCT plan assertion|
|[#15692](https://github.com/NVIDIA/cudf-spark/pull/15692)|[BUG] Restrict Delta provider test to Spark 4.1.1|
|[#15585](https://github.com/NVIDIA/cudf-spark/pull/15585)|Add performance test rule for AI review [skip ci]|
|[#15679](https://github.com/NVIDIA/cudf-spark/pull/15679)|Fix NVSkills workflow permissions [skip ci]|
|[#15484](https://github.com/NVIDIA/cudf-spark/pull/15484)|Support case-insensitive inline / scoped flags (?i) / (?i:…) in regex patterns [fast-ut][reduced-it]|
|[#15674](https://github.com/NVIDIA/cudf-spark/pull/15674)|Run Roadmap automation when pull requests merge [skip ci]|
|[#15571](https://github.com/NVIDIA/cudf-spark/pull/15571)|[fast-ut] [reduced-it] [SkipRecovery] Recover JSON datetime option tests|
|[#15615](https://github.com/NVIDIA/cudf-spark/pull/15615)|[AutoSparkUT] Migrate final four Spark 3.3 SQL suites|
|[#15617](https://github.com/NVIDIA/cudf-spark/pull/15617)|[fast-ut] [reduced-it] Add ArrayAggregate and ArraySort corner cases|
|[#15622](https://github.com/NVIDIA/cudf-spark/pull/15622)|[fast-ut] [reduced-it] Add AnsiCast corner cases|
|[#15626](https://github.com/NVIDIA/cudf-spark/pull/15626)|[fast-ut] [SkipRecovery] Re-enable JSON float and double recovery cases|
|[#15660](https://github.com/NVIDIA/cudf-spark/pull/15660)|Classify shuffle-only changelog issues as features [skip ci]|
|[#15569](https://github.com/NVIDIA/cudf-spark/pull/15569)|[AutoSparkUT] Migrate ten Spark 3.3 SQL suites [fast-ut] [reduced-it]|
|[#15658](https://github.com/NVIDIA/cudf-spark/pull/15658)|Skip Iceberg V3 tests for REST catalogs [fast-ut] [reduced-it]|
|[#15636](https://github.com/NVIDIA/cudf-spark/pull/15636)|Fix non-UTC from_json ANSI test allowlist [fast-ut] [reduced-it]|
|[#15649](https://github.com/NVIDIA/cudf-spark/pull/15649)|Update premerge log guardwords [skip ci]|
|[#15635](https://github.com/NVIDIA/cudf-spark/pull/15635)|Guard collated PivotFirst against GPU execution|
|[#15648](https://github.com/NVIDIA/cudf-spark/pull/15648)|[DOC] Fix RAPIDS installation link to pass premerge check [skip ci]|
|[#15554](https://github.com/NVIDIA/cudf-spark/pull/15554)|[AutoSparkUT] Migrate ten Spark 3.3 SQL suites [fast-ut] [reduced-it]|
|[#15590](https://github.com/NVIDIA/cudf-spark/pull/15590)|[BUG] Limit Delta Lake 4.1.0 support to compatible Spark versions|
|[#15619](https://github.com/NVIDIA/cudf-spark/pull/15619)|[fast-ut] [reduced-it] Add BloomFilterMightContain corner cases|
|[#15546](https://github.com/NVIDIA/cudf-spark/pull/15546)|Normalize float/double inside GpuCollectSet for Spark 4.2|
|[#15610](https://github.com/NVIDIA/cudf-spark/pull/15610)|Fix pre-merge auto test-selection for databricks tests [skip ci]|
|[#14773](https://github.com/NVIDIA/cudf-spark/pull/14773)|[AutoSparkUT] RapidsJsonFunctionsSuite: cover SPARK-33134 root structs|
|[#15604](https://github.com/NVIDIA/cudf-spark/pull/15604)|Support GroupPartitionsExec on GPU [fast-ut][reduced-it]|
|[#15628](https://github.com/NVIDIA/cudf-spark/pull/15628)|Upgrade UCX to 1.22.0|
|[#15356](https://github.com/NVIDIA/cudf-spark/pull/15356)|Chunk large native libraries for parallel extraction|
|[#15492](https://github.com/NVIDIA/cudf-spark/pull/15492)|Fix ORC numeric-only field name parsing|
|[#15498](https://github.com/NVIDIA/cudf-spark/pull/15498)|[fast-ut] [reduced-it] [BUG] Handle oversized regex quantifier integers|
|[#15505](https://github.com/NVIDIA/cudf-spark/pull/15505)|[AutoSparkUT] Add Spark version metadata to GPU ORC files [fast-ut][reduced-it]|
|[#15570](https://github.com/NVIDIA/cudf-spark/pull/15570)|[fast-ut] [reduced-it] [TEST] Add regex range-start regression coverage|
|[#15456](https://github.com/NVIDIA/cudf-spark/pull/15456)|[AutoSparkUT] Migrate ten Spark SQL suites [fast-ut] [reduced-it]|
|[#15565](https://github.com/NVIDIA/cudf-spark/pull/15565)|Fall back to CPU for Iceberg v3 tables by default|
|[#15520](https://github.com/NVIDIA/cudf-spark/pull/15520)|Reduce redundant JSON premerge cases [fast-ut] [reduced-it]|
|[#15407](https://github.com/NVIDIA/cudf-spark/pull/15407)|Pre-touch pages concurrently during pinned memory pool initialization|
|[#15374](https://github.com/NVIDIA/cudf-spark/pull/15374)|Support `NullType` in `InMemoryTableScanExec`|
|[#15324](https://github.com/NVIDIA/cudf-spark/pull/15324)|Perfio Orc Support|
|[#15607](https://github.com/NVIDIA/cudf-spark/pull/15607)|Fix auto merge conflict 15606 [skip ci]|
|[#15593](https://github.com/NVIDIA/cudf-spark/pull/15593)|Preserve AQE coalesced hash partition boundaries|
|[#15578](https://github.com/NVIDIA/cudf-spark/pull/15578)|[BUG] Fix reverse for truncated trailing UTF-8|
|[#15513](https://github.com/NVIDIA/cudf-spark/pull/15513)|Clarify expected parallel test failures [fast-ut] [reduced-it]|
|[#15515](https://github.com/NVIDIA/cudf-spark/pull/15515)|[AutoSparkUT] Migrate ten Spark SQL suites [fast-ut] [reduced-it]|
|[#15477](https://github.com/NVIDIA/cudf-spark/pull/15477)|[fast-ut] [reduced-it] [SkipRecovery] Restore Delta deletion vector write coverage|
|[#15490](https://github.com/NVIDIA/cudf-spark/pull/15490)|[fast-ut] [SkipRecovery] Narrow CSV timestamp inference exemptions|
|[#15417](https://github.com/NVIDIA/cudf-spark/pull/15417)|[FEA] Add GPU support for NTILE|
|[#15321](https://github.com/NVIDIA/cudf-spark/pull/15321)|[FEA] Support Delta Lake 4.1 MERGE NOT MATCHED BY SOURCE on GPU|
|[#15025](https://github.com/NVIDIA/cudf-spark/pull/15025)|Add common unshim packaging tooling|
|[#15493](https://github.com/NVIDIA/cudf-spark/pull/15493)|Separate JSON floating-point and boolean test patterns [fast-ut]|
|[#15494](https://github.com/NVIDIA/cudf-spark/pull/15494)|[fast-ut] [reduced-it] [SkipRecovery] Re-enable JSON partition-pruning tests|
|[#15466](https://github.com/NVIDIA/cudf-spark/pull/15466)|[AutoSparkUT] Migrate ten Spark SQL suites [fast-ut] [reduced-it]|
|[#15480](https://github.com/NVIDIA/cudf-spark/pull/15480)|[fast-ut] [reduced-it] [SkipRecovery] Restore Delta REORG coverage on Spark 4.1.1+|
|[#15377](https://github.com/NVIDIA/cudf-spark/pull/15377)|[FEA] Enable per-expression legacy AST projection  [fast-ut]|
|[#15425](https://github.com/NVIDIA/cudf-spark/pull/15425)|Migrate to ColumnVector mergeAndSetValidity API for substring|
|[#15488](https://github.com/NVIDIA/cudf-spark/pull/15488)|Isolate cached serializers between SQL test suites [fast-ut] [reduced-it]|
|[#15453](https://github.com/NVIDIA/cudf-spark/pull/15453)|[fast-ut] [reduced-it] [SkipRecovery] Restore randomized window aggregation coverage|
|[#15433](https://github.com/NVIDIA/cudf-spark/pull/15433)|[AutoSparkUT] Migrate five Spark SQL suites [fast-ut] [reduced-it]|
|[#15460](https://github.com/NVIDIA/cudf-spark/pull/15460)|[fast-ut] [reduced-it] [SkipRecovery] Preserve Delta DELETE file statistics|
|[#15458](https://github.com/NVIDIA/cudf-spark/pull/15458)|[fast-ut] [reduced-it] [SkipRecovery] Restore float struct join coverage|
|[#15459](https://github.com/NVIDIA/cudf-spark/pull/15459)|Record release/26.08 merge ancestry [skip ci]|
|[#15457](https://github.com/NVIDIA/cudf-spark/pull/15457)|[FEA] [fast-ut] [reduced-it] Remove unreachable cost branch from RapidsMeta.getIndicatorChar|
|[#15393](https://github.com/NVIDIA/cudf-spark/pull/15393)|[SKILLS] Upgrade plugin version in skills to 26.06 |
|[#15434](https://github.com/NVIDIA/cudf-spark/pull/15434)|Merge latest release/26.08 into main [skip ci]|
|[#15448](https://github.com/NVIDIA/cudf-spark/pull/15448)|[AutoSparkUT] Migrate four Spark SQL suites [fast-ut] [reduced-it]|
|[#15423](https://github.com/NVIDIA/cudf-spark/pull/15423)|Merge release/26.08 into main|
|[#15075](https://github.com/NVIDIA/cudf-spark/pull/15075)|Remove inaccessible and unused AccessibleArrowColumnVector|
|[#15130](https://github.com/NVIDIA/cudf-spark/pull/15130)|GCS PerfIO Auto-Enable|
|[#15273](https://github.com/NVIDIA/cudf-spark/pull/15273)|[SkipRecovery] Restore decimal cast data generation|
|[#15269](https://github.com/NVIDIA/cudf-spark/pull/15269)|[SkipRecovery] Re-enable CSV null parsing test|
|[#15294](https://github.com/NVIDIA/cudf-spark/pull/15294)|Add opt-in parallel unit test runner to speedup premerge [fast-ut] [reduced-it]|
|[#15399](https://github.com/NVIDIA/cudf-spark/pull/15399)|Update dependency version JNI, private, hybrid to 26.10.0-SNAPSHOT|
|[#15371](https://github.com/NVIDIA/cudf-spark/pull/15371)|Reduce premerge integration test combinations [reduced-it]|
|[#15387](https://github.com/NVIDIA/cudf-spark/pull/15387)|Bump up version to 26.10|

## Release 26.08

### Features
|||
|:---|:---|
|[#15263](https://github.com/NVIDIA/cudf-spark/issues/15263)|[FEA] Delta Lake DB-17.3: Enable GPU data-file writes for managed CTAS/RTAS|
|[#10159](https://github.com/NVIDIA/cudf-spark/issues/10159)|[FEA] provide configuration to automatically set spark.shuffle.manager|
|[#15272](https://github.com/NVIDIA/cudf-spark/issues/15272)|[FEA] Add support for Apache Spark 3.5.9|
|[#15168](https://github.com/NVIDIA/cudf-spark/issues/15168)|[FEA] Remove shim for Databricks 13.3|
|[#15270](https://github.com/NVIDIA/cudf-spark/issues/15270)|[FEA] Add support for Apache Spark 4.0.4|
|[#15271](https://github.com/NVIDIA/cudf-spark/issues/15271)|[FEA] Add support for Apache Spark 4.1.3|
|[#14599](https://github.com/NVIDIA/cudf-spark/issues/14599)|[FEA] Delta Lake DB-17.3: Enable GPU OPTIMIZE + auto-compaction|
|[#14624](https://github.com/NVIDIA/cudf-spark/issues/14624)|[FEA] Add support for Apache Spark 4.2.0|
|[#14960](https://github.com/NVIDIA/cudf-spark/issues/14960)|[FEA] Support multiple order-by columns for RANGE window functions|
|[#14853](https://github.com/NVIDIA/cudf-spark/issues/14853)|[FEA] Add support for Apache Iceberg 1.11|
|[#13649](https://github.com/NVIDIA/cudf-spark/issues/13649)|[FEA] BinaryType support for HostColumnarToGpu|
|[#15065](https://github.com/NVIDIA/cudf-spark/issues/15065)|[FEA] Add support for Apache Spark 4.0.3|
|[#14832](https://github.com/NVIDIA/cudf-spark/issues/14832)|[FEA] Add support for Spark 4.1.2|

### Performance
|||
|:---|:---|
|[#14868](https://github.com/NVIDIA/cudf-spark/issues/14868)|[FEA][Follow-up] Emit multiple batches from GpuProjectExec split-retry instead of concatenating|

### Bugs Fixed
|||
|:---|:---|
|[#15449](https://github.com/NVIDIA/cudf-spark/issues/15449)|[BUG] ORC timestamp reads produce incorrect results in non-UTC DST timezones|
|[#15499](https://github.com/NVIDIA/cudf-spark/issues/15499)|[BUG] RapidsShuffleThreadedWriterSuite leaks host buffers in Spark 340 focused run|
|[#14731](https://github.com/NVIDIA/cudf-spark/issues/14731)|[AUDIT 4.2] [SPARK-54830][CORE] Enable checksum based indeterminate shuffle retry by default|
|[#15394](https://github.com/NVIDIA/cudf-spark/issues/15394)|[BUG] Spark 4 Delta RTAS fails on GPU because staged table lacks TRUNCATE support|
|[#14741](https://github.com/NVIDIA/cudf-spark/issues/14741)|[BUG] regexp_replace does not validate replacement backref ranges; out-of-range `$N` silently substitutes empty where Spark CPU throws|
|[#15390](https://github.com/NVIDIA/cudf-spark/issues/15390)|[BUG] Spark 3.5.9 package build cannot resolve CreateNamedStructShims|
|[#15234](https://github.com/NVIDIA/cudf-spark/issues/15234)|[BUG] Delta merge into write falls back from GPU due to unsupported CheckOverflowInTableWrite on Databricks 17.3|
|[#15317](https://github.com/NVIDIA/cudf-spark/issues/15317)|[AI-AUDIT] Harden GPU ORC Reader close under interrupt like SPARK-57958|
|[#15318](https://github.com/NVIDIA/cudf-spark/issues/15318)|[AI-AUDIT] Mirror SPARK-56045 Parquet UNKNOWN annotation config in GPU schema clipping|
|[#15293](https://github.com/NVIDIA/cudf-spark/issues/15293)|[BUG] string split anchor fuzz test fails with cuDF Glushkov fast path|
|[#14744](https://github.com/NVIDIA/cudf-spark/issues/14744)|[BUG] Transpiler truncates supplementary codepoints (`\\x{1F600}` becomes U+F600); silent wrong matches for non-BMP characters|
|[#15004](https://github.com/NVIDIA/cudf-spark/issues/15004)|[BUG] GPU Parquet writing has a different statistics of the row group when a column has NaN value|
|[#15316](https://github.com/NVIDIA/cudf-spark/issues/15316)|[AI-AUDIT] Mirror SPARK-57736 null-safe field names in GpuCreateNamedStruct.dataType|
|[#14484](https://github.com/NVIDIA/cudf-spark/issues/14484)|[AI-AUDIT] Update GPU Python runners for runnerConf protocol change (SPARK-54615)|
|[#15325](https://github.com/NVIDIA/cudf-spark/issues/15325)|[BUG] Multithreaded shuffle merge fails with IndexOutOfBounds for partial files >2g|
|[#15226](https://github.com/NVIDIA/cudf-spark/issues/15226)|[BUG] Spark 4 AQE planning can construct GPU scans with a null SparkSession|
|[#15256](https://github.com/NVIDIA/cudf-spark/issues/15256)|[BUG] test_parquet_interleaved_file_splits_partition_value_alignment fails with OSError: HDFS connection failed (CLASSPATH not set)|
|[#15274](https://github.com/NVIDIA/cudf-spark/issues/15274)|[BUG] Iceberg REST catalog IT (Spark 3.5.0): 38 write tests fail because Parquet codec 'gzip' is not supported by GPU writer|
|[#15287](https://github.com/NVIDIA/cudf-spark/issues/15287)|[BUG] test_regexp_replace_trailing_backslash_throws tests failing on premerge-CI on Databricks|
|[#15122](https://github.com/NVIDIA/cudf-spark/issues/15122)|[non-BMP regex patterns] - GPU Execution Issue|
|[#15275](https://github.com/NVIDIA/cudf-spark/issues/15275)|[BUG] CsvScanForIntervalSuite: castStringToDTInterval tests fail (sign inversion & null mismatch) across all Spark shims|
|[#13723](https://github.com/NVIDIA/cudf-spark/issues/13723)|[BUG] cuda illegal memory access error while reading parquet files|
|[#15266](https://github.com/NVIDIA/cudf-spark/issues/15266)|[Bug] `GpuRowToColumnarExec` omits terminal LIST offset, causing spill to corrupt batch|
|[#15244](https://github.com/NVIDIA/cudf-spark/issues/15244)|[BUG] Changelog generator excludes PRs when commit messages contain bot co-author trailers|
|[#14742](https://github.com/NVIDIA/cudf-spark/issues/14742)|[BUG] Replacement-string parser diverges from Java spec in five places: `\\N` as backref, trailing `\\`, bare `$X`, and malformed `${...}`|
|[#14747](https://github.com/NVIDIA/cudf-spark/issues/14747)|[BUG] GpuRegExpUtils.getChoicesFromRegex flattens mixed sequences; `foo(cat|dog)` is treated as the character set `{f,o,cat,dog}` and replaced character-wise|
|[#15203](https://github.com/NVIDIA/cudf-spark/issues/15203)|[BUG] test_delta_dv_cpu_bridge_filter_after_native_scan fails: 'Part of the plan is not columnar class FilterExec'|
|[#14737](https://github.com/NVIDIA/cudf-spark/issues/14737)|[BUG] updateGroupsForExtract misses arms for RegexChoice and non-capturing RegexGroup; regexp_extract on `(a)|(b)` returns the wrong group|
|[#15231](https://github.com/NVIDIA/cudf-spark/issues/15231)|[BUG] 3 test_from_json_allow_unquoted_control_chars* integration tests failed in pre merge|
|[#15144](https://github.com/NVIDIA/cudf-spark/issues/15144)|[regex] RegexParser.countCaptureGroups omits RegexChoice — capture-group undercount|
|[#14735](https://github.com/NVIDIA/cudf-spark/issues/14735)|[BUG] CudfRegexTranspiler.countCaptureGroups misses arms for `RegexChoice` and `RegexRepetition`; replacement-string semantics wrong for very common patterns|
|[#14745](https://github.com/NVIDIA/cudf-spark/issues/14745)|[BUG] CudfRegexTranspiler.rewrite does not recurse into RegexCharacterRange endpoints; non-BMP / non-ASCII range endpoints get the wrong match|
|[#15205](https://github.com/NVIDIA/cudf-spark/issues/15205)|[BUG] Nightly Scala 2.13 IT: test_collate_expr_fallback failed on Spark 4.x (ProjectExec not columnar)|
|[#14739](https://github.com/NVIDIA/cudf-spark/issues/14739)|[BUG] RegexParser.parseHexDigit greedily consumes more than 2 hex digits for non-braced `\\xNN`; valid patterns rejected|
|[#10350](https://github.com/NVIDIA/cudf-spark/issues/10350)|[BUG] Plugin shutdown should catch exceptions from subcomponent shutdown|
|[#14748](https://github.com/NVIDIA/cudf-spark/issues/14748)|[BUG] transpileToSplittableString treats top-level `\\b` as literal backspace U+0008 instead of word boundary; `regexp_replace(..., '\\b', ...)` and `split(..., '\\b')` produce wrong results|
|[#15006](https://github.com/NVIDIA/cudf-spark/issues/15006)|Drop the (\r\n)?$ regex line-anchor workaround in RegexParser once cuDF #22763 (CRLF EOL) lands|
|[#15118](https://github.com/NVIDIA/cudf-spark/issues/15118)|[BUG] to_json on GPU emits unquoted NaN for float/double values|
|[#15093](https://github.com/NVIDIA/cudf-spark/issues/15093)|[BUG] Delta Lake integration tests fail with NoClassDefFoundError: Could not initialize class DelegatingLogStore$ (Spark 3.3.0 / Ubuntu 24.04)|
|[#15098](https://github.com/NVIDIA/cudf-spark/issues/15098)|[BUG] Iceberg REST catalog integration tests fail: java.lang.IllegalArgumentException: 'Part of the plan is not columnar' for V2 write execs|
|[#15005](https://github.com/NVIDIA/cudf-spark/issues/15005)|[BUG] NDS hang: all task slots on some executors blocked until SparkContext timeout|
|[#14967](https://github.com/NVIDIA/cudf-spark/issues/14967)|[BUG] Int truncation: GpuPartitioning serialized buffer position/length .toInt (#14471)|
|[#14926](https://github.com/NVIDIA/cudf-spark/issues/14926)|[BUG] regexp_replace: user $N backrefs not remapped after the synthetic $ line-anchor group ((a$|b)(c), T$|(E) produce wrong output)|
|[#15020](https://github.com/NVIDIA/cudf-spark/issues/15020)|[BUG] The script build/make-scala-version-build-files.sh fails while regenerating scala2.13/*.pom.xml files|
|[#15062](https://github.com/NVIDIA/cudf-spark/issues/15062)|[BUG] Main branch build fails: GpuJsonToStructs.scala compile error - JSONUtils.FromJSONResult vs ColumnVector type mismatch|
|[#14996](https://github.com/NVIDIA/cudf-spark/issues/14996)|RTCX failure loading nvJitLink/nvrtc in AST CompiledExpression tests across multiple Spark shims|
|[#14574](https://github.com/NVIDIA/cudf-spark/issues/14574)|[BUG] PERFILE reader skips deletion vector filtering for zero-column scans|
|[#14972](https://github.com/NVIDIA/cudf-spark/issues/14972)|[BUG] Spark SQL UI / History Server shows pre-AQE CPU plan for GPU plans (AQE final plan not reflected); GPU V2 write child operators missing|
|[#14905](https://github.com/NVIDIA/cudf-spark/issues/14905)|[Iceberg][BUG] GPU Iceberg Parquet writer uses spark.sql.parquet.compression.codec; CPU Iceberg does not|
|[#14582](https://github.com/NVIDIA/cudf-spark/issues/14582)|[BUG] Databricks nightly CI: test_buckets OOM failure (CPU) on DB 17.3|
|[#14743](https://github.com/NVIDIA/cudf-spark/issues/14743)|[BUG] GpuRegExpUtils.backrefConversion consumes too many digits; `regexp_replace` mishandles `$N` followed by literal digits|

### PRs
|||
|:---|:---|
|[#15752](https://github.com/NVIDIA/cudf-spark/pull/15752)|Fix class-initialization deadlock between GpuOverrides and SparkShimImpl (26.08)|
|[#15754](https://github.com/NVIDIA/cudf-spark/pull/15754)|[DOC] Update download links for v26.08.1 [skip ci]|
|[#15678](https://github.com/NVIDIA/cudf-spark/pull/15678)|Exclude AWS Netty jars from Spark 4.1 S3Tables classpath|
|[#15673](https://github.com/NVIDIA/cudf-spark/pull/15673)|Update changelog for the v26.08 release [skip ci]|
|[#15672](https://github.com/NVIDIA/cudf-spark/pull/15672)|Update dependency version private to 26.08.1 [skip ci]|
|[#15667](https://github.com/NVIDIA/cudf-spark/pull/15667)|Clarify Databricks shuffle config and fix install link [skip ci]|
|[#15654](https://github.com/NVIDIA/cudf-spark/pull/15654)|Fix Iceberg support for Spark 4.1.2 and 4.1.3 [fast-ut]|
|[#15548](https://github.com/NVIDIA/cudf-spark/pull/15548)|Update changelog for the v26.08 release [skip ci]|
|[#15595](https://github.com/NVIDIA/cudf-spark/pull/15595)|Stabilize AQE SMJ-to-BHJ local-shuffle-reader unit test [fast-ut] [reduced-ci]|
|[#15547](https://github.com/NVIDIA/cudf-spark/pull/15547)|Update dependency version JNI, private, hybrid to 26.08.0|
|[#15599](https://github.com/NVIDIA/cudf-spark/pull/15599)|Fix mergeIdenticalProjects dropping alias-producing GpuProjects in DV predicate pushdown|
|[#15597](https://github.com/NVIDIA/cudf-spark/pull/15597)|Preserve AQE coalesced hash partition boundaries|
|[#15577](https://github.com/NVIDIA/cudf-spark/pull/15577)|[BUG] Fix Iceberg 1.9 constant conversion IllegalAccessError|
|[#15450](https://github.com/NVIDIA/cudf-spark/pull/15450)|Fix non-UTC ORC timestamp read correctness  [fast-ut] [reduced-it]|
|[#15555](https://github.com/NVIDIA/cudf-spark/pull/15555)|[BUG] Preserve GroupPartitionsExec CPU fallback partitioning|
|[#15544](https://github.com/NVIDIA/cudf-spark/pull/15544)|Fall back to CPU for to_json sortKeys|
|[#15509](https://github.com/NVIDIA/cudf-spark/pull/15509)|[DOC] update download page for 26.08 release [skip ci]|
|[#15435](https://github.com/NVIDIA/cudf-spark/pull/15435)|Fix Iceberg S3 PerfIO access with split classloaders|
|[#15518](https://github.com/NVIDIA/cudf-spark/pull/15518)|Avoid shell command injection in databricks CI scripts [fast-ut][reduced-it]|
|[#15501](https://github.com/NVIDIA/cudf-spark/pull/15501)|[BUG] Fix SpillablePartialFileHandle host memory leak seen in tests only|
|[#15397](https://github.com/NVIDIA/cudf-spark/pull/15397)|Preserve partial clustering across Spark versions|
|[#15429](https://github.com/NVIDIA/cudf-spark/pull/15429)|Pass DSv2 WriteSummary from GPU MERGE commits|
|[#15476](https://github.com/NVIDIA/cudf-spark/pull/15476)|Fix Scala 2.12 eta-expansion for verifyParquetMagic|
|[#15378](https://github.com/NVIDIA/cudf-spark/pull/15378)|Checksum enable fallback fixes for Spark 4.2|
|[#15462](https://github.com/NVIDIA/cudf-spark/pull/15462)|DV read tests with cdf should run with spark 353+|
|[#15384](https://github.com/NVIDIA/cudf-spark/pull/15384)|Enable optimized S3 tail reads for Iceberg Parquet footers|
|[#15428](https://github.com/NVIDIA/cudf-spark/pull/15428)|Fix Parquet UNKNOWN annotation IT writes on Dataproc|
|[#15455](https://github.com/NVIDIA/cudf-spark/pull/15455)|Fix Spark 4.2 collect_set float buffer conversion for mixed aggs|
|[#15420](https://github.com/NVIDIA/cudf-spark/pull/15420)|Skip Dataproc shuffle manager auto-configuration|
|[#15416](https://github.com/NVIDIA/cudf-spark/pull/15416)|Match Spark 4.2 date_trunc overflow at Long.MinValue|
|[#15411](https://github.com/NVIDIA/cudf-spark/pull/15411)|Fix OSS Delta RTAS on Spark 4.x+|
|[#15368](https://github.com/NVIDIA/cudf-spark/pull/15368)|Support IF_NOT_CONTAINED filter type and loading inline deletion vectors for OSS delta|
|[#15422](https://github.com/NVIDIA/cudf-spark/pull/15422)|  [skip ci] Fix Iceberg REST S3 path regression coverage|
|[#15413](https://github.com/NVIDIA/cudf-spark/pull/15413)|Preserve BroadcastHashJoin isSkewJoin in GPU plan display|
|[#15415](https://github.com/NVIDIA/cudf-spark/pull/15415)|Allow WriteFilesExec fallback for non-UTC ORC writes|
|[#15366](https://github.com/NVIDIA/cudf-spark/pull/15366)|CheckOverflowInTableWrite Support|
|[#15114](https://github.com/NVIDIA/cudf-spark/pull/15114)|Documentation updates for RAPIDS for Apache Spark -> NVIDIA cuDF plugin for Apache Spark rename|
|[#15396](https://github.com/NVIDIA/cudf-spark/pull/15396)|Harden GPU ORC reader close under interrupt|
|[#15408](https://github.com/NVIDIA/cudf-spark/pull/15408)|Add the missing DeletionVectorInfo constructor parameter|
|[#15376](https://github.com/NVIDIA/cudf-spark/pull/15376)|Mirror SPARK-56045 Parquet UNKNOWN annotation in GPU schema clipping|
|[#15360](https://github.com/NVIDIA/cudf-spark/pull/15360)|Fix double escaping of Iceberg S3 input-file URIs|
|[#15320](https://github.com/NVIDIA/cudf-spark/pull/15320)|Add DBR 17.3 Delta CTAS/RTAS support and fix optimized writes|
|[#15388](https://github.com/NVIDIA/cudf-spark/pull/15388)|Add Spark 3.5.9 support for CreateNamedStruct shims|
|[#14544](https://github.com/NVIDIA/cudf-spark/pull/14544)|Support DST timezones conversion for ORC|
|[#15372](https://github.com/NVIDIA/cudf-spark/pull/15372)|Support collect_set RESPECT NULLS|
|[#15285](https://github.com/NVIDIA/cudf-spark/pull/15285)|Auto-configure the RAPIDS shuffle manager|
|[#15286](https://github.com/NVIDIA/cudf-spark/pull/15286)|Add support for Apache Spark 3.5.9|
|[#15381](https://github.com/NVIDIA/cudf-spark/pull/15381)|Re-enable string split anchor fuzz test|
|[#14869](https://github.com/NVIDIA/cudf-spark/pull/14869)|[BUG] Fix regex parser truncating supplementary codepoints in \\x{...} escapes|
|[#15375](https://github.com/NVIDIA/cudf-spark/pull/15375)|Enable unwrap cast max literal test|
|[#15358](https://github.com/NVIDIA/cudf-spark/pull/15358)|Fix named struct dataType null field names|
|[#15276](https://github.com/NVIDIA/cudf-spark/pull/15276)|Remove Databricks 13.3 shim support|
|[#15331](https://github.com/NVIDIA/cudf-spark/pull/15331)|Refactor regex group parsing and explicitly reject unsupported group types|
|[#15355](https://github.com/NVIDIA/cudf-spark/pull/15355)|Support quantified \D and \W in regex patterns|
|[#15327](https://github.com/NVIDIA/cudf-spark/pull/15327)|Fix SpillablePartialFileHandle read overflow when written more than 2GB|
|[#15322](https://github.com/NVIDIA/cudf-spark/pull/15322)|Handle collect_set signed zeros by Scala version|
|[#15313](https://github.com/NVIDIA/cudf-spark/pull/15313)|Add Spark 4.0.4 shim support|
|[#15208](https://github.com/NVIDIA/cudf-spark/pull/15208)|Prevent multithreaded shuffle merger deadlock|
|[#15310](https://github.com/NVIDIA/cudf-spark/pull/15310)|Add Spark 4.1.3 shim support|
|[#15303](https://github.com/NVIDIA/cudf-spark/pull/15303)|[BUG] Parse Java lookahead groups as (?=) and (?!)|
|[#15252](https://github.com/NVIDIA/cudf-spark/pull/15252)|[BUG] Trigger liquid clustering in Delta Lake integration tests|
|[#15278](https://github.com/NVIDIA/cudf-spark/pull/15278)|Add DBR 17.3 Delta liquid clustering support|
|[#15302](https://github.com/NVIDIA/cudf-spark/pull/15302)|Align opportunistic PerfIO S3 enablement|
|[#15277](https://github.com/NVIDIA/cudf-spark/pull/15277)|[BUG] Run GPU AQE planning with registered SparkSession|
|[#15279](https://github.com/NVIDIA/cudf-spark/pull/15279)|Add Spark 4.2 shim support|
|[#15301](https://github.com/NVIDIA/cudf-spark/pull/15301)|Normalize blossom-ci allowlist [skip ci]|
|[#15289](https://github.com/NVIDIA/cudf-spark/pull/15289)|Fix test_parquet_interleaved_file_splits_partition_value_alignment on GCS again|
|[#15153](https://github.com/NVIDIA/cudf-spark/pull/15153)|Add skipped-path coverage for skewed BHJ private optimizer|
|[#15295](https://github.com/NVIDIA/cudf-spark/pull/15295)|Fix Iceberg REST compression defaults|
|[#15290](https://github.com/NVIDIA/cudf-spark/pull/15290)|Fix regexp_replace no-op when '+' is the only metacharacter|
|[#15296](https://github.com/NVIDIA/cudf-spark/pull/15296)|Append new authorized user to blossom-ci allowlist [skip ci]|
|[#15258](https://github.com/NVIDIA/cudf-spark/pull/15258)|[SkipRecovery] Re-enable Spark 3.5 Hive simple UDF test|
|[#15297](https://github.com/NVIDIA/cudf-spark/pull/15297)|[DOC] update download page for 26.06.1 release [skip ci]|
|[#15291](https://github.com/NVIDIA/cudf-spark/pull/15291)|Fix regexp_replace error assertions on Databricks|
|[#14961](https://github.com/NVIDIA/cudf-spark/pull/14961)|Support for multi orderby columns for RANGE window functions|
|[#15210](https://github.com/NVIDIA/cudf-spark/pull/15210)|[AutoSparkUT] Fix ORC reads with missing nested fields|
|[#15280](https://github.com/NVIDIA/cudf-spark/pull/15280)|[BUG] Handle null regex captures in interval and regexp_extract_all|
|[#15267](https://github.com/NVIDIA/cudf-spark/pull/15267)|Fix missing terminal list offset in `GpuRowToColumnarExec#fillBatch`|
|[#15260](https://github.com/NVIDIA/cudf-spark/pull/15260)|[AutoSparkUT] Re-enable Iceberg delete fallback test|
|[#15261](https://github.com/NVIDIA/cudf-spark/pull/15261)|Fix changelog filtering for bot co-author trailers [skip ci]|
|[#15259](https://github.com/NVIDIA/cudf-spark/pull/15259)|[AutoSparkUT] Re-enable escaped json_tuple test|
|[#14862](https://github.com/NVIDIA/cudf-spark/pull/14862)|[BUG] Fix regex replacement-string parser Java spec gaps|
|[#15262](https://github.com/NVIDIA/cudf-spark/pull/15262)|[cudf-udf]: fix conda dependency resolution and CUDA header discovery [skip test]|
|[#15236](https://github.com/NVIDIA/cudf-spark/pull/15236)|Fix allow_non_gpu_conditional to gate allowances on its condition|
|[#15196](https://github.com/NVIDIA/cudf-spark/pull/15196)|NVSkills Request CI Workflow [skip ci]|
|[#15134](https://github.com/NVIDIA/cudf-spark/pull/15134)|Add support for output `MapType[StringType, ArrayType[StringType]]` in `from_json` SQL function|
|[#15227](https://github.com/NVIDIA/cudf-spark/pull/15227)|Limit collate ProjectExec fallback to Spark 4.0.x|
|[#15242](https://github.com/NVIDIA/cudf-spark/pull/15242)|Use configured copy buffer for Hadoop vectored reads|
|[#15246](https://github.com/NVIDIA/cudf-spark/pull/15246)|[AutoSparkUT] Recover JSON timestamp fallback tests|
|[#15191](https://github.com/NVIDIA/cudf-spark/pull/15191)|[BUG] Validate repeated regex choices|
|[#15241](https://github.com/NVIDIA/cudf-spark/pull/15241)|Add Skills premerge pipeline|
|[#14939](https://github.com/NVIDIA/cudf-spark/pull/14939)|[AutoSparkUT] Un-skip approx_percentile tests (#13049 follow-up; isolate #14634)|
|[#15190](https://github.com/NVIDIA/cudf-spark/pull/15190)|[BUG] Preserve regex sequence semantics in multi-replace|
|[#15255](https://github.com/NVIDIA/cudf-spark/pull/15255)|Revert "[AutoSparkUT] Fix Parquet reads with empty nested schemas (#15209)"|
|[#15225](https://github.com/NVIDIA/cudf-spark/pull/15225)|Add FilterExec to the allow_non_gpu list for test_delta_dv_cpu_bridge_filter_after_native_scan|
|[#15250](https://github.com/NVIDIA/cudf-spark/pull/15250)|[BUG] Enable YearMonthInterval arithmetic on Databricks|
|[#15209](https://github.com/NVIDIA/cudf-spark/pull/15209)|[AutoSparkUT] Fix Parquet reads with empty nested schemas|
|[#15229](https://github.com/NVIDIA/cudf-spark/pull/15229)|[AutoSparkUT] Recover repeated JSON array cases|
|[#15192](https://github.com/NVIDIA/cudf-spark/pull/15192)|[BUG] Preserve regex extract capture-group indexing|
|[#15249](https://github.com/NVIDIA/cudf-spark/pull/15249)|Revert "[Coverage] Add YearMonthInterval multiply/divide IT parallel to DayTime" [skip ci]|
|[#15243](https://github.com/NVIDIA/cudf-spark/pull/15243)|Revert "Add protobuf integration-test dependency infrastructure (plugin-0)" [skip ci]|
|[#15215](https://github.com/NVIDIA/cudf-spark/pull/15215)|Fix test_bloom_filter_join_cpu_probe failures on Dataproc|
|[#15193](https://github.com/NVIDIA/cudf-spark/pull/15193)|[BUG] Treat anchors in regex character classes as literals|
|[#14938](https://github.com/NVIDIA/cudf-spark/pull/14938)|[Coverage] Add YearMonthInterval multiply/divide IT parallel to DayTime|
|[#14940](https://github.com/NVIDIA/cudf-spark/pull/14940)|[Coverage] Exercise uncovered CPU bridge paths|
|[#14877](https://github.com/NVIDIA/cudf-spark/pull/14877)|Emit multiple batches from GpuProjectExec split-retry instead of concatenating|
|[#14958](https://github.com/NVIDIA/cudf-spark/pull/14958)|[Coverage] Cover CudfUnsafeRowBase primitive-type getter arms|
|[#14885](https://github.com/NVIDIA/cudf-spark/pull/14885)|Add protobuf integration-test dependency infrastructure (plugin-0)|
|[#15158](https://github.com/NVIDIA/cudf-spark/pull/15158)|Remove obsolete is_before_spark_330 integration test guards|
|[#15214](https://github.com/NVIDIA/cudf-spark/pull/15214)|Allow collate bridge fallback in Spark 4.x tests|
|[#15207](https://github.com/NVIDIA/cudf-spark/pull/15207)|Fix Spark 4.x JSON ProjectExec test allowlist|
|[#15140](https://github.com/NVIDIA/cudf-spark/pull/15140)|Add pre-merge CI and Docker image for skill integration tests [skip ci]|
|[#15188](https://github.com/NVIDIA/cudf-spark/pull/15188)|[AutoSparkUT]Add RAPIDS SQL core migrated suites|
|[#14860](https://github.com/NVIDIA/cudf-spark/pull/14860)|[BUG] Fix RegexParser.parseHexDigit greedy consumption of non-braced \xNN|
|[#15198](https://github.com/NVIDIA/cudf-spark/pull/15198)|Fix structs_to_json fallback tests for Spark 4.x|
|[#15174](https://github.com/NVIDIA/cudf-spark/pull/15174)|Support Iceberg 1.11 on Spark 4.0.2 and 4.0.3|
|[#15200](https://github.com/NVIDIA/cudf-spark/pull/15200)|Suppress JSON map parsing deprecation warning [skip ci]|
|[#15061](https://github.com/NVIDIA/cudf-spark/pull/15061)|Fuse array higher-order functions in Project|
|[#15185](https://github.com/NVIDIA/cudf-spark/pull/15185)|Fix DBR 17.3 build after SessionCatalog partition API change|
|[#15131](https://github.com/NVIDIA/cudf-spark/pull/15131)|Add integration tests for skill templates [skip ci]|
|[#15162](https://github.com/NVIDIA/cudf-spark/pull/15162)|[AutoSparkUT] Fix V2 GPU scan sameResult equality|
|[#15159](https://github.com/NVIDIA/cudf-spark/pull/15159)|Harden plugin shutdown to run all steps on failure|
|[#15170](https://github.com/NVIDIA/cudf-spark/pull/15170)|Fix legacy timestamp fallback test with CPU bridge|
|[#15165](https://github.com/NVIDIA/cudf-spark/pull/15165)|[BUG] Fix regex transpiler corner case: split word boundaries (#14748)|
|[#15175](https://github.com/NVIDIA/cudf-spark/pull/15175)|Update license check for skill source files [skip ci]|
|[#14883](https://github.com/NVIDIA/cudf-spark/pull/14883)|Iceberg 1.11 support for Spark 411, part (3/3): accelerate SparkIncrementalAppendScan on GPU|
|[#14132](https://github.com/NVIDIA/cudf-spark/pull/14132)|Add Full GPU CPU Bridge Support|
|[#15143](https://github.com/NVIDIA/cudf-spark/pull/15143)|Support LEGACY millisecond timestamp formatting|
|[#15023](https://github.com/NVIDIA/cudf-spark/pull/15023)|Drop the regex line-anchor CRLF workaround now that cuDF #22763 landed|
|[#15003](https://github.com/NVIDIA/cudf-spark/pull/15003)|[Coverage] Scala UT for RapidsHostColumnBuilder nested-append, restoreState, and GpuExplode elementSchema|
|[#14993](https://github.com/NVIDIA/cudf-spark/pull/14993)|[Coverage] Scala UT for CoalescedBatchPartitioner, HostByteBufferIterator, GpuSerializableBatch|
|[#14992](https://github.com/NVIDIA/cudf-spark/pull/14992)|[Coverage] Cover copy shuffle-compression codec in GpuPartitioningSuite|
|[#15103](https://github.com/NVIDIA/cudf-spark/pull/15103)|[AutoSparkUT] Fix ORC coalescing ignoreMissingFiles|
|[#15157](https://github.com/NVIDIA/cudf-spark/pull/15157)|Balance Databricks CI test split|
|[#15151](https://github.com/NVIDIA/cudf-spark/pull/15151)|Add Spark 4.0.3 shim support|
|[#15160](https://github.com/NVIDIA/cudf-spark/pull/15160)|Fix Iceberg class packaging across shims|
|[#15149](https://github.com/NVIDIA/cudf-spark/pull/15149)|Support array and map argument in array_aggregate|
|[#15115](https://github.com/NVIDIA/cudf-spark/pull/15115)|Fix map type alignment and add deep comparison in assertDataFrameEquals [skip ci]|
|[#15071](https://github.com/NVIDIA/cudf-spark/pull/15071)|Add Spark 4.1.2 shim support|
|[#15138](https://github.com/NVIDIA/cudf-spark/pull/15138)|Fix PyArrow timestamp inference for Spark 3.3.4|
|[#15146](https://github.com/NVIDIA/cudf-spark/pull/15146)|Skip skewed BHJ marker test on all Databricks runtimes|
|[#15148](https://github.com/NVIDIA/cudf-spark/pull/15148)|Enable license header check for Skills [skip ci]|
|[#15124](https://github.com/NVIDIA/cudf-spark/pull/15124)|Quote non-finite floating point values in to_json|
|[#15116](https://github.com/NVIDIA/cudf-spark/pull/15116)|Misc cleanups for error handling, naming/signatures, and partitioning in skill templates [skip ci]|
|[#15121](https://github.com/NVIDIA/cudf-spark/pull/15121)|Allow foldable non-literal `Coalesce` to run on GPU|
|[#15113](https://github.com/NVIDIA/cudf-spark/pull/15113)|Use supported sort_array expr instead of array_sort in UDF example [skip ci]|
|[#14882](https://github.com/NVIDIA/cudf-spark/pull/14882)|Iceberg 1.11 support for Spark 411, part (2/3): add iceberg-1-11-x module|
|[#15126](https://github.com/NVIDIA/cudf-spark/pull/15126)|Fix GPU V2 write AQE metrics|
|[#15133](https://github.com/NVIDIA/cudf-spark/pull/15133)|Add explicit Delta storage dependency to tests|
|[#15139](https://github.com/NVIDIA/cudf-spark/pull/15139)|Set Iceberg REST write compression defaults|
|[#15137](https://github.com/NVIDIA/cudf-spark/pull/15137)|[BUG] Skip skewed BHJ marker test on DB 17.x|
|[#14907](https://github.com/NVIDIA/cudf-spark/pull/14907)|[Coverage] Widen shim json-lines on 15 existing test suites to cover Spark 3.5+|
|[#15132](https://github.com/NVIDIA/cudf-spark/pull/15132)|[DOC] update Iceberg scan options wording [skip ci]|
|[#15104](https://github.com/NVIDIA/cudf-spark/pull/15104)|Fix deadlock of RMM pool waits for task threads|
|[#15108](https://github.com/NVIDIA/cudf-spark/pull/15108)|Add GPU support for `array_sort` with the default comparator|
|[#14974](https://github.com/NVIDIA/cudf-spark/pull/14974)|[BUG] Fail fast on >2GB GPU-serialized shuffle batch instead of truncating slice offsets (#14967)|
|[#15074](https://github.com/NVIDIA/cudf-spark/pull/15074)|Make shared-scan optimizer test use a structural marker|
|[#15076](https://github.com/NVIDIA/cudf-spark/pull/15076)|Add Greptile rule to flag missing databricks CI tag on test changes|
|[#15106](https://github.com/NVIDIA/cudf-spark/pull/15106)|Add some RAPIDS migrated SQL core test suites|
|[#15111](https://github.com/NVIDIA/cudf-spark/pull/15111)|Deduplicate Java/Scala template projects in skills [skip ci]|
|[#15039](https://github.com/NVIDIA/cudf-spark/pull/15039)|GCS Range Copier|
|[#15102](https://github.com/NVIDIA/cudf-spark/pull/15102)|Update NVIDIA Pages links [skip ci]|
|[#15099](https://github.com/NVIDIA/cudf-spark/pull/15099)|Skip test_bit_count[Boolean] under Spark testing mode before Spark 4.0.0 (SPARK-48128)|
|[#15096](https://github.com/NVIDIA/cudf-spark/pull/15096)|Append my id to blossom-ci list [skip ci]|
|[#14878](https://github.com/NVIDIA/cudf-spark/pull/14878)|Expose cuDF Parquet writer dictionary configs|
|[#15058](https://github.com/NVIDIA/cudf-spark/pull/15058)|Publish UDF agent skills [skip ci]|
|[#15089](https://github.com/NVIDIA/cudf-spark/pull/15089)|Render GPU operator metrics for V2 table writes in the SQL UI|
|[#15087](https://github.com/NVIDIA/cudf-spark/pull/15087)|Update link check config for cudf-spark rename [skip ci]|
|[#15070](https://github.com/NVIDIA/cudf-spark/pull/15070)|Add more suites from Spark UT|
|[#15022](https://github.com/NVIDIA/cudf-spark/pull/15022)|Run nightly integration tests with Spark testing mode enabled|
|[#15085](https://github.com/NVIDIA/cudf-spark/pull/15085)|Fix auto merge conflict 15081 [skip ci]|
|[#15021](https://github.com/NVIDIA/cudf-spark/pull/15021)|Avoid Maven when generating Scala 2.13 POMs|
|[#15012](https://github.com/NVIDIA/cudf-spark/pull/15012)|Fix parquet partition verification on GCS paths|
|[#15019](https://github.com/NVIDIA/cudf-spark/pull/15019)|Fix flaky iceberg test_v2_write_sql_ui_shows_gpu_child_operators by scoping to its own write execution [skip ci]|
|[#15067](https://github.com/NVIDIA/cudf-spark/pull/15067)|Fix auto merge conflict 15009 [skip ci]|
|[#14781](https://github.com/NVIDIA/cudf-spark/pull/14781)|[AutoSparkUT] Add DynamicPartitionPruningSuite coverage|
|[#15057](https://github.com/NVIDIA/cudf-spark/pull/15057)|Add RapidsUnwrapCastInComparisonEndToEndSuite|
|[#15001](https://github.com/NVIDIA/cudf-spark/pull/15001)|Allow CPU CreateTableExec in iceberg SQL UI write test [skip ci]|
|[#15002](https://github.com/NVIDIA/cudf-spark/pull/15002)|Support configurable parent POM deployment [skip ci]|
|[#14838](https://github.com/NVIDIA/cudf-spark/pull/14838)|[AutoSparkUT] Recover SPARK-10136 nested-list parquet reads (#11589, #11592)|
|[#14902](https://github.com/NVIDIA/cudf-spark/pull/14902)|[Coverage] Add IT coverage for private optimizer rules|
|[#14975](https://github.com/NVIDIA/cudf-spark/pull/14975)|Show GPU plan for V2 table writes in the SQL UI / History Server|
|[#14923](https://github.com/NVIDIA/cudf-spark/pull/14923)|Honor Iceberg-resolved Parquet codec in GPU writer|
|[#14881](https://github.com/NVIDIA/cudf-spark/pull/14881)|Iceberg 1.11 support for Spark 411, part (1/3): extract version-divergent scan APIs behind a shim|
|[#14918](https://github.com/NVIDIA/cudf-spark/pull/14918)|Fix AQE transition cleanup for late ensure-requirements shuffles|
|[#14936](https://github.com/NVIDIA/cudf-spark/pull/14936)|Exclude shuffle-read op time from consumers across AQE query stages|
|[#14821](https://github.com/NVIDIA/cudf-spark/pull/14821)|[AutoSparkUT] Recover RapidsParquetProtobufCompatibilitySuite single-field repeated group cases|
|[#14863](https://github.com/NVIDIA/cudf-spark/pull/14863)|[BUG] Fix GpuRegExpUtils.backrefConversion greedy digit consumption (#14743)|
|[#14872](https://github.com/NVIDIA/cudf-spark/pull/14872)|[AutoSparkUT] Propagate SQL query context for decimal-overflow exceptions (SPARK-39190)|
|[#14901](https://github.com/NVIDIA/cudf-spark/pull/14901)|Fix op_time / op_time-excl-SemWait accounting on file writes and nested wraps|
|[#14913](https://github.com/NVIDIA/cudf-spark/pull/14913)|Fall back from /dev/tty to /dev/stdout in buildall single-shim builds|
|[#14802](https://github.com/NVIDIA/cudf-spark/pull/14802)|[AutoSparkUT] Recover 5 RapidsJsonSuite tests after spark-rapids-jni#4560|
|[#14888](https://github.com/NVIDIA/cudf-spark/pull/14888)|Remove the regex complexity estimator and GPU-memory gate|
|[#14932](https://github.com/NVIDIA/cudf-spark/pull/14932)|Use debug bundle upload in premerge CI [skip ci]|
|[#14837](https://github.com/NVIDIA/cudf-spark/pull/14837)|[BUG] Dedup GpuBroadcastExchange across DPP subqueries in non-AQE mode|
|[#14891](https://github.com/NVIDIA/cudf-spark/pull/14891)|[AutoSparkUT] regexp_test: raise maxStateMemoryBytes to 3 GiB (#14867)|
|[#14651](https://github.com/NVIDIA/cudf-spark/pull/14651)|Re-enable accelerated columnar-to-row path after fix in spark-rapids-jni|
|[#14884](https://github.com/NVIDIA/cudf-spark/pull/14884)|bump up iceberg scala 2.13 [skip ci]|
|[#14875](https://github.com/NVIDIA/cudf-spark/pull/14875)|Update dependency version JNI, private, hybrid to 26.08.0-SNAPSHOT|
|[#14871](https://github.com/NVIDIA/cudf-spark/pull/14871)|Bump up version to 26.08 [skip ci]|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
