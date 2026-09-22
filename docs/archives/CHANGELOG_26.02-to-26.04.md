# Change log
Generated on 2026-08-05
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


## Release 26.02

### Features
|||
|:---|:---|
|[#13381](https://github.com/NVIDIA/spark-rapids/issues/13381)|[FEA] Add support for iceberg partition calculation.|
|[#14137](https://github.com/NVIDIA/spark-rapids/issues/14137)|[FEA] Switch from Java 17 release to Java 8 target for Spark 411 shim.|
|[#14083](https://github.com/NVIDIA/spark-rapids/issues/14083)|[FEA][AUDIT][SPARK-52921][SQL] Specify outputPartitioning for UnionExec for same output partitoning as children operators|
|[#14056](https://github.com/NVIDIA/spark-rapids/issues/14056)|[FEA] Add support for Spark 4.1.1|
|[#13388](https://github.com/NVIDIA/spark-rapids/issues/13388)|[FEA] Add support for iceberg void transform.|
|[#13389](https://github.com/NVIDIA/spark-rapids/issues/13389)|[FEA] Add support for iceberg identity transform.|
|[#13382](https://github.com/NVIDIA/spark-rapids/issues/13382)|[FEA] Add support for iceberg bucket transform.|
|[#9080](https://github.com/NVIDIA/spark-rapids/issues/9080)|[FEA] Support sha2|
|[#13935](https://github.com/NVIDIA/spark-rapids/issues/13935)|[FEA] Add support for iceberg view.|
|[#14066](https://github.com/NVIDIA/spark-rapids/issues/14066)|[FEA] Set correct iceberg table property for spark rapids.|
|[#13750](https://github.com/NVIDIA/spark-rapids/issues/13750)|[FEA]Drop Spark 3.2.x support|
|[#13882](https://github.com/NVIDIA/spark-rapids/issues/13882)|[FEA] Support spark 3.5 + iceberg 1.9.2|

### Performance
|||
|:---|:---|
|[#14131](https://github.com/NVIDIA/spark-rapids/issues/14131)|[FEA] Enabled Kudo GPU reads by default|
|[#13715](https://github.com/NVIDIA/spark-rapids/issues/13715)|[FEA] Expand gpuSpillTime and gpuReadSpillTime metrics to include full spill operations|
|[#13812](https://github.com/NVIDIA/spark-rapids/issues/13812)|[PERF] Unnecessary data transformation steps in GPU aggregations|

### Bugs Fixed
|||
|:---|:---|
|[#14353](https://github.com/NVIDIA/spark-rapids/issues/14353)|[BUG] java.lang.NoSuchMethodError: org.apache.spark.sql.catalyst.catalog.CatalogTable of release artifacts in DB runtimes|
|[#14267](https://github.com/NVIDIA/spark-rapids/issues/14267)|[BUG] Spark 3.5.3 delta reads slower then 3.4.1|
|[#14262](https://github.com/NVIDIA/spark-rapids/issues/14262)|[BUG] NDS query66 unmatched results error in Spark 4.1.1 + ANSI run|
|[#14233](https://github.com/NVIDIA/spark-rapids/issues/14233)|[BUG] test_parquet_testing_valid_files for parquet-testing/data/null_list.parquet falls back|
|[#14197](https://github.com/NVIDIA/spark-rapids/issues/14197)|[BUG] LHA test fails with cudaErrorInvalidResourceHandle during CSV reading on L40S cluster|
|[#14215](https://github.com/NVIDIA/spark-rapids/issues/14215)|[BUG] Test Failure: SparkNumberFormatException - [CAST_INVALID_INPUT] The value '+1.2' of the type "STRING" cannot be cast to "INT" because it is malformed.|
|[#14218](https://github.com/NVIDIA/spark-rapids/issues/14218)|[BUG] Build errors on databricks with shuffle v2 phase 2|
|[#14134](https://github.com/NVIDIA/spark-rapids/issues/14134)|Change calls to cuDF partitioning apis to account for the numRows offset|
|[#14174](https://github.com/NVIDIA/spark-rapids/issues/14174)|OOM when a single shuffle partition exceeds 2GB in RapidsShuffleThreadedWriter|
|[#14201](https://github.com/NVIDIA/spark-rapids/issues/14201)|AB-BA deadlock between SpillableHostStore and SpillableHandle during shutdown|
|[#14196](https://github.com/NVIDIA/spark-rapids/issues/14196)|[BUG] Iceberg identity partition fallback tests failing after GPU support implementation|
|[#14099](https://github.com/NVIDIA/spark-rapids/issues/14099)|[BUG] IllegalStateException during Shuffle Write when Join result exceeds 2GB string limit.|
|[#14037](https://github.com/NVIDIA/spark-rapids/issues/14037)|[AUDIT] [SPARK-52962][SQL] BroadcastExchangeExec should not reset metrics|
|[#14188](https://github.com/NVIDIA/spark-rapids/issues/14188)|[BUG] JDK11 Nightly Build: DatasetSuite test failures for RelationalGroupedDataset toString format|
|[#7520](https://github.com/NVIDIA/spark-rapids/issues/7520)|[AUDIT][SPARK-42045][SQL] ANSI SQL mode: Round/Bround should return an error on tiny/small/big integer overflow|
|[#14179](https://github.com/NVIDIA/spark-rapids/issues/14179)|Merger thread can become zombie when task is killed, blocking subsequent tasks|
|[#14096](https://github.com/NVIDIA/spark-rapids/issues/14096)|[BUG] Window function tests fail with GPU/CPU string value mismatches in min aggregations in cuda13|
|[#13954](https://github.com/NVIDIA/spark-rapids/issues/13954)|[BUG] gpu kudo does not make its inputs spillable|
|[#14030](https://github.com/NVIDIA/spark-rapids/issues/14030)|[BUG] Iceberg GPU result is diff from CPU when doing merge|
|[#13365](https://github.com/NVIDIA/spark-rapids/issues/13365)|[BUG] NDS query57 completed with "IllegalArgumentException : Invalid kudo offset buffer content" error on dataproc|
|[#14092](https://github.com/NVIDIA/spark-rapids/issues/14092)|[BUG] Fix incorrect Iceberg-write row in supportedDataSource.csv‎|
|[#14075](https://github.com/NVIDIA/spark-rapids/issues/14075)|[BUG] Iceberg integration tests fail with pyspark.errors.exceptions.captured.UnsupportedOperationException: Creating a view is not supported by catalog: spark_catalog|
|[#14078](https://github.com/NVIDIA/spark-rapids/issues/14078)|[BUG] SizeInBytes metric display incorrect for data sizes exceeding TB|
|[#14025](https://github.com/NVIDIA/spark-rapids/issues/14025)|[BUG] Integration tests timeout on Dataproc 2.1-debian11: Job 0 exceeded 3600 second limit|
|[#13629](https://github.com/NVIDIA/spark-rapids/issues/13629)|[BUG] error using UCX shuffle with Spark 4.0|
|[#14057](https://github.com/NVIDIA/spark-rapids/issues/14057)|[BUG] Spark 4.0.0 master/worker startup failure: ClassNotFoundException and spark-class script error|
|[#14043](https://github.com/NVIDIA/spark-rapids/issues/14043)|[BUG] `test_avg_divide_by_zero` failed for OSS Spark 3.3.0  and across all Databricks runtime versions|
|[#13739](https://github.com/NVIDIA/spark-rapids/issues/13739)|[AutoSparkUT] Read row group containing both dictionary and plain encoded pages - Missing GPU verification test|
|[#13760](https://github.com/NVIDIA/spark-rapids/issues/13760)|[AutoSparkUT]DATE_FROM_UNIX_DATE test case in RapidsDateExpressionsSuite threw java.lang.ArithmeticException: integer overflow|
|[#13907](https://github.com/NVIDIA/spark-rapids/issues/13907)|[BUG] `join on` the condition `cast <row> to <boolean>` brings errors on the GPU engines|
|[#13953](https://github.com/NVIDIA/spark-rapids/issues/13953)|[BUG] Test Failure: SPARK-24788 RelationalGroupedDataset.toString missing 'type: GroupBy' in output spark330 JDK11|
|[#13914](https://github.com/NVIDIA/spark-rapids/issues/13914)|[BUG] An empty table  `crossJoin`  another table brings errors on the GPU engines|
|[#13765](https://github.com/NVIDIA/spark-rapids/issues/13765)|[AutoSparkUT] SPARK-38237 shuffle distribution test needs GPU-specific implementation|
|[#13892](https://github.com/NVIDIA/spark-rapids/issues/13892)|[BUG] CUDF UDF tests fail with kvikio symbol undefined error: _ZN6kvikio12RemoteHandle5preadEPvmmmPN2BS11thread_poolE 26.02 cudf-nightly|
|[#13899](https://github.com/NVIDIA/spark-rapids/issues/13899)|[BUG] Race condition in host-to-disk spill due to premature disk handle exposure|
|[#13820](https://github.com/NVIDIA/spark-rapids/issues/13820)|[BUG] NullPointerException - aggregate.TypedImperativeAggregate.merge when final agg on CPU|

### PRs
|||
|:---|:---|
|[#14554](https://github.com/NVIDIA/spark-rapids/pull/14554)|Fix coalesce tostring oom 26.02|
|[#14534](https://github.com/NVIDIA/spark-rapids/pull/14534)|[DOC] Add cuda13 jars release 2602 [skip ci]|
|[#14510](https://github.com/NVIDIA/spark-rapids/pull/14510)|Update changelog for the v26.02 release [skip ci]|
|[#14386](https://github.com/NVIDIA/spark-rapids/pull/14386)|Update changelog for the v26.02.1 release [skip ci]|
|[#14385](https://github.com/NVIDIA/spark-rapids/pull/14385)|hotfix release 2602.1 [skip ci]|
|[#14336](https://github.com/NVIDIA/spark-rapids/pull/14336)|Update changelog for the v26.02 release [skip ci]|
|[#14328](https://github.com/NVIDIA/spark-rapids/pull/14328)|Writing field id when writing iceberg's data file|
|[#14275](https://github.com/NVIDIA/spark-rapids/pull/14275)|Fix auto merge conflict 14274 [skip ci]|
|[#14255](https://github.com/NVIDIA/spark-rapids/pull/14255)|Update changelog for the v26.02 release [skip ci]|
|[#14254](https://github.com/NVIDIA/spark-rapids/pull/14254)|Update dependency version JNI, private, hybrid to 26.02.0|
|[#14271](https://github.com/NVIDIA/spark-rapids/pull/14271)|Fix combining small files when reading Delta tables using multi-threaded reader|
|[#14241](https://github.com/NVIDIA/spark-rapids/pull/14241)|[DOC] update for download page 2602 release [skip ci]|
|[#14264](https://github.com/NVIDIA/spark-rapids/pull/14264)|Fix GpuHashAggregateExec outputPartitioning for aliased grouping keys|
|[#14243](https://github.com/NVIDIA/spark-rapids/pull/14243)|[SPARK-54220] Xfail null_list.parquet for Spark 4.1.0+ due to array<void> inference|
|[#14226](https://github.com/NVIDIA/spark-rapids/pull/14226)|Disable RAPIDS Shuffle Manager when spark.shuffle.checksum.enabled is true|
|[#14230](https://github.com/NVIDIA/spark-rapids/pull/14230)|Fallback to CPU for hash joins with struct keys having different field names|
|[#14206](https://github.com/NVIDIA/spark-rapids/pull/14206)|Spark-4.1.1: Resolve integration tests in map_test.py and bloom_filter tests|
|[#14164](https://github.com/NVIDIA/spark-rapids/pull/14164)|Align GpuUnionExec with Spark 4.1's partitioner-aware union behavior|
|[#14120](https://github.com/NVIDIA/spark-rapids/pull/14120)|[FEA] Add support for Spark 4.1.1|
|[#14217](https://github.com/NVIDIA/spark-rapids/pull/14217)|Add tests for dml operations after schema evolution.|
|[#14175](https://github.com/NVIDIA/spark-rapids/pull/14175)|Fix OOM when shuffle partition exceeds 2GB in threaded writer|
|[#14202](https://github.com/NVIDIA/spark-rapids/pull/14202)|Fix AB-BA deadlock between SpillableHostStore and SpillableHandle during shutdown|
|[#14204](https://github.com/NVIDIA/spark-rapids/pull/14204)|Fix iceberg identity test failure.|
|[#14189](https://github.com/NVIDIA/spark-rapids/pull/14189)|BroadcastExchangeExec should not reset metrics|
|[#14192](https://github.com/NVIDIA/spark-rapids/pull/14192)|Fix JDK version diff in RelationalGroupedDataset|
|[#14183](https://github.com/NVIDIA/spark-rapids/pull/14183)|Support for Iceberg identity partitioning|
|[#14182](https://github.com/NVIDIA/spark-rapids/pull/14182)|Artifactory credentials for wget used in spark-premerge-build.sh|
|[#14180](https://github.com/NVIDIA/spark-rapids/pull/14180)|Fix merger thread deadlock when task is killed|
|[#14139](https://github.com/NVIDIA/spark-rapids/pull/14139)|Add withRetry to GpuBatchedBoundedWindowIterator|
|[#14166](https://github.com/NVIDIA/spark-rapids/pull/14166)|Fix failed cases due to Spark 41x changed the default mode from unsafe to safe|
|[#14170](https://github.com/NVIDIA/spark-rapids/pull/14170)|Update authorized users|
|[#14161](https://github.com/NVIDIA/spark-rapids/pull/14161)|Add in the proper output ordering and partitioning to GpuWindowLimitExec|
|[#13724](https://github.com/NVIDIA/spark-rapids/pull/13724)|rapids shuffle manager V2 phase 1: writer use as much memory as allowed and pipelined write|
|[#14157](https://github.com/NVIDIA/spark-rapids/pull/14157)|[AutoSparkUT]Add Dataset, DataFrameFunctions and ColumnExpression suites|
|[#14151](https://github.com/NVIDIA/spark-rapids/pull/14151)|[AutoSparkUT]Enable several Spark UT suites|
|[#14141](https://github.com/NVIDIA/spark-rapids/pull/14141)|[AutoSparkUT]Enable several UT Suites|
|[#14125](https://github.com/NVIDIA/spark-rapids/pull/14125)|enable GPU kudo reads by default|
|[#14003](https://github.com/NVIDIA/spark-rapids/pull/14003)|Add in basic GPU/CPU bridge operation|
|[#14130](https://github.com/NVIDIA/spark-rapids/pull/14130)|[AutoSparkUT]Add RapidsCollectionExpressionsSuite|
|[#13995](https://github.com/NVIDIA/spark-rapids/pull/13995)|Add a debug option to check if memory allocation is covered by retry framework|
|[#14031](https://github.com/NVIDIA/spark-rapids/pull/14031)|Fix Iceberg data corruption when meets null in the condition in merge process|
|[#14095](https://github.com/NVIDIA/spark-rapids/pull/14095)|Fix Scala 2.13 compilation warnings and enforce stricter checks|
|[#14035](https://github.com/NVIDIA/spark-rapids/pull/14035)|Allow for AST join build side selection and add some heuristics|
|[#14089](https://github.com/NVIDIA/spark-rapids/pull/14089)|Add length check for materializing to host memory buffer from DiskHandle.|
|[#14001](https://github.com/NVIDIA/spark-rapids/pull/14001)|Supports all types for Iceberg bucket transform|
|[#14093](https://github.com/NVIDIA/spark-rapids/pull/14093)|Fix incorrect Iceberg-write row in supportedDataSource.csv|
|[#14101](https://github.com/NVIDIA/spark-rapids/pull/14101)|Migrate pre-merge CI image usage to new artifactory instance|
|[#14100](https://github.com/NVIDIA/spark-rapids/pull/14100)|Add Spark version notice to generated documentation|
|[#14091](https://github.com/NVIDIA/spark-rapids/pull/14091)|[SparkUT]Fix a from-json case to expect a different exception|
|[#14094](https://github.com/NVIDIA/spark-rapids/pull/14094)|Fix compile error in `HashFunctions.scala`|
|[#14038](https://github.com/NVIDIA/spark-rapids/pull/14038)|[FEA] SHA-2 hash support.|
|[#14087](https://github.com/NVIDIA/spark-rapids/pull/14087)|Delete useless UT suites which run on CPU only|
|[#14086](https://github.com/NVIDIA/spark-rapids/pull/14086)|Remove the 3rd `DataType` parameter from all the calls to GpuColumnVector.from(GpuScalar, int, DataType)|
|[#14082](https://github.com/NVIDIA/spark-rapids/pull/14082)|Fix Iceberg view test failures in CICD|
|[#14070](https://github.com/NVIDIA/spark-rapids/pull/14070)|Kudo supports schema check when serializing batches|
|[#14080](https://github.com/NVIDIA/spark-rapids/pull/14080)|Set JDK 17 as the default for nightly builds across both scala2.12 and scala2.13|
|[#14079](https://github.com/NVIDIA/spark-rapids/pull/14079)|Fix metric display for data sizes exceeding TB (#14078)|
|[#14010](https://github.com/NVIDIA/spark-rapids/pull/14010)|support withRetry with split for GPU shuffle coalesce|
|[#14071](https://github.com/NVIDIA/spark-rapids/pull/14071)|[DOC] update supported spark versions [skip ci]|
|[#14033](https://github.com/NVIDIA/spark-rapids/pull/14033)|[BUG] Fix initialization order NPE for RapidsShuffleManager in UCX mode for Spark 4+|
|[#14042](https://github.com/NVIDIA/spark-rapids/pull/14042)|Add Iceberg view test|
|[#14058](https://github.com/NVIDIA/spark-rapids/pull/14058)|Update documentation to reflect the shims updates [skip ci]|
|[#14039](https://github.com/NVIDIA/spark-rapids/pull/14039)|Add support for docs generation in `buildall`|
|[#13975](https://github.com/NVIDIA/spark-rapids/pull/13975)|support withRetry with split for shuffle exchange exec base|
|[#14044](https://github.com/NVIDIA/spark-rapids/pull/14044)|Adds ignore_order for groupBy agg test that returns multiple rows|
|[#14026](https://github.com/NVIDIA/spark-rapids/pull/14026)|Add in the missing RmmSpark calls for the coalescing reader|
|[#14029](https://github.com/NVIDIA/spark-rapids/pull/14029)|Increase executor memory for iceberg tests to avoid OOM error [skip ci]|
|[#14022](https://github.com/NVIDIA/spark-rapids/pull/14022)|Add RapidsDateFunctionsSuite|
|[#13993](https://github.com/NVIDIA/spark-rapids/pull/13993)|Update plugin scala212 to build 330+ only|
|[#14024](https://github.com/NVIDIA/spark-rapids/pull/14024)|Update actions/setup-java@v5 [skip ci]|
|[#13986](https://github.com/NVIDIA/spark-rapids/pull/13986)|Add iceberg 1.9.2 support.|
|[#14008](https://github.com/NVIDIA/spark-rapids/pull/14008)|Fix auto merge conflict 14007 [skip ci]|
|[#13982](https://github.com/NVIDIA/spark-rapids/pull/13982)|Add parquet mixed-encodings test|
|[#13994](https://github.com/NVIDIA/spark-rapids/pull/13994)|[SparkUT]Add try-catch on dataframe.collect in UT framework|
|[#13997](https://github.com/NVIDIA/spark-rapids/pull/13997)|Add pmattione to blossom: Attempt 2  [skip ci]|
|[#13992](https://github.com/NVIDIA/spark-rapids/pull/13992)|Building scala213 plugin for spark350+|
|[#13987](https://github.com/NVIDIA/spark-rapids/pull/13987)|Add layer of indirection when converting expressions to the GPU|
|[#13981](https://github.com/NVIDIA/spark-rapids/pull/13981)|Add in support for getting SQL metrics from expressions|
|[#13983](https://github.com/NVIDIA/spark-rapids/pull/13983)|Support custom parallelism in non-default test modes [skip ci]|
|[#13964](https://github.com/NVIDIA/spark-rapids/pull/13964)|Fix a join where only a single column is used as a condition|
|[#13970](https://github.com/NVIDIA/spark-rapids/pull/13970)|Make the GPU UDF has different name than the CPU one|
|[#13933](https://github.com/NVIDIA/spark-rapids/pull/13933)|[DOC] fix dead link in testing page [skip ci]|
|[#13845](https://github.com/NVIDIA/spark-rapids/pull/13845)|Add a non strict mode for lore dump|
|[#13959](https://github.com/NVIDIA/spark-rapids/pull/13959)|[SparkUT]Check Java version to decide the expected string in one case of DataFrameAggregateSuite|
|[#13955](https://github.com/NVIDIA/spark-rapids/pull/13955)|[DOC] Update RapidsUDF output types with decimal 128 [skip ci]|
|[#13938](https://github.com/NVIDIA/spark-rapids/pull/13938)|Fix a special case in limit where it could return an empty batch with the wrong number of columns|
|[#13945](https://github.com/NVIDIA/spark-rapids/pull/13945)|Set WONT_FIX_ISSUE cases in RadpisDataFrameWindowFunctionsSuite|
|[#13947](https://github.com/NVIDIA/spark-rapids/pull/13947)|Fix auto merge conflict 13946 [skip ci]|
|[#13936](https://github.com/NVIDIA/spark-rapids/pull/13936)|Add testRapids case to match GPU execution in RapidsDataFrameWindowFunctionsSuite|
|[#13905](https://github.com/NVIDIA/spark-rapids/pull/13905)|Refine GpuTaskMetrics over SpillFrameWork|
|[#13931](https://github.com/NVIDIA/spark-rapids/pull/13931)|Use strict priority in conda process [skip ci]|
|[#13911](https://github.com/NVIDIA/spark-rapids/pull/13911)|[AutoSparkUT]Enable RapidsCsvExpressionsSuite & RapidsCSVInferSchemaSuite|
|[#13900](https://github.com/NVIDIA/spark-rapids/pull/13900)|fix race condition due to premature disk handle exposure|
|[#13902](https://github.com/NVIDIA/spark-rapids/pull/13902)|[AutoSparkUT]Add RapidsCsvSuite|
|[#13875](https://github.com/NVIDIA/spark-rapids/pull/13875)|Persist the bufConverter of TypedImperativeAggregate into LogicalPlan instead of PhysicalPlan|
|[#13883](https://github.com/NVIDIA/spark-rapids/pull/13883)|[AutoSparkUT]Enable several Spark UT suites|
|[#13887](https://github.com/NVIDIA/spark-rapids/pull/13887)|Fix auto merge conflict 13877 [skip ci]|
|[#13857](https://github.com/NVIDIA/spark-rapids/pull/13857)|Use shim to identify whether DataWriting is supported for LoRe|
|[#13819](https://github.com/NVIDIA/spark-rapids/pull/13819)|Add the missing spark 357 version to GpuWriteFilesUnsupportedVersions|
|[#13777](https://github.com/NVIDIA/spark-rapids/pull/13777)|[AutoSparkUT] Migrate DataFrameNaFunctionsSuite tests to RAPIDS|
|[#13796](https://github.com/NVIDIA/spark-rapids/pull/13796)|Update dependency version JNI, private, hybrid to 26.02.0-SNAPSHOT [skip ci]|
|[#13767](https://github.com/NVIDIA/spark-rapids/pull/13767)|[AutoSparkUT] Migrate DataFrameComplexTypeSuite tests to RAPIDS|
|[#13762](https://github.com/NVIDIA/spark-rapids/pull/13762)|[AutoSparkUT]Enable Spark UT DateExpressionsSuite|
|[#13795](https://github.com/NVIDIA/spark-rapids/pull/13795)|Bump up version to 26.02 [skip ci]|
|[#13790](https://github.com/NVIDIA/spark-rapids/pull/13790)|Use wildcard mark to import all Rapids test suites migrated from Apache Spark Suites|

