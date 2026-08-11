
# Change log
Generated on 2024-04-10
## Release 23.12

### Features
|||
|:---|:---|
|[#6832](https://github.com/NVIDIA/spark-rapids/issues/6832)|[FEA] Convert Timestamp/Timezone tests/checks to be per operator instead of generic |
|[#9805](https://github.com/NVIDIA/spark-rapids/issues/9805)|[FEA] Support ```current_date``` expression function with CST (UTC + 8) timezone support|
|[#9515](https://github.com/NVIDIA/spark-rapids/issues/9515)|[FEA] Support temporal types in to_json|
|[#9872](https://github.com/NVIDIA/spark-rapids/issues/9872)|[FEA][JSON] Support Decimal type in `to_json`|
|[#9802](https://github.com/NVIDIA/spark-rapids/issues/9802)|[FEA] Support FromUTCTimestamp on the GPU with a non-UTC time zone|
|[#6831](https://github.com/NVIDIA/spark-rapids/issues/6831)|[FEA] Support timestamp transitions to and from UTC for single time zones with no repeating rules|
|[#9590](https://github.com/NVIDIA/spark-rapids/issues/9590)|[FEA][JSON] Support temporal types in `from_json`|
|[#9804](https://github.com/NVIDIA/spark-rapids/issues/9804)|[FEA] Support CPU path for from_utc_timestamp function with timezone|
|[#9461](https://github.com/NVIDIA/spark-rapids/issues/9461)|[FEA] Validate nvcomp-3.0 with spark rapids plugin|
|[#8832](https://github.com/NVIDIA/spark-rapids/issues/8832)|[FEA] rewrite join conditions where only part of it can fit on the AST|
|[#9059](https://github.com/NVIDIA/spark-rapids/issues/9059)|[FEA] Support spark.sql.parquet.datetimeRebaseModeInRead=LEGACY|
|[#9037](https://github.com/NVIDIA/spark-rapids/issues/9037)|[FEA] Support spark.sql.parquet.int96RebaseModeInWrite= LEGACY|
|[#9632](https://github.com/NVIDIA/spark-rapids/issues/9632)|[FEA] Take into account `org.apache.spark.timeZone` in Parquet/Avro from Spark 3.2|
|[#8770](https://github.com/NVIDIA/spark-rapids/issues/8770)|[FEA] add more metrics to Eventlogs or Executor logs|
|[#9597](https://github.com/NVIDIA/spark-rapids/issues/9597)|[FEA][JSON] Support boolean type in `from_json`|
|[#9516](https://github.com/NVIDIA/spark-rapids/issues/9516)|[FEA] Add support for JSON data source option `ignoreNullFields=false` in `to_json`|
|[#9520](https://github.com/NVIDIA/spark-rapids/issues/9520)|[FEA] Add support for `LAST()` as running window function|
|[#9518](https://github.com/NVIDIA/spark-rapids/issues/9518)|[FEA] Add support for relevant JSON data source options in `to_json`|
|[#9218](https://github.com/NVIDIA/spark-rapids/issues/9218)|[FEA] Support stack function|
|[#9532](https://github.com/NVIDIA/spark-rapids/issues/9532)|[FEA] Support Delta Lake 2.3.0|
|[#1525](https://github.com/NVIDIA/spark-rapids/issues/1525)|[FEA] Support Scala 2.13|
|[#7279](https://github.com/NVIDIA/spark-rapids/issues/7279)|[FEA] Support OverwriteByExpressionExecV1 for Delta Lake|
|[#9326](https://github.com/NVIDIA/spark-rapids/issues/9326)|[FEA] Specify `recover_with_null` when reading JSON files|
|[#8780](https://github.com/NVIDIA/spark-rapids/issues/8780)|[FEA] Support to_json function|
|[#7278](https://github.com/NVIDIA/spark-rapids/issues/7278)|[FEA] Support AppendDataExecV1 for Delta Lake|
|[#6266](https://github.com/NVIDIA/spark-rapids/issues/6266)|[FEA] Support Percentile|
|[#7277](https://github.com/NVIDIA/spark-rapids/issues/7277)|[FEA] Support AtomicReplaceTableAsSelect for Delta Lake|
|[#7276](https://github.com/NVIDIA/spark-rapids/issues/7276)|[FEA] Support AtomicCreateTableAsSelect for Delta Lake|

### Performance
|||
|:---|:---|
|[#8137](https://github.com/NVIDIA/spark-rapids/issues/8137)|[FEA] Upgrade to UCX 1.15|
|[#8157](https://github.com/NVIDIA/spark-rapids/issues/8157)|[FEA] Add string comparison to AST expressions|
|[#9398](https://github.com/NVIDIA/spark-rapids/issues/9398)|[FEA] Compress/encrypt spill to disk|

### Bugs Fixed
|||
|:---|:---|
|[#9687](https://github.com/NVIDIA/spark-rapids/issues/9687)|[BUG] `test_in_set` fails when DATAGEN_SEED=1698940723|
|[#9659](https://github.com/NVIDIA/spark-rapids/issues/9659)|[BUG] executor crash intermittantly in scala2.13-built spark332 integration tests|
|[#9923](https://github.com/NVIDIA/spark-rapids/issues/9923)|[BUG] Failed case about ```test_timestamp_seconds_rounding_necessary[Decimal(20,7)][DATAGEN_SEED=1701412018] – src.main.python.date_time_test```|
|[#9982](https://github.com/NVIDIA/spark-rapids/issues/9982)|[BUG] test "convert large InternalRow iterator to cached batch single col" failed with arena pool|
|[#9683](https://github.com/NVIDIA/spark-rapids/issues/9683)|[BUG] test_map_scalars_supported_key_types fails with DATAGEN_SEED=1698940723|
|[#9976](https://github.com/NVIDIA/spark-rapids/issues/9976)|[BUG] test_part_write_round_trip[Float] Failed on -0.0 partition|
|[#9948](https://github.com/NVIDIA/spark-rapids/issues/9948)|[BUG] parquet reader data corruption in nested schema after https://github.com/rapidsai/cudf/pull/13302|
|[#9867](https://github.com/NVIDIA/spark-rapids/issues/9867)|[BUG] Unable to use Spark Rapids with Spark Thrift Server|
|[#9934](https://github.com/NVIDIA/spark-rapids/issues/9934)|[BUG] test_delta_multi_part_write_round_trip_unmanaged and test_delta_part_write_round_trip_unmanaged failed DATA_SEED=1701608331 |
|[#9933](https://github.com/NVIDIA/spark-rapids/issues/9933)|[BUG] collection_ops_test.py::test_sequence_too_long_sequence[Long(not_null)][DATAGEN_SEED=1701553915, INJECT_OOM]|
|[#9837](https://github.com/NVIDIA/spark-rapids/issues/9837)|[BUG] test_part_write_round_trip failed|
|[#9932](https://github.com/NVIDIA/spark-rapids/issues/9932)|[BUG] Failed test_multi_tier_ast[DATAGEN_SEED=1701445668] on CI|
|[#9829](https://github.com/NVIDIA/spark-rapids/issues/9829)|[BUG] Java OOM when testing non-UTC time zone with lots of cases fallback.|
|[#9403](https://github.com/NVIDIA/spark-rapids/issues/9403)|[BUG] test_cogroup_apply_udf[Short(not_null)] failed with pandas 2.1.X|
|[#9684](https://github.com/NVIDIA/spark-rapids/issues/9684)|[BUG] test_coalesce fails with DATAGEN_SEED=1698940723|
|[#9685](https://github.com/NVIDIA/spark-rapids/issues/9685)|[BUG] test_case_when fails with DATAGEN_SEED=1698940723|
|[#9776](https://github.com/NVIDIA/spark-rapids/issues/9776)|[BUG] fastparquet compatibility tests fail with data mismatch if TZ is not set and system timezone is not UTC|
|[#9733](https://github.com/NVIDIA/spark-rapids/issues/9733)|[BUG] Complex AST expressions can crash with non-matching operand type error|
|[#9877](https://github.com/NVIDIA/spark-rapids/issues/9877)|[BUG] Fix resource leak in to_json|
|[#9722](https://github.com/NVIDIA/spark-rapids/issues/9722)|[BUG] test_floor_scale_zero fails with DATAGEN_SEED=1700009407|
|[#9846](https://github.com/NVIDIA/spark-rapids/issues/9846)|[BUG] test_ceil_scale_zero may fail with different datagen_seed|
|[#9781](https://github.com/NVIDIA/spark-rapids/issues/9781)|[BUG] test_cast_string_date_valid_format fails on DATAGEN_SEED=1700250017|
|[#9714](https://github.com/NVIDIA/spark-rapids/issues/9714)|Scala Map class not found when executing the benchmark on Spark 3.5.0 with Scala 2.13|
|[#9856](https://github.com/NVIDIA/spark-rapids/issues/9856)|collection_ops_test.py failed on Dataproc-2.1 with: Column 'None' does not exist|
|[#9397](https://github.com/NVIDIA/spark-rapids/issues/9397)|[BUG] RapidsShuffleManager MULTITHREADED on Databricks, we see loss of executors due to Rpc issues|
|[#9738](https://github.com/NVIDIA/spark-rapids/issues/9738)|[BUG] `test_delta_part_write_round_trip_unmanaged` and `test_delta_multi_part_write_round_trip_unmanaged` fail with `DATAGEN_SEED=1700105176`|
|[#9771](https://github.com/NVIDIA/spark-rapids/issues/9771)|[BUG] ast_test.py::test_X[(String, True)][DATAGEN_SEED=1700205785] failed|
|[#9782](https://github.com/NVIDIA/spark-rapids/issues/9782)|[BUG] Error messages appear in a clean build|
|[#9798](https://github.com/NVIDIA/spark-rapids/issues/9798)|[BUG] GpuCheckOverflowInTableInsert should be added to databricks shim|
|[#9820](https://github.com/NVIDIA/spark-rapids/issues/9820)|[BUG] test_parquet_write_roundtrip_datetime_with_legacy_rebase fails with "year 0 is out of range"|
|[#9817](https://github.com/NVIDIA/spark-rapids/issues/9817)|[BUG] FAILED dpp_test.py::test_dpp_reuse_broadcast_exchange[false-0-parquet][DATAGEN_SEED=1700572856, IGNORE_ORDER]|
|[#9768](https://github.com/NVIDIA/spark-rapids/issues/9768)|[BUG] `cast decimal to string` ScalaTest relies on a side effects |
|[#9711](https://github.com/NVIDIA/spark-rapids/issues/9711)|[BUG] test_lte fails with DATAGEN_SEED=1699987762|
|[#9751](https://github.com/NVIDIA/spark-rapids/issues/9751)|[BUG] cmp_test test_gte failed with DATAGEN_SEED=1700149611|
|[#9469](https://github.com/NVIDIA/spark-rapids/issues/9469)|[BUG] [main] ERROR com.nvidia.spark.rapids.GpuOverrideUtil - Encountered an exception applying GPU overrides java.lang.IllegalStateException: the broadcast must be on the GPU too|
|[#9648](https://github.com/NVIDIA/spark-rapids/issues/9648)|[BUG] Existence default values in schema are not being honored|
|[#9676](https://github.com/NVIDIA/spark-rapids/issues/9676)|Fix Delta Lake Integration tests; `test_delta_atomic_create_table_as_select` and `test_delta_atomic_replace_table_as_select`|
|[#9701](https://github.com/NVIDIA/spark-rapids/issues/9701)|[BUG] test_ts_formats_round_trip and test_datetime_roundtrip_with_legacy_rebase fail with DATAGEN_SEED=1699915317|
|[#9691](https://github.com/NVIDIA/spark-rapids/issues/9691)|[BUG] Repeated Maven invocations w/o changes recompile too many Scala sources despite recompileMode=incremental |
|[#9547](https://github.com/NVIDIA/spark-rapids/issues/9547)|Update buildall and doc to generate bloop projects for test debugging|
|[#9697](https://github.com/NVIDIA/spark-rapids/issues/9697)|[BUG] Iceberg multiple file readers can not read files if the file paths contain encoded URL unsafe chars|
|[#9681](https://github.com/NVIDIA/spark-rapids/issues/9681)|Databricks Build Failing For 330db+|
|[#9521](https://github.com/NVIDIA/spark-rapids/issues/9521)|[BUG] Multi Threaded Shuffle Writer needs flow control|
|[#9675](https://github.com/NVIDIA/spark-rapids/issues/9675)|Failing Delta Lake Tests for Databricks 13.3 Due to WriteIntoDeltaCommand|
|[#9669](https://github.com/NVIDIA/spark-rapids/issues/9669)|[BUG] Rebase exception states not in UTC but timezone is Etc/UTC|
|[#7940](https://github.com/NVIDIA/spark-rapids/issues/7940)|[BUG] UCX peer connection issue in multi-nic single node cluster|
|[#9650](https://github.com/NVIDIA/spark-rapids/issues/9650)|[BUG] Github workflow for missing scala2.13 updates fails to detect when pom is new|
|[#9621](https://github.com/NVIDIA/spark-rapids/issues/9621)|[BUG] Scala 2.13 with-classifier profile is picking up Scala2.12 spark.version|
|[#9636](https://github.com/NVIDIA/spark-rapids/issues/9636)|[BUG] All parquet integration tests failed "Part of the plan is not columnar class" in databricks runtimes|
|[#9108](https://github.com/NVIDIA/spark-rapids/issues/9108)|[BUG] nullability on some decimal operations is wrong|
|[#9625](https://github.com/NVIDIA/spark-rapids/issues/9625)|[BUG] Typo in github Maven check install-modules |
|[#9603](https://github.com/NVIDIA/spark-rapids/issues/9603)|[BUG] fastparquet_compatibility_test fails on dataproc|
|[#8729](https://github.com/NVIDIA/spark-rapids/issues/8729)|[BUG] nightly integration test failed OOM kill in JDK11 ENV|
|[#9589](https://github.com/NVIDIA/spark-rapids/issues/9589)|[BUG] Scala 2.13 build hard-codes Java 8 target |
|[#9581](https://github.com/NVIDIA/spark-rapids/issues/9581)|Delta Lake 2.4 missing equals/hashCode override for file format and some metrics for merge|
|[#9507](https://github.com/NVIDIA/spark-rapids/issues/9507)|[BUG] Spark 3.2+/ParquetFilterSuite/Parquet filter pushdown - timestamp/ FAILED  |
|[#9540](https://github.com/NVIDIA/spark-rapids/issues/9540)|[BUG] Job failed with SparkUpgradeException no matter which value are set for spark.sql.parquet.datetimeRebaseModeInRead|
|[#9545](https://github.com/NVIDIA/spark-rapids/issues/9545)|[BUG] Dataproc 2.0 test_reading_file_rewritten_with_fastparquet tests failing|
|[#9552](https://github.com/NVIDIA/spark-rapids/issues/9552)|[BUG] Inconsistent CDH dependency overrides across submodules|
|[#9571](https://github.com/NVIDIA/spark-rapids/issues/9571)|[BUG] non-deterministic compiled SQLExecPlugin.class with scala 2.13 deployment|
|[#9569](https://github.com/NVIDIA/spark-rapids/issues/9569)|[BUG] test_window_running failed in 3.1.2+3.1.3|
|[#9480](https://github.com/NVIDIA/spark-rapids/issues/9480)|[BUG] mapInPandas doesn't invoke udf on empty partitions|
|[#8644](https://github.com/NVIDIA/spark-rapids/issues/8644)|[BUG] Parquet file with malformed dictionary does not error when loaded|
|[#9310](https://github.com/NVIDIA/spark-rapids/issues/9310)|[BUG] Improve support for reading JSON files with malformed rows|
|[#9457](https://github.com/NVIDIA/spark-rapids/issues/9457)|[BUG] CDH 332 unit tests failing|
|[#9404](https://github.com/NVIDIA/spark-rapids/issues/9404)|[BUG] Spark reports a decimal error when create lit scalar when generate Decimal(34, -5) data.|
|[#9110](https://github.com/NVIDIA/spark-rapids/issues/9110)|[BUG] GPU Reader fails due to partition column creating column larger then cudf column size limit|
|[#8631](https://github.com/NVIDIA/spark-rapids/issues/8631)|[BUG] Parquet load failure on repeated_no_annotation.parquet|
|[#9364](https://github.com/NVIDIA/spark-rapids/issues/9364)|[BUG] CUDA illegal access error is triggering split and retry logic|

### PRs
|||
|:---|:---|
|[#10384](https://github.com/NVIDIA/spark-rapids/pull/10384)|[DOC] Update docs for 23.12.2 release [skip ci] |
|[#10341](https://github.com/NVIDIA/spark-rapids/pull/10341)|Update changelog for v23.12.2 [skip ci]|
|[#10340](https://github.com/NVIDIA/spark-rapids/pull/10340)|Copyright to 2024 [skip ci]|
|[#10323](https://github.com/NVIDIA/spark-rapids/pull/10323)|Upgrade version to 23.12.2-SNAPSHOT|
|[#10329](https://github.com/NVIDIA/spark-rapids/pull/10329)|update download page for v23.12.2 release [skip ci]|
|[#10274](https://github.com/NVIDIA/spark-rapids/pull/10274)|PythonRunner Changes|
|[#10124](https://github.com/NVIDIA/spark-rapids/pull/10124)|Update changelog for v23.12.1 [skip ci]|
|[#10123](https://github.com/NVIDIA/spark-rapids/pull/10123)|Change version to v23.12.1 [skip ci]|
|[#10122](https://github.com/NVIDIA/spark-rapids/pull/10122)|Init changelog for v23.12.1 [skip ci]|
|[#10121](https://github.com/NVIDIA/spark-rapids/pull/10121)|[DOC] update download page for db hot fix  [skip ci]|
|[#10116](https://github.com/NVIDIA/spark-rapids/pull/10116)|Upgrade to 23.12.1-SNAPSHOT|
|[#10069](https://github.com/NVIDIA/spark-rapids/pull/10069)|Revert "Support split broadcast join condition into ast and non-ast […|
|[#9470](https://github.com/NVIDIA/spark-rapids/pull/9470)|Use float to string kernel|
|[#9481](https://github.com/NVIDIA/spark-rapids/pull/9481)|Use parse_url kernel for PROTOCOL parsing|
|[#9935](https://github.com/NVIDIA/spark-rapids/pull/9935)|Init 23.12 changelog [skip ci]|
|[#9943](https://github.com/NVIDIA/spark-rapids/pull/9943)|[DOC] Update docs for 23.12.0 release [skip ci]|
|[#10014](https://github.com/NVIDIA/spark-rapids/pull/10014)|Add documentation for how to run tests with a fixed datagen seed [skip ci]|
|[#9954](https://github.com/NVIDIA/spark-rapids/pull/9954)|Update private and JNI version to released 23.12.0|
|[#10009](https://github.com/NVIDIA/spark-rapids/pull/10009)|Using fix seed to unblock 23.12 release; Move the blocked issues to 24.02|
|[#10007](https://github.com/NVIDIA/spark-rapids/pull/10007)|Fix Java OOM in non-UTC case with lots of xfail (#9944)|
|[#9985](https://github.com/NVIDIA/spark-rapids/pull/9985)|Avoid allocating GPU memory out of RMM managed pool in test|
|[#9970](https://github.com/NVIDIA/spark-rapids/pull/9970)|Avoid leading and trailing zeros in test_timestamp_seconds_rounding_necessary|
|[#9978](https://github.com/NVIDIA/spark-rapids/pull/9978)|Avoid using floating point values as partition values in tests|
|[#9979](https://github.com/NVIDIA/spark-rapids/pull/9979)|Add compatibility notes for writing ORC with lost Gregorian days [skip ci]|
|[#9949](https://github.com/NVIDIA/spark-rapids/pull/9949)|Override the seed for `test_map_scalars_supported_key_types ` for version of Spark before 3.4.0 [Databricks]|
|[#9961](https://github.com/NVIDIA/spark-rapids/pull/9961)|Avoid using floating point for partition values in Delta Lake tests|
|[#9960](https://github.com/NVIDIA/spark-rapids/pull/9960)|Fix LongGen accidentally using special cases when none are desired|
|[#9950](https://github.com/NVIDIA/spark-rapids/pull/9950)|Avoid generating NaNs as partition values in test_part_write_round_trip|
|[#9940](https://github.com/NVIDIA/spark-rapids/pull/9940)|Fix 'year 0 is out of range' by setting a fix seed|
|[#9946](https://github.com/NVIDIA/spark-rapids/pull/9946)|Fix test_multi_tier_ast to ignore ordering of output rows|
|[#9928](https://github.com/NVIDIA/spark-rapids/pull/9928)|Test `inset` with `NaN` only for Spark from 3.1.3|
|[#9906](https://github.com/NVIDIA/spark-rapids/pull/9906)|Fix test_initcap to use the intended limited character set|
|[#9831](https://github.com/NVIDIA/spark-rapids/pull/9831)|Skip fastparquet timestamp tests when plugin cannot read/write timestamps|
|[#9893](https://github.com/NVIDIA/spark-rapids/pull/9893)|Add multiple expression tier regression test for AST|
|[#9889](https://github.com/NVIDIA/spark-rapids/pull/9889)|Fix test_cast_string_ts_valid_format test|
|[#9833](https://github.com/NVIDIA/spark-rapids/pull/9833)|Fix a hang for Pandas UDFs on DB 13.3|
|[#9873](https://github.com/NVIDIA/spark-rapids/pull/9873)|Add support for decimal in `to_json`|
|[#9890](https://github.com/NVIDIA/spark-rapids/pull/9890)|Remove Databricks 13.3 from release 23.12|
|[#9874](https://github.com/NVIDIA/spark-rapids/pull/9874)|Fix zero-scale floor and ceil tests|
|[#9879](https://github.com/NVIDIA/spark-rapids/pull/9879)|Fix resource leak in to_json|
|[#9600](https://github.com/NVIDIA/spark-rapids/pull/9600)|Add date and timestamp support to to_json|
|[#9871](https://github.com/NVIDIA/spark-rapids/pull/9871)|Fix test_cast_string_date_valid_format generating year 0|
|[#9885](https://github.com/NVIDIA/spark-rapids/pull/9885)|Preparation for non-UTC nightly CI [skip ci]|
|[#9810](https://github.com/NVIDIA/spark-rapids/pull/9810)|Support from_utc_timestamp on the GPU for non-UTC timezones (non-DST)|
|[#9865](https://github.com/NVIDIA/spark-rapids/pull/9865)|Fix problems with nulls in sequence tests|
|[#9864](https://github.com/NVIDIA/spark-rapids/pull/9864)|Add compatibility documentation with respect to decimal overflow detection [skip ci]|
|[#9860](https://github.com/NVIDIA/spark-rapids/pull/9860)|Fixing FAQ deadlink in plugin code [skip ci]|
|[#9840](https://github.com/NVIDIA/spark-rapids/pull/9840)|Avoid using NaNs as Delta Lake partition values|
|[#9773](https://github.com/NVIDIA/spark-rapids/pull/9773)|xfail all the impacted cases when using non-UTC time zone|
|[#9849](https://github.com/NVIDIA/spark-rapids/pull/9849)|Instantly Delete pre-merge content of stage workspace if success|
|[#9848](https://github.com/NVIDIA/spark-rapids/pull/9848)|Force datagen_seed for test_ceil_scale_zero and test_decimal_round|
|[#9677](https://github.com/NVIDIA/spark-rapids/pull/9677)|Enable build for Databricks 13.3|
|[#9809](https://github.com/NVIDIA/spark-rapids/pull/9809)|Re-enable AST string integration cases|
|[#9835](https://github.com/NVIDIA/spark-rapids/pull/9835)|Avoid pre-Gregorian dates in schema_evolution_test|
|[#9786](https://github.com/NVIDIA/spark-rapids/pull/9786)|Check paths for existence to prevent ignorable error messages during build|
|[#9824](https://github.com/NVIDIA/spark-rapids/pull/9824)|UCX 1.15 upgrade|
|[#9800](https://github.com/NVIDIA/spark-rapids/pull/9800)|Add GpuCheckOverflowInTableInsert to Databricks 11.3+|
|[#9821](https://github.com/NVIDIA/spark-rapids/pull/9821)|Update timestamp gens to avoid "year 0 is out of range" errors|
|[#9826](https://github.com/NVIDIA/spark-rapids/pull/9826)|Set seed to 0 for test_hash_reduction_sum|
|[#9720](https://github.com/NVIDIA/spark-rapids/pull/9720)|Support timestamp in `from_json`|
|[#9818](https://github.com/NVIDIA/spark-rapids/pull/9818)|Specify nullable=False when generating filter values in dpp tests|
|[#9689](https://github.com/NVIDIA/spark-rapids/pull/9689)|Support CPU path for from_utc_timestamp function with timezone |
|[#9769](https://github.com/NVIDIA/spark-rapids/pull/9769)|Use withGpuSparkSession to customize SparkConf|
|[#9780](https://github.com/NVIDIA/spark-rapids/pull/9780)|Fix NaN handling in GpuLessThanOrEqual and GpuGreaterThanOrEqual|
|[#9795](https://github.com/NVIDIA/spark-rapids/pull/9795)|xfail AST string tests|
|[#9666](https://github.com/NVIDIA/spark-rapids/pull/9666)|Add support for parsing strings as dates in `from_json`|
|[#9673](https://github.com/NVIDIA/spark-rapids/pull/9673)|Fix the broadcast joins issues caused by InputFileBlockRule|
|[#9785](https://github.com/NVIDIA/spark-rapids/pull/9785)|Force datagen_seed for 9781 and 9784 [skip ci]|
|[#9765](https://github.com/NVIDIA/spark-rapids/pull/9765)|Let GPU scans fall back when default values exist in schema|
|[#9729](https://github.com/NVIDIA/spark-rapids/pull/9729)|Fix Delta Lake atomic table operations on spark341db|
|[#9770](https://github.com/NVIDIA/spark-rapids/pull/9770)|[BUG] Fix the doc for Maven and Scala 2.13 test example [skip ci]|
|[#9761](https://github.com/NVIDIA/spark-rapids/pull/9761)|Fix bug in tagging of JsonToStructs|
|[#9758](https://github.com/NVIDIA/spark-rapids/pull/9758)|Remove forced seed from Delta Lake part_write_round_trip_unmanaged tests|
|[#9652](https://github.com/NVIDIA/spark-rapids/pull/9652)|Add time zone config to set non-UTC|
|[#9736](https://github.com/NVIDIA/spark-rapids/pull/9736)|Fix `TimestampGen` to generate value not too close to the minimum allowed timestamp|
|[#9698](https://github.com/NVIDIA/spark-rapids/pull/9698)|Speed up build: unnecessary invalidation in the incremental recompile mode|
|[#9748](https://github.com/NVIDIA/spark-rapids/pull/9748)|Fix Delta Lake part_write_round_trip_unmanaged tests with floating point|
|[#9702](https://github.com/NVIDIA/spark-rapids/pull/9702)|Support split BroadcastNestedLoopJoin condition for AST and non-AST|
|[#9746](https://github.com/NVIDIA/spark-rapids/pull/9746)|Force test_hypot to be single seed for now|
|[#9745](https://github.com/NVIDIA/spark-rapids/pull/9745)|Avoid generating null filter values in test_delta_dfp_reuse_broadcast_exchange|
|[#9741](https://github.com/NVIDIA/spark-rapids/pull/9741)|Set seed=0 for the delta lake part roundtrip tests|
|[#9660](https://github.com/NVIDIA/spark-rapids/pull/9660)|Fully support date/time legacy rebase for nested input|
|[#9672](https://github.com/NVIDIA/spark-rapids/pull/9672)|Support String type for AST|
|[#9716](https://github.com/NVIDIA/spark-rapids/pull/9716)|Initiate project version 24.02.0-SNAPSHOT|
|[#9732](https://github.com/NVIDIA/spark-rapids/pull/9732)|Temporarily force `datagen_seed=0` for `test_re_replace_all` to unblock CI|
|[#9726](https://github.com/NVIDIA/spark-rapids/pull/9726)|Fix leak in BatchWithPartitionData|
|[#9717](https://github.com/NVIDIA/spark-rapids/pull/9717)|Encode the file path from Iceberg when converting to a PartitionedFile|
|[#9441](https://github.com/NVIDIA/spark-rapids/pull/9441)|Add a random seed specific to datagen cases|
|[#9649](https://github.com/NVIDIA/spark-rapids/pull/9649)|Support `spark.sql.parquet.datetimeRebaseModeInRead=LEGACY` and `spark.sql.parquet.int96RebaseModeInRead=LEGACY`|
|[#9612](https://github.com/NVIDIA/spark-rapids/pull/9612)|Escape quotes and newlines when converting strings to json format in to_json|
|[#9644](https://github.com/NVIDIA/spark-rapids/pull/9644)|Add Partial Delta Lake Support for Databricks 13.3|
|[#9690](https://github.com/NVIDIA/spark-rapids/pull/9690)|Changed `extractExecutedPlan` to consider ResultQueryStageExec for Databricks 13.3|
|[#9686](https://github.com/NVIDIA/spark-rapids/pull/9686)|Removed Maven Profiles From `tests/pom.xml`|
|[#9509](https://github.com/NVIDIA/spark-rapids/pull/9509)|Fine-grained spill metrics|
|[#9658](https://github.com/NVIDIA/spark-rapids/pull/9658)|Support `spark.sql.parquet.int96RebaseModeInWrite=LEGACY`|
|[#9695](https://github.com/NVIDIA/spark-rapids/pull/9695)|Revert "Support split non-AST-able join condition for BroadcastNested…|
|[#9693](https://github.com/NVIDIA/spark-rapids/pull/9693)|Enable automerge from 23.12 to 24.02 [skip ci]|
|[#9679](https://github.com/NVIDIA/spark-rapids/pull/9679)|[Doc] update the dead link in download page [skip ci]|
|[#9678](https://github.com/NVIDIA/spark-rapids/pull/9678)|Add flow control for multithreaded shuffle writer|
|[#9635](https://github.com/NVIDIA/spark-rapids/pull/9635)|Support split non-AST-able join condition for BroadcastNestedLoopJoin|
|[#9646](https://github.com/NVIDIA/spark-rapids/pull/9646)|Fix Integration Test Failures for Databricks 13.3 Support|
|[#9670](https://github.com/NVIDIA/spark-rapids/pull/9670)|Normalize file timezone and handle missing file timezone in datetimeRebaseUtils|
|[#9657](https://github.com/NVIDIA/spark-rapids/pull/9657)|Update verify check to handle new pom files [skip ci]|
|[#9663](https://github.com/NVIDIA/spark-rapids/pull/9663)|Making User Guide info in bold and adding it as top right link in github.io [skip ci]|
|[#9609](https://github.com/NVIDIA/spark-rapids/pull/9609)|Add valid retry solution to mvn-verify [skip ci]|
|[#9655](https://github.com/NVIDIA/spark-rapids/pull/9655)|Document problem with handling of invalid characters in CSV reader|
|[#9620](https://github.com/NVIDIA/spark-rapids/pull/9620)|Add support for parsing boolean values in `from_json`|
|[#9615](https://github.com/NVIDIA/spark-rapids/pull/9615)|Bloop updates - require JDK11 in buildall + docs, build bloop for all targets.|
|[#9631](https://github.com/NVIDIA/spark-rapids/pull/9631)|Refactor Parquet readers|
|[#9637](https://github.com/NVIDIA/spark-rapids/pull/9637)|Added Support For Various Execs for Databricks 13.3 |
|[#9640](https://github.com/NVIDIA/spark-rapids/pull/9640)|Add support for `ignoreNullFields=false` in `to_json`|
|[#9623](https://github.com/NVIDIA/spark-rapids/pull/9623)|Running window optimization for `LAST()`|
|[#9641](https://github.com/NVIDIA/spark-rapids/pull/9641)|Revert "Support rebase checking for nested dates and timestamps (#9617)"|
|[#9423](https://github.com/NVIDIA/spark-rapids/pull/9423)|Re-enable `from_json` / `JsonToStructs`|
|[#9624](https://github.com/NVIDIA/spark-rapids/pull/9624)|Add jenkins-level retry for pre-merge build in databricks runtimes|
|[#9608](https://github.com/NVIDIA/spark-rapids/pull/9608)|Fix nullability issues for some decimal operations|
|[#9617](https://github.com/NVIDIA/spark-rapids/pull/9617)|Support rebase checking for nested dates and timestamps|
|[#9611](https://github.com/NVIDIA/spark-rapids/pull/9611)|Move simple classes after refactoring to sql-plugin-api|
|[#9618](https://github.com/NVIDIA/spark-rapids/pull/9618)|Remove unused dataTypes argument from HostShuffleCoalesceIterator|
|[#9626](https://github.com/NVIDIA/spark-rapids/pull/9626)|Fix ENV typo in pre-merge github actions [skip ci]|
|[#9593](https://github.com/NVIDIA/spark-rapids/pull/9593)|PythonRunner and RapidsErrorUtils Changes For Databricks 13.3|
|[#9607](https://github.com/NVIDIA/spark-rapids/pull/9607)|Integration tests: Install specific fastparquet version.|
|[#9610](https://github.com/NVIDIA/spark-rapids/pull/9610)|Propagate local properties to broadcast execs|
|[#9544](https://github.com/NVIDIA/spark-rapids/pull/9544)|Support batching for `RANGE` running window aggregations. Including on|
|[#9601](https://github.com/NVIDIA/spark-rapids/pull/9601)|Remove usage of deprecated scala.Proxy|
|[#9591](https://github.com/NVIDIA/spark-rapids/pull/9591)|Enable implicit JDK profile activation|
|[#9586](https://github.com/NVIDIA/spark-rapids/pull/9586)|Merge metrics and file format fixes to Delta 2.4 support|
|[#9594](https://github.com/NVIDIA/spark-rapids/pull/9594)|Revert "Ignore failing Parquet filter test to unblock CI (#9519)"|
|[#9454](https://github.com/NVIDIA/spark-rapids/pull/9454)|Support encryption and compression in disk store|
|[#9439](https://github.com/NVIDIA/spark-rapids/pull/9439)|Support stack function|
|[#9583](https://github.com/NVIDIA/spark-rapids/pull/9583)|Fix fastparquet tests to work with HDFS|
|[#9508](https://github.com/NVIDIA/spark-rapids/pull/9508)|Consolidate deps switching in an intermediate pom|
|[#9562](https://github.com/NVIDIA/spark-rapids/pull/9562)|Delta Lake 2.3.0 support|
|[#9576](https://github.com/NVIDIA/spark-rapids/pull/9576)|Move Stack classes to wrapper classes to fix non-deterministic build issue|
|[#9572](https://github.com/NVIDIA/spark-rapids/pull/9572)|Add retry for CrossJoinIterator and ConditionalNestedLoopJoinIterator|
|[#9575](https://github.com/NVIDIA/spark-rapids/pull/9575)|Fix `test_window_running*()` for `NTH_VALUE IGNORE NULLS`.|
|[#9574](https://github.com/NVIDIA/spark-rapids/pull/9574)|Fix broken #endif scala comments [skip ci]|
|[#9568](https://github.com/NVIDIA/spark-rapids/pull/9568)|Enforce Apache 3.3.0+ for Scala 2.13|
|[#9557](https://github.com/NVIDIA/spark-rapids/pull/9557)|Support launching Map Pandas UDF on empty partitions|
|[#9489](https://github.com/NVIDIA/spark-rapids/pull/9489)|Batching support for ROW-based `FIRST()` window function|
|[#9510](https://github.com/NVIDIA/spark-rapids/pull/9510)|Add Databricks 13.3 shim boilerplate code and refactor Databricks 12.2 shim|
|[#9554](https://github.com/NVIDIA/spark-rapids/pull/9554)|Fix fastparquet installation for|
|[#9536](https://github.com/NVIDIA/spark-rapids/pull/9536)|Add CPU POC of TimeZoneDB; Test some time zones by comparing CPU POC and Spark|
|[#9558](https://github.com/NVIDIA/spark-rapids/pull/9558)|Support integration test against scala2.13 spark binaries[skip ci]|
|[#8592](https://github.com/NVIDIA/spark-rapids/pull/8592)|Scala 2.13 Support|
|[#9551](https://github.com/NVIDIA/spark-rapids/pull/9551)|Enable malformed Parquet failure test|
|[#9546](https://github.com/NVIDIA/spark-rapids/pull/9546)|Support OverwriteByExpressionExecV1 for Delta Lake tables|
|[#9527](https://github.com/NVIDIA/spark-rapids/pull/9527)|Support Split And Retry for GpuProjectAstExec|
|[#9541](https://github.com/NVIDIA/spark-rapids/pull/9541)|Move simple classes to  API|
|[#9548](https://github.com/NVIDIA/spark-rapids/pull/9548)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#9418](https://github.com/NVIDIA/spark-rapids/pull/9418)|Fix STRUCT comparison between Pandas and Spark dataframes in fastparquet tests|
|[#9468](https://github.com/NVIDIA/spark-rapids/pull/9468)|Add SplitAndRetry to GpuRunningWindowIterator|
|[#9486](https://github.com/NVIDIA/spark-rapids/pull/9486)|Add partial support for `to_json`|
|[#9538](https://github.com/NVIDIA/spark-rapids/pull/9538)|Fix tiered project breaking higher order functions|
|[#9539](https://github.com/NVIDIA/spark-rapids/pull/9539)|Add delta-24x to delta-lake/README.md [skip ci]|
|[#9534](https://github.com/NVIDIA/spark-rapids/pull/9534)|Add pyarrow tests for Databricks runtime|
|[#9444](https://github.com/NVIDIA/spark-rapids/pull/9444)|Remove redundant pass-through shuffle manager classes|
|[#9531](https://github.com/NVIDIA/spark-rapids/pull/9531)|Fix relative path for spark-shell nightly test [skip ci]|
|[#9525](https://github.com/NVIDIA/spark-rapids/pull/9525)|Follow-up to dbdeps consolidation|
|[#9506](https://github.com/NVIDIA/spark-rapids/pull/9506)|Move ProxyShuffleInternalManagerBase to api|
|[#9504](https://github.com/NVIDIA/spark-rapids/pull/9504)|Add a spark-shell smoke test to premerge and nightly|
|[#9519](https://github.com/NVIDIA/spark-rapids/pull/9519)|Ignore failing Parquet filter test to unblock CI|
|[#9478](https://github.com/NVIDIA/spark-rapids/pull/9478)|Support AppendDataExecV1 for Delta Lake tables|
|[#9366](https://github.com/NVIDIA/spark-rapids/pull/9366)|Add tests to check compatibility with `fastparquet`|
|[#9419](https://github.com/NVIDIA/spark-rapids/pull/9419)|Add retry to RoundRobin Partitioner and Range Partitioner|
|[#9502](https://github.com/NVIDIA/spark-rapids/pull/9502)|Install Dependencies Needed For Databricks 13.3|
|[#9296](https://github.com/NVIDIA/spark-rapids/pull/9296)|Implement `percentile` aggregation|
|[#9488](https://github.com/NVIDIA/spark-rapids/pull/9488)|Add Shim JSON Headers for Databricks 13.3|
|[#9443](https://github.com/NVIDIA/spark-rapids/pull/9443)|Add AtomicReplaceTableAsSelectExec support for Delta Lake|
|[#9476](https://github.com/NVIDIA/spark-rapids/pull/9476)|Refactor common Delta Lake test code|
|[#9463](https://github.com/NVIDIA/spark-rapids/pull/9463)|Fix Cloudera 3.3.2 shim for handling CheckOverflowInTableInsert and orc zstd support|
|[#9460](https://github.com/NVIDIA/spark-rapids/pull/9460)|Update links in old release notes to new doc locations [skip ci]|
|[#9405](https://github.com/NVIDIA/spark-rapids/pull/9405)|Wrap scalar generation into spark session in integration test|
|[#9459](https://github.com/NVIDIA/spark-rapids/pull/9459)|Fix 332cdh build [skip ci]|
|[#9425](https://github.com/NVIDIA/spark-rapids/pull/9425)|Add support for AtomicCreateTableAsSelect with Delta Lake|
|[#9434](https://github.com/NVIDIA/spark-rapids/pull/9434)|Add retry support to `HostToGpuCoalesceIterator.concatAllAndPutOnGPU`|
|[#9453](https://github.com/NVIDIA/spark-rapids/pull/9453)|Update codeowner and blossom-ci ACL [skip ci]|
|[#9396](https://github.com/NVIDIA/spark-rapids/pull/9396)|Add support for Cloudera CDS-3.3.2|
|[#9380](https://github.com/NVIDIA/spark-rapids/pull/9380)|Fix parsing of Parquet legacy list-of-struct format|
|[#9438](https://github.com/NVIDIA/spark-rapids/pull/9438)|Fix auto merge conflict 9437 [skip ci]|
|[#9424](https://github.com/NVIDIA/spark-rapids/pull/9424)|Refactor aggregate functions|
|[#9414](https://github.com/NVIDIA/spark-rapids/pull/9414)|Add retry to GpuHashJoin.filterNulls|
|[#9388](https://github.com/NVIDIA/spark-rapids/pull/9388)|Add developer documentation about working with data sources [skip ci]|
|[#9369](https://github.com/NVIDIA/spark-rapids/pull/9369)|Improve JSON empty row fix to use less memory|
|[#9373](https://github.com/NVIDIA/spark-rapids/pull/9373)|Fix auto merge conflict 9372|
|[#9308](https://github.com/NVIDIA/spark-rapids/pull/9308)|Initiate arm64 CI support [skip ci]|
|[#9292](https://github.com/NVIDIA/spark-rapids/pull/9292)|Init project version 23.12.0-SNAPSHOT|


## Release 23.10

### Features
|||
|:---|:---|
|[#9220](https://github.com/NVIDIA/spark-rapids/issues/9220)|[FEA] Add GPU support for converting binary data to a hex string in REPL|
|[#9171](https://github.com/NVIDIA/spark-rapids/issues/9171)|[FEA] Add GPU version of ToPrettyString|
|[#5314](https://github.com/NVIDIA/spark-rapids/issues/5314)|[FEA] Support window.rowsBetween(Window.unboundedPreceding, -1) |
|[#9057](https://github.com/NVIDIA/spark-rapids/issues/9057)|[FEA] Add unbounded to unbounded fixers for min and max|
|[#8121](https://github.com/NVIDIA/spark-rapids/issues/8121)|[FEA] Add Spark 3.5.0 shim layer|
|[#9224](https://github.com/NVIDIA/spark-rapids/issues/9224)|[FEA] Allow } and }} to be transpiled to static strings|
|[#8596](https://github.com/NVIDIA/spark-rapids/issues/8596)|[FEA] Support spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY|
|[#8767](https://github.com/NVIDIA/spark-rapids/issues/8767)|[AUDIT][SPARK-43302][SQL] Make Python UDAF an AggregateFunction|
|[#9055](https://github.com/NVIDIA/spark-rapids/issues/9055)|[FEA]  Support Spark 3.3.3 official release|
|[#8672](https://github.com/NVIDIA/spark-rapids/issues/8672)|[FEA] Make GPU readers easier to debug on failure (any failure including OOM)|
|[#8965](https://github.com/NVIDIA/spark-rapids/issues/8965)|[FEA] Enable Bloom filter join acceleration by default|
|[#8625](https://github.com/NVIDIA/spark-rapids/issues/8625)|[FEA] Support outputTimestampType being INT96|

### Performance
|||
|:---|:---|
|[#9512](https://github.com/NVIDIA/spark-rapids/issues/9512)|[DOC] Multi-Threaded shuffle documentation is not accurate on the read side|
|[#7803](https://github.com/NVIDIA/spark-rapids/issues/7803)|[FEA] Accelerate Bloom filtered joins |

### Bugs Fixed
|||
|:---|:---|
|[#8662](https://github.com/NVIDIA/spark-rapids/issues/8662)|[BUG] Dataproc spark-rapids.sh fails due to cuda driver version issue|
|[#9428](https://github.com/NVIDIA/spark-rapids/issues/9428)|[Audit] SPARK-44448 Wrong results for dense_rank() <= k|
|[#9485](https://github.com/NVIDIA/spark-rapids/issues/9485)|[BUG] GpuSemaphore can deadlock if there are multiple threads per task|
|[#9498](https://github.com/NVIDIA/spark-rapids/issues/9498)|[BUG] spark 3.5.0 shim spark-shell is broken in spark-rapids 23.10 and 23.12|
|[#9060](https://github.com/NVIDIA/spark-rapids/issues/9060)|[BUG] OOM error in split and retry with multifile coalesce reader with parquet data|
|[#8916](https://github.com/NVIDIA/spark-rapids/issues/8916)|[BUG] Databricks - move init scripts off DBFS|
|[#9416](https://github.com/NVIDIA/spark-rapids/issues/9416)|[BUG] CDH build failed due to missing dependencies|
|[#9357](https://github.com/NVIDIA/spark-rapids/issues/9357)|[BUG] json_test failed on "NameError: name 'TimestampNTZType' is not defined"|
|[#9271](https://github.com/NVIDIA/spark-rapids/issues/9271)|[BUG] ThreadPool size is deduced incorrectly in MultiFileReaderThreadPool on YARN clusters|
|[#9309](https://github.com/NVIDIA/spark-rapids/issues/9309)|[BUG] bround  and round do not return the correct result for some decimal values.|
|[#9153](https://github.com/NVIDIA/spark-rapids/issues/9153)|[BUG] netty OOM with MULTITHREADED shuffle|
|[#9311](https://github.com/NVIDIA/spark-rapids/issues/9311)|[BUG] test_hash_groupby_collect_list fails|
|[#9180](https://github.com/NVIDIA/spark-rapids/issues/9180)|[FEA][AUDIT][SPARK-44641] Incorrect result in certain scenarios when SPJ is not triggered|
|[#9290](https://github.com/NVIDIA/spark-rapids/issues/9290)|[BUG] delta_lake_test FAILED on "column mapping mode id is not supported for this Delta version"|
|[#9255](https://github.com/NVIDIA/spark-rapids/issues/9255)|[BUG] Unable to read DeltaTable with columnMapping.mode = name|
|[#9261](https://github.com/NVIDIA/spark-rapids/issues/9261)|[BUG] Leaks and Double Frees in Unit Tests|
|[#9246](https://github.com/NVIDIA/spark-rapids/issues/9246)|[BUG] `test_predefined_character_classes` failed with seed 4|
|[#9208](https://github.com/NVIDIA/spark-rapids/issues/9208)|[BUG] SplitAndRetryOOM query14_part1 at 100TB with spark.executor.cores=64|
|[#9106](https://github.com/NVIDIA/spark-rapids/issues/9106)|[BUG] Configuring GDS breaks new host spillable buffers and batches|
|[#9131](https://github.com/NVIDIA/spark-rapids/issues/9131)|[BUG] ConcurrentModificationException in ScalableTaskCompletion|
|[#9263](https://github.com/NVIDIA/spark-rapids/issues/9263)|[BUG] Unit test logging is not captured when running against Spark 3.5.0|
|[#9168](https://github.com/NVIDIA/spark-rapids/issues/9168)|[BUG] Calling RmmSpark.getAndResetNumRetryThrow from tests is not working|
|[#8776](https://github.com/NVIDIA/spark-rapids/issues/8776)|[BUG] FileCacheIntegrationSuite intermittent failure|
|[#9223](https://github.com/NVIDIA/spark-rapids/issues/9223)|[BUG] Failed to create memory map on query14_part1 at 100TB with spark.executor.cores=64|
|[#9116](https://github.com/NVIDIA/spark-rapids/issues/9116)|[BUG] spark350 shim build failed in mvn-verify github checks and nightly due to dependencies not released|
|[#8984](https://github.com/NVIDIA/spark-rapids/issues/8984)|[BUG] Check that keys are not null when creating a map|
|[#9233](https://github.com/NVIDIA/spark-rapids/issues/9233)|[BUG] test_parquet_testing_error_files - Failed: DID NOT RAISE <class 'Exception'> in databricks runtime 12.2|
|[#9142](https://github.com/NVIDIA/spark-rapids/issues/9142)|[BUG] AWS EMR 6.12 NDS SF3k query9 Failure on g4dn.4xlarge|
|[#9214](https://github.com/NVIDIA/spark-rapids/issues/9214)|[BUG] mvn resolve dependencies failed missing rapids-4-spark-sql-plugin-api_2.12 of 311 shim|
|[#9204](https://github.com/NVIDIA/spark-rapids/issues/9204)|[BUG] SplitAndRetryOOM query78 at 100TB with spark.executor.cores=64|
|[#9213](https://github.com/NVIDIA/spark-rapids/issues/9213)|[BUG] Missing revision info in databricks shims failed nightly build|
|[#9206](https://github.com/NVIDIA/spark-rapids/issues/9206)|[BUG] test_datetime_roundtrip_with_legacy_rebase failed in databricks runtimes|
|[#9165](https://github.com/NVIDIA/spark-rapids/issues/9165)|[BUG] Data gen for key groups produces type-mismatch columns|
|[#9129](https://github.com/NVIDIA/spark-rapids/issues/9129)|[BUG] Writing Parquet map(map) column can not set the outer key as non-null.|
|[#9194](https://github.com/NVIDIA/spark-rapids/issues/9194)|[BUG] missing sql-plugin-api databricks artifacts in the nightly CI |
|[#9167](https://github.com/NVIDIA/spark-rapids/issues/9167)|[BUG] Ensure no udf-compiler internal nodes escape|
|[#9092](https://github.com/NVIDIA/spark-rapids/issues/9092)|[BUG] NDS query 64 falls back to CPU only for a shuffle|
|[#9071](https://github.com/NVIDIA/spark-rapids/issues/9071)|[BUG] `test_numeric_running_sum_window_no_part_unbounded` failed in MT tests|
|[#9154](https://github.com/NVIDIA/spark-rapids/issues/9154)|[BUG] Spark 3.5.0 nightly build failures (test_parquet_testing_error_files)|
|[#9149](https://github.com/NVIDIA/spark-rapids/issues/9149)|[BUG] compile failed in databricks runtimes due to new added TestReport|
|[#9041](https://github.com/NVIDIA/spark-rapids/issues/9041)|[BUG] Fix regression in Python UDAF support when running against Spark 3.5.0|
|[#9064](https://github.com/NVIDIA/spark-rapids/issues/9064)|[BUG][Spark 3.5.0] Re-enable test_hive_empty_simple_udf when 3.5.0-rc2 is available|
|[#9065](https://github.com/NVIDIA/spark-rapids/issues/9065)|[BUG][Spark 3.5.0] Reinstate cast map/array to string tests when 3.5.0-rc2 is available|
|[#9119](https://github.com/NVIDIA/spark-rapids/issues/9119)|[BUG] Predicate pushdown doesn't work for parquet files written by GPU|
|[#9103](https://github.com/NVIDIA/spark-rapids/issues/9103)|[BUG] test_select_complex_field fails in MT tests|
|[#9086](https://github.com/NVIDIA/spark-rapids/issues/9086)|[BUG] GpuBroadcastNestedLoopJoinExec can assert in doUnconditionalJoin|
|[#8939](https://github.com/NVIDIA/spark-rapids/issues/8939)|[BUG]  q95 odd task failure in query95 at 30TB|
|[#9082](https://github.com/NVIDIA/spark-rapids/issues/9082)|[BUG] Race condition while spilling and aliasing a RapidsBuffer (regression)|
|[#9069](https://github.com/NVIDIA/spark-rapids/issues/9069)|[BUG] ParquetFormatScanSuite does not pass locally|
|[#8980](https://github.com/NVIDIA/spark-rapids/issues/8980)|[BUG] invalid escape sequences in pytests|
|[#7807](https://github.com/NVIDIA/spark-rapids/issues/7807)|[BUG] Round robin partitioning sort check falls back to CPU for cases that can be supported|
|[#8482](https://github.com/NVIDIA/spark-rapids/issues/8482)|[BUG] Potential leak on SplitAndRetry when iterator not fully drained|
|[#8942](https://github.com/NVIDIA/spark-rapids/issues/8942)|[BUG] NDS query 14 parts 1 and 2 both fail at SF100K|
|[#8778](https://github.com/NVIDIA/spark-rapids/issues/8778)|[BUG] GPU Parquet output for TIMESTAMP_MICROS is misinteterpreted by fastparquet as nanos|

### PRs
|||
|:---|:---|
|[#9304](https://github.com/NVIDIA/spark-rapids/pull/9304)|Specify recoverWithNull when reading JSON files|
|[#9474](https://github.com/NVIDIA/spark-rapids/pull/9474)| Improve configuration handling in BatchWithPartitionData|
|[#9289](https://github.com/NVIDIA/spark-rapids/pull/9289)|Add tests to check compatibility with pyarrow|
|[#9522](https://github.com/NVIDIA/spark-rapids/pull/9522)|Update 23.10 changelog [skip ci]|
|[#9501](https://github.com/NVIDIA/spark-rapids/pull/9501)|Fix GpuSemaphore to support multiple threads per task|
|[#9500](https://github.com/NVIDIA/spark-rapids/pull/9500)|Fix Spark 3.5.0 shell classloader issue with the plugin|
|[#9230](https://github.com/NVIDIA/spark-rapids/pull/9230)|Fix reading partition value columns larger than cudf column size limit|
|[#9427](https://github.com/NVIDIA/spark-rapids/pull/9427)|[DOC] Update docs for 23.10.0 release [skip ci]|
|[#9421](https://github.com/NVIDIA/spark-rapids/pull/9421)|Init changelog of 23.10 [skip ci]|
|[#9445](https://github.com/NVIDIA/spark-rapids/pull/9445)|Only run test_csv_infer_schema_timestamp_ntz tests with PySpark >= 3.4.1|
|[#9420](https://github.com/NVIDIA/spark-rapids/pull/9420)|Update private and jni dep version to released 23.10.0|
|[#9415](https://github.com/NVIDIA/spark-rapids/pull/9415)|[BUG] fix docker modified check in premerge [skip ci]|
|[#9407](https://github.com/NVIDIA/spark-rapids/pull/9407)|[Doc]Update docs for 23.08.2 version[skip ci]|
|[#9392](https://github.com/NVIDIA/spark-rapids/pull/9392)|Only run test_json_ts_formats_round_trip_ntz tests with PySpark >= 3.4.1|
|[#9401](https://github.com/NVIDIA/spark-rapids/pull/9401)|Remove using mamba before they fix the incompatibility issue [skip ci]|
|[#9381](https://github.com/NVIDIA/spark-rapids/pull/9381)|Change the executor core calculation to take into account the cluster manager|
|[#9351](https://github.com/NVIDIA/spark-rapids/pull/9351)|Put back in full decimal support for format_number|
|[#9374](https://github.com/NVIDIA/spark-rapids/pull/9374)|GpuCoalesceBatches should throw SplitAndRetyOOM on GPU OOM error|
|[#9238](https://github.com/NVIDIA/spark-rapids/pull/9238)|Simplified handling of GPU core dumps|
|[#9362](https://github.com/NVIDIA/spark-rapids/pull/9362)|[DOC] Removing User Guide pages that will be source of truth on docs.nvidia…|
|[#9365](https://github.com/NVIDIA/spark-rapids/pull/9365)|Update DataWriteCommandExec docs to reflect ORC support for nested types|
|[#9277](https://github.com/NVIDIA/spark-rapids/pull/9277)|[Doc]Remove CUDA related requirement from download page.[Skip CI]|
|[#9352](https://github.com/NVIDIA/spark-rapids/pull/9352)|Refine rules for skipping `test_csv_infer_schema_timestamp_ntz_*` tests|
|[#9334](https://github.com/NVIDIA/spark-rapids/pull/9334)|Add NaNs to Data Generators In Floating-Point Testing|
|[#9344](https://github.com/NVIDIA/spark-rapids/pull/9344)|Update MULTITHREADED shuffle maxBytesInFlight default to 128MB|
|[#9330](https://github.com/NVIDIA/spark-rapids/pull/9330)|Add Hao to blossom-ci whitelist|
|[#9328](https://github.com/NVIDIA/spark-rapids/pull/9328)|Building different Cuda versions section profile does not take effect [skip ci]|
|[#9329](https://github.com/NVIDIA/spark-rapids/pull/9329)|Add kuhushukla to blossom ci yml|
|[#9281](https://github.com/NVIDIA/spark-rapids/pull/9281)|Support `format_number`|
|[#9335](https://github.com/NVIDIA/spark-rapids/pull/9335)|Temporarily skip failing tests test_csv_infer_schema_timestamp_ntz*|
|[#9318](https://github.com/NVIDIA/spark-rapids/pull/9318)|Update authorized user in blossom-ci whitelist [skip ci]|
|[#9221](https://github.com/NVIDIA/spark-rapids/pull/9221)|Add GPU version of ToPrettyString|
|[#9321](https://github.com/NVIDIA/spark-rapids/pull/9321)|[DOC] Fix some incorrect config links in doc [skip ci]|
|[#9314](https://github.com/NVIDIA/spark-rapids/pull/9314)|Fix RMM crash in FileCacheIntegrationSuite with ARENA memory allocator|
|[#9287](https://github.com/NVIDIA/spark-rapids/pull/9287)|Allow checkpoint and restore on non-deterministic expressions in GpuFilter and GpuProject|
|[#9146](https://github.com/NVIDIA/spark-rapids/pull/9146)|Improve some CSV integration tests|
|[#9159](https://github.com/NVIDIA/spark-rapids/pull/9159)|Update tests and documentation for `spark.sql.timestampType` when reading CSV/JSON|
|[#9313](https://github.com/NVIDIA/spark-rapids/pull/9313)|Sort results of collect_list test before comparing since it is not guaranteed|
|[#9286](https://github.com/NVIDIA/spark-rapids/pull/9286)|[FEA][AUDIT][SPARK-44641] Incorrect result in certain scenarios when SPJ is not triggered|
|[#9229](https://github.com/NVIDIA/spark-rapids/pull/9229)|Support negative preceding/following for ROW-based window functions|
|[#9297](https://github.com/NVIDIA/spark-rapids/pull/9297)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#9294](https://github.com/NVIDIA/spark-rapids/pull/9294)|Fix test_delta_read_column_mapping test failures on Spark 3.2.x and 3.3.x|
|[#9285](https://github.com/NVIDIA/spark-rapids/pull/9285)|Add CastOptions to make GpuCast extendible to handle more options|
|[#9279](https://github.com/NVIDIA/spark-rapids/pull/9279)|Fix file format checks to be exact and handle Delta Lake column mapping|
|[#9283](https://github.com/NVIDIA/spark-rapids/pull/9283)|Refactor ExternalSource to move some APIs to converted GPU format or scan|
|[#9264](https://github.com/NVIDIA/spark-rapids/pull/9264)|Fix leak in test and double free in corner case|
|[#9280](https://github.com/NVIDIA/spark-rapids/pull/9280)|Fix some issues found with different seeds in integration tests|
|[#9257](https://github.com/NVIDIA/spark-rapids/pull/9257)|Have host spill use the new HostAlloc API|
|[#9253](https://github.com/NVIDIA/spark-rapids/pull/9253)|Enforce Scala method syntax over deprecated procedure syntax|
|[#9273](https://github.com/NVIDIA/spark-rapids/pull/9273)|Add arm64 profile to build arm artifacts|
|[#9270](https://github.com/NVIDIA/spark-rapids/pull/9270)|Remove GDS spilling|
|[#9267](https://github.com/NVIDIA/spark-rapids/pull/9267)|Roll our own BufferedIterator so we can close cleanly|
|[#9266](https://github.com/NVIDIA/spark-rapids/pull/9266)|Specify correct dependency versions for 350 build|
|[#9262](https://github.com/NVIDIA/spark-rapids/pull/9262)|Add Delta Lake support for Spark 3.4.1 and Delta Lake tests on Spark 3.4.x|
|[#9256](https://github.com/NVIDIA/spark-rapids/pull/9256)|Test Parquet double column stat without NaN|
|[#9254](https://github.com/NVIDIA/spark-rapids/pull/9254)|[Doc]update the emr getting started doc for emr-6130 release[skip ci]|
|[#9228](https://github.com/NVIDIA/spark-rapids/pull/9228)|Add in unbounded to unbounded optimization for min/max|
|[#9252](https://github.com/NVIDIA/spark-rapids/pull/9252)|Add Spark 3.5.0 to list of supported Spark versions [skip ci]|
|[#9251](https://github.com/NVIDIA/spark-rapids/pull/9251)|Enable a couple of retry asserts in internal row to cudf row iterator suite|
|[#9239](https://github.com/NVIDIA/spark-rapids/pull/9239)|Handle escaping the dangling right ] and right } in the regexp transpiler|
|[#9090](https://github.com/NVIDIA/spark-rapids/pull/9090)|Add test cases for Parquet statistics|
|[#9240](https://github.com/NVIDIA/spark-rapids/pull/9240)|Fix flaky ORC filecache test|
|[#9053](https://github.com/NVIDIA/spark-rapids/pull/9053)|[DOC] update the turning guide document issues [skip ci]|
|[#9211](https://github.com/NVIDIA/spark-rapids/pull/9211)|Allow skipping host spill for a direct device->disk spill|
|[#9234](https://github.com/NVIDIA/spark-rapids/pull/9234)|Enable Spark 350 builds|
|[#9237](https://github.com/NVIDIA/spark-rapids/pull/9237)|Check for null keys when creating map|
|[#9235](https://github.com/NVIDIA/spark-rapids/pull/9235)|xfail fixed_length_byte_array.parquet test due to rapidsai/cudf#14104|
|[#9231](https://github.com/NVIDIA/spark-rapids/pull/9231)|Use conda libmamba solver to resolve intermittent libarchive issue [skip ci]|
|[#8404](https://github.com/NVIDIA/spark-rapids/pull/8404)|Add in support for FIXED_LEN_BYTE_ARRAY as binary|
|[#9225](https://github.com/NVIDIA/spark-rapids/pull/9225)|Add in a HostAlloc API for high priority and add in spilling|
|[#9207](https://github.com/NVIDIA/spark-rapids/pull/9207)|Support SplitAndRetry for GpuRangeExec|
|[#9217](https://github.com/NVIDIA/spark-rapids/pull/9217)|Fix leak in aggregate when there are retries|
|[#9200](https://github.com/NVIDIA/spark-rapids/pull/9200)|Fix a few minor things with scale test|
|[#9222](https://github.com/NVIDIA/spark-rapids/pull/9222)|Deploy classified aggregator for Databricks [skip ci]|
|[#9209](https://github.com/NVIDIA/spark-rapids/pull/9209)|Fix tests for datetime rebase in Databricks|
|[#9181](https://github.com/NVIDIA/spark-rapids/pull/9181)|[DOC] address document issues [skip ci]|
|[#9132](https://github.com/NVIDIA/spark-rapids/pull/9132)|Support `spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY`|
|[#9196](https://github.com/NVIDIA/spark-rapids/pull/9196)|Fix host memory leak for R2C|
|[#9192](https://github.com/NVIDIA/spark-rapids/pull/9192)|Throw overflow exception when interval seconds are outside of range [0, 59]|
|[#9150](https://github.com/NVIDIA/spark-rapids/pull/9150)|add error section in report and the rest queries|
|[#9189](https://github.com/NVIDIA/spark-rapids/pull/9189)|Expose host store spill|
|[#9147](https://github.com/NVIDIA/spark-rapids/pull/9147)|Make map column non-nullable when it's a key in another map.|
|[#9193](https://github.com/NVIDIA/spark-rapids/pull/9193)|Support Retry for GpuLocalLimitExec and GpuGlobalLimitExec|
|[#9183](https://github.com/NVIDIA/spark-rapids/pull/9183)|Add test to verify UDT fallback for parquet|
|[#9195](https://github.com/NVIDIA/spark-rapids/pull/9195)|Deploy sql-plugin-api artifact in DBR CI pipelines [skip ci]|
|[#9170](https://github.com/NVIDIA/spark-rapids/pull/9170)|Add in new HostAlloc API|
|[#9182](https://github.com/NVIDIA/spark-rapids/pull/9182)|Consolidate Spark vendor shim dependency management|
|[#9190](https://github.com/NVIDIA/spark-rapids/pull/9190)|Prevent returning internal compiler expressions when compiling UDFs|
|[#9164](https://github.com/NVIDIA/spark-rapids/pull/9164)|Support Retry for GpuTopN and GpuSortEachBatchIterator|
|[#9134](https://github.com/NVIDIA/spark-rapids/pull/9134)|Fix shuffle fallback due to AQE on AWS EMR|
|[#9188](https://github.com/NVIDIA/spark-rapids/pull/9188)|Fix flaky tests in FileCacheIntegrationSuite|
|[#9148](https://github.com/NVIDIA/spark-rapids/pull/9148)|Add minimum Maven module eventually containing all non-shimmable source code|
|[#9169](https://github.com/NVIDIA/spark-rapids/pull/9169)|Add retry-without-split in InternalRowToColumnarBatchIterator|
|[#9172](https://github.com/NVIDIA/spark-rapids/pull/9172)|Remove doSetSpillable in favor of setSpillable|
|[#9152](https://github.com/NVIDIA/spark-rapids/pull/9152)|Add test cases for testing Parquet compression types|
|[#9157](https://github.com/NVIDIA/spark-rapids/pull/9157)|XFAIL parquet `lz4_raw` tests for Spark 3.5.0 or later|
|[#9128](https://github.com/NVIDIA/spark-rapids/pull/9128)|Test parquet predicate pushdown for basic types and fields having dots in names|
|[#9158](https://github.com/NVIDIA/spark-rapids/pull/9158)|Add json4s dependencies for Databricks integration_tests build|
|[#9102](https://github.com/NVIDIA/spark-rapids/pull/9102)|Add retry support to GpuOutOfCoreSortIterator.mergeSortEnoughToOutput |
|[#9089](https://github.com/NVIDIA/spark-rapids/pull/9089)|Add application to run Scale Test|
|[#9143](https://github.com/NVIDIA/spark-rapids/pull/9143)|[DOC] update spark.rapids.sql.concurrentGpuTasks default value in tuning guide [skip ci]|
|[#8476](https://github.com/NVIDIA/spark-rapids/pull/8476)|Use retry with split in GpuCachedDoublePassWindowIterator|
|[#9141](https://github.com/NVIDIA/spark-rapids/pull/9141)|Removed resultDecimalType in GpuIntegralDecimalDivide|
|[#9099](https://github.com/NVIDIA/spark-rapids/pull/9099)|Spark 3.5.0 follow-on work (rc2 support + Python UDAF)|
|[#9140](https://github.com/NVIDIA/spark-rapids/pull/9140)|Bump Jython to 2.7.3|
|[#9136](https://github.com/NVIDIA/spark-rapids/pull/9136)|Moving row column conversion code from cudf to jni|
|[#9133](https://github.com/NVIDIA/spark-rapids/pull/9133)|Add 350 tag to InSubqueryShims|
|[#9124](https://github.com/NVIDIA/spark-rapids/pull/9124)|Import `scala.collection` intead of `collection`|
|[#9122](https://github.com/NVIDIA/spark-rapids/pull/9122)|Fall back to CPU if `spark.sql.execution.arrow.useLargeVarTypes` is true|
|[#9115](https://github.com/NVIDIA/spark-rapids/pull/9115)|[DOC] updates documentation related to java compatibility [skip ci]|
|[#9098](https://github.com/NVIDIA/spark-rapids/pull/9098)|Add SpillableHostColumnarBatch|
|[#9091](https://github.com/NVIDIA/spark-rapids/pull/9091)|GPU support for DynamicPruningExpression and InSubqueryExec|
|[#9117](https://github.com/NVIDIA/spark-rapids/pull/9117)|Temply disable spark 350 shim build in nightly [skip ci]|
|[#9113](https://github.com/NVIDIA/spark-rapids/pull/9113)|Instantiate execution plan capture callback via shim loader|
|[#8969](https://github.com/NVIDIA/spark-rapids/pull/8969)|Initial support for Spark 3.5.0-rc1|
|[#9100](https://github.com/NVIDIA/spark-rapids/pull/9100)|Support broadcast nested loop existence joins with no condition|
|[#8925](https://github.com/NVIDIA/spark-rapids/pull/8925)|Add GpuConv operator for the `conv` 10<->16 expression|
|[#9109](https://github.com/NVIDIA/spark-rapids/pull/9109)|[DOC] adding java 11 to download docs [skip ci]|
|[#9085](https://github.com/NVIDIA/spark-rapids/pull/9085)|Retry with smaller split on `CudfColumnSizeOverflowException`|
|[#8961](https://github.com/NVIDIA/spark-rapids/pull/8961)|Save Databricks init scripts in the workspace|
|[#9088](https://github.com/NVIDIA/spark-rapids/pull/9088)|Add retry and SplitAndRetry support to AcceleratedColumnarToRowIterator|
|[#9095](https://github.com/NVIDIA/spark-rapids/pull/9095)|Support released spark 3.3.3|
|[#9084](https://github.com/NVIDIA/spark-rapids/pull/9084)|Fix race when a rapids buffer is aliased while it is spilled|
|[#9093](https://github.com/NVIDIA/spark-rapids/pull/9093)|Update ParquetFormatScanSuite to not call CUDF directly|
|[#9068](https://github.com/NVIDIA/spark-rapids/pull/9068)|Test ORC predicate pushdown (PPD) with timestamps decimals booleans|
|[#9054](https://github.com/NVIDIA/spark-rapids/pull/9054)|Initial entry point to data generation for scale test|
|[#9070](https://github.com/NVIDIA/spark-rapids/pull/9070)|Spillable host buffer|
|[#9066](https://github.com/NVIDIA/spark-rapids/pull/9066)|Add retry support to RowToColumnarIterator|
|[#9073](https://github.com/NVIDIA/spark-rapids/pull/9073)|Stop using invalid escape sequences|
|[#9018](https://github.com/NVIDIA/spark-rapids/pull/9018)|Add test for selecting a single complex field array and its parent struct array|
|[#9067](https://github.com/NVIDIA/spark-rapids/pull/9067)|Add array support for round robin partition; Refactor pluginSupportedOrderableSig|
|[#9072](https://github.com/NVIDIA/spark-rapids/pull/9072)|Revert "Implement SumUnboundedToUnboundedFixer (#8934)"|
|[#9056](https://github.com/NVIDIA/spark-rapids/pull/9056)|Add in configs for host memory limits|
|[#9061](https://github.com/NVIDIA/spark-rapids/pull/9061)|Fix import order|
|[#8934](https://github.com/NVIDIA/spark-rapids/pull/8934)|Implement SumUnboundedToUnboundedFixer|
|[#9051](https://github.com/NVIDIA/spark-rapids/pull/9051)|Use number of threads on executor instead of driver to set core count|
|[#9040](https://github.com/NVIDIA/spark-rapids/pull/9040)|Fix issues from 23.08 merge in join_test|
|[#9045](https://github.com/NVIDIA/spark-rapids/pull/9045)|Fix auto merge conflict 9043 [skip ci]|
|[#9009](https://github.com/NVIDIA/spark-rapids/pull/9009)|Add in a layer of indirection for task completion callbacks|
|[#9013](https://github.com/NVIDIA/spark-rapids/pull/9013)|Create a two-shim jar by default on Databricks|
|[#8995](https://github.com/NVIDIA/spark-rapids/pull/8995)|Add test case for ORC statistics test|
|[#8970](https://github.com/NVIDIA/spark-rapids/pull/8970)|Add ability to debug dump input data only on errors|
|[#9003](https://github.com/NVIDIA/spark-rapids/pull/9003)|Fix auto merge conflict 9002 [skip ci]|
|[#8989](https://github.com/NVIDIA/spark-rapids/pull/8989)|Mark lazy spillables as allowSpillable in during gatherer construction|
|[#8988](https://github.com/NVIDIA/spark-rapids/pull/8988)|Move big data generator to a separate module|
|[#8987](https://github.com/NVIDIA/spark-rapids/pull/8987)|Fix host memory buffer leaks in SerializationSuite|
|[#8968](https://github.com/NVIDIA/spark-rapids/pull/8968)|Enable GPU acceleration of Bloom filter join expressions by default|
|[#8947](https://github.com/NVIDIA/spark-rapids/pull/8947)|Add ArrowUtilsShims in preparation for Spark 3.5.0|
|[#8946](https://github.com/NVIDIA/spark-rapids/pull/8946)|[Spark 3.5.0] Shim access to StructType.fromAttributes|
|[#8824](https://github.com/NVIDIA/spark-rapids/pull/8824)|Drop the in-range check at INT96 output path|
|[#8924](https://github.com/NVIDIA/spark-rapids/pull/8924)|Deprecate and delegate GpuCV.debug to cudf TableDebug|
|[#8915](https://github.com/NVIDIA/spark-rapids/pull/8915)|Move LegacyBehaviorPolicy references to shim layer|
|[#8918](https://github.com/NVIDIA/spark-rapids/pull/8918)|Output unified diff when GPU output deviates|
|[#8857](https://github.com/NVIDIA/spark-rapids/pull/8857)|Remove the pageable pool|
|[#8854](https://github.com/NVIDIA/spark-rapids/pull/8854)|Fix auto merge conflict 8853 [skip ci]|
|[#8805](https://github.com/NVIDIA/spark-rapids/pull/8805)|Bump up dep versions to 23.10.0-SNAPSHOT|
|[#8796](https://github.com/NVIDIA/spark-rapids/pull/8796)|Init version 23.10.0-SNAPSHOT|

## Release 23.08

### Features
|||
|:---|:---|
|[#5509](https://github.com/NVIDIA/spark-rapids/issues/5509)|[FEA] Support order-by on Array|
|[#7876](https://github.com/NVIDIA/spark-rapids/issues/7876)|[FEA] Add initial support for Databricks 12.2 ML LTS|
|[#8547](https://github.com/NVIDIA/spark-rapids/issues/8547)|[FEA] Add support for Delta Lake 2.4 with Spark 3.4|
|[#8633](https://github.com/NVIDIA/spark-rapids/issues/8633)|[FEA] Add support for xxHash64 function|
|[#4929](https://github.com/NVIDIA/spark-rapids/issues/4929)|[FEA] Support min/max aggregation/reduction for arrays of structs and arrays of strings|
|[#8668](https://github.com/NVIDIA/spark-rapids/issues/8668)|[FEA] Support min and max for arrays|
|[#4887](https://github.com/NVIDIA/spark-rapids/issues/4887)|[FEA] Hash partitioning on ArrayType|
|[#6680](https://github.com/NVIDIA/spark-rapids/issues/6680)|[FEA] Support hashaggregate for Array[Any]|
|[#8085](https://github.com/NVIDIA/spark-rapids/issues/8085)|[FEA] Add support for MillisToTimestamp|
|[#7801](https://github.com/NVIDIA/spark-rapids/issues/7801)|[FEA] Window Expression orderBy column is not supported in a window range function, found  DoubleType|
|[#8556](https://github.com/NVIDIA/spark-rapids/issues/8556)|[FEA] [Delta Lake] Add support for new metrics in MERGE|
|[#308](https://github.com/NVIDIA/spark-rapids/issues/308)|[FEA] Spark 3.1 adding support for  TIMESTAMP_SECONDS, TIMESTAMP_MILLIS and TIMESTAMP_MICROS functions|
|[#8122](https://github.com/NVIDIA/spark-rapids/issues/8122)|[FEA] Add spark 3.4.1 snapshot shim|
|[#8525](https://github.com/NVIDIA/spark-rapids/issues/8525)|[FEA] Add support for org.apache.spark.sql.functions.flatten|
|[#8202](https://github.com/NVIDIA/spark-rapids/issues/8202)|[FEA] List supported Spark builds when the Shim is not found|

### Performance
|||
|:---|:---|
|[#8231](https://github.com/NVIDIA/spark-rapids/issues/8231)|[FEA] Add filecache support to ORC scans|
|[#8141](https://github.com/NVIDIA/spark-rapids/issues/8141)|[FEA] Explore how to best deal with large numbers of aggregations in the short term|

### Bugs Fixed
|||
|:---|:---|
|[#9034](https://github.com/NVIDIA/spark-rapids/issues/9034)|[BUG] java.lang.ClassCastException: com.nvidia.spark.rapids.RuleNotFoundExprMeta cannot be cast to com.nvidia.spark.rapids.GeneratorExprMeta|
|[#9032](https://github.com/NVIDIA/spark-rapids/issues/9032)|[BUG] Multiple NDS queries fail with Spark-3.4.1 with bloom filter exception|
|[#8962](https://github.com/NVIDIA/spark-rapids/issues/8962)|[BUG] Nightly build failed: ExecutionPlanCaptureCallback$.class is not bitwise-identical across shims|
|[#9021](https://github.com/NVIDIA/spark-rapids/issues/9021)|[BUG] test_map_scalars_supported_key_types failed in dataproc 2.1|
|[#9020](https://github.com/NVIDIA/spark-rapids/issues/9020)|[BUG] auto-disable snapshot shims test in github action for pre-release branch|
|[#9010](https://github.com/NVIDIA/spark-rapids/issues/9010)|[BUG] Customer failure 23.08: Cannot compute hash of a table with a LIST of STRUCT columns.|
|[#8922](https://github.com/NVIDIA/spark-rapids/issues/8922)|[BUG] integration map_test:test_map_scalars_supported_key_types failures|
|[#8982](https://github.com/NVIDIA/spark-rapids/issues/8982)|[BUG] Nightly prerelease failures - OrcSuite|
|[#8978](https://github.com/NVIDIA/spark-rapids/issues/8978)|[BUG] compiling error due to OrcSuite&OrcStatisticShim in databricks runtimes|
|[#8610](https://github.com/NVIDIA/spark-rapids/issues/8610)|[BUG] query 95 @ SF30K fails with OOM exception|
|[#8955](https://github.com/NVIDIA/spark-rapids/issues/8955)|[BUG] Bloom filter join tests can fail with multiple join columns|
|[#45](https://github.com/NVIDIA/spark-rapids/issues/45)|[BUG] very large shuffles can fail|
|[#8779](https://github.com/NVIDIA/spark-rapids/issues/8779)|[BUG] Put shared Databricks test script together for ease of maintenance|
|[#8930](https://github.com/NVIDIA/spark-rapids/issues/8930)|[BUG] checkoutSCM plugin is unstable for pre-merge CI, it is often unable to clone submodules|
|[#8923](https://github.com/NVIDIA/spark-rapids/issues/8923)|[BUG] Mortgage test failing with 'JavaPackage' error on AWS Databricks|
|[#8303](https://github.com/NVIDIA/spark-rapids/issues/8303)|[BUG] GpuExpression columnarEval can return scalars from subqueries that may be unhandled|
|[#8318](https://github.com/NVIDIA/spark-rapids/issues/8318)|[BUG][Databricks 12.2] GpuRowBasedHiveGenericUDF ClassCastException|
|[#8822](https://github.com/NVIDIA/spark-rapids/issues/8822)|[BUG] Early terminate CI if submodule init failed|
|[#8847](https://github.com/NVIDIA/spark-rapids/issues/8847)|[BUG] github actions CI messed up w/ JDK versions intermittently|
|[#8716](https://github.com/NVIDIA/spark-rapids/issues/8716)|[BUG] `test_hash_groupby_collect_set_on_nested_type` and `test_hash_reduction_collect_set_on_nested_type` failed|
|[#8827](https://github.com/NVIDIA/spark-rapids/issues/8827)|[BUG] databricks cudf_udf night build failing with pool size exceeded errors|
|[#8630](https://github.com/NVIDIA/spark-rapids/issues/8630)|[BUG] Parquet with RLE encoded booleans loads corrupted data|
|[#8735](https://github.com/NVIDIA/spark-rapids/issues/8735)|[BUG] test_orc_column_name_with_dots fails in nightly EGX tests|
|[#6980](https://github.com/NVIDIA/spark-rapids/issues/6980)|[BUG] Partitioned writes release GPU semaphore with unspillable GPU memory|
|[#8784](https://github.com/NVIDIA/spark-rapids/issues/8784)|[BUG] hash_aggregate_test.py::test_min_max_in_groupby_and_reduction failed on "TypeError: object of type 'NoneType' has no len()"|
|[#8756](https://github.com/NVIDIA/spark-rapids/issues/8756)|[BUG] [Databricks 12.2] RapidsDeltaWrite queries that reference internal metadata fail to run|
|[#8636](https://github.com/NVIDIA/spark-rapids/issues/8636)|[BUG] AWS Databricks 12.2 integration tests failed due to Iceberg check|
|[#8754](https://github.com/NVIDIA/spark-rapids/issues/8754)|[BUG] databricks build broke after adding bigDataGen|
|[#8726](https://github.com/NVIDIA/spark-rapids/issues/8726)|[BUG] Test "parquet_write_test.py::test_hive_timestamp_value[INJECT_OOM]" failed on Databricks |
|[#8690](https://github.com/NVIDIA/spark-rapids/issues/8690)|[BUG buildall script does not support JDK11 profile|
|[#8702](https://github.com/NVIDIA/spark-rapids/issues/8702)|[BUG] test_min_max_for_single_level_struct failed|
|[#8727](https://github.com/NVIDIA/spark-rapids/issues/8727)|[BUG] test_column_add_after_partition failed in databricks 10.4 runtime|
|[#8669](https://github.com/NVIDIA/spark-rapids/issues/8669)|[BUG] SpillableColumnarBatch doesn't always take ownership|
|[#8655](https://github.com/NVIDIA/spark-rapids/issues/8655)|[BUG] There are some potential device memory leaks in `AbstractGpuCoalesceIterator`|
|[#8685](https://github.com/NVIDIA/spark-rapids/issues/8685)|[BUG] install build fails with Maven 3.9.3|
|[#8156](https://github.com/NVIDIA/spark-rapids/issues/8156)|[BUG] Install phase for modules with Spark build classifier fails for install plugin versions 3.0.0+|
|[#1130](https://github.com/NVIDIA/spark-rapids/issues/1130)|[BUG] TIMESTAMP_MILLIS not handled in isDateTimeRebaseNeeded|
|[#7676](https://github.com/NVIDIA/spark-rapids/issues/7676)|[BUG] SparkShimsImpl  class initialization in SparkShimsSuite for 340 too eager|
|[#8278](https://github.com/NVIDIA/spark-rapids/issues/8278)|[BUG] NDS query 16 hangs at SF30K|
|[#8665](https://github.com/NVIDIA/spark-rapids/issues/8665)|[BUG] EGX nightly tests fail to detect Spark version on startup|
|[#8647](https://github.com/NVIDIA/spark-rapids/issues/8647)|[BUG] array_test.py::test_array_min_max[Float][INJECT_OOM] failed mismatched CPU and GPU output in nightly|
|[#8640](https://github.com/NVIDIA/spark-rapids/issues/8640)|[BUG] Optimize Databricks pre-merge scripts, move it out into a new CI file|
|[#8308](https://github.com/NVIDIA/spark-rapids/issues/8308)|[BUG] Device Memory leak seen in integration_tests when AssertEmptyNulls are enabled|
|[#8602](https://github.com/NVIDIA/spark-rapids/issues/8602)|[BUG] AutoCloseable Broadcast results are getting closed by Spark|
|[#8603](https://github.com/NVIDIA/spark-rapids/issues/8603)|[BUG] SerializeConcatHostBuffersDeserializeBatch.writeObject fails with ArrayIndexOutOfBoundsException on rows-only table|
|[#8615](https://github.com/NVIDIA/spark-rapids/issues/8615)|[BUG] RapidsShuffleThreadedWriterSuite temp shuffle file test failure|
|[#6872](https://github.com/NVIDIA/spark-rapids/issues/6872)|[BUG] awk: cmd. line:1: warning: regexp escape sequence `\ ' is not a known regexp operator|
|[#8588](https://github.com/NVIDIA/spark-rapids/issues/8588)|[BUG] Spark 3.3.x integration tests failed due to missing jars|
|[#7775](https://github.com/NVIDIA/spark-rapids/issues/7775)|[BUG] scala version hardcoded irrespective of Spark dependency|
|[#8548](https://github.com/NVIDIA/spark-rapids/issues/8548)|[BUG] cache_test:test_batch_no_cols test FAILED on spark-3.3.0+|
|[#8579](https://github.com/NVIDIA/spark-rapids/issues/8579)|[BUG] build failed on Databricks clusters "GpuDeleteCommand.scala:104: type mismatch" |
|[#8187](https://github.com/NVIDIA/spark-rapids/issues/8187)|[BUG] Integration test test_window_running_no_part can produce non-empty nulls (cudf scan)|
|[#8493](https://github.com/NVIDIA/spark-rapids/issues/8493)|[BUG] branch-23.08 fails to build on Databricks 12.2|

### PRs
|||
|:---|:---|
|[#9407](https://github.com/NVIDIA/spark-rapids/pull/9407)|[Doc]Update docs for 23.08.2 version[skip ci]|
|[#9382](https://github.com/NVIDIA/spark-rapids/pull/9382)|Bump up project version to 23.08.2|
|[#8476](https://github.com/NVIDIA/spark-rapids/pull/8476)|Use retry with split in GpuCachedDoublePassWindowIterator|
|[#9048](https://github.com/NVIDIA/spark-rapids/pull/9048)|Update 23.08 changelog 23/08/15 [skip ci]|
|[#9044](https://github.com/NVIDIA/spark-rapids/pull/9044)|[DOC] update release version from v2308.0 to 2308.1 [skip ci]|
|[#9036](https://github.com/NVIDIA/spark-rapids/pull/9036)|Fix meta class cast exception when generator not supported|
|[#9042](https://github.com/NVIDIA/spark-rapids/pull/9042)|Bump up project version to 23.08.1-SNAPSHOT|
|[#9035](https://github.com/NVIDIA/spark-rapids/pull/9035)|Handle null values when merging Bloom filters|
|[#9029](https://github.com/NVIDIA/spark-rapids/pull/9029)|Update 23.08 changelog to latest [skip ci]|
|[#9023](https://github.com/NVIDIA/spark-rapids/pull/9023)|Allow WindowLocalExec to run on CPU for a map test.|
|[#9024](https://github.com/NVIDIA/spark-rapids/pull/9024)|Do not trigger snapshot spark version test in pre-release maven-verify checks [skip ci]|
|[#8975](https://github.com/NVIDIA/spark-rapids/pull/8975)|Init 23.08 changelog [skip ci]|
|[#9016](https://github.com/NVIDIA/spark-rapids/pull/9016)|Fix issue where murmur3 tried to work on array of structs|
|[#9014](https://github.com/NVIDIA/spark-rapids/pull/9014)|Updating link to download jar [skip ci]|
|[#9006](https://github.com/NVIDIA/spark-rapids/pull/9006)|Revert test changes to fix binary dedup error|
|[#9001](https://github.com/NVIDIA/spark-rapids/pull/9001)|[Doc]update the emr getting started doc for emr-6120 release[skip ci]|
|[#8949](https://github.com/NVIDIA/spark-rapids/pull/8949)|Update JNI and private version to released 23.08.0|
|[#8977](https://github.com/NVIDIA/spark-rapids/pull/8977)|Create an anonymous subclass of AdaptiveSparkPlanHelper in ExecutionPlanCaptureCallback.scala|
|[#8972](https://github.com/NVIDIA/spark-rapids/pull/8972)|[Doc]Add best practice doc[skip ci]|
|[#8948](https://github.com/NVIDIA/spark-rapids/pull/8948)|[Doc]update download docs for 2308 version[skip ci]|
|[#8971](https://github.com/NVIDIA/spark-rapids/pull/8971)|Fix test_map_scalars_supported_key_types|
|[#8990](https://github.com/NVIDIA/spark-rapids/pull/8990)|Remove doc references to 312db [skip ci]|
|[#8960](https://github.com/NVIDIA/spark-rapids/pull/8960)|[Doc] address profiling tool formatted issue [skip ci]|
|[#8983](https://github.com/NVIDIA/spark-rapids/pull/8983)|Revert OrcSuite to fix deployment build|
|[#8979](https://github.com/NVIDIA/spark-rapids/pull/8979)|Fix Databricks build error for new added ORC test cases|
|[#8920](https://github.com/NVIDIA/spark-rapids/pull/8920)|Add test case to test orc dictionary encoding with lots of rows for nested types|
|[#8940](https://github.com/NVIDIA/spark-rapids/pull/8940)|Add test case for ORC statistics test|
|[#8909](https://github.com/NVIDIA/spark-rapids/pull/8909)|Match Spark's NaN handling in collect_set|
|[#8892](https://github.com/NVIDIA/spark-rapids/pull/8892)|Experimental support for BloomFilterAggregate expression in a reduction context|
|[#8957](https://github.com/NVIDIA/spark-rapids/pull/8957)|Fix building dockerfile.cuda hanging at tzdata installation [skip ci]|
|[#8944](https://github.com/NVIDIA/spark-rapids/pull/8944)|Fix issues around bloom filter with multple columns|
|[#8744](https://github.com/NVIDIA/spark-rapids/pull/8744)|Add test for selecting a single complex field array and its parent struct array|
|[#8936](https://github.com/NVIDIA/spark-rapids/pull/8936)|Device synchronize prior to freeing a set of RapidsBuffer|
|[#8935](https://github.com/NVIDIA/spark-rapids/pull/8935)|Don't go over shuffle limits on CPU|
|[#8927](https://github.com/NVIDIA/spark-rapids/pull/8927)|Skipping test_map_scalars_supported_key_types because of distributed …|
|[#8931](https://github.com/NVIDIA/spark-rapids/pull/8931)|Clone submodule using git command instead of checkoutSCM plugin|
|[#8917](https://github.com/NVIDIA/spark-rapids/pull/8917)|Databricks shim version for integration test|
|[#8775](https://github.com/NVIDIA/spark-rapids/pull/8775)|Support BloomFilterMightContain expression|
|[#8833](https://github.com/NVIDIA/spark-rapids/pull/8833)|Binary and ternary handling of scalar audit and some fixes|
|[#7233](https://github.com/NVIDIA/spark-rapids/pull/7233)|[FEA] Support `order by` on single-level array|
|[#8893](https://github.com/NVIDIA/spark-rapids/pull/8893)|Fix regression in Hive Generic UDF support on Databricks 12.2|
|[#8828](https://github.com/NVIDIA/spark-rapids/pull/8828)|Put shared part together for Databricks test scripts|
|[#8872](https://github.com/NVIDIA/spark-rapids/pull/8872)|Terminate CI if fail to clone submodule|
|[#8787](https://github.com/NVIDIA/spark-rapids/pull/8787)|Add in support for ExponentialDistribution|
|[#8868](https://github.com/NVIDIA/spark-rapids/pull/8868)|Add a test case for testing ORC version V_0_11 and V_0_12|
|[#8795](https://github.com/NVIDIA/spark-rapids/pull/8795)|Add ORC writing test cases for not implicitly lowercase columns|
|[#8871](https://github.com/NVIDIA/spark-rapids/pull/8871)|Adjust parallelism in spark-tests script to reduce memory footprint [skip ci]|
|[#8869](https://github.com/NVIDIA/spark-rapids/pull/8869)|Specify expected JAVA_HOME and bin for mvn-verify-check [skip ci]|
|[#8785](https://github.com/NVIDIA/spark-rapids/pull/8785)|Add test cases for ORC writing according to options orc.compress and compression|
|[#8810](https://github.com/NVIDIA/spark-rapids/pull/8810)|Fall back to CPU for deletion vectors writes on Databricks|
|[#8830](https://github.com/NVIDIA/spark-rapids/pull/8830)|Update documentation to add Databricks 12.2 as a supported platform [skip ci]|
|[#8799](https://github.com/NVIDIA/spark-rapids/pull/8799)|Add tests to cover some odd corner cases with nulls and empty arrays|
|[#8783](https://github.com/NVIDIA/spark-rapids/pull/8783)|Fix collect_set_on_nested_type tests failed|
|[#8855](https://github.com/NVIDIA/spark-rapids/pull/8855)|Fix bug: Check GPU file instead of CPU file [skip ci]|
|[#8852](https://github.com/NVIDIA/spark-rapids/pull/8852)|Update test scripts and dockerfiles to match cudf conda pkg change [skip ci]|
|[#8848](https://github.com/NVIDIA/spark-rapids/pull/8848)|Try mitigate mismatched JDK versions in mvn-verify checks [skip ci]|
|[#8825](https://github.com/NVIDIA/spark-rapids/pull/8825)|Add a case to test ORC writing/reading with lots of nulls|
|[#8802](https://github.com/NVIDIA/spark-rapids/pull/8802)|Treat unbounded windows as truly non-finite.|
|[#8798](https://github.com/NVIDIA/spark-rapids/pull/8798)|Add ORC writing test cases for dictionary compression|
|[#8829](https://github.com/NVIDIA/spark-rapids/pull/8829)|Enable rle_boolean_encoding.parquet test|
|[#8667](https://github.com/NVIDIA/spark-rapids/pull/8667)|Make state spillable in partitioned writer|
|[#8801](https://github.com/NVIDIA/spark-rapids/pull/8801)|Fix shuffling an empty Struct() column with UCX|
|[#8748](https://github.com/NVIDIA/spark-rapids/pull/8748)|Add driver log warning when GPU is limiting scheduling resource|
|[#8786](https://github.com/NVIDIA/spark-rapids/pull/8786)|Add support for row-based execution in RapidsDeltaWrite|
|[#8791](https://github.com/NVIDIA/spark-rapids/pull/8791)|Auto merge to branch-23.10 from branch-23.08[skip ci]|
|[#8790](https://github.com/NVIDIA/spark-rapids/pull/8790)|Update ubuntu dockerfiles default to 20.04 and deprecating centos one [skip ci]|
|[#8777](https://github.com/NVIDIA/spark-rapids/pull/8777)|Install python packages with shared scripts on Databricks|
|[#8772](https://github.com/NVIDIA/spark-rapids/pull/8772)|Test concurrent writer update file metrics|
|[#8646](https://github.com/NVIDIA/spark-rapids/pull/8646)|Add testing of Parquet files from apache/parquet-testing|
|[#8684](https://github.com/NVIDIA/spark-rapids/pull/8684)|Add 'submodule update --init' when build spark-rapids|
|[#8769](https://github.com/NVIDIA/spark-rapids/pull/8769)|Remove iceberg scripts from Databricks test scripts|
|[#8773](https://github.com/NVIDIA/spark-rapids/pull/8773)|Add a test case for reading/write null to ORC|
|[#8749](https://github.com/NVIDIA/spark-rapids/pull/8749)|Add test cases for read/write User Defined Type (UDT) to ORC|
|[#8768](https://github.com/NVIDIA/spark-rapids/pull/8768)|Add support for xxhash64|
|[#8751](https://github.com/NVIDIA/spark-rapids/pull/8751)|Ensure columnarEval always returns a GpuColumnVector|
|[#8765](https://github.com/NVIDIA/spark-rapids/pull/8765)|Add in support for maps to big data gen|
|[#8758](https://github.com/NVIDIA/spark-rapids/pull/8758)|Normal and Multi Distributions for BigDataGen|
|[#8755](https://github.com/NVIDIA/spark-rapids/pull/8755)|Add in dependency for databricks on integration tests|
|[#8737](https://github.com/NVIDIA/spark-rapids/pull/8737)|Fix parquet_write_test.py::test_hive_timestamp_value failure for Databricks|
|[#8745](https://github.com/NVIDIA/spark-rapids/pull/8745)|Conventional jar layout is not required for JDK9+|
|[#8706](https://github.com/NVIDIA/spark-rapids/pull/8706)|Add a tool to support generating large amounts of data|
|[#8747](https://github.com/NVIDIA/spark-rapids/pull/8747)|xfail hash_groupby_collect_set and hash_reduction_collect_set on nested type cases|
|[#8689](https://github.com/NVIDIA/spark-rapids/pull/8689)|Support nested arrays for `min`/`max` aggregations in groupby and reduction|
|[#8699](https://github.com/NVIDIA/spark-rapids/pull/8699)|Regression test for array of struct with a single field name "element" in Parquet|
|[#8733](https://github.com/NVIDIA/spark-rapids/pull/8733)|Avoid generating numeric null partition values on Databricks 10.4|
|[#8728](https://github.com/NVIDIA/spark-rapids/pull/8728)|Use specific mamba version and install libarchive explictly [skip ci]|
|[#8594](https://github.com/NVIDIA/spark-rapids/pull/8594)|String generation from complex regex in integration tests|
|[#8700](https://github.com/NVIDIA/spark-rapids/pull/8700)|Add regression test to ensure Parquet doesn't interpret timestamp values differently from Hive 0.14.0+|
|[#8711](https://github.com/NVIDIA/spark-rapids/pull/8711)|Factor out modules shared among shim profiles|
|[#8697](https://github.com/NVIDIA/spark-rapids/pull/8697)|Spillable columnar batch takes ownership and improve code coverage|
|[#8705](https://github.com/NVIDIA/spark-rapids/pull/8705)|Add schema evolution integration tests for partitioned data|
|[#8673](https://github.com/NVIDIA/spark-rapids/pull/8673)|Fix some potential memory leaks|
|[#8707](https://github.com/NVIDIA/spark-rapids/pull/8707)|Update config docs for new filecache configs [skip ci]|
|[#8695](https://github.com/NVIDIA/spark-rapids/pull/8695)|Always create the main artifact along with a shim-classifier artifact|
|[#8704](https://github.com/NVIDIA/spark-rapids/pull/8704)|Add tests for column names with dots|
|[#8703](https://github.com/NVIDIA/spark-rapids/pull/8703)|Comment out min/max agg test for nested structs to unblock CI|
|[#8698](https://github.com/NVIDIA/spark-rapids/pull/8698)|Cache last ORC stripe footer to avoid redundant remote reads|
|[#8687](https://github.com/NVIDIA/spark-rapids/pull/8687)|Handle TIMESTAMP_MILLIS for rebase check|
|[#8688](https://github.com/NVIDIA/spark-rapids/pull/8688)|Enable the 340 shim test|
|[#8656](https://github.com/NVIDIA/spark-rapids/pull/8656)|Return result from filecache message instead of null|
|[#8659](https://github.com/NVIDIA/spark-rapids/pull/8659)|Filter out nulls for build batches when needed in hash joins|
|[#8682](https://github.com/NVIDIA/spark-rapids/pull/8682)|[DOC] Update CUDA requirements in documentation and Dockerfiles[skip ci]|
|[#8637](https://github.com/NVIDIA/spark-rapids/pull/8637)|Support Float order-by columns for RANGE window functions|
|[#8681](https://github.com/NVIDIA/spark-rapids/pull/8681)|changed container name to adapt to blossom-lib refactor [skip ci]|
|[#8573](https://github.com/NVIDIA/spark-rapids/pull/8573)|Add support for Delta Lake 2.4.0|
|[#8671](https://github.com/NVIDIA/spark-rapids/pull/8671)|Fix use-after-freed bug in `GpuFloatArrayMin`|
|[#8650](https://github.com/NVIDIA/spark-rapids/pull/8650)|Support TIMESTAMP_SECONDS, TIMESTAMP_MILLIS and TIMESTAMP_MICROS|
|[#8495](https://github.com/NVIDIA/spark-rapids/pull/8495)|Speed up PCBS CPU read path by not recalculating as much|
|[#8389](https://github.com/NVIDIA/spark-rapids/pull/8389)|Add filecache support for ORC|
|[#8658](https://github.com/NVIDIA/spark-rapids/pull/8658)|Check if need to run Databricks pre-merge|
|[#8649](https://github.com/NVIDIA/spark-rapids/pull/8649)|Add Spark 3.4.1 shim|
|[#8624](https://github.com/NVIDIA/spark-rapids/pull/8624)|Rename numBytesAdded/Removed metrics and add deletion vector metrics in Databricks 12.2 shims|
|[#8645](https://github.com/NVIDIA/spark-rapids/pull/8645)|Fix "PytestUnknownMarkWarning: Unknown pytest.mark.inject_oom" warning|
|[#8608](https://github.com/NVIDIA/spark-rapids/pull/8608)|Matrix stages to dynamically build Databricks shims|
|[#8517](https://github.com/NVIDIA/spark-rapids/pull/8517)|Revert "Disable asserts for non-empty nulls (#8183)"|
|[#8628](https://github.com/NVIDIA/spark-rapids/pull/8628)|Enable Delta Write fallback tests on Databricks 12.2|
|[#8632](https://github.com/NVIDIA/spark-rapids/pull/8632)|Fix GCP examples and getting started guide [skip ci]|
|[#8638](https://github.com/NVIDIA/spark-rapids/pull/8638)|Support nested structs for `min`/`max` aggregations in groupby and reduction|
|[#8639](https://github.com/NVIDIA/spark-rapids/pull/8639)|Add iceberg test for nightly DB12.2 IT pipeline[skip ci]|
|[#8618](https://github.com/NVIDIA/spark-rapids/pull/8618)|Heuristic to speed up partial aggregates that get larger|
|[#8605](https://github.com/NVIDIA/spark-rapids/pull/8605)|[Doc] Fix demo link in index.md [skip ci]|
|[#8619](https://github.com/NVIDIA/spark-rapids/pull/8619)|Enable output batches metric for GpuShuffleCoalesceExec by default|
|[#8617](https://github.com/NVIDIA/spark-rapids/pull/8617)|Fixes broadcast spill serialization/deserialization|
|[#8531](https://github.com/NVIDIA/spark-rapids/pull/8531)|filecache: Modify FileCacheLocalityManager.init to pass in Spark context|
|[#8613](https://github.com/NVIDIA/spark-rapids/pull/8613)|Try print JVM core dump files if any test failures in CI|
|[#8616](https://github.com/NVIDIA/spark-rapids/pull/8616)|Wait for futures in multi-threaded writers even on exception|
|[#8578](https://github.com/NVIDIA/spark-rapids/pull/8578)|Add in metric to see how much computation time is lost due to retry|
|[#8590](https://github.com/NVIDIA/spark-rapids/pull/8590)|Drop ".dev0" suffix from Spark SNASHOT distro builds|
|[#8604](https://github.com/NVIDIA/spark-rapids/pull/8604)|Upgrade scalatest version to 3.2.16|
|[#8555](https://github.com/NVIDIA/spark-rapids/pull/8555)|Support `flatten`  SQL function|
|[#8599](https://github.com/NVIDIA/spark-rapids/pull/8599)|Fix broken links in advanced_configs.md|
|[#8589](https://github.com/NVIDIA/spark-rapids/pull/8589)|Revert to the JVM-based Spark version extraction in pytests|
|[#8582](https://github.com/NVIDIA/spark-rapids/pull/8582)|Fix databricks shims build errors caused by DB updates|
|[#8564](https://github.com/NVIDIA/spark-rapids/pull/8564)|Fold `verify-all-modules-with-headSparkVersion` into `verify-all-modules` [skip ci]|
|[#8553](https://github.com/NVIDIA/spark-rapids/pull/8553)|Handle empty batch in ParquetCachedBatchSerializer|
|[#8575](https://github.com/NVIDIA/spark-rapids/pull/8575)|Corrected typos in CONTRIBUTING.md [skip ci]|
|[#8574](https://github.com/NVIDIA/spark-rapids/pull/8574)|Remove maxTaskFailures=4 for pre-3.1.1 Spark|
|[#8503](https://github.com/NVIDIA/spark-rapids/pull/8503)|Remove hard-coded version numbers for dependencies when building on|
|[#8544](https://github.com/NVIDIA/spark-rapids/pull/8544)|Fix auto merge conflict 8543 [skip ci]|
|[#8521](https://github.com/NVIDIA/spark-rapids/pull/8521)|List supported Spark versions when no shim found|
|[#8520](https://github.com/NVIDIA/spark-rapids/pull/8520)|Add support for first, last, nth, and collect_list aggregations for BinaryType|
|[#8509](https://github.com/NVIDIA/spark-rapids/pull/8509)|Remove legacy spark version check|
|[#8494](https://github.com/NVIDIA/spark-rapids/pull/8494)|Fix 23.08 build on Databricks 12.2|
|[#8487](https://github.com/NVIDIA/spark-rapids/pull/8487)|Move MockTaskContext to tests project|
|[#8426](https://github.com/NVIDIA/spark-rapids/pull/8426)|Pre-merge CI to support Databricks 12.2|
|[#8282](https://github.com/NVIDIA/spark-rapids/pull/8282)|Databricks 12.2 Support|
|[#8407](https://github.com/NVIDIA/spark-rapids/pull/8407)|Bump up dep version to 23.08.0-SNAPSHOT|
|[#8359](https://github.com/NVIDIA/spark-rapids/pull/8359)|Init version 23.08.0-SNAPSHOT|

## Release 23.06

### Features
|||
|:---|:---|
|[#6201](https://github.com/NVIDIA/spark-rapids/issues/6201)|[FEA] experiment with memoizing datagens in the integration_tests|
|[#8079](https://github.com/NVIDIA/spark-rapids/issues/8079)|[FEA] Release Spark 3.4 Support|
|[#7043](https://github.com/NVIDIA/spark-rapids/issues/7043)|[FEA] Support Empty2Null expression on Spark 3.4.0|
|[#8222](https://github.com/NVIDIA/spark-rapids/issues/8222)|[FEA] String Split Unsupported escaped character '.'|
|[#8211](https://github.com/NVIDIA/spark-rapids/issues/8211)|[FEA] Add tencent blob store uri to spark rapids cloudScheme defaults|
|[#4103](https://github.com/NVIDIA/spark-rapids/issues/4103)|[FEA] jdk17 support|
|[#7094](https://github.com/NVIDIA/spark-rapids/issues/7094)|[FEA] Add a shim layer for Spark 3.2.4|
|[#6202](https://github.com/NVIDIA/spark-rapids/issues/6202)|[SPARK-39528][SQL] Use V2 Filter in SupportsRuntimeFiltering|
|[#6034](https://github.com/NVIDIA/spark-rapids/issues/6034)|[FEA] Support `offset` parameter in `TakeOrderedAndProject`|
|[#8196](https://github.com/NVIDIA/spark-rapids/issues/8196)|[FEA] Add retry handling to GpuGenerateExec.fixedLenLazyArrayGenerate path|
|[#7891](https://github.com/NVIDIA/spark-rapids/issues/7891)|[FEA] Support StddevSamp with cast(col as double)  for input|
|[#62](https://github.com/NVIDIA/spark-rapids/issues/62)|[FEA] stddevsamp function|
|[#7867](https://github.com/NVIDIA/spark-rapids/issues/7867)|[FEA] support json to struct function|
|[#7883](https://github.com/NVIDIA/spark-rapids/issues/7883)|[FEA] support order by string in windowing function|
|[#7882](https://github.com/NVIDIA/spark-rapids/issues/7882)|[FEA] support StringTranslate function|
|[#7843](https://github.com/NVIDIA/spark-rapids/issues/7843)|[FEA] build with CUDA 12|
|[#8045](https://github.com/NVIDIA/spark-rapids/issues/8045)|[FEA] Support repetition in choice on regular expressions|
|[#6882](https://github.com/NVIDIA/spark-rapids/issues/6882)|[FEA] Regular expressions - support line anchors in choice|
|[#7901](https://github.com/NVIDIA/spark-rapids/issues/7901)|[FEA] better rlike function supported|
|[#7784](https://github.com/NVIDIA/spark-rapids/issues/7784)|[FEA] Add Spark 3.3.3-SNAPSHOT to shims|
|[#7260](https://github.com/NVIDIA/spark-rapids/issues/7260)|[FEA] Create a new Expression execution framework|

### Performance
|||
|:---|:---|
|[#7870](https://github.com/NVIDIA/spark-rapids/issues/7870)|[FEA] Turn on spark.rapids.sql.castDecimalToString.enabled by default|
|[#7321](https://github.com/NVIDIA/spark-rapids/issues/7321)|[FEA] Improve performance of small file ORC reads from blobstores|
|[#7672](https://github.com/NVIDIA/spark-rapids/issues/7672)|Make all buffers/columnar batches spillable by default|

### Bugs Fixed
|||
|:---|:---|
|[#6339](https://github.com/NVIDIA/spark-rapids/issues/6339)|[BUG] 0 in some cases for decimal being cast to a string returns different results.|
|[#8522](https://github.com/NVIDIA/spark-rapids/issues/8522)|[BUG] `from_json` function failed testing with input column containing empty or null string|
|[#8483](https://github.com/NVIDIA/spark-rapids/issues/8483)|[BUG] `test_read_compressed_hive_text` fails on CDH|
|[#8330](https://github.com/NVIDIA/spark-rapids/issues/8330)|[BUG] Handle Decimal128 computation with overflow of Remainder on Spark 3.4|
|[#8448](https://github.com/NVIDIA/spark-rapids/issues/8448)|[BUG] GpuRegExpReplaceWithBackref with empty string input produces incorrect result on GPU in Spark 3.1.1|
|[#8323](https://github.com/NVIDIA/spark-rapids/issues/8323)|[BUG] regexp_replace hangs with specific inputs and patterns|
|[#8473](https://github.com/NVIDIA/spark-rapids/issues/8473)|[BUG] Complete aggregation with non-trivial grouping expression fails|
|[#8440](https://github.com/NVIDIA/spark-rapids/issues/8440)|[BUG] the jar with scaladoc overwrites the jar with javadoc |
|[#8469](https://github.com/NVIDIA/spark-rapids/issues/8469)|[BUG] Multi-threaded reader can't be toggled on/off|
|[#8460](https://github.com/NVIDIA/spark-rapids/issues/8460)|[BUG] Compile failure on Databricks 11.3 with GpuHiveTableScanExec.scala|
|[#8114](https://github.com/NVIDIA/spark-rapids/issues/8114)|[BUG] [AUDIT] [SPARK-42478] Make a serializable jobTrackerId instead of a non-serializable JobID in FileWriterFactory|
|[#6786](https://github.com/NVIDIA/spark-rapids/issues/6786)|[BUG] NDS q95 fails with OOM at 10TB|
|[#8419](https://github.com/NVIDIA/spark-rapids/issues/8419)|[BUG] Hive Text reader fails for GZIP compressed input|
|[#8409](https://github.com/NVIDIA/spark-rapids/issues/8409)|[BUG] JVM agent crashed SIGFPE cudf::detail::repeat in integration tests|
|[#8411](https://github.com/NVIDIA/spark-rapids/issues/8411)|[BUG] Close called too many times in Gpu json reader|
|[#8400](https://github.com/NVIDIA/spark-rapids/issues/8400)|[BUG] Cloudera IT test failures - test_timesub_from_subquery|
|[#8240](https://github.com/NVIDIA/spark-rapids/issues/8240)|[BUG] NDS power run hits GPU OOM on Databricks.|
|[#8375](https://github.com/NVIDIA/spark-rapids/issues/8375)|[BUG] test_empty_filter[>] failed in 23.06 nightly|
|[#8363](https://github.com/NVIDIA/spark-rapids/issues/8363)|[BUG] ORC reader NullPointerExecption|
|[#8281](https://github.com/NVIDIA/spark-rapids/issues/8281)|[BUG] ParquetCachedBatchSerializer is crashing on count|
|[#8331](https://github.com/NVIDIA/spark-rapids/issues/8331)|[BUG] Filter on dates with subquery results in ArrayIndexOutOfBoundsException|
|[#8293](https://github.com/NVIDIA/spark-rapids/issues/8293)|[BUG] GpuTimeAdd throws UnsupportedOperationException takes column and interval as an argument only|
|[#8161](https://github.com/NVIDIA/spark-rapids/issues/8161)|Add support for Remainder[DecimalType] for Spark 3.4 and DB 11.3|
|[#8321](https://github.com/NVIDIA/spark-rapids/issues/8321)|[BUG] `test_read_hive_fixed_length_char` integ test fails on Spark 3.4|
|[#8225](https://github.com/NVIDIA/spark-rapids/issues/8225)|[BUG] GpuGetArrayItem only supports ints as the ordinal.|
|[#8294](https://github.com/NVIDIA/spark-rapids/issues/8294)|[BUG] ORC `CHAR(N)` columns written from Hive unreadable with RAPIDS plugin|
|[#8186](https://github.com/NVIDIA/spark-rapids/issues/8186)|[BUG] integration test test_cast_nested can fail with non-empty nulls|
|[#6190](https://github.com/NVIDIA/spark-rapids/issues/6190)|[SPARK-39731][SQL] Fix issue in CSV data sources when parsing dates in "yyyyMMdd" format with CORRECTED time parser policy|
|[#8185](https://github.com/NVIDIA/spark-rapids/issues/8185)|[BUG] Scala Test md5 can produce non-empty nulls (merge and set validity)|
|[#8235](https://github.com/NVIDIA/spark-rapids/issues/8235)|[BUG] Java agent crashed intermittently running integration tests|
|[#7485](https://github.com/NVIDIA/spark-rapids/issues/7485)|[BUG] stop using mergeAndSetValidity for any nested type|
|[#8263](https://github.com/NVIDIA/spark-rapids/issues/8263)|[BUG] Databricks 11.3 - Task failed while writing rows for Delta table -  java.lang.Integer cannot be cast to java.lang.Long|
|[#7898](https://github.com/NVIDIA/spark-rapids/issues/7898)|Override `canonicalized` method to the Expressions|
|[#8254](https://github.com/NVIDIA/spark-rapids/issues/8254)|[BUG] Unable to determine Databricks version in azure Databricks instances|
|[#6967](https://github.com/NVIDIA/spark-rapids/issues/6967)|[BUG] Parquet List corner cases fail to be parsed|
|[#6991](https://github.com/NVIDIA/spark-rapids/issues/6991)|[BUG] Integration test failures in Spark - 3.4 SNAPSHOT build|
|[#7773](https://github.com/NVIDIA/spark-rapids/issues/7773)|[BUG] udf test failed cudf-py 23.04 ENV setup on databricks 11.3 runtime|
|[#7934](https://github.com/NVIDIA/spark-rapids/issues/7934)|[BUG] User app fails with OOM - GpuOutOfCoreSortIterator|
|[#8214](https://github.com/NVIDIA/spark-rapids/issues/8214)|[BUG] Exception when counting rows in an ORC file that has no column names|
|[#8160](https://github.com/NVIDIA/spark-rapids/issues/8160)|[BUG] Arithmetic_ops_test failure for Spark 3.4|
|[#7495](https://github.com/NVIDIA/spark-rapids/issues/7495)|Update GpuDataSource to match the change in Spark 3.4|
|[#8189](https://github.com/NVIDIA/spark-rapids/issues/8189)|[BUG] test_array_element_at_zero_index_fail test failures in Spark 3.4 |
|[#8043](https://github.com/NVIDIA/spark-rapids/issues/8043)|[BUG] Host memory leak in SerializedBatchIterator|
|[#8194](https://github.com/NVIDIA/spark-rapids/issues/8194)|[BUG] JVM agent crash intermittently in CI integration test |
|[#6182](https://github.com/NVIDIA/spark-rapids/issues/6182)|[SPARK-39319][CORE][SQL] Make query contexts as a part of `SparkThrowable`|
|[#7491](https://github.com/NVIDIA/spark-rapids/issues/7491)|[AUDIT][SPARK-41448][SQL] Make consistent MR job IDs in FileBatchWriter and FileFormatWriter|
|[#8149](https://github.com/NVIDIA/spark-rapids/issues/8149)|[BUG] dataproc init script does not fail clearly with newer versions of CUDA|
|[#7624](https://github.com/NVIDIA/spark-rapids/issues/7624)|[BUG] `test_parquet_write_encryption_option_fallback` failed|
|[#8019](https://github.com/NVIDIA/spark-rapids/issues/8019)|[BUG] Spark-3.4 - Integration test failures due to GpuCreateDataSourceTableAsSelectCommand|
|[#8017](https://github.com/NVIDIA/spark-rapids/issues/8017)|[BUG]Spark-3.4 Integration tests failure  due to InsertIntoHadoopFsRelationCommand not running on GPU|
|[#7492](https://github.com/NVIDIA/spark-rapids/issues/7492)|[AUDIT][SPARK-41468][SQL][FOLLOWUP] Handle NamedLambdaVariables in EquivalentExpressions|
|[#6987](https://github.com/NVIDIA/spark-rapids/issues/6987)|[BUG] Unit Test failures in Spark-3.4 SNAPSHOT build|
|[#8171](https://github.com/NVIDIA/spark-rapids/issues/8171)|[BUG] ORC read failure when reading decimals with different precision/scale from write schema|
|[#7216](https://github.com/NVIDIA/spark-rapids/issues/7216)|[BUG] The PCBS tests fail on Spark 340|
|[#8016](https://github.com/NVIDIA/spark-rapids/issues/8016)|[BUG] Spark-3.4 -  Integration tests failure due to missing InsertIntoHiveTable operator in GPU |
|[#8166](https://github.com/NVIDIA/spark-rapids/issues/8166)|Databricks Delta defaults to LEGACY for int96RebaseModeInWrite|
|[#8147](https://github.com/NVIDIA/spark-rapids/issues/8147)|[BUG] test_substring_column failed|
|[#8164](https://github.com/NVIDIA/spark-rapids/issues/8164)|[BUG] failed AnsiCastShim build in datasbricks 11.3 runtime|
|[#7757](https://github.com/NVIDIA/spark-rapids/issues/7757)|[BUG] Unit tests failure in AnsiCastOpSuite on Spark-3.4|
|[#7756](https://github.com/NVIDIA/spark-rapids/issues/7756)|[BUG] Unit test failure in AdaptiveQueryExecSuite on Spark-3.4|
|[#8153](https://github.com/NVIDIA/spark-rapids/issues/8153)|[BUG] `get-shim-versions-from-dist` workflow failing in CI|
|[#7961](https://github.com/NVIDIA/spark-rapids/issues/7961)|[BUG] understand why unspill can throw an OutOfMemoryError and not a RetryOOM|
|[#7755](https://github.com/NVIDIA/spark-rapids/issues/7755)|[BUG] Unit tests  failures in WindowFunctionSuite and CostBasedOptimizerSuite on Spark-3.4|
|[#7752](https://github.com/NVIDIA/spark-rapids/issues/7752)|[BUG] Test in CastOpSuite fails on Spark-3.4|
|[#7754](https://github.com/NVIDIA/spark-rapids/issues/7754)|[BUG]  unit test `nz timestamp` fails on Spark-3.4|
|[#7018](https://github.com/NVIDIA/spark-rapids/issues/7018)|[BUG] The unit test `sorted partitioned write` fails on Spark 3.4|
|[#8015](https://github.com/NVIDIA/spark-rapids/issues/8015)|[BUG] Spark 3.4 - Integration tests failure due to unsupported KnownNullable operator in Window|
|[#7751](https://github.com/NVIDIA/spark-rapids/issues/7751)|[BUG]  Unit test `Write encrypted ORC fallback` fails on Spark-3.4|
|[#8117](https://github.com/NVIDIA/spark-rapids/issues/8117)|[BUG] Compile error in RapidsErrorUtils when building against Spark 3.4.0 release |
|[#5659](https://github.com/NVIDIA/spark-rapids/issues/5659)|[BUG] Minimize false positives when falling back to CPU for end of line/string anchors and newlines|
|[#8012](https://github.com/NVIDIA/spark-rapids/issues/8012)|[BUG] Integration tests failing due to CreateDataSourceTableAsSelectCommand in Spark-3.4|
|[#8061](https://github.com/NVIDIA/spark-rapids/issues/8061)|[BUG] join_test failed in integration tests|
|[#8018](https://github.com/NVIDIA/spark-rapids/issues/8018)|[BUG] Spark-3.4 - Integration test failures in window aggregations for decimal types|
|[#7581](https://github.com/NVIDIA/spark-rapids/issues/7581)|[BUG] INC AFTER CLOSE for ColumnVector during shutdown in the join code|

### PRs
|||
|:---|:---|
|[#7465](https://github.com/NVIDIA/spark-rapids/pull/7465)|Add support for arrays in hashaggregate|
|[#8584](https://github.com/NVIDIA/spark-rapids/pull/8584)|Update 23.06 changelog 6/19 [skip ci]|
|[#8581](https://github.com/NVIDIA/spark-rapids/pull/8581)|Fix 321db 330db shims build errors caused by DB updates|
|[#8570](https://github.com/NVIDIA/spark-rapids/pull/8570)|Update changelog to latest [skip ci]|
|[#8567](https://github.com/NVIDIA/spark-rapids/pull/8567)|Fixed a link in config doc[skip ci]|
|[#8562](https://github.com/NVIDIA/spark-rapids/pull/8562)|Update changelog to latest 230612 [skip ci]|
|[#8560](https://github.com/NVIDIA/spark-rapids/pull/8560)|Fix relative path in config doc [skip ci]|
|[#8557](https://github.com/NVIDIA/spark-rapids/pull/8557)|Disable `JsonToStructs` for input schema other than `Map<String, String>`|
|[#8549](https://github.com/NVIDIA/spark-rapids/pull/8549)|Revert "Handle caching empty batch (#8507)"|
|[#8507](https://github.com/NVIDIA/spark-rapids/pull/8507)|Handle caching empty batch|
|[#8528](https://github.com/NVIDIA/spark-rapids/pull/8528)|Update deps JNI and private version to 23.06.0|
|[#8492](https://github.com/NVIDIA/spark-rapids/pull/8492)|[Doc]update download docs for 2306 version[skip ci]|
|[#8510](https://github.com/NVIDIA/spark-rapids/pull/8510)|[Doc] address getting-started-on-prem document issues [skip ci]|
|[#8537](https://github.com/NVIDIA/spark-rapids/pull/8537)|Add limitation for the UCX shuffle keep_alive workaround [skip ci]|
|[#8526](https://github.com/NVIDIA/spark-rapids/pull/8526)|Fix `from_json` function failure when input contains empty or null strings|
|[#8529](https://github.com/NVIDIA/spark-rapids/pull/8529)|Init changelog 23.06 [skip ci]|
|[#8338](https://github.com/NVIDIA/spark-rapids/pull/8338)|Moved some configs to an advanced config page|
|[#8441](https://github.com/NVIDIA/spark-rapids/pull/8441)|Memoizing DataGens in integration tests|
|[#8516](https://github.com/NVIDIA/spark-rapids/pull/8516)|Avoid calling Table.merge with BinaryType columns|
|[#8515](https://github.com/NVIDIA/spark-rapids/pull/8515)|Fix warning about deprecated parquet config|
|[#8427](https://github.com/NVIDIA/spark-rapids/pull/8427)|[Doc] address Spark RAPIDS NVAIE VDR issues [skip ci]|
|[#8486](https://github.com/NVIDIA/spark-rapids/pull/8486)|Move task completion listener registration to after variables are initialized|
|[#8481](https://github.com/NVIDIA/spark-rapids/pull/8481)|Removed spark.rapids.sql.castDecimalToString.enabled and enabled GPU decimal to string by default|
|[#8485](https://github.com/NVIDIA/spark-rapids/pull/8485)|Disable `test_read_compressed_hive_text` on CDH.|
|[#8488](https://github.com/NVIDIA/spark-rapids/pull/8488)|Adds note on multi-threaded shuffle targetting <= 200 partitions and on TCP keep-alive for UCX [skip ci]|
|[#8414](https://github.com/NVIDIA/spark-rapids/pull/8414)|Add support for computing remainder with Decimal128 operands with more precision on Spark 3.4|
|[#8467](https://github.com/NVIDIA/spark-rapids/pull/8467)|Add retry support to GpuExpandExec|
|[#8433](https://github.com/NVIDIA/spark-rapids/pull/8433)|Add regression test for regexp_replace hanging with some inputs|
|[#8477](https://github.com/NVIDIA/spark-rapids/pull/8477)|Fix input binding of grouping expressions for complete aggregations|
|[#8464](https://github.com/NVIDIA/spark-rapids/pull/8464)|Remove NOP Maven javadoc plugin definition|
|[#8402](https://github.com/NVIDIA/spark-rapids/pull/8402)|Bring back UCX 1.14|
|[#8470](https://github.com/NVIDIA/spark-rapids/pull/8470)|Ensure the MT shuffle reader enables/disables with spark.rapids.shuff…|
|[#8462](https://github.com/NVIDIA/spark-rapids/pull/8462)|Fix compressed Hive text read on|
|[#8458](https://github.com/NVIDIA/spark-rapids/pull/8458)|Add check for negative id when creating new MR job id|
|[#8435](https://github.com/NVIDIA/spark-rapids/pull/8435)|Add in a few more retry improvements|
|[#8437](https://github.com/NVIDIA/spark-rapids/pull/8437)|Implement the bug fix for SPARK-41448 and shim it for Spark 3.2.4 and Spark 3.3.{2,3}|
|[#8420](https://github.com/NVIDIA/spark-rapids/pull/8420)|Fix reads for GZIP compressed Hive Text.|
|[#8445](https://github.com/NVIDIA/spark-rapids/pull/8445)|Document errors/warns in the logs during catalog shutdown [skip ci]|
|[#8438](https://github.com/NVIDIA/spark-rapids/pull/8438)|Revert "skip test_array_repeat_with_count_scalar for now (#8424)"|
|[#8385](https://github.com/NVIDIA/spark-rapids/pull/8385)|Reduce memory usage in GpuFileFormatDataWriter and GpuDynamicPartitionDataConcurrentWriter|
|[#8304](https://github.com/NVIDIA/spark-rapids/pull/8304)|Support combining small files for multi-threaded ORC reads|
|[#8413](https://github.com/NVIDIA/spark-rapids/pull/8413)|Stop double closing in json scan + skip test|
|[#8430](https://github.com/NVIDIA/spark-rapids/pull/8430)|Update docs for spark.rapids.filecache.checkStale default change [skip ci]|
|[#8424](https://github.com/NVIDIA/spark-rapids/pull/8424)|skip test_array_repeat_with_count_scalar to wait for fix #8409|
|[#8405](https://github.com/NVIDIA/spark-rapids/pull/8405)|Change TimeAdd/Sub subquery tests to use min/max|
|[#8408](https://github.com/NVIDIA/spark-rapids/pull/8408)|Document conventional dist jar layout for single-shim deployments [skip ci]|
|[#8394](https://github.com/NVIDIA/spark-rapids/pull/8394)|Removed "peak device memory" metric|
|[#8378](https://github.com/NVIDIA/spark-rapids/pull/8378)|Use spillable batch with retry in GpuCachedDoublePassWindowIterator|
|[#8392](https://github.com/NVIDIA/spark-rapids/pull/8392)|Update IDEA dev instructions [skip ci]|
|[#8387](https://github.com/NVIDIA/spark-rapids/pull/8387)|Rename inconsinstent profiles in api_validation|
|[#8374](https://github.com/NVIDIA/spark-rapids/pull/8374)|Avoid processing empty batch in ParquetCachedBatchSerializer|
|[#8386](https://github.com/NVIDIA/spark-rapids/pull/8386)|Fix check to do positional indexing in ORC|
|[#8360](https://github.com/NVIDIA/spark-rapids/pull/8360)|use matrix to combine multiple jdk* jobs in maven-verify CI [skip ci]|
|[#8371](https://github.com/NVIDIA/spark-rapids/pull/8371)|Fix V1 column name match is case-sensitive when dropping partition by columns|
|[#8368](https://github.com/NVIDIA/spark-rapids/pull/8368)|Doc Update: Clarify both line anchors ^ and $ for regular expression compatibility [skip ci]|
|[#8377](https://github.com/NVIDIA/spark-rapids/pull/8377)|Avoid a possible race in test_empty_filter|
|[#8354](https://github.com/NVIDIA/spark-rapids/pull/8354)|[DOCS] Updating tools docs in spark-rapids [skip ci]|
|[#8341](https://github.com/NVIDIA/spark-rapids/pull/8341)|Enable CachedBatchWriterSuite.testCompressColBatch|
|[#8264](https://github.com/NVIDIA/spark-rapids/pull/8264)|Make tables spillable by default|
|[#8364](https://github.com/NVIDIA/spark-rapids/pull/8364)|Fix NullPointerException in ORC multithreaded reader where we access context that could be null|
|[#8322](https://github.com/NVIDIA/spark-rapids/pull/8322)|Avoid out of bounds on GpuInMemoryTableScan when reading no columns|
|[#8342](https://github.com/NVIDIA/spark-rapids/pull/8342)|Elimnate javac warnings|
|[#8334](https://github.com/NVIDIA/spark-rapids/pull/8334)|Add in support for filter on empty batch|
|[#8355](https://github.com/NVIDIA/spark-rapids/pull/8355)|Speed up github verify checks [skip ci]|
|[#8356](https://github.com/NVIDIA/spark-rapids/pull/8356)|Enable auto-merge from branch-23.06 to branch-23.08 [skip ci]|
|[#8339](https://github.com/NVIDIA/spark-rapids/pull/8339)|Fix withResource order in GpuGenerateExec|
|[#8340](https://github.com/NVIDIA/spark-rapids/pull/8340)|Stop calling contiguousSplit without splits from GpuSortExec|
|[#8333](https://github.com/NVIDIA/spark-rapids/pull/8333)|Fix GpuTimeAdd handling both input expressions being GpuScalar|
|[#8302](https://github.com/NVIDIA/spark-rapids/pull/8302)|Add support for DecimalType in Remainder for Spark 3.4 and DB 11.3|
|[#8325](https://github.com/NVIDIA/spark-rapids/pull/8325)|Disable `test_read_hive_fixed_length_char` on Spark 3.4+.|
|[#8327](https://github.com/NVIDIA/spark-rapids/pull/8327)|Enable spark.sql.legacy.parquet.nanosAsLong for Spark 3.2.4|
|[#8328](https://github.com/NVIDIA/spark-rapids/pull/8328)|Fix Hive text file write to deal with CUDF changes|
|[#8309](https://github.com/NVIDIA/spark-rapids/pull/8309)|Fix GpuTopN with offset for multiple batches|
|[#8306](https://github.com/NVIDIA/spark-rapids/pull/8306)|Update code to deal with new retry semantics|
|[#8307](https://github.com/NVIDIA/spark-rapids/pull/8307)|Full ordinal support in GetArrayItem|
|[#8243](https://github.com/NVIDIA/spark-rapids/pull/8243)|Enable retry for Parquet writes|
|[#8295](https://github.com/NVIDIA/spark-rapids/pull/8295)|Fix ORC reader for `CHAR(N)` columns written from Hive|
|[#8298](https://github.com/NVIDIA/spark-rapids/pull/8298)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#8276](https://github.com/NVIDIA/spark-rapids/pull/8276)|Fallback to CPU for `enableDateTimeParsingFallback` configuration|
|[#8296](https://github.com/NVIDIA/spark-rapids/pull/8296)|Fix Multithreaded Readers working with Unity Catalog on Databricks|
|[#8273](https://github.com/NVIDIA/spark-rapids/pull/8273)|Add support for escaped dot in character class in regexp parser|
|[#8266](https://github.com/NVIDIA/spark-rapids/pull/8266)|Add test to confirm correct behavior for decimal average in Spark 3.4|
|[#8291](https://github.com/NVIDIA/spark-rapids/pull/8291)|Fix delta stats tracker conf|
|[#8287](https://github.com/NVIDIA/spark-rapids/pull/8287)|Fix Delta write stats if data schema is missing columns relative to table schema|
|[#8286](https://github.com/NVIDIA/spark-rapids/pull/8286)|Add Tencent cosn:// to default cloud schemes|
|[#8283](https://github.com/NVIDIA/spark-rapids/pull/8283)|Add split and retry support for filter|
|[#8290](https://github.com/NVIDIA/spark-rapids/pull/8290)|Pre-merge docker build stage to support containerd runtime [skip ci]|
|[#8257](https://github.com/NVIDIA/spark-rapids/pull/8257)|Support cuda12 jar's release [skip CI]|
|[#8274](https://github.com/NVIDIA/spark-rapids/pull/8274)|Add a unit test for reordered canonicalized expressions in BinaryComparison|
|[#8265](https://github.com/NVIDIA/spark-rapids/pull/8265)|Small code cleanup for pattern matching on Decimal type|
|[#8255](https://github.com/NVIDIA/spark-rapids/pull/8255)|Enable locals,patvars,privates unused Scalac checks|
|[#8234](https://github.com/NVIDIA/spark-rapids/pull/8234)|JDK17 build support in CI|
|[#8256](https://github.com/NVIDIA/spark-rapids/pull/8256)|Use env var with version files as fallback for IT DBR version|
|[#8239](https://github.com/NVIDIA/spark-rapids/pull/8239)|Add Spark 3.2.4 shim|
|[#8221](https://github.com/NVIDIA/spark-rapids/pull/8221)|[Doc] update getting started guide based on latest databricks env [skip ci]|
|[#8224](https://github.com/NVIDIA/spark-rapids/pull/8224)|Fix misinterpretation of Parquet's legacy ARRAY schemas.|
|[#8241](https://github.com/NVIDIA/spark-rapids/pull/8241)|Update to filecache API changes|
|[#8244](https://github.com/NVIDIA/spark-rapids/pull/8244)|Remove semicolon at the end of the package statement in Scala files|
|[#8245](https://github.com/NVIDIA/spark-rapids/pull/8245)|Remove redundant open of ORC file|
|[#8252](https://github.com/NVIDIA/spark-rapids/pull/8252)|Fix auto merge conflict 8250 [skip ci]|
|[#8170](https://github.com/NVIDIA/spark-rapids/pull/8170)|Update GpuRunningWindowExec to use OOM retry framework|
|[#8218](https://github.com/NVIDIA/spark-rapids/pull/8218)|Update to add 340 build and unit test in premerge and in JDK 11 build|
|[#8232](https://github.com/NVIDIA/spark-rapids/pull/8232)|Add integration tests for inferred schema|
|[#8223](https://github.com/NVIDIA/spark-rapids/pull/8223)|Use SupportsRuntimeV2Filtering in Spark 3.4.0|
|[#8233](https://github.com/NVIDIA/spark-rapids/pull/8233)|cudf-udf integration test against python3.9 [skip ci]|
|[#8226](https://github.com/NVIDIA/spark-rapids/pull/8226)|Offset support for TakeOrderedAndProject|
|[#8237](https://github.com/NVIDIA/spark-rapids/pull/8237)|Use weak keys in executor broadcast plan cache|
|[#8229](https://github.com/NVIDIA/spark-rapids/pull/8229)|Upgrade to jacoco 0.8.8 for JDK 17 support|
|[#8216](https://github.com/NVIDIA/spark-rapids/pull/8216)|Add oom retry handling for GpuGenerate.fixedLenLazyArrayGenerate|
|[#8191](https://github.com/NVIDIA/spark-rapids/pull/8191)|Add in retry-work to GPU OutOfCore Sort|
|[#8228](https://github.com/NVIDIA/spark-rapids/pull/8228)|Partial JDK 17 support|
|[#8227](https://github.com/NVIDIA/spark-rapids/pull/8227)|Adjust defaults for better performance out of the box|
|[#8212](https://github.com/NVIDIA/spark-rapids/pull/8212)|Add file caching|
|[#8179](https://github.com/NVIDIA/spark-rapids/pull/8179)|Fall back to CPU for try_cast in Spark 3.4.0|
|[#8220](https://github.com/NVIDIA/spark-rapids/pull/8220)|Batch install-file executions in a single JVM|
|[#8215](https://github.com/NVIDIA/spark-rapids/pull/8215)|Fix count from ORC files with no column names|
|[#8192](https://github.com/NVIDIA/spark-rapids/pull/8192)|Handle PySparkException in case of literal expressions|
|[#8190](https://github.com/NVIDIA/spark-rapids/pull/8190)|Fix element_at_index_zero integration test by using newer error message from Spark 3.4.0|
|[#8203](https://github.com/NVIDIA/spark-rapids/pull/8203)|Clean up queued batches on task failures in RapidsShuffleThreadedBlockIterator|
|[#8207](https://github.com/NVIDIA/spark-rapids/pull/8207)|Support `std` aggregation in reduction|
|[#8174](https://github.com/NVIDIA/spark-rapids/pull/8174)|[FEA] support json to struct function |
|[#8195](https://github.com/NVIDIA/spark-rapids/pull/8195)|Bump mockito to 3.12.4|
|[#8193](https://github.com/NVIDIA/spark-rapids/pull/8193)|Increase databricks cluster autotermination to 6.5 hours [skip ci]|
|[#8182](https://github.com/NVIDIA/spark-rapids/pull/8182)|Support STRING order-by columns for RANGE window functions|
|[#8167](https://github.com/NVIDIA/spark-rapids/pull/8167)|Add oom retry handling to GpuGenerateExec.doGenerate path|
|[#8183](https://github.com/NVIDIA/spark-rapids/pull/8183)|Disable asserts for non-empty nulls|
|[#8177](https://github.com/NVIDIA/spark-rapids/pull/8177)|Fix 340 shim of GpuCreateDataSourceTableAsSelectCommand and shim GpuDataSource for 3.4.0|
|[#8159](https://github.com/NVIDIA/spark-rapids/pull/8159)|Verify CPU fallback class when creating HIVE table [Databricks]|
|[#8180](https://github.com/NVIDIA/spark-rapids/pull/8180)|Follow-up for ORC Decimal read failure (#8172)|
|[#8172](https://github.com/NVIDIA/spark-rapids/pull/8172)|Fix ORC decimal read when precision/scale changes|
|[#7227](https://github.com/NVIDIA/spark-rapids/pull/7227)|Fix PCBS integration tests for Spark-3.4|
|[#8175](https://github.com/NVIDIA/spark-rapids/pull/8175)|Restore test_substring_column|
|[#8162](https://github.com/NVIDIA/spark-rapids/pull/8162)|Support Java 17 for packaging|
|[#8169](https://github.com/NVIDIA/spark-rapids/pull/8169)|Fix AnsiCastShim for 330db|
|[#8168](https://github.com/NVIDIA/spark-rapids/pull/8168)|[DOC] Updating profiling/qualification docs for usability improvements [skip ci]|
|[#8144](https://github.com/NVIDIA/spark-rapids/pull/8144)|Add 340 shim for GpuInsertIntoHiveTable|
|[#8143](https://github.com/NVIDIA/spark-rapids/pull/8143)|Add handling for SplitAndRetryOOM in nextCbFromGatherer|
|[#8102](https://github.com/NVIDIA/spark-rapids/pull/8102)|Rewrite two tests from AnsiCastOpSuite in Python and make compatible with Spark 3.4.0|
|[#8152](https://github.com/NVIDIA/spark-rapids/pull/8152)|Fix Spark-3.4 test failure in AdaptiveQueryExecSuite|
|[#8154](https://github.com/NVIDIA/spark-rapids/pull/8154)|Use repo1.maven.org/maven2 instead of default apache central url |
|[#8150](https://github.com/NVIDIA/spark-rapids/pull/8150)|xfail test_substring_column|
|[#8128](https://github.com/NVIDIA/spark-rapids/pull/8128)|Fix CastOpSuite failures with Spark 3.4|
|[#8145](https://github.com/NVIDIA/spark-rapids/pull/8145)|Fix nz timestamp unit tests|
|[#8146](https://github.com/NVIDIA/spark-rapids/pull/8146)|Set version of slf4j for Spark 3.4.0|
|[#8058](https://github.com/NVIDIA/spark-rapids/pull/8058)|Add retry to BatchByKeyIterator|
|[#8142](https://github.com/NVIDIA/spark-rapids/pull/8142)|Enable ParquetWriterSuite test 'sorted partitioned write' on Spark 3.4.0|
|[#8035](https://github.com/NVIDIA/spark-rapids/pull/8035)|[FEA] support StringTranslate function|
|[#8136](https://github.com/NVIDIA/spark-rapids/pull/8136)|Add GPU support for KnownNullable expression (Spark 3.4.0)|
|[#8096](https://github.com/NVIDIA/spark-rapids/pull/8096)|Add OOM retry handling for existence joins|
|[#8139](https://github.com/NVIDIA/spark-rapids/pull/8139)|Fix auto merge conflict 8138 [skip ci]|
|[#8135](https://github.com/NVIDIA/spark-rapids/pull/8135)|Fix Orc writer test failure with Spark 3.4|
|[#8129](https://github.com/NVIDIA/spark-rapids/pull/8129)|Fix compile error with Spark 3.4.0 release and bump to use 3.4.0 release JAR|
|[#8093](https://github.com/NVIDIA/spark-rapids/pull/8093)|Add cuda12 build support [skip ci]|
|[#8108](https://github.com/NVIDIA/spark-rapids/pull/8108)|Make Arm methods static|
|[#8060](https://github.com/NVIDIA/spark-rapids/pull/8060)|Support repetitions in regexp choice expressions|
|[#8081](https://github.com/NVIDIA/spark-rapids/pull/8081)|Re-enable empty repetition near end-of-line anchor for rlike, regexp_extract and regexp_replace|
|[#8075](https://github.com/NVIDIA/spark-rapids/pull/8075)|Update some integration tests so that they are compatible with Spark 3.4.0|
|[#8063](https://github.com/NVIDIA/spark-rapids/pull/8063)|Update docker to support integration tests against JDK17 [skip ci]|
|[#8047](https://github.com/NVIDIA/spark-rapids/pull/8047)|Enable line/string anchors in choice|
|[#7996](https://github.com/NVIDIA/spark-rapids/pull/7996)|Sub-partitioning supports repartitioning the input data multiple times|
|[#8009](https://github.com/NVIDIA/spark-rapids/pull/8009)|Add in some more retry blocks|
|[#8051](https://github.com/NVIDIA/spark-rapids/pull/8051)|MINOR: Improve assertion error in assert_py4j_exception|
|[#8020](https://github.com/NVIDIA/spark-rapids/pull/8020)|[FEA] Add Spark 3.3.3-SNAPSHOT to shims|
|[#8034](https://github.com/NVIDIA/spark-rapids/pull/8034)|Fix the check for dedicated per-shim files [skip ci]|
|[#7978](https://github.com/NVIDIA/spark-rapids/pull/7978)|Update JNI and private deps version to 23.06.0-SNAPSHOT|
|[#7965](https://github.com/NVIDIA/spark-rapids/pull/7965)|Remove stale references to the pre-shimplify dirs|
|[#7948](https://github.com/NVIDIA/spark-rapids/pull/7948)|Init plugin version 23.06.0-SNAPSHOT|

## Release 23.04

### Features
|||
|:---|:---|
|[#7992](https://github.com/NVIDIA/spark-rapids/issues/7992)|[Audit][SPARK-40819][SQL][3.3] Timestamp nanos behaviour regression (parquet reader)|
|[#7985](https://github.com/NVIDIA/spark-rapids/issues/7985)|[FEA] Expose Alluxio master URL to support K8s Env|
|[#7880](https://github.com/NVIDIA/spark-rapids/issues/7880)|[FEA] retry framework task level metrics|
|[#7394](https://github.com/NVIDIA/spark-rapids/issues/7394)|[FEA] Support Delta Lake auto compaction|
|[#7920](https://github.com/NVIDIA/spark-rapids/issues/7920)|[FEA] Remove SpillCallback and executor level spill metrics|
|[#7463](https://github.com/NVIDIA/spark-rapids/issues/7463)|[FEA] Drop support for Databricks-9.1 ML LTS|
|[#7253](https://github.com/NVIDIA/spark-rapids/issues/7253)|[FEA] Implement OOM retry framework|
|[#7042](https://github.com/NVIDIA/spark-rapids/issues/7042)|[FEA] Add support in the tools event parsing for ML functions, libraries, and expressions|

### Performance
|||
|:---|:---|
|[#7907](https://github.com/NVIDIA/spark-rapids/issues/7907)|[FEA] Optimize regexp_replace in multi-replace scenarios|
|[#7691](https://github.com/NVIDIA/spark-rapids/issues/7691)|[FEA] Upgrade and document UCX 1.14|
|[#6516](https://github.com/NVIDIA/spark-rapids/issues/6516)|[FEA] Enable RAPIDS Shuffle Manager smoke testing for the databricks environment|
|[#7695](https://github.com/NVIDIA/spark-rapids/issues/7695)|[FEA] Transpile regexp_extract expression to only have the single capture group that is needed|
|[#7393](https://github.com/NVIDIA/spark-rapids/issues/7393)|[FEA] Support Delta Lake optimized write|
|[#6561](https://github.com/NVIDIA/spark-rapids/issues/6561)|[FEA] Make SpillableColumnarBatch inform Spill code of actual usage of the batch|
|[#6864](https://github.com/NVIDIA/spark-rapids/issues/6864)|[BUG] Spilling logic can spill data that cannot be freed|

### Bugs Fixed
|||
|:---|:---|
|[#8111](https://github.com/NVIDIA/spark-rapids/issues/8111)|[BUG] test_delta_delete_entire_table failed in databricks 10.4 runtime|
|[#8074](https://github.com/NVIDIA/spark-rapids/issues/8074)|[BUG] test_parquet_read_nano_as_longs_31x failed on Dataproc|
|[#7997](https://github.com/NVIDIA/spark-rapids/issues/7997)|[BUG] executors died with too much off heap in yarn UCX CI `udf_test`|
|[#8067](https://github.com/NVIDIA/spark-rapids/issues/8067)|[BUG] extras jar sometimes fails to load|
|[#8038](https://github.com/NVIDIA/spark-rapids/issues/8038)|[BUG] vector leaked when running NDS 3TB with memory restricted|
|[#8030](https://github.com/NVIDIA/spark-rapids/issues/8030)|[BUG] test_re_replace_no_unicode_fallback test failes on integratoin tests Yarn|
|[#7971](https://github.com/NVIDIA/spark-rapids/issues/7971)|[BUG] withRestoreOnRetry should look at Throwable causes in addition to retry OOMs|
|[#6990](https://github.com/NVIDIA/spark-rapids/issues/6990)|[BUG] Several integration test failures in Spark-3.4 SNAPSHOT build|
|[#7924](https://github.com/NVIDIA/spark-rapids/issues/7924)|[BUG] Physical plan for regexp_extract does not escape newlines|
|[#7341](https://github.com/NVIDIA/spark-rapids/issues/7341)|[BUG] Leverage OOM retry framework for ORC writes|
|[#7921](https://github.com/NVIDIA/spark-rapids/issues/7921)|[BUG] ORC writes with bloom filters enabled do not fall back to the CPU|
|[#7818](https://github.com/NVIDIA/spark-rapids/issues/7818)|[BUG] Reuse of broadcast exchange can lead to unnecessary CPU fallback|
|[#7904](https://github.com/NVIDIA/spark-rapids/issues/7904)|[BUG] test_write_sql_save_table sporadically fails on Pascal|
|[#7922](https://github.com/NVIDIA/spark-rapids/issues/7922)|[BUG] YARN IT test test_optimized_hive_ctas_basic failures|
|[#7933](https://github.com/NVIDIA/spark-rapids/issues/7933)|[BUG] NDS running hits DPP error on Databricks 10.4 when enable Alluxio cache.|
|[#7850](https://github.com/NVIDIA/spark-rapids/issues/7850)|[BUG] nvcomp usage for the UCX mode of the shuffle manager is broken|
|[#7927](https://github.com/NVIDIA/spark-rapids/issues/7927)|[BUG] Shimplify adding new shim layer fails|
|[#6138](https://github.com/NVIDIA/spark-rapids/issues/6138)|[BUG] cast timezone-awareness check positive for date/time-unrelated types|
|[#7914](https://github.com/NVIDIA/spark-rapids/issues/7914)|[BUG] Parquet read with integer upcast crashes|
|[#6961](https://github.com/NVIDIA/spark-rapids/issues/6961)|[BUG] Using `\d` (or others) inside a character class results in "Unsupported escape character" |
|[#7908](https://github.com/NVIDIA/spark-rapids/issues/7908)|[BUG] Interpolate spark.version.classifier into scala:compile `secondaryCacheDir`|
|[#7707](https://github.com/NVIDIA/spark-rapids/issues/7707)|[BUG] IndexOutOfBoundsException when joining on 2 integer columns with DPP|
|[#7892](https://github.com/NVIDIA/spark-rapids/issues/7892)|[BUG] Invalid or unsupported escape character `t` when trying to use tab in regexp_replace|
|[#7640](https://github.com/NVIDIA/spark-rapids/issues/7640)|[BUG] GPU OOM using GpuRegExpExtract|
|[#7814](https://github.com/NVIDIA/spark-rapids/issues/7814)|[BUG] GPU's output differs from CPU's for big decimals when joining by sub-partitioning algorithm|
|[#7796](https://github.com/NVIDIA/spark-rapids/issues/7796)|[BUG] Parquet chunked reader size of output exceeds column size limit|
|[#7833](https://github.com/NVIDIA/spark-rapids/issues/7833)|[BUG] run_pyspark_from_build computes 5 MiB per runner instead of 5 GiB|
|[#7855](https://github.com/NVIDIA/spark-rapids/issues/7855)|[BUG] shuffle_test test_hash_grpby_sum failed OOM in premerge CI|
|[#7858](https://github.com/NVIDIA/spark-rapids/issues/7858)|[BUG] HostToGpuCoalesceIterator leaks all host batches|
|[#7826](https://github.com/NVIDIA/spark-rapids/issues/7826)|[BUG] buildall dist jar contains aggregator dependency|
|[#7729](https://github.com/NVIDIA/spark-rapids/issues/7729)|[BUG] Active GPU thread not holding the semaphore|
|[#7820](https://github.com/NVIDIA/spark-rapids/issues/7820)|[BUG] Restore pandas require_minimum_pandas_version() check|
|[#7829](https://github.com/NVIDIA/spark-rapids/issues/7829)|[BUG] Parquet buffer time not correct with multithreaded combining reader|
|[#7819](https://github.com/NVIDIA/spark-rapids/issues/7819)|[BUG] GpuDeviceManager allows setting UVM regardless of other RMM configs|
|[#7643](https://github.com/NVIDIA/spark-rapids/issues/7643)|[BUG] Databricks init scripts can fail silently|
|[#7799](https://github.com/NVIDIA/spark-rapids/issues/7799)|[BUG] Cannot lexicographic compare a table with a LIST of STRUCT column at ai.rapids.cudf.Table.sortOrder|
|[#7767](https://github.com/NVIDIA/spark-rapids/issues/7767)|[BUG] VS Code / Metals / Bloop integration fails with java.lang.RuntimeException: 'boom' |
|[#6383](https://github.com/NVIDIA/spark-rapids/issues/6383)|[SPARK-40066][SQL] ANSI mode: always return null on invalid access to map column|
|[#7093](https://github.com/NVIDIA/spark-rapids/issues/7093)|[BUG] Spark-3.4 - Integration test failures in map_test|
|[#7779](https://github.com/NVIDIA/spark-rapids/issues/7779)|[BUG] AlluxioUtilsSuite uses illegal character underscore in URI scheme|
|[#7725](https://github.com/NVIDIA/spark-rapids/issues/7725)|[BUG] cache_test failed w/ ParquetCachedBatchSerializer in spark 3.3.2-SNAPSHOT|
|[#7639](https://github.com/NVIDIA/spark-rapids/issues/7639)|[BUG] Databricks premerge failing with cannot find pytest|
|[#7694](https://github.com/NVIDIA/spark-rapids/issues/7694)|[BUG] Spark-3.4 build breaks due to removing InternalRowSet|
|[#6598](https://github.com/NVIDIA/spark-rapids/issues/6598)|[BUG] CUDA error when casting large column vector from long to string|
|[#7739](https://github.com/NVIDIA/spark-rapids/issues/7739)|[BUG] udf_test failed in databricks 11.3 ENV|
|[#5748](https://github.com/NVIDIA/spark-rapids/issues/5748)|[BUG] 3 cast tests fails on Spark 3.4.0|
|[#7688](https://github.com/NVIDIA/spark-rapids/issues/7688)|[BUG] GpuParquetScan fails with NullPointerException - Delta CDF query|
|[#7648](https://github.com/NVIDIA/spark-rapids/issues/7648)|[BUG] java.lang.ClassCastException: SerializeConcatHostBuffersDeserializeBatch cannot be cast to.HashedRelation|
|[#6988](https://github.com/NVIDIA/spark-rapids/issues/6988)|[BUG] Integration test failures with DecimalType on Spark-3.4 SNAPSHOT build|
|[#7615](https://github.com/NVIDIA/spark-rapids/issues/7615)|[BUG] Build fails on Spark 3.4|
|[#7557](https://github.com/NVIDIA/spark-rapids/issues/7557)|[AUDIT][SPARK-41970] Introduce SparkPath for typesafety|
|[#7617](https://github.com/NVIDIA/spark-rapids/issues/7617)|[BUG] Build 340 failed due to miss shim code for GpuShuffleMeta|

### PRs
|||
|:---|:---|
|[#8251](https://github.com/NVIDIA/spark-rapids/pull/8251)|Update 23.04 changelog w/ hotfix [skip ci]|
|[#8247](https://github.com/NVIDIA/spark-rapids/pull/8247)|Bump up plugin version to 23.04.1-SNAPSHOT|
|[#8248](https://github.com/NVIDIA/spark-rapids/pull/8248)|[Doc] update versions for 2304 hot fix [skip ci]|
|[#8246](https://github.com/NVIDIA/spark-rapids/pull/8246)|Cherry-pick hotfix: Use weak keys in executor broadcast plan cache|
|[#8092](https://github.com/NVIDIA/spark-rapids/pull/8092)|Init changelog for 23.04 [skip ci]|
|[#8109](https://github.com/NVIDIA/spark-rapids/pull/8109)|Bump up JNI and private version to released 23.04.0|
|[#7939](https://github.com/NVIDIA/spark-rapids/pull/7939)|[Doc]update download docs for 2304 version[skip ci]|
|[#8127](https://github.com/NVIDIA/spark-rapids/pull/8127)|Avoid SQL result check of Delta Lake full delete on Databricks|
|[#8098](https://github.com/NVIDIA/spark-rapids/pull/8098)|Fix loading of ORC files with missing column names|
|[#8110](https://github.com/NVIDIA/spark-rapids/pull/8110)|Update ML integration page docs page [skip ci]|
|[#8103](https://github.com/NVIDIA/spark-rapids/pull/8103)|Add license of spark-rapids private in NOTICE-binary[skip ci]|
|[#8100](https://github.com/NVIDIA/spark-rapids/pull/8100)|Update/improve EMR getting started documentation [skip ci]|
|[#8101](https://github.com/NVIDIA/spark-rapids/pull/8101)|Improve OOM exception messages|
|[#8087](https://github.com/NVIDIA/spark-rapids/pull/8087)|Add an FAQ entry on encryption support [skip ci]|
|[#8076](https://github.com/NVIDIA/spark-rapids/pull/8076)|Add in docs about RetryOOM [skip ci]|
|[#8077](https://github.com/NVIDIA/spark-rapids/pull/8077)|Temporarily skip `test_parquet_read_nano_as_longs_31x` on dataproc|
|[#8071](https://github.com/NVIDIA/spark-rapids/pull/8071)|Fix error in deploy script [skip ci]|
|[#8070](https://github.com/NVIDIA/spark-rapids/pull/8070)|Fixes closed RapidsShuffleHandleImpl leak in ShuffleBufferCatalog|
|[#8069](https://github.com/NVIDIA/spark-rapids/pull/8069)|Fix loading extra jar|
|[#8044](https://github.com/NVIDIA/spark-rapids/pull/8044)|Fall back to CPU if  `spark.sql.legacy.parquet.nanosAsLong` is set|
|[#8049](https://github.com/NVIDIA/spark-rapids/pull/8049)|[DOC] Adding user tool info to main qualification docs page [skip ci]|
|[#8040](https://github.com/NVIDIA/spark-rapids/pull/8040)|Fix device vector leak in RmmRetryIterator.splitSpillableInHalfByRows|
|[#8031](https://github.com/NVIDIA/spark-rapids/pull/8031)|Fix regexp_replace integration test that should fallback when unicode is disabled|
|[#7828](https://github.com/NVIDIA/spark-rapids/pull/7828)|Fallback to arena allocator if RMM failed to initialize with async allocator|
|[#8006](https://github.com/NVIDIA/spark-rapids/pull/8006)|Handle caused-by retry exceptions in withRestoreOnRetry|
|[#8013](https://github.com/NVIDIA/spark-rapids/pull/8013)|[Doc] Adding user tools info into EMR getting started guide [skip ci]|
|[#8007](https://github.com/NVIDIA/spark-rapids/pull/8007)|Fix leak where RapidsShuffleIterator for a completed task was kept alive|
|[#8010](https://github.com/NVIDIA/spark-rapids/pull/8010)|Specify that UCX should be 1.12.1 only [skip ci]|
|[#7967](https://github.com/NVIDIA/spark-rapids/pull/7967)|Transpile simple choice-type regular expressions into lists of choices to use with string replace multi|
|[#7902](https://github.com/NVIDIA/spark-rapids/pull/7902)|Add oom retry handling for createGatherer in gpu hash joins|
|[#7986](https://github.com/NVIDIA/spark-rapids/pull/7986)|Provides a config to expose Alluxio master URL to support K8s Env|
|[#7936](https://github.com/NVIDIA/spark-rapids/pull/7936)|Stop showing internal details of ternary expressions in SparkPlan.toString|
|[#7972](https://github.com/NVIDIA/spark-rapids/pull/7972)|Add in retry for ORC writes|
|[#7975](https://github.com/NVIDIA/spark-rapids/pull/7975)|Publish documentation for private configs|
|[#7976](https://github.com/NVIDIA/spark-rapids/pull/7976)|Disable GPU write for ORC and Parquet, if bloom-filters are enabled.|
|[#7925](https://github.com/NVIDIA/spark-rapids/pull/7925)|Inject RetryOOM in CI where retry iterator is used|
|[#7970](https://github.com/NVIDIA/spark-rapids/pull/7970)|[DOCS] Updating qual tool docs from latest in tools repo|
|[#7952](https://github.com/NVIDIA/spark-rapids/pull/7952)|Add in minimal retry metrics|
|[#7884](https://github.com/NVIDIA/spark-rapids/pull/7884)|Add Python requirements file for integration tests|
|[#7958](https://github.com/NVIDIA/spark-rapids/pull/7958)|Add CheckpointRestore trait and withRestoreOnRetry|
|[#7849](https://github.com/NVIDIA/spark-rapids/pull/7849)|Fix CPU broadcast exchanges being left unreplaced due to AQE and reuse|
|[#7944](https://github.com/NVIDIA/spark-rapids/pull/7944)|Fix issue with dynamicpruning filters used in converted GPU scans when S3 paths are replaced with alluxio|
|[#7949](https://github.com/NVIDIA/spark-rapids/pull/7949)|Lazily unspill the stream batches for joins by sub-partitioning|
|[#7951](https://github.com/NVIDIA/spark-rapids/pull/7951)|Fix PMD docs URL [skip ci]|
|[#7945](https://github.com/NVIDIA/spark-rapids/pull/7945)|Enable automerge from 2304 to 2306 [skip ci]|
|[#7935](https://github.com/NVIDIA/spark-rapids/pull/7935)|Add GPU level task metrics|
|[#7930](https://github.com/NVIDIA/spark-rapids/pull/7930)|Add OOM Retry handling for join gather next|
|[#7942](https://github.com/NVIDIA/spark-rapids/pull/7942)|Revert "Upgrade to UCX 1.14.0 (#7877)"|
|[#7889](https://github.com/NVIDIA/spark-rapids/pull/7889)|Support auto-compaction for Delta tables on|
|[#7937](https://github.com/NVIDIA/spark-rapids/pull/7937)|Support hashing different types for sub-partitioning|
|[#7877](https://github.com/NVIDIA/spark-rapids/pull/7877)|Upgrade to UCX 1.14.0|
|[#7926](https://github.com/NVIDIA/spark-rapids/pull/7926)|Fixes issue where UCX compressed tables would be decompressed multiple times|
|[#7928](https://github.com/NVIDIA/spark-rapids/pull/7928)|Adjust assert for SparkShims: no longer a per-shim file [skip ci]|
|[#7895](https://github.com/NVIDIA/spark-rapids/pull/7895)|Some refactor of shuffled hash join|
|[#7894](https://github.com/NVIDIA/spark-rapids/pull/7894)|Support tagging `Cast` for timezone conditionally|
|[#7915](https://github.com/NVIDIA/spark-rapids/pull/7915)|Fix upcast of signed integral values when reading from Parquet|
|[#7879](https://github.com/NVIDIA/spark-rapids/pull/7879)|Retry for file read operations|
|[#7905](https://github.com/NVIDIA/spark-rapids/pull/7905)|[Doc] Fix some documentation issue based on VPR feedback on 23.04 branch (new PR) [skip CI] |
|[#7912](https://github.com/NVIDIA/spark-rapids/pull/7912)|[Doc] Hotfix gh-pages for compatibility page format issue [skip ci]|
|[#7913](https://github.com/NVIDIA/spark-rapids/pull/7913)|Fix resolution of GpuRapidsProcessDeltaMergeJoinExec expressions|
|[#7916](https://github.com/NVIDIA/spark-rapids/pull/7916)|Add clarification for Delta Lake optimized write fallback due to sorting [skip ci]|
|[#7906](https://github.com/NVIDIA/spark-rapids/pull/7906)|ColumnarToRowIterator should release the semaphore if parent is empty|
|[#7909](https://github.com/NVIDIA/spark-rapids/pull/7909)|Interpolate buildver into secondaryCacheDir|
|[#7844](https://github.com/NVIDIA/spark-rapids/pull/7844)|Update alluxio version to 2.9.0|
|[#7896](https://github.com/NVIDIA/spark-rapids/pull/7896)|Update regular expression parser to handle escape character sequences|
|[#7885](https://github.com/NVIDIA/spark-rapids/pull/7885)|Add Join Reordering Integration Test|
|[#7862](https://github.com/NVIDIA/spark-rapids/pull/7862)|Reduce shimming of GpuFlatMapGroupsInPandasExec|
|[#7859](https://github.com/NVIDIA/spark-rapids/pull/7859)|Remove 3.1.4-SNAPSHOT shim code|
|[#7835](https://github.com/NVIDIA/spark-rapids/pull/7835)|Update to pull the rapids spark extra plugin jar|
|[#7863](https://github.com/NVIDIA/spark-rapids/pull/7863)|[Doc] Address document issues [skip ci]|
|[#7794](https://github.com/NVIDIA/spark-rapids/pull/7794)|Implement sub partitioning for large/skewed hash joins|
|[#7864](https://github.com/NVIDIA/spark-rapids/pull/7864)|Add in basic support for OOM retry for project and filter|
|[#7878](https://github.com/NVIDIA/spark-rapids/pull/7878)|Fixing host memory calculation to properly be 5GiB|
|[#7860](https://github.com/NVIDIA/spark-rapids/pull/7860)|Enable manual copy-and-paste code detection [skip ci]|
|[#7852](https://github.com/NVIDIA/spark-rapids/pull/7852)|Use withRetry in GpuCoalesceBatches|
|[#7857](https://github.com/NVIDIA/spark-rapids/pull/7857)|Unshim getSparkShimVersion|
|[#7854](https://github.com/NVIDIA/spark-rapids/pull/7854)|Optimize `regexp_extract*` by transpiling capture groups to non-capturing groups so that only the required capturing group is manifested|
|[#7853](https://github.com/NVIDIA/spark-rapids/pull/7853)|Remove support for Databricks-9.1 ML LTS|
|[#7856](https://github.com/NVIDIA/spark-rapids/pull/7856)|Update references to reduced dependencies pom [skip ci]|
|[#7848](https://github.com/NVIDIA/spark-rapids/pull/7848)|Initialize only sql-plugin  to prevent missing submodule artifacts in buildall [skip ci]|
|[#7839](https://github.com/NVIDIA/spark-rapids/pull/7839)|Add reduced pom to dist jar in the packaging phase|
|[#7822](https://github.com/NVIDIA/spark-rapids/pull/7822)|Add in support for OOM retry|
|[#7846](https://github.com/NVIDIA/spark-rapids/pull/7846)|Stop releasing semaphore in GpuUserDefinedFunction|
|[#7840](https://github.com/NVIDIA/spark-rapids/pull/7840)|Execute mvn initialize before parallel build [skip ci]|
|[#7222](https://github.com/NVIDIA/spark-rapids/pull/7222)|Automatic conversion to shimplified directory structure|
|[#7824](https://github.com/NVIDIA/spark-rapids/pull/7824)|Use withRetryNoSplit in BasicWindowCalc|
|[#7842](https://github.com/NVIDIA/spark-rapids/pull/7842)|Try fix broken blackduck scan [skip ci]|
|[#7841](https://github.com/NVIDIA/spark-rapids/pull/7841)|Hardcode scan projects [skip ci]|
|[#7830](https://github.com/NVIDIA/spark-rapids/pull/7830)|Fix buffer and Filter time with Parquet multithreaded combine reader|
|[#7678](https://github.com/NVIDIA/spark-rapids/pull/7678)|Premerge CI to drop support for Databricks-9.1 ML LTS|
|[#7823](https://github.com/NVIDIA/spark-rapids/pull/7823)|[BUG] Enable managed memory only if async allocator is not used|
|[#7821](https://github.com/NVIDIA/spark-rapids/pull/7821)|Restore pandas import check in db113 runtime|
|[#7810](https://github.com/NVIDIA/spark-rapids/pull/7810)|UnXfail large decimal window range queries|
|[#7771](https://github.com/NVIDIA/spark-rapids/pull/7771)|Add withRetry and withRetryNoSplit and PoC with hash aggregate|
|[#7815](https://github.com/NVIDIA/spark-rapids/pull/7815)|Fix the hyperlink to shimplify.py [skip ci]|
|[#7812](https://github.com/NVIDIA/spark-rapids/pull/7812)|Fallback Delta Lake optimized writes if GPU cannot support partitioning|
|[#7791](https://github.com/NVIDIA/spark-rapids/pull/7791)|Doc changes for new nested JSON reader [skip ci]|
|[#7797](https://github.com/NVIDIA/spark-rapids/pull/7797)|Add GPU support for EphemeralSubstring|
|[#7561](https://github.com/NVIDIA/spark-rapids/pull/7561)|Ant task to automatically convert to a simple shim layout|
|[#7789](https://github.com/NVIDIA/spark-rapids/pull/7789)|Update script for integration tests on Databricks|
|[#7798](https://github.com/NVIDIA/spark-rapids/pull/7798)|Do not error out DB IT test script when pytest code 5 [skip ci]|
|[#7787](https://github.com/NVIDIA/spark-rapids/pull/7787)|Document a workaround to RuntimeException 'boom' [skip ci]|
|[#7786](https://github.com/NVIDIA/spark-rapids/pull/7786)|Fix nested loop joins when there's no build-side columns|
|[#7730](https://github.com/NVIDIA/spark-rapids/pull/7730)|[FEA] Switch to `regex_program` APIs|
|[#7788](https://github.com/NVIDIA/spark-rapids/pull/7788)|Support released spark 3.3.2|
|[#7095](https://github.com/NVIDIA/spark-rapids/pull/7095)|Fix the failure in `map_test.py` on Spark 3.4|
|[#7769](https://github.com/NVIDIA/spark-rapids/pull/7769)|Fix issue where GpuSemaphore can throw NPE when logDebug is on|
|[#7780](https://github.com/NVIDIA/spark-rapids/pull/7780)|Make AlluxioUtilsSuite pass for 340|
|[#7772](https://github.com/NVIDIA/spark-rapids/pull/7772)|Fix cache test for Spark 3.3.2|
|[#7717](https://github.com/NVIDIA/spark-rapids/pull/7717)|Move Databricks variables into blossom-lib|
|[#7749](https://github.com/NVIDIA/spark-rapids/pull/7749)|Support Delta Lake optimized write on Databricks|
|[#7696](https://github.com/NVIDIA/spark-rapids/pull/7696)|Create new version of  GpuBatchScanExec to fix Spark-3.4 build|
|[#7747](https://github.com/NVIDIA/spark-rapids/pull/7747)|batched full join tracking batch does not need to be lazy|
|[#7758](https://github.com/NVIDIA/spark-rapids/pull/7758)|Hardcode python 3.8 to be used in databricks runtime for cudf_udf ENV|
|[#7716](https://github.com/NVIDIA/spark-rapids/pull/7716)|Clean the code of `GpuMetrics`|
|[#7746](https://github.com/NVIDIA/spark-rapids/pull/7746)|Merge branch-23.02 into branch-23.04 [skip ci]|
|[#7740](https://github.com/NVIDIA/spark-rapids/pull/7740)|Revert 7737 workaround for cudf setup in databricks 11.3 runtime [skip ci]|
|[#7737](https://github.com/NVIDIA/spark-rapids/pull/7737)|Workaround for cudf setup in databricks 11.3 runtime|
|[#7734](https://github.com/NVIDIA/spark-rapids/pull/7734)|Temporarily skip the test_parquet_read_ignore_missing on Databricks|
|[#7728](https://github.com/NVIDIA/spark-rapids/pull/7728)|Fix estimatedNumBatches in case of OOM for Full Outer Join|
|[#7718](https://github.com/NVIDIA/spark-rapids/pull/7718)|GpuParquetScan fails with NullPointerException during combining|
|[#7712](https://github.com/NVIDIA/spark-rapids/pull/7712)|Enable Dynamic FIle Pruning on|
|[#7702](https://github.com/NVIDIA/spark-rapids/pull/7702)|Merge 23.02 into 23.04|
|[#7572](https://github.com/NVIDIA/spark-rapids/pull/7572)|Enables spillable/unspillable state for RapidsBuffer and allow buffer sharing|
|[#7687](https://github.com/NVIDIA/spark-rapids/pull/7687)|Fix window tests for Spark-3.4|
|[#7667](https://github.com/NVIDIA/spark-rapids/pull/7667)|Reenable tests originally bypassed for 3.4|
|[#7542](https://github.com/NVIDIA/spark-rapids/pull/7542)|Support WriteFilesExec in Spark-3.4 to fix several tests|
|[#7673](https://github.com/NVIDIA/spark-rapids/pull/7673)|Add missing spark shim test suites |
|[#7655](https://github.com/NVIDIA/spark-rapids/pull/7655)|Fix Spark 3.4 build|
|[#7621](https://github.com/NVIDIA/spark-rapids/pull/7621)|Document GNU sed for macOS auto-copyrighter users [skip ci]|
|[#7618](https://github.com/NVIDIA/spark-rapids/pull/7618)|Update JNI to 23.04.0-SNAPSHOT and update new delta-stub ver to 23.04|
|[#7541](https://github.com/NVIDIA/spark-rapids/pull/7541)|Init version 23.04.0-SNAPSHOT|

## Release 23.02

### Features
|||
|:---|:---|
|[#6420](https://github.com/NVIDIA/spark-rapids/issues/6420)|[FEA]Support HiveTableScanExec to scan a Hive text table|
|[#4897](https://github.com/NVIDIA/spark-rapids/issues/4897)|Profiling tool: create a section to focus on I/O metrics|
|[#6419](https://github.com/NVIDIA/spark-rapids/issues/6419)|[FEA] Support write a Hive text table |
|[#7280](https://github.com/NVIDIA/spark-rapids/issues/7280)|[FEA] Support UpdateCommand for Delta Lake|
|[#7281](https://github.com/NVIDIA/spark-rapids/issues/7281)|[FEA] Support DeleteCommand for Delta Lake|
|[#5272](https://github.com/NVIDIA/spark-rapids/issues/5272)|[FEA] Support from_json to get a MapType|
|[#7007](https://github.com/NVIDIA/spark-rapids/issues/7007)|[FEA] Support Delta table MERGE INTO on Databricks.|
|[#7521](https://github.com/NVIDIA/spark-rapids/issues/7521)|[FEA] Allow user to set concurrentGpuTasks after startup|
|[#3300](https://github.com/NVIDIA/spark-rapids/issues/3300)|[FEA] Support batched full join|
|[#6698](https://github.com/NVIDIA/spark-rapids/issues/6698)|[FEA] Support json_tuple|
|[#6885](https://github.com/NVIDIA/spark-rapids/issues/6885)|[FEA] Support reverse|
|[#6879](https://github.com/NVIDIA/spark-rapids/issues/6879)|[FEA] Support Databricks 11.3 ML LTS|

### Performance
|||
|:---|:---|
|[#7436](https://github.com/NVIDIA/spark-rapids/issues/7436)|[FEA] Pruning partition columns supports cases of GPU file scan with CPU project and filter|
|[#7219](https://github.com/NVIDIA/spark-rapids/issues/7219)|Improve performance of small file parquet reads from blobstores|
|[#6807](https://github.com/NVIDIA/spark-rapids/issues/6807)|Improve the current documentation on RapidsShuffleManager|
|[#5039](https://github.com/NVIDIA/spark-rapids/issues/5039)|[FEA] Parallelize shuffle compress/decompress with opportunistic idle task threads|
|[#7196](https://github.com/NVIDIA/spark-rapids/issues/7196)|RegExpExtract does both extract and contains_re which is inefficient|
|[#6862](https://github.com/NVIDIA/spark-rapids/issues/6862)|[FEA] GpuRowToColumnarExec for Binary is really slow compared to string|

### Bugs Fixed
|||
|:---|:---|
|[#7069](https://github.com/NVIDIA/spark-rapids/issues/7069)|[BUG] GPU Hive Text Reader reads empty strings as null|
|[#7068](https://github.com/NVIDIA/spark-rapids/issues/7068)|[BUG] GPU Hive Text Reader skips empty lines|
|[#7448](https://github.com/NVIDIA/spark-rapids/issues/7448)|[BUG] GDS cufile test failed in elder cuda runtime|
|[#7686](https://github.com/NVIDIA/spark-rapids/issues/7686)|[BUG] Large floating point values written as `Inf` not `Infinity` in Hive text writer|
|[#7703](https://github.com/NVIDIA/spark-rapids/issues/7703)|[BUG] test_basic_hive_text_write fails|
|[#7693](https://github.com/NVIDIA/spark-rapids/issues/7693)|[BUG] `test_partitioned_sql_parquet_write` fails on CDH|
|[#7382](https://github.com/NVIDIA/spark-rapids/issues/7382)|[BUG] add dynamic partition overwrite tests for all formats|
|[#7597](https://github.com/NVIDIA/spark-rapids/issues/7597)|[BUG] Incompatible timestamps in Hive delimited text writes|
|[#7675](https://github.com/NVIDIA/spark-rapids/issues/7675)|[BUG] Multi-threaded shuffle bails with division-by-zero ArithmeticException|
|[#7530](https://github.com/NVIDIA/spark-rapids/issues/7530)|[BUG] Add tests for RapidsShuffleManager|
|[#7679](https://github.com/NVIDIA/spark-rapids/issues/7679)|[BUG] test_partitioned_parquet_write[PartitionWriteMode.Dynamic] failed in databricks runtimes|
|[#7637](https://github.com/NVIDIA/spark-rapids/issues/7637)|[BUG] GpuBroadcastNestedLoopJoinExec:  Close called too many times|
|[#7595](https://github.com/NVIDIA/spark-rapids/issues/7595)|[BUG] test_mod_mixed decimal test fails on 330db (Databricks 11.3) and TBD 340|
|[#7575](https://github.com/NVIDIA/spark-rapids/issues/7575)|[BUG] On Databricks 11.3, Executor broadcast shuffles should stay on GPU even without columnar children |
|[#7607](https://github.com/NVIDIA/spark-rapids/issues/7607)|[BUG] RegularExpressionTranspilerSuite exhibits multiple failures|
|[#7574](https://github.com/NVIDIA/spark-rapids/issues/7574)|[BUG] simple json_tuple test failing with cudf error|
|[#7446](https://github.com/NVIDIA/spark-rapids/issues/7446)|[BUG] prune_partition_column_test fails for Json in UCX CI|
|[#7603](https://github.com/NVIDIA/spark-rapids/issues/7603)|[BUG] GpuIntervalUtilsTest leaks memory|
|[#7090](https://github.com/NVIDIA/spark-rapids/issues/7090)|[BUG] Refactor line terminator handling code|
|[#7472](https://github.com/NVIDIA/spark-rapids/issues/7472)|[BUG] The parquet chunked reader can fail for certain list cases.|
|[#7560](https://github.com/NVIDIA/spark-rapids/issues/7560)|[BUG] Outstanding allocations detected at shutdown in python integration tests|
|[#7516](https://github.com/NVIDIA/spark-rapids/issues/7516)|[BUG] multiple join cases cpu and gpu outputs mismatched in yarn|
|[#7537](https://github.com/NVIDIA/spark-rapids/issues/7537)|[AUDIT] [SPARK-42039][SQL] SPJ: Remove Option in KeyGroupedPartitioning#partitionValuesOpt|
|[#7535](https://github.com/NVIDIA/spark-rapids/issues/7535)|[BUG] Timestamp test cases failed due to Python time zone is not UTC|
|[#7432](https://github.com/NVIDIA/spark-rapids/issues/7432)|[BUG][SPARK-41713][SPARK-41726] Spark 3.4 build fails.|
|[#7517](https://github.com/NVIDIA/spark-rapids/issues/7517)|[DOC] doc/source of spark330db Shuffle Manager for Databricks-11.3 shim|
|[#7505](https://github.com/NVIDIA/spark-rapids/issues/7505)|[BUG] Delta Lake writes with AQE can have unnecessary row transitions |
|[#7454](https://github.com/NVIDIA/spark-rapids/issues/7454)|[BUG] Investigate binaryop.cpp:205: Unsupported operator for these types on Databricks 11.3|
|[#7469](https://github.com/NVIDIA/spark-rapids/issues/7469)|[BUG] test_arrays_zip failures on nightly |
|[#6894](https://github.com/NVIDIA/spark-rapids/issues/6894)|[BUG] Multithreaded rapids shuffle metrics incorrect|
|[#7325](https://github.com/NVIDIA/spark-rapids/issues/7325)|[BUG] Fix integration test failures on Databricks-11.3|
|[#7348](https://github.com/NVIDIA/spark-rapids/issues/7348)|[BUG] Fix integration tests for decimal type on db330 shim|
|[#6978](https://github.com/NVIDIA/spark-rapids/issues/6978)|[BUG] NDS query 16 fails on EMR 6.8.0 with AQE enabled|
|[#7133](https://github.com/NVIDIA/spark-rapids/issues/7133)|[BUG] Followup to AQE issues with reused columnar broadcast exchanges|
|[#7208](https://github.com/NVIDIA/spark-rapids/issues/7208)|[BUG] Explicitly check if the platform is supported|
|[#7397](https://github.com/NVIDIA/spark-rapids/issues/7397)|[BUG] shuffle smoke test hash_aggregate_test.py::test_hash_grpby_sum failed OOM intermittently in premerge|
|[#7443](https://github.com/NVIDIA/spark-rapids/issues/7443)|[BUG] prune_partition_column_test fails for Avro|
|[#7415](https://github.com/NVIDIA/spark-rapids/issues/7415)|[BUG] regex integration test failures on Databricks-11.3|
|[#7226](https://github.com/NVIDIA/spark-rapids/issues/7226)|[BUG] GpuFileSourceScanExec always generates all partition columns|
|[#7402](https://github.com/NVIDIA/spark-rapids/issues/7402)|[BUG] array_test.py::test_array_intersect failing cloudera|
|[#7324](https://github.com/NVIDIA/spark-rapids/issues/7324)|[BUG] Support DayTimeIntervalType in Databricks-11.3 to fix integration tests|
|[#7426](https://github.com/NVIDIA/spark-rapids/issues/7426)|[BUG] bloop compile times out on Databricks 11.3|
|[#7328](https://github.com/NVIDIA/spark-rapids/issues/7328)|[BUG] Databricks-11.3 integration test failing due to IllegalStateException: the broadcast must be on the GPU too|
|[#7327](https://github.com/NVIDIA/spark-rapids/issues/7327)|[BUG] Update Exception to fix Assertion error in Databricks-11.3 integration tests.|
|[#7403](https://github.com/NVIDIA/spark-rapids/issues/7403)|[BUG] test_dynamic_partition_write_round_trip failed in CDH tests|
|[#7368](https://github.com/NVIDIA/spark-rapids/issues/7368)|[BUG] with_hidden_metadata_fallback test failures on Databricks 11.3|
|[#7400](https://github.com/NVIDIA/spark-rapids/issues/7400)|[BUG] parquet multithreaded combine reader can calculate size wrong|
|[#7383](https://github.com/NVIDIA/spark-rapids/issues/7383)|[BUG] hive text partitioned reads using the `GpuHiveTableScanExec` are broken|
|[#7350](https://github.com/NVIDIA/spark-rapids/issues/7350)|[BUG] test_conditional_with_side_effects_case_when fails|
|[#7373](https://github.com/NVIDIA/spark-rapids/issues/7373)|[BUG] RapidsUDF does not support UDFs with no inputs|
|[#7213](https://github.com/NVIDIA/spark-rapids/issues/7213)|[BUG] Parquet unsigned int scan test failure|
|[#7344](https://github.com/NVIDIA/spark-rapids/issues/7344)|[BUG] Add PythonMapInArrowExec in 330db shim to fix integration test.|
|[#7367](https://github.com/NVIDIA/spark-rapids/issues/7367)|[BUG] test_re_replace_repetition failed|
|[#7345](https://github.com/NVIDIA/spark-rapids/issues/7345)|[BUG] Integration tests failing on Databricks-11.3 due to mixing of aggregations in HashAggregateExec and SortAggregateExec |
|[#7378](https://github.com/NVIDIA/spark-rapids/issues/7378)|[BUG][ORC] `GpuInsertIntoHadoopFsRelationCommand` should use staging directory for dynamic partition overwrite|
|[#7374](https://github.com/NVIDIA/spark-rapids/issues/7374)|[BUG] Hive reader does not always fall back to CPU when table contains nested types|
|[#7284](https://github.com/NVIDIA/spark-rapids/issues/7284)|[BUG] Generated supported data source file is inaccurate for write data formats|
|[#7347](https://github.com/NVIDIA/spark-rapids/issues/7347)|[BUG] Fix integration tests in Databricks-11.3 runtime by removing config spark.sql.ansi.strictIndexOperator|
|[#7326](https://github.com/NVIDIA/spark-rapids/issues/7326)|[BUG] Support RoundCeil and RoundFloor on Databricks-11.3 shim to fix integration tests.|
|[#7352](https://github.com/NVIDIA/spark-rapids/issues/7352)|[BUG] Databricks-11.3 IT failures - IllegalArgumentException: requirement failed for complexTypeExtractors|
|[#6285](https://github.com/NVIDIA/spark-rapids/issues/6285)|[BUG] Add null values back to test_array_intersect for Spark 3.3+ and Databricks 10.4+|
|[#7329](https://github.com/NVIDIA/spark-rapids/issues/7329)|[BUG] intermittently could not find ExecutedCommandExec in the GPU plan in delta_lake_write test|
|[#7184](https://github.com/NVIDIA/spark-rapids/issues/7184)|[BUG] Fix integration test failures on Databricks 11.3|
|[#7225](https://github.com/NVIDIA/spark-rapids/issues/7225)|[BUG] Partition columns mishandled when Parquet read is coalesced and chunked|
|[#7303](https://github.com/NVIDIA/spark-rapids/issues/7303)|[BUG] Fix CPU fallback for custom timestamp formats in Hive delimited text|
|[#7086](https://github.com/NVIDIA/spark-rapids/issues/7086)|[BUG] GPU Hive delimited text reader is more permissive than `LazySimpleSerDe` for timestamps|
|[#7089](https://github.com/NVIDIA/spark-rapids/issues/7089)|[BUG] Reading invalid `DATE` strings yields exceptions instead of nulls|
|[#6047](https://github.com/NVIDIA/spark-rapids/issues/6047)|[BUG] Look into field IDs when reading parquet using the native footer parser|
|[#6989](https://github.com/NVIDIA/spark-rapids/issues/6989)|[BUG] Spark-3.4 - Integration test failures in array_test|
|[#7122](https://github.com/NVIDIA/spark-rapids/issues/7122)|[BUG] Some tests of `limit_test` fail on Spark 3.4|
|[#7144](https://github.com/NVIDIA/spark-rapids/issues/7144)|[BUG] Spark-3.4 integration test failures due to code update in FileFormatWriter.|
|[#6915](https://github.com/NVIDIA/spark-rapids/issues/6915)|[BUG] Parquet of a binary decimal is not supported|

### PRs
|||
|:---|:---|
|[#7763](https://github.com/NVIDIA/spark-rapids/pull/7763)|23.02 changelog update 2/14 [skip ci]|
|[#7761](https://github.com/NVIDIA/spark-rapids/pull/7761)|[Doc] remove xgboost demo from aws-emr doc due to nccl issue [skip ci]|
|[#7760](https://github.com/NVIDIA/spark-rapids/pull/7760)|Add notice in gds to install cuda 11.8 [skip ci]|
|[#7570](https://github.com/NVIDIA/spark-rapids/pull/7570)|[Doc] 23.02 doc updates [skip ci]|
|[#7735](https://github.com/NVIDIA/spark-rapids/pull/7735)|Update JNI version to released 23.02.0|
|[#7721](https://github.com/NVIDIA/spark-rapids/pull/7721)|Fix issue where UCX mode was trying to use catalog from the driver|
|[#7713](https://github.com/NVIDIA/spark-rapids/pull/7713)|Workaround for incompatible serialization for `Inf` floats in Hive text write|
|[#7700](https://github.com/NVIDIA/spark-rapids/pull/7700)|Init 23.02 changelog and move 22 changelog to archives [skip ci]|
|[#7708](https://github.com/NVIDIA/spark-rapids/pull/7708)|Set Hive write test to ignore order on verification.|
|[#7697](https://github.com/NVIDIA/spark-rapids/pull/7697)|Disable Dynamic partitioning tests on CDH|
|[#7556](https://github.com/NVIDIA/spark-rapids/pull/7556)|Write support for Hive delimited text tables|
|[#7681](https://github.com/NVIDIA/spark-rapids/pull/7681)|Disable Hive delimited text reader tests for CDH.|
|[#7610](https://github.com/NVIDIA/spark-rapids/pull/7610)|Add multithreaded Shuffle test|
|[#7652](https://github.com/NVIDIA/spark-rapids/pull/7652)|Support Delta Lake UpdateCommand|
|[#7680](https://github.com/NVIDIA/spark-rapids/pull/7680)|Fix Parquet dynamic partition test for|
|[#7653](https://github.com/NVIDIA/spark-rapids/pull/7653)|Add dynamic partitioning test for Parquet writer.|
|[#7576](https://github.com/NVIDIA/spark-rapids/pull/7576)|Support EXECUTOR_BROADCAST on Databricks 11.3 in BroadcastNestedLoopJoin|
|[#7620](https://github.com/NVIDIA/spark-rapids/pull/7620)|Support Delta Lake DeleteCommand|
|[#7638](https://github.com/NVIDIA/spark-rapids/pull/7638)|Update UCX docs to call out Ubuntu 22.04 is not supported yet [skip ci]|
|[#7644](https://github.com/NVIDIA/spark-rapids/pull/7644)|Create a new ColumnarBatch from the broadcast when LazySpillable takes ownership|
|[#7632](https://github.com/NVIDIA/spark-rapids/pull/7632)|Update UCX shuffle doc w/ memlock limit config [skip ci]|
|[#7628](https://github.com/NVIDIA/spark-rapids/pull/7628)|Disable HiveTextReaders for CDH due to NVIDIA#7423|
|[#7633](https://github.com/NVIDIA/spark-rapids/pull/7633)|Bump up add-to-project version to 0.4.0 [skip ci]|
|[#7609](https://github.com/NVIDIA/spark-rapids/pull/7609)|Fallback to CPU for mod only for DB11.3 [Databricks]|
|[#7591](https://github.com/NVIDIA/spark-rapids/pull/7591)|Prevent fixup of GpuShuffleExchangeExec when using EXECUTOR_BROADCAST|
|[#7626](https://github.com/NVIDIA/spark-rapids/pull/7626)|Revert "Skip/xfail some regexp tests (#7608)"|
|[#7598](https://github.com/NVIDIA/spark-rapids/pull/7598)|Disable decimal `pmod` due to #7553|
|[#7571](https://github.com/NVIDIA/spark-rapids/pull/7571)|Refact deploy script to support gpg and nvsec signature [skip ci]|
|[#7590](https://github.com/NVIDIA/spark-rapids/pull/7590)|Fix `json_tuple` cudf error and java array out-of-bound issue|
|[#7604](https://github.com/NVIDIA/spark-rapids/pull/7604)|Fix memory leak in GpuIntervalUtilsTest|
|[#7600](https://github.com/NVIDIA/spark-rapids/pull/7600)|Centralize source-related properties in parent pom for consistent usage in submodules|
|[#7608](https://github.com/NVIDIA/spark-rapids/pull/7608)|Skip/xfail some regexp tests|
|[#7211](https://github.com/NVIDIA/spark-rapids/pull/7211)|Fix regressions related to cuDF changes in handline of end-of-line/string anchors|
|[#7596](https://github.com/NVIDIA/spark-rapids/pull/7596)|Fix deprecation warnings|
|[#7578](https://github.com/NVIDIA/spark-rapids/pull/7578)|Fix double close on exception in GpuCoalesceBatches|
|[#7584](https://github.com/NVIDIA/spark-rapids/pull/7584)|Write test for multithreaded combine wrong buffer size bug |
|[#7580](https://github.com/NVIDIA/spark-rapids/pull/7580)|Support Delta Lake MergeIntoCommand|
|[#7554](https://github.com/NVIDIA/spark-rapids/pull/7554)|Add mixed decimal testing for binary arithmetic ops|
|[#7567](https://github.com/NVIDIA/spark-rapids/pull/7567)|Fix a small leak in generate|
|[#7563](https://github.com/NVIDIA/spark-rapids/pull/7563)|Add patched Hive Metastore Client jar to  deps|
|[#7533](https://github.com/NVIDIA/spark-rapids/pull/7533)|Support EXECUTOR_BROADCAST on Databricks 11.3 in BroadcastHashJoin|
|[#7532](https://github.com/NVIDIA/spark-rapids/pull/7532)|Update Databricks docs to add a limitation against DB 11.3 [skip ci]|
|[#7555](https://github.com/NVIDIA/spark-rapids/pull/7555)|Fix case where tracking batch is never updated in batched full join|
|[#7547](https://github.com/NVIDIA/spark-rapids/pull/7547)|Refactor Delta Lake code to handle multiple versions per Spark version|
|[#7513](https://github.com/NVIDIA/spark-rapids/pull/7513)|Remove usage to `ColumnView.repeatStringsSizes`|
|[#7538](https://github.com/NVIDIA/spark-rapids/pull/7538)|Fix Spark 340 build error due to change in KeyGroupedPartitioning|
|[#7549](https://github.com/NVIDIA/spark-rapids/pull/7549)|Remove unused Maven property shim.module.name|
|[#7548](https://github.com/NVIDIA/spark-rapids/pull/7548)|Make RapidsBufferHandle AutoCloseable to prevent extra attempts to remove buffers|
|[#7499](https://github.com/NVIDIA/spark-rapids/pull/7499)|README.md for auditing purposes [skip ci]|
|[#7512](https://github.com/NVIDIA/spark-rapids/pull/7512)|Adds RapidsBufferHandle as an indirection layer to RapidsBufferId|
|[#7527](https://github.com/NVIDIA/spark-rapids/pull/7527)|Allow concurrentGpuTasks to be set per job|
|[#7539](https://github.com/NVIDIA/spark-rapids/pull/7539)|Enable automerge from 23.02 to 23.04 [skip ci]|
|[#7536](https://github.com/NVIDIA/spark-rapids/pull/7536)|Fix datetime out-of-range error in pytest when timezone is not UTC|
|[#7502](https://github.com/NVIDIA/spark-rapids/pull/7502)|Fix Spark 3.4 build errors|
|[#7522](https://github.com/NVIDIA/spark-rapids/pull/7522)|Update docs and add Rapids shuffle manager for Databricks-11.3|
|[#7507](https://github.com/NVIDIA/spark-rapids/pull/7507)|Fallback to CPU for unrecognized Distributions|
|[#7515](https://github.com/NVIDIA/spark-rapids/pull/7515)|Fix leak in GpuHiveTableScanExec|
|[#7504](https://github.com/NVIDIA/spark-rapids/pull/7504)|Align CI test scripts with new init scripts for Databricks [skip ci]|
|[#7506](https://github.com/NVIDIA/spark-rapids/pull/7506)|Add RapidsDeltaWrite node to fix undesired transitions with AQE|
|[#7414](https://github.com/NVIDIA/spark-rapids/pull/7414)|batched full hash join|
|[#7509](https://github.com/NVIDIA/spark-rapids/pull/7509)|Fix json_test.py imports|
|[#7269](https://github.com/NVIDIA/spark-rapids/pull/7269)|Add hadoop-def.sh to support multiple spark release tarballs|
|[#7494](https://github.com/NVIDIA/spark-rapids/pull/7494)|Implement a simplified version of `from_json`|
|[#7434](https://github.com/NVIDIA/spark-rapids/pull/7434)|Support `json_tuple`|
|[#7489](https://github.com/NVIDIA/spark-rapids/pull/7489)|Remove the MIT license from tools jar[skip ci]|
|[#7497](https://github.com/NVIDIA/spark-rapids/pull/7497)|Inserting multiple times to HashedPriorityQueue should not corrupt the heap|
|[#7475](https://github.com/NVIDIA/spark-rapids/pull/7475)|Inject GpuCast for decimal AddSub when operands' precision/scale differ on 340, 330db|
|[#7486](https://github.com/NVIDIA/spark-rapids/pull/7486)|Allow Shims to replace Hive execs|
|[#7484](https://github.com/NVIDIA/spark-rapids/pull/7484)|Fix arrays_zip to not rely on broken segmented gather|
|[#7460](https://github.com/NVIDIA/spark-rapids/pull/7460)|More cases support partition column pruning|
|[#7462](https://github.com/NVIDIA/spark-rapids/pull/7462)|Changing metric component counters to avoid extraneous accruals|
|[#7467](https://github.com/NVIDIA/spark-rapids/pull/7467)|Remove spark2-sql-plugin|
|[#7474](https://github.com/NVIDIA/spark-rapids/pull/7474)|xfail array zip tests|
|[#7464](https://github.com/NVIDIA/spark-rapids/pull/7464)|Enable integration tests against Databricks 11.3 in premerge|
|[#7455](https://github.com/NVIDIA/spark-rapids/pull/7455)|Use GpuAlias when handling Empty2Null in GpuOptimisticTransaction|
|[#7456](https://github.com/NVIDIA/spark-rapids/pull/7456)|Sort Delta log objects when comparing and avoid caching all logs|
|[#7431](https://github.com/NVIDIA/spark-rapids/pull/7431)|Remove release script of spark-rapids/tools [skip ci]|
|[#7421](https://github.com/NVIDIA/spark-rapids/pull/7421)|Remove spark-rapids/tools|
|[#7418](https://github.com/NVIDIA/spark-rapids/pull/7418)|Fix for AQE+DPP issue on AWS EMR|
|[#7444](https://github.com/NVIDIA/spark-rapids/pull/7444)|Explicitly check if the platform is supported|
|[#7417](https://github.com/NVIDIA/spark-rapids/pull/7417)|Make cudf-udf tests runnable on Databricks 11.3|
|[#7420](https://github.com/NVIDIA/spark-rapids/pull/7420)|Ubuntu build&test images default as 20.04|
|[#7442](https://github.com/NVIDIA/spark-rapids/pull/7442)|Fix parsing of Delta Lake logs containing multi-line JSON records|
|[#7438](https://github.com/NVIDIA/spark-rapids/pull/7438)|Add documentation for runnable command enable configs|
|[#7439](https://github.com/NVIDIA/spark-rapids/pull/7439)|Suppress unknown RunnableCommand warnings by default|
|[#7346](https://github.com/NVIDIA/spark-rapids/pull/7346)|[Doc]revert the changes in FAQ for deltalake table support[skip ci]|
|[#7413](https://github.com/NVIDIA/spark-rapids/pull/7413)|[Doc]update getting started doc for EMR and databricks[skip ci]|
|[#7445](https://github.com/NVIDIA/spark-rapids/pull/7445)|Support pruning partition columns for avro file scan|
|[#7447](https://github.com/NVIDIA/spark-rapids/pull/7447)|Xfail the test of pruning partition column for json read|
|[#7440](https://github.com/NVIDIA/spark-rapids/pull/7440)|Xfail largest decimals window aggregation|
|[#7437](https://github.com/NVIDIA/spark-rapids/pull/7437)|Fix the regexp test failures on DB11.3|
|[#7428](https://github.com/NVIDIA/spark-rapids/pull/7428)|Support pruning partition columns for GpuFileSourceScan|
|[#7435](https://github.com/NVIDIA/spark-rapids/pull/7435)|Update IntelliJ IDEA doc [skip ci]|
|[#7416](https://github.com/NVIDIA/spark-rapids/pull/7416)|Reorganize and shim ScanExecMeta overrides and fix interval file IO|
|[#7427](https://github.com/NVIDIA/spark-rapids/pull/7427)|Install-file log4j-core on Databricks 11.3|
|[#7424](https://github.com/NVIDIA/spark-rapids/pull/7424)|xfail Hive text tests failing on CDH|
|[#7408](https://github.com/NVIDIA/spark-rapids/pull/7408)|Fallback to CPU for unrecognized ShuffleOrigin|
|[#7422](https://github.com/NVIDIA/spark-rapids/pull/7422)|Skip test_dynamic_partition_write_round_trip in 321cdh|
|[#7406](https://github.com/NVIDIA/spark-rapids/pull/7406)|Fix array_test.py::test_array_intersect for cloudera spark330|
|[#6761](https://github.com/NVIDIA/spark-rapids/pull/6761)|Switch string to float casting to use new kernel|
|[#7411](https://github.com/NVIDIA/spark-rapids/pull/7411)|Skip Int division test that causes scale less than precision|
|[#7410](https://github.com/NVIDIA/spark-rapids/pull/7410)|Remove 314 from dist build list|
|[#7412](https://github.com/NVIDIA/spark-rapids/pull/7412)|Fix the `with_hidden_metadata_fallback` test failures on DB11.3|
|[#7362](https://github.com/NVIDIA/spark-rapids/pull/7362)|Fix multiplication and division test failures in 330db and 340 shim|
|[#7405](https://github.com/NVIDIA/spark-rapids/pull/7405)|Fix multithreaded combine code initial size calculation|
|[#7395](https://github.com/NVIDIA/spark-rapids/pull/7395)|Enable Delta Lake write acceleration by default|
|[#7384](https://github.com/NVIDIA/spark-rapids/pull/7384)|Fix implementation of createReadRDDForDirectories to match DataSource…|
|[#7391](https://github.com/NVIDIA/spark-rapids/pull/7391)|Exclude GDS test suite as default|
|[#7390](https://github.com/NVIDIA/spark-rapids/pull/7390)|Aggregate Databricks 11.3 shim in the nightly dist jar|
|[#7381](https://github.com/NVIDIA/spark-rapids/pull/7381)|Filter out some new timestamp-related Delta Lake tags when comparing logs|
|[#7371](https://github.com/NVIDIA/spark-rapids/pull/7371)|Avoid shutting down RMM until all allocations have cleared|
|[#7377](https://github.com/NVIDIA/spark-rapids/pull/7377)|Update RapidsUDF interface to support UDFs with no input parameters|
|[#7388](https://github.com/NVIDIA/spark-rapids/pull/7388)|Fix `test_array_element_at_zero_index_fail` and `test_div_overflow_exception_when_ansi` DB 11.3 integration test failures|
|[#7380](https://github.com/NVIDIA/spark-rapids/pull/7380)|[FEA] Support `reverse` for arrays|
|[#7365](https://github.com/NVIDIA/spark-rapids/pull/7365)|Move PythonMapInArrowExec to shim for shared 330+ functionality (db11.3)|
|[#7372](https://github.com/NVIDIA/spark-rapids/pull/7372)|Fix for incorrect nested-unsigned test|
|[#7386](https://github.com/NVIDIA/spark-rapids/pull/7386)|Spark 3.1.4 snapshot fix setting of reproduceEmptyStringBug|
|[#7385](https://github.com/NVIDIA/spark-rapids/pull/7385)|Fix Databricks version comparison in pytests|
|[#7379](https://github.com/NVIDIA/spark-rapids/pull/7379)|Fixes bug where dynamic partition overwrite mode didn't work for ORC|
|[#7370](https://github.com/NVIDIA/spark-rapids/pull/7370)|Fix non file read DayTimeInterval errors|
|[#7375](https://github.com/NVIDIA/spark-rapids/pull/7375)|Fix CPU fallback for GpuHiveTableScanExec|
|[#7355](https://github.com/NVIDIA/spark-rapids/pull/7355)|Fix Add and Subtract test failures in 330db and 340 shim|
|[#7299](https://github.com/NVIDIA/spark-rapids/pull/7299)|Qualification tool: Update parsing of write data format|
|[#7357](https://github.com/NVIDIA/spark-rapids/pull/7357)|Update the integration tests  to fit the removed config ` spark.sql.ansi.strictIndexOperator` in DB11.3 and Spark 3.4|
|[#7369](https://github.com/NVIDIA/spark-rapids/pull/7369)|Re-enable some tests in ParseDateTimeSuite|
|[#7366](https://github.com/NVIDIA/spark-rapids/pull/7366)|Support Databricks 11.3|
|[#7354](https://github.com/NVIDIA/spark-rapids/pull/7354)|Fix arithmetic error messages in 330db, 340 shims|
|[#7364](https://github.com/NVIDIA/spark-rapids/pull/7364)|Move Rounding ops to shim for 330+ (db11.3)|
|[#7298](https://github.com/NVIDIA/spark-rapids/pull/7298)|Improve performance of small file parquet reads from blob stores|
|[#7363](https://github.com/NVIDIA/spark-rapids/pull/7363)|Fix ExtractValue assertion with 330db shim|
|[#7340](https://github.com/NVIDIA/spark-rapids/pull/7340)|Fix nested-unsigned test issues.|
|[#7358](https://github.com/NVIDIA/spark-rapids/pull/7358)|Update Delta version to 1.1.0|
|[#7301](https://github.com/NVIDIA/spark-rapids/pull/7301)|Add null values back to test_array_intersect for Spark 3.3.1+ and Databricks 10.4+|
|[#7152](https://github.com/NVIDIA/spark-rapids/pull/7152)|Add a shim for Databricks 11.3 spark330db|
|[#7333](https://github.com/NVIDIA/spark-rapids/pull/7333)|Avoid row number computation when the partition schema is empty|
|[#7317](https://github.com/NVIDIA/spark-rapids/pull/7317)|Make multi-threaded shuffle not experimental and update docs|
|[#7342](https://github.com/NVIDIA/spark-rapids/pull/7342)|Update plan capture listener to handle multiple plans per query.|
|[#7338](https://github.com/NVIDIA/spark-rapids/pull/7338)|Fix auto merge conflict 7336 [skip ci]|
|[#7335](https://github.com/NVIDIA/spark-rapids/pull/7335)|Fix auto merge conflict 7334[skip ci]|
|[#7312](https://github.com/NVIDIA/spark-rapids/pull/7312)|Fix documentation bug|
|[#7315](https://github.com/NVIDIA/spark-rapids/pull/7315)|Fix merge conflict with branch-22.12|
|[#7296](https://github.com/NVIDIA/spark-rapids/pull/7296)|Enable hive text reads by default|
|[#7304](https://github.com/NVIDIA/spark-rapids/pull/7304)|Correct partition columns handling for coalesced and chunked read|
|[#7305](https://github.com/NVIDIA/spark-rapids/pull/7305)|Fix CPU fallback for custom timestamp formats in Hive text tables|
|[#7293](https://github.com/NVIDIA/spark-rapids/pull/7293)|Refine '$LOCAL_JAR_PATH' as optional for integration test on databricks [skip ci]|
|[#7285](https://github.com/NVIDIA/spark-rapids/pull/7285)|[FEA] Support `reverse` for strings|
|[#7297](https://github.com/NVIDIA/spark-rapids/pull/7297)|Remove Decimal Support Section from compatibility docs [skip ci]|
|[#7291](https://github.com/NVIDIA/spark-rapids/pull/7291)|Improve IntelliJ IDEA doc and usability|
|[#7245](https://github.com/NVIDIA/spark-rapids/pull/7245)|Fix boolean, int, and float parsing. Improve decimal parsing for hive|
|[#7265](https://github.com/NVIDIA/spark-rapids/pull/7265)|Fix Hive Delimited Text timestamp parsing|
|[#7221](https://github.com/NVIDIA/spark-rapids/pull/7221)|Fix date parsing in Hive Delimited Text reader|
|[#7287](https://github.com/NVIDIA/spark-rapids/pull/7287)|Don't use native parquet footer parser if field ids for read are needed|
|[#7262](https://github.com/NVIDIA/spark-rapids/pull/7262)|Moving generated files to standalone tools dir|
|[#7268](https://github.com/NVIDIA/spark-rapids/pull/7268)|Remove deprecated compatibility support in premerge|
|[#7248](https://github.com/NVIDIA/spark-rapids/pull/7248)|Fix AlluxioUtilsSuite build on Databricks|
|[#7207](https://github.com/NVIDIA/spark-rapids/pull/7207)|Change the hive text file parser to not use CSV input format|
|[#7209](https://github.com/NVIDIA/spark-rapids/pull/7209)|Change RegExpExtract to use isNull checks instead of contains_re|
|[#7212](https://github.com/NVIDIA/spark-rapids/pull/7212)|Removing common module and adding common files to sql-plugin|
|[#7202](https://github.com/NVIDIA/spark-rapids/pull/7202)|Avoid sort in v1 write for static columns|
|[#7167](https://github.com/NVIDIA/spark-rapids/pull/7167)|Handle two changes related to `FileFormatWriter` since Spark 340|
|[#7194](https://github.com/NVIDIA/spark-rapids/pull/7194)|Skip tests that fail due to recent cuDF changes related to end of string/line anchors|
|[#7170](https://github.com/NVIDIA/spark-rapids/pull/7170)|Fix the `limit_test` failures on Spark 3.4|
|[#7075](https://github.com/NVIDIA/spark-rapids/pull/7075)|Fix the failure of `test_array_element_at_zero_index_fail` on Spark3.4|
|[#7126](https://github.com/NVIDIA/spark-rapids/pull/7126)|Fix support for binary encoded decimal for parquet|
|[#7113](https://github.com/NVIDIA/spark-rapids/pull/7113)|Use an improved API for appending binary to host vector|
|[#7130](https://github.com/NVIDIA/spark-rapids/pull/7130)|Enable chunked parquet reads by default|
|[#7074](https://github.com/NVIDIA/spark-rapids/pull/7074)|Update JNI and cudf-py version to 23.02|
|[#7063](https://github.com/NVIDIA/spark-rapids/pull/7063)|Init version 23.02.0|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
