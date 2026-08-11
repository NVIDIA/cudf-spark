# Change log
Generated on 2025-04-10

## Release 24.12

### Features
|||
|:---|:---|
|[#11630](https://github.com/NVIDIA/spark-rapids/issues/11630)|[FEA] enable from_json and json scan by default|
|[#11709](https://github.com/NVIDIA/spark-rapids/issues/11709)|[FEA] Add support for `MonthsBetween`|
|[#11666](https://github.com/NVIDIA/spark-rapids/issues/11666)|[FEA] support task limit profiling for specified stages|
|[#11662](https://github.com/NVIDIA/spark-rapids/issues/11662)|[FEA] Support Apache Spark 3.4.4|
|[#11657](https://github.com/NVIDIA/spark-rapids/issues/11657)|[FEA] Support format 'yyyyMMdd HH:mm:ss' for legacy mode|
|[#11419](https://github.com/NVIDIA/spark-rapids/issues/11419)|[FEA] Support Spark 3.5.3 release|
|[#11505](https://github.com/NVIDIA/spark-rapids/issues/11505)|[FEA] Support yyyymmdd format for GetTimestamp for LEGACY mode.|

### Performance
|||
|:---|:---|
|[#8391](https://github.com/NVIDIA/spark-rapids/issues/8391)|[FEA] Do a hash based re-partition instead of a sort based fallback for hash aggregate|
|[#11560](https://github.com/NVIDIA/spark-rapids/issues/11560)|[FEA] Improve `GpuJsonToStructs` performance|
|[#11458](https://github.com/NVIDIA/spark-rapids/issues/11458)|[FEA] enable prune_columns for from_json|

### Bugs Fixed
|||
|:---|:---|
|[#11842](https://github.com/NVIDIA/spark-rapids/issues/11842)|[BUG] udf-examples-native case failed core dump|
|[#11718](https://github.com/NVIDIA/spark-rapids/issues/11718)|[BUG] update date/time APIs in CUDF java to avoid deprecated functions|
|[#10907](https://github.com/NVIDIA/spark-rapids/issues/10907)|from_json function parses a column containing an empty array, throws an exception.|
|[#11807](https://github.com/NVIDIA/spark-rapids/issues/11807)|[BUG] mismatched cpu and gpu result in test_lead_lag_for_structs_with_arrays intermittently|
|[#11793](https://github.com/NVIDIA/spark-rapids/issues/11793)|[BUG] "Time in Heuristic" should not include previous operator's compute time|
|[#11798](https://github.com/NVIDIA/spark-rapids/issues/11798)|[BUG] mismatch CPU and GPU result in test_months_between_first_day[DATAGEN_SEED=1733006411, TZ=Africa/Casablanca]|
|[#11790](https://github.com/NVIDIA/spark-rapids/issues/11790)|[BUG] test_hash_* failed "java.util.NoSuchElementException: head of empty list" or "Too many times of repartition, may hit a bug?"|
|[#11643](https://github.com/NVIDIA/spark-rapids/issues/11643)|[BUG] Support AQE with Broadcast Hash Join and DPP on Databricks 14.3|
|[#10910](https://github.com/NVIDIA/spark-rapids/issues/10910)|from_json, when input = empty object, rapids throws an exception.|
|[#10891](https://github.com/NVIDIA/spark-rapids/issues/10891)|Parsing a column containing invalid json into StructureType with schema throws an Exception.|
|[#11741](https://github.com/NVIDIA/spark-rapids/issues/11741)|[BUG] Fix spark400 build due to writeWithV1 return value change|
|[#11533](https://github.com/NVIDIA/spark-rapids/issues/11533)|Fix JSON Matrix tests on Databricks 14.3|
|[#11722](https://github.com/NVIDIA/spark-rapids/issues/11722)|[BUG] Spark 4.0.0 has moved `NullIntolerant` and builds are breaking because they are unable to find it.|
|[#11726](https://github.com/NVIDIA/spark-rapids/issues/11726)|[BUG] Databricks 14.3 nightly deploy fails due to incorrect DB_SHIM_NAME|
|[#11293](https://github.com/NVIDIA/spark-rapids/issues/11293)|[BUG] A user query with from_json failed with "JSON Parser encountered an invalid format at location"|
|[#9592](https://github.com/NVIDIA/spark-rapids/issues/9592)|[BUG][JSON] `from_json` to Map type should produce null for invalid entries|
|[#11715](https://github.com/NVIDIA/spark-rapids/issues/11715)|[BUG] parquet_testing_test.py failed on "AssertionError: GPU and CPU boolean values are different"|
|[#11716](https://github.com/NVIDIA/spark-rapids/issues/11716)|[BUG] delta_lake_write_test.py failed on "AssertionError: GPU and CPU boolean values are different"|
|[#11684](https://github.com/NVIDIA/spark-rapids/issues/11684)|[BUG] 24.12 Precommit fails with wrong number of arguments in `GpuDataSource`|
|[#11168](https://github.com/NVIDIA/spark-rapids/issues/11168)|[BUG] reserve allocation should be displayed when erroring due to lack of memory on startup|
|[#7585](https://github.com/NVIDIA/spark-rapids/issues/7585)|[BUG] [Regexp] Line anchor '$' incorrect matching of unicode line terminators|
|[#11622](https://github.com/NVIDIA/spark-rapids/issues/11622)|[BUG] GPU Parquet scan filter pushdown fails with timestamp/INT96 column|
|[#11646](https://github.com/NVIDIA/spark-rapids/issues/11646)|[BUG] NullPointerException in GpuRand|
|[#10498](https://github.com/NVIDIA/spark-rapids/issues/10498)|[BUG] Unit tests failed: [INTERVAL_ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and return NULL instead|
|[#11659](https://github.com/NVIDIA/spark-rapids/issues/11659)|[BUG] parse_url throws exception if partToExtract is invalid while Spark returns null|
|[#10894](https://github.com/NVIDIA/spark-rapids/issues/10894)|Parsing a column containing a nested structure to json thows an exception|
|[#10895](https://github.com/NVIDIA/spark-rapids/issues/10895)|Converting a column containing a map into json throws an exception|
|[#10896](https://github.com/NVIDIA/spark-rapids/issues/10896)|Converting an column containing an array into json throws an exception|
|[#10915](https://github.com/NVIDIA/spark-rapids/issues/10915)|to_json when converts an array will throw an exception:|
|[#10916](https://github.com/NVIDIA/spark-rapids/issues/10916)|to_json function doesn't support map[string, struct] to json conversion.|
|[#10919](https://github.com/NVIDIA/spark-rapids/issues/10919)|to_json converting map[string, integer] to json, throws an exception|
|[#10920](https://github.com/NVIDIA/spark-rapids/issues/10920)|to_json converting an array with maps throws an exception.|
|[#10921](https://github.com/NVIDIA/spark-rapids/issues/10921)|to_json - array with single map|
|[#10923](https://github.com/NVIDIA/spark-rapids/issues/10923)|[BUG] Spark UT framework: to_json function to convert the array with a single empty row to a JSON string throws an exception.|
|[#10924](https://github.com/NVIDIA/spark-rapids/issues/10924)|[BUG] Spark UT framework: to_json when converts an empty array into json throws an exception. |
|[#11024](https://github.com/NVIDIA/spark-rapids/issues/11024)|Fix tests failures in parquet_write_test.py|
|[#11174](https://github.com/NVIDIA/spark-rapids/issues/11174)|Opcode Suite fails for Scala 2.13.8+ |
|[#10483](https://github.com/NVIDIA/spark-rapids/issues/10483)|[BUG] JsonToStructs fails to parse all empty dicts and invalid lines|
|[#10489](https://github.com/NVIDIA/spark-rapids/issues/10489)|[BUG] from_json does not support input with \n in it.|
|[#10347](https://github.com/NVIDIA/spark-rapids/issues/10347)|[BUG] Failures in Integration Tests on Dataproc Serverless|
|[#11021](https://github.com/NVIDIA/spark-rapids/issues/11021)|Fix tests failures in orc_cast_test.py|
|[#11609](https://github.com/NVIDIA/spark-rapids/issues/11609)|[BUG] test_hash_repartition_long_overflow_ansi_exception failed on 341DB|
|[#11600](https://github.com/NVIDIA/spark-rapids/issues/11600)|[BUG] regex_test failed mismatched cpu and gpu values in UT and IT|
|[#11611](https://github.com/NVIDIA/spark-rapids/issues/11611)|[BUG] Spark 4.0 build failure - value cannotSaveIntervalIntoExternalStorageError is not a member of object org.apache.spark.sql.errors.QueryCompilationErrors|
|[#10922](https://github.com/NVIDIA/spark-rapids/issues/10922)|from_json cannot support line separator in the input string.|
|[#11009](https://github.com/NVIDIA/spark-rapids/issues/11009)|Fix tests failures in cast_test.py|
|[#11572](https://github.com/NVIDIA/spark-rapids/issues/11572)|[BUG] MultiFileReaderThreadPool may flood the console with log messages|

### PRs
|||
|:---|:---|
|[#11950](https://github.com/NVIDIA/spark-rapids/pull/11950)|Update latest changelog [skip ci]|
|[#11947](https://github.com/NVIDIA/spark-rapids/pull/11947)|Update version to 24.12.1-SNAPSHOT [skip ci]|
|[#11943](https://github.com/NVIDIA/spark-rapids/pull/11943)|Update rapids JNI dependency to 24.12.1|
|[#11944](https://github.com/NVIDIA/spark-rapids/pull/11944)|Update download page for 24.12.1 hot fix release [skip ci]|
|[#11876](https://github.com/NVIDIA/spark-rapids/pull/11876)|Update latest changelog [skip ci]|
|[#11874](https://github.com/NVIDIA/spark-rapids/pull/11874)|Remove 350db143 shim's build [skip ci]|
|[#11851](https://github.com/NVIDIA/spark-rapids/pull/11851)|Update latest changelog [skip ci]|
|[#11849](https://github.com/NVIDIA/spark-rapids/pull/11849)|Update rapids JNI and private dependency to 24.12.0|
|[#11841](https://github.com/NVIDIA/spark-rapids/pull/11841)|[DOC] update doc for 24.12 release [skip ci]|
|[#11857](https://github.com/NVIDIA/spark-rapids/pull/11857)|Increase the pre-merge CI timeout to 6 hours|
|[#11845](https://github.com/NVIDIA/spark-rapids/pull/11845)|Fix leak in isTimeStamp|
|[#11823](https://github.com/NVIDIA/spark-rapids/pull/11823)|Fix for `LEAD/LAG` window function test failures.|
|[#11832](https://github.com/NVIDIA/spark-rapids/pull/11832)|Fix leak in GpuBroadcastNestedLoopJoinExecBase|
|[#11763](https://github.com/NVIDIA/spark-rapids/pull/11763)|Orc writes don't fully support Booleans with nulls |
|[#11794](https://github.com/NVIDIA/spark-rapids/pull/11794)|exclude previous operator's time out of firstBatchHeuristic|
|[#11802](https://github.com/NVIDIA/spark-rapids/pull/11802)|Fall back to CPU for non-UTC months_between|
|[#11792](https://github.com/NVIDIA/spark-rapids/pull/11792)|[BUG] Fix issue 11790|
|[#11768](https://github.com/NVIDIA/spark-rapids/pull/11768)|Fix `dpp_test.py` failures on  14.3|
|[#11752](https://github.com/NVIDIA/spark-rapids/pull/11752)|Ability to decompress snappy and zstd Parquet files via CPU|
|[#11777](https://github.com/NVIDIA/spark-rapids/pull/11777)|Append knoguchi22 to blossom-ci whitelist [skip ci]|
|[#11712](https://github.com/NVIDIA/spark-rapids/pull/11712)|repartition-based fallback for hash aggregate v3|
|[#11771](https://github.com/NVIDIA/spark-rapids/pull/11771)|Fix query hang when using rapids multithread shuffle manager with kudo|
|[#11759](https://github.com/NVIDIA/spark-rapids/pull/11759)|Avoid using StringBuffer in single-threaded methods.|
|[#11766](https://github.com/NVIDIA/spark-rapids/pull/11766)|Fix Kudo batch serializer to only read header in hasNext|
|[#11730](https://github.com/NVIDIA/spark-rapids/pull/11730)|Add support for asynchronous writing for parquet|
|[#11750](https://github.com/NVIDIA/spark-rapids/pull/11750)|Fix aqe_test failures on  14.3.|
|[#11753](https://github.com/NVIDIA/spark-rapids/pull/11753)|Enable JSON Scan and from_json by default|
|[#11733](https://github.com/NVIDIA/spark-rapids/pull/11733)|Print out the current attempt object when OOM inside a retry block|
|[#11618](https://github.com/NVIDIA/spark-rapids/pull/11618)|Execute `from_json` with struct schema using `JSONUtils.fromJSONToStructs`|
|[#11725](https://github.com/NVIDIA/spark-rapids/pull/11725)|host watermark metric|
|[#11746](https://github.com/NVIDIA/spark-rapids/pull/11746)|Remove batch size bytes limits|
|[#11723](https://github.com/NVIDIA/spark-rapids/pull/11723)|Add NVIDIA Copyright|
|[#11721](https://github.com/NVIDIA/spark-rapids/pull/11721)|Add a few more JSON tests for MAP<STRING,STRING>|
|[#11744](https://github.com/NVIDIA/spark-rapids/pull/11744)|Do not package the Databricks 14.3 shim into the dist jar [skip ci]|
|[#11724](https://github.com/NVIDIA/spark-rapids/pull/11724)|Integrate with kudo|
|[#11739](https://github.com/NVIDIA/spark-rapids/pull/11739)|Update to Spark 4.0 changing signature of SupportsV1Write.writeWithV1|
|[#11737](https://github.com/NVIDIA/spark-rapids/pull/11737)|Add in support for months_between|
|[#11700](https://github.com/NVIDIA/spark-rapids/pull/11700)|Fix leak with RapidsHostColumnBuilder in GpuUserDefinedFunction|
|[#11727](https://github.com/NVIDIA/spark-rapids/pull/11727)|Widen type promotion for decimals with larger scale in Parquet Read|
|[#11719](https://github.com/NVIDIA/spark-rapids/pull/11719)|Skip `from_json` overflow tests for  14.3|
|[#11708](https://github.com/NVIDIA/spark-rapids/pull/11708)|Support profiling for specific stages on a limited number of tasks|
|[#11731](https://github.com/NVIDIA/spark-rapids/pull/11731)|Add NullIntolerantShim to adapt to Spark 4.0 removing NullIntolerant|
|[#11413](https://github.com/NVIDIA/spark-rapids/pull/11413)|Support multi string contains|
|[#11728](https://github.com/NVIDIA/spark-rapids/pull/11728)|Change Databricks 14.3 shim name to spark350db143 [skip ci]|
|[#11702](https://github.com/NVIDIA/spark-rapids/pull/11702)|Improve JSON scan and `from_json`|
|[#11635](https://github.com/NVIDIA/spark-rapids/pull/11635)|Added Shims for adding Databricks 14.3 Support|
|[#11714](https://github.com/NVIDIA/spark-rapids/pull/11714)|Let AWS Databricks automatically choose an Availability Zone|
|[#11703](https://github.com/NVIDIA/spark-rapids/pull/11703)|Simplify $ transpiling and fix newline character bug|
|[#11707](https://github.com/NVIDIA/spark-rapids/pull/11707)|impalaFile cannot be found by UT framework. |
|[#11697](https://github.com/NVIDIA/spark-rapids/pull/11697)|Make delta-lake shim dependencies parametrizable|
|[#11710](https://github.com/NVIDIA/spark-rapids/pull/11710)|Add shim version 344 to LogicalPlanShims.scala|
|[#11706](https://github.com/NVIDIA/spark-rapids/pull/11706)|Add retry support in sub hash join|
|[#11673](https://github.com/NVIDIA/spark-rapids/pull/11673)|Fix Parquet Writer tests on  14.3|
|[#11669](https://github.com/NVIDIA/spark-rapids/pull/11669)|Fix `string_test` for  14.3|
|[#11692](https://github.com/NVIDIA/spark-rapids/pull/11692)|Add Spark 3.4.4 Shim |
|[#11695](https://github.com/NVIDIA/spark-rapids/pull/11695)|Fix spark400 build due to LogicalRelation signature changes|
|[#11689](https://github.com/NVIDIA/spark-rapids/pull/11689)|Update the Maven repository to download Spark JAR files [skip ci]|
|[#11670](https://github.com/NVIDIA/spark-rapids/pull/11670)|Fix `misc_expr_test` for  14.3|
|[#11652](https://github.com/NVIDIA/spark-rapids/pull/11652)|Fix skipping fixed_length_char ORC tests on  > 13.3|
|[#11644](https://github.com/NVIDIA/spark-rapids/pull/11644)|Skip AQE-join-DPP tests for  14.3|
|[#11667](https://github.com/NVIDIA/spark-rapids/pull/11667)|Preparation for the coming Kudo support|
|[#11685](https://github.com/NVIDIA/spark-rapids/pull/11685)|Exclude shimplify-generated files from scalastyle|
|[#11282](https://github.com/NVIDIA/spark-rapids/pull/11282)|Reserve allocation should be displayed when erroring due to lack of memory on startup|
|[#11671](https://github.com/NVIDIA/spark-rapids/pull/11671)|Use the new host memory allocation API|
|[#11682](https://github.com/NVIDIA/spark-rapids/pull/11682)|Fix auto merge conflict 11679 [skip ci]|
|[#11663](https://github.com/NVIDIA/spark-rapids/pull/11663)|Simplify Transpilation of $ with Extended Line Separator Support in cuDF Regex|
|[#11672](https://github.com/NVIDIA/spark-rapids/pull/11672)|Fix race condition with Parquet filter pushdown modifying shared hadoop Configuration|
|[#11596](https://github.com/NVIDIA/spark-rapids/pull/11596)|Add a new NVTX range for task GPU ownership|
|[#11664](https://github.com/NVIDIA/spark-rapids/pull/11664)|Fix `orc_write_test.py` for  14.3|
|[#11656](https://github.com/NVIDIA/spark-rapids/pull/11656)|[DOC] update the supported OS in download page [skip ci]|
|[#11665](https://github.com/NVIDIA/spark-rapids/pull/11665)|Generate classes identical up to the shim package name|
|[#11647](https://github.com/NVIDIA/spark-rapids/pull/11647)|Fix a NPE issue in GpuRand|
|[#11658](https://github.com/NVIDIA/spark-rapids/pull/11658)|Support format 'yyyyMMdd HH:mm:ss' for legacy mode|
|[#11661](https://github.com/NVIDIA/spark-rapids/pull/11661)|Support invalid partToExtract for parse_url|
|[#11520](https://github.com/NVIDIA/spark-rapids/pull/11520)|UT adjust override checkScanSchemata & enabling ut of exclude_by_suffix fea.|
|[#11634](https://github.com/NVIDIA/spark-rapids/pull/11634)|Put DF_UDF plugin code into the main uber jar.|
|[#11522](https://github.com/NVIDIA/spark-rapids/pull/11522)|UT adjust test SPARK-26677: negated null-safe equality comparison|
|[#11521](https://github.com/NVIDIA/spark-rapids/pull/11521)|Datetime rebasing issue fixed|
|[#11642](https://github.com/NVIDIA/spark-rapids/pull/11642)|Update to_json to be more generic and fix some bugs|
|[#11615](https://github.com/NVIDIA/spark-rapids/pull/11615)|Spark 4 parquet_writer_test.py fixes|
|[#11623](https://github.com/NVIDIA/spark-rapids/pull/11623)|Fix `collection_ops_test` for  14.3|
|[#11553](https://github.com/NVIDIA/spark-rapids/pull/11553)|Fix udf-compiler scala2.13 internal return statements|
|[#11640](https://github.com/NVIDIA/spark-rapids/pull/11640)|Disable date/timestamp types by default when parsing JSON|
|[#11570](https://github.com/NVIDIA/spark-rapids/pull/11570)|Add support for Spark 3.5.3|
|[#11591](https://github.com/NVIDIA/spark-rapids/pull/11591)|Spark UT framework: Read Parquet file generated by parquet-thrift Rapids, UT case adjust.|
|[#11631](https://github.com/NVIDIA/spark-rapids/pull/11631)|Update JSON tests based on a closed/fixed issues|
|[#11617](https://github.com/NVIDIA/spark-rapids/pull/11617)|Quick fix for the build script failure of Scala 2.13 jars [skip ci]|
|[#11614](https://github.com/NVIDIA/spark-rapids/pull/11614)|Ensure repartition overflow test always overflows|
|[#11612](https://github.com/NVIDIA/spark-rapids/pull/11612)|Revert "Disable regex tests to unblock CI (#11606)"|
|[#11597](https://github.com/NVIDIA/spark-rapids/pull/11597)|`install_deps` changes for Databricks 14.3|
|[#11608](https://github.com/NVIDIA/spark-rapids/pull/11608)|Use mvn -f scala2.13/ in the build scripts to build the 2.13 jars|
|[#11610](https://github.com/NVIDIA/spark-rapids/pull/11610)|Change DataSource calendar interval error to fix spark400 build|
|[#11549](https://github.com/NVIDIA/spark-rapids/pull/11549)|Adopt `JSONUtils.concatenateJsonStrings` for concatenating JSON strings|
|[#11595](https://github.com/NVIDIA/spark-rapids/pull/11595)|Remove an unused config shuffle.spillThreads|
|[#11606](https://github.com/NVIDIA/spark-rapids/pull/11606)|Disable regex tests to unblock CI|
|[#11605](https://github.com/NVIDIA/spark-rapids/pull/11605)|Fix auto merge conflict 11604 [skip ci]|
|[#11414](https://github.com/NVIDIA/spark-rapids/pull/11414)|Fix `collection_ops_tests` for Spark 4.0|
|[#11587](https://github.com/NVIDIA/spark-rapids/pull/11587)|avoid long tail tasks due to PrioritySemaphore, remaing part|
|[#11574](https://github.com/NVIDIA/spark-rapids/pull/11574)|avoid long tail tasks due to PrioritySemaphore|
|[#11559](https://github.com/NVIDIA/spark-rapids/pull/11559)|[Spark 4.0] Address test failures in cast_test.py|
|[#11579](https://github.com/NVIDIA/spark-rapids/pull/11579)|Fix merge conflict with branch-24.10|
|[#11571](https://github.com/NVIDIA/spark-rapids/pull/11571)|Log reconfigure multi-file thread pool only once|
|[#11564](https://github.com/NVIDIA/spark-rapids/pull/11564)|Disk spill metric|
|[#11561](https://github.com/NVIDIA/spark-rapids/pull/11561)|Add in a basic plugin for dataframe UDF support in Apache Spark|
|[#11544](https://github.com/NVIDIA/spark-rapids/pull/11544)|Update test case related to LEACY datetime format to unblock nightly CI|
|[#11519](https://github.com/NVIDIA/spark-rapids/pull/11519)|Spark 4:  Fix parquet_test.py|
|[#11563](https://github.com/NVIDIA/spark-rapids/pull/11563)|Fix the latest merge conflict in integration tests|
|[#11542](https://github.com/NVIDIA/spark-rapids/pull/11542)|Update rapids JNI and private dependency to 24.12.0-SNAPSHOT [skip ci]|
|[#11493](https://github.com/NVIDIA/spark-rapids/pull/11493)|Support legacy mode for yyyymmdd format|

## Release 24.10

### Features
|||
|:---|:---|
|[#11525](https://github.com/NVIDIA/spark-rapids/issues/11525)|[FEA] If dump always is enabled dump before decoding the file|
|[#11461](https://github.com/NVIDIA/spark-rapids/issues/11461)|[FEA] Support non-UTC timezone for casting from date to timestamp|
|[#11445](https://github.com/NVIDIA/spark-rapids/issues/11445)|[FEA] Support format 'yyyyMMdd' in GetTimestamp operator|
|[#11442](https://github.com/NVIDIA/spark-rapids/issues/11442)|[FEA] Add in support for setting row group sizes for parquet|
|[#11330](https://github.com/NVIDIA/spark-rapids/issues/11330)|[FEA] Add companion metrics for all nsTiming metrics to measure time elapsed excluding semaphore wait|
|[#5223](https://github.com/NVIDIA/spark-rapids/issues/5223)|[FEA] Support array_join|
|[#10968](https://github.com/NVIDIA/spark-rapids/issues/10968)|[FEA] support min_by function|
|[#10437](https://github.com/NVIDIA/spark-rapids/issues/10437)|[FEA] Add Spark 3.5.2 snapshot support|

### Performance
|||
|:---|:---|
|[#10799](https://github.com/NVIDIA/spark-rapids/issues/10799)|[FEA] Optimize count distinct performance optimization with null columns reuse and post expand coalesce|
|[#8301](https://github.com/NVIDIA/spark-rapids/issues/8301)|[FEA] semaphore prioritization|
|[#11234](https://github.com/NVIDIA/spark-rapids/issues/11234)|Explore swapping build table for left outer joins|
|[#11263](https://github.com/NVIDIA/spark-rapids/issues/11263)|[FEA] Cluster/pack multi_get_json_object paths by common prefixes|

### Bugs Fixed
|||
|:---|:---|
|[#11558](https://github.com/NVIDIA/spark-rapids/issues/11558)|[BUG] test_sortmerge_join_ridealong fails on DB 13.3|
|[#11573](https://github.com/NVIDIA/spark-rapids/issues/11573)|[BUG] very long tail task is observed when many tasks are contending for PrioritySemaphore|
|[#11367](https://github.com/NVIDIA/spark-rapids/issues/11367)|[BUG] Error "table_view.cpp:36: Column size mismatch" when using approx_percentile on a string column|
|[#11543](https://github.com/NVIDIA/spark-rapids/issues/11543)|[BUG] test_yyyyMMdd_format_for_legacy_mode[DATAGEN_SEED=1727619674, TZ=UTC] failed GPU and CPU are not both null|
|[#11500](https://github.com/NVIDIA/spark-rapids/issues/11500)|[BUG] dataproc serverless Integration tests failing in json_matrix_test.py|
|[#11384](https://github.com/NVIDIA/spark-rapids/issues/11384)|[BUG] "rs. shuffle write time" negative values seen in app history log|
|[#11509](https://github.com/NVIDIA/spark-rapids/issues/11509)|[BUG] buildall no longer works|
|[#11501](https://github.com/NVIDIA/spark-rapids/issues/11501)|[BUG] test_yyyyMMdd_format_for_legacy_mode failed in Dataproc Serverless integration tests|
|[#11502](https://github.com/NVIDIA/spark-rapids/issues/11502)|[BUG] IT script failed get jars as we stop deploying intermediate jars since 24.10|
|[#11479](https://github.com/NVIDIA/spark-rapids/issues/11479)|[BUG] spark400 build failed do not conform to class UnaryExprMeta's type parameter|
|[#8558](https://github.com/NVIDIA/spark-rapids/issues/8558)|[BUG] `from_json` generated inconsistent result comparing with CPU for input column with nested json strings|
|[#11485](https://github.com/NVIDIA/spark-rapids/issues/11485)|[BUG] Integration tests failing in join_test.py|
|[#11481](https://github.com/NVIDIA/spark-rapids/issues/11481)|[BUG] non-utc integration tests failing in json_test.py|
|[#10911](https://github.com/NVIDIA/spark-rapids/issues/10911)|from_json: when input is a bad json string, rapids would throw an exception.|
|[#10457](https://github.com/NVIDIA/spark-rapids/issues/10457)|[BUG] ScanJson and JsonToStructs allow unquoted control chars by default|
|[#10479](https://github.com/NVIDIA/spark-rapids/issues/10479)|[BUG] JsonToStructs and ScanJson should return null for non-numeric, non-boolean non-quoted strings|
|[#10534](https://github.com/NVIDIA/spark-rapids/issues/10534)|[BUG] Need Improved JSON Validation |
|[#11436](https://github.com/NVIDIA/spark-rapids/issues/11436)|[BUG] Mortgage unit tests fail with RAPIDS shuffle manager|
|[#11437](https://github.com/NVIDIA/spark-rapids/issues/11437)|[BUG] array and map casts to string tests failed|
|[#11463](https://github.com/NVIDIA/spark-rapids/issues/11463)|[BUG] hash_groupby_approx_percentile failed assert is None|
|[#11465](https://github.com/NVIDIA/spark-rapids/issues/11465)|[BUG] java.lang.NoClassDefFoundError: org/apache/spark/BuildInfo$ in non-databricks environment|
|[#11359](https://github.com/NVIDIA/spark-rapids/issues/11359)|[BUG] a couple of arithmetic_ops_test.py cases failed mismatching cpu and gpu values with [DATAGEN_SEED=1723985531, TZ=UTC, INJECT_OOM]|
|[#11392](https://github.com/NVIDIA/spark-rapids/issues/11392)|[AUDIT] Handle IgnoreNulls Expressions for Window Expressions|
|[#10770](https://github.com/NVIDIA/spark-rapids/issues/10770)|[BUG] Slow/no progress with cascaded pandas udfs/mapInPandas in Databricks|
|[#11397](https://github.com/NVIDIA/spark-rapids/issues/11397)|[BUG] We should not be using copyWithBooleanColumnAsValidity unless we can prove it is 100% safe|
|[#11372](https://github.com/NVIDIA/spark-rapids/issues/11372)|[BUG] spark400 failed compiling datagen_2.13|
|[#11364](https://github.com/NVIDIA/spark-rapids/issues/11364)|[BUG] Missing numRows in the ColumnarBatch created in GpuBringBackToHost|
|[#11350](https://github.com/NVIDIA/spark-rapids/issues/11350)|[BUG] spark400 compile failed in scala213|
|[#11346](https://github.com/NVIDIA/spark-rapids/issues/11346)|[BUG] databrick nightly failing with not able to get spark-version-info.properties|
|[#9604](https://github.com/NVIDIA/spark-rapids/issues/9604)|[BUG] Delta Lake metadata query detection can trigger extra file listing jobs|
|[#11318](https://github.com/NVIDIA/spark-rapids/issues/11318)|[BUG] GPU query is case sensitive on Hive text table's column name|
|[#10596](https://github.com/NVIDIA/spark-rapids/issues/10596)|[BUG] ScanJson and JsonToStructs does not deal with escaped single quotes properly|
|[#10351](https://github.com/NVIDIA/spark-rapids/issues/10351)|[BUG] test_from_json_mixed_types_list_struct failed|
|[#11294](https://github.com/NVIDIA/spark-rapids/issues/11294)|[BUG] binary-dedupe leaves  around a copy of "unshimmed" class files in spark-shared|
|[#11183](https://github.com/NVIDIA/spark-rapids/issues/11183)|[BUG] Failed to split an empty string with error "ai.rapids.cudf.CudfException: parallel_for failed: cudaErrorInvalidDevice: invalid device ordinal"|
|[#11008](https://github.com/NVIDIA/spark-rapids/issues/11008)|Fix tests failures in ast_test.py|
|[#11265](https://github.com/NVIDIA/spark-rapids/issues/11265)|[BUG] segfaults seen in cuDF after prefetch calls intermittently|

### PRs
|||
|:---|:---|
|[#11683](https://github.com/NVIDIA/spark-rapids/pull/11683)|[DOC] update download page for 2410 hot fix release [skip ci]|
|[#11680](https://github.com/NVIDIA/spark-rapids/pull/11680)|Update latest changelog [skip ci]|
|[#11678](https://github.com/NVIDIA/spark-rapids/pull/11678)|Update version to 24.10.1-SNAPSHOT [skip ci]|
|[#11676](https://github.com/NVIDIA/spark-rapids/pull/11676)| Fix race condition with Parquet filter pushdown modifying shared hadoop Configuration|
|[#11626](https://github.com/NVIDIA/spark-rapids/pull/11626)|Update latest changelog [skip ci]|
|[#11624](https://github.com/NVIDIA/spark-rapids/pull/11624)|Update the download link [skip ci]|
|[#11577](https://github.com/NVIDIA/spark-rapids/pull/11577)|Update latest changelog [skip ci]|
|[#11576](https://github.com/NVIDIA/spark-rapids/pull/11576)|Update rapids JNI and private dependency to 24.10.0|
|[#11582](https://github.com/NVIDIA/spark-rapids/pull/11582)|[DOC] update doc for 24.10 release [skip ci]|
|[#11414](https://github.com/NVIDIA/spark-rapids/pull/11414)|Fix `collection_ops_tests` for Spark 4.0|
|[#11588](https://github.com/NVIDIA/spark-rapids/pull/11588)|backport fixes of #11573 to branch 24.10|
|[#11569](https://github.com/NVIDIA/spark-rapids/pull/11569)|Have "dump always" dump input files before trying to decode them|
|[#11544](https://github.com/NVIDIA/spark-rapids/pull/11544)|Update test case related to LEACY datetime format to unblock nightly CI|
|[#11567](https://github.com/NVIDIA/spark-rapids/pull/11567)|Fix test case unix_timestamp(col, 'yyyyMMdd') failed for Africa/Casablanca timezone and LEGACY mode|
|[#11519](https://github.com/NVIDIA/spark-rapids/pull/11519)|Spark 4:  Fix parquet_test.py|
|[#11496](https://github.com/NVIDIA/spark-rapids/pull/11496)|Update test now that code is fixed|
|[#11548](https://github.com/NVIDIA/spark-rapids/pull/11548)|Fix negative rs. shuffle write time|
|[#11545](https://github.com/NVIDIA/spark-rapids/pull/11545)|Update test case related to LEACY datetime format to unblock nightly CI|
|[#11515](https://github.com/NVIDIA/spark-rapids/pull/11515)|Propagate default DIST_PROFILE_OPT profile to Maven in buildall|
|[#11497](https://github.com/NVIDIA/spark-rapids/pull/11497)|Update from_json to use new cudf features|
|[#11516](https://github.com/NVIDIA/spark-rapids/pull/11516)|Deploy all submodules for default sparkver in nightly [skip ci]|
|[#11484](https://github.com/NVIDIA/spark-rapids/pull/11484)|Fix FileAlreadyExistsException in LORE dump process|
|[#11457](https://github.com/NVIDIA/spark-rapids/pull/11457)|GPU device watermark metrics|
|[#11507](https://github.com/NVIDIA/spark-rapids/pull/11507)|Replace libmamba-solver with mamba command [skip ci]|
|[#11503](https://github.com/NVIDIA/spark-rapids/pull/11503)|Download artifacts via wget [skip ci]|
|[#11490](https://github.com/NVIDIA/spark-rapids/pull/11490)|Use UnaryLike instead of UnaryExpression|
|[#10798](https://github.com/NVIDIA/spark-rapids/pull/10798)|Optimizing Expand+Aggregate in sqls with many count distinct|
|[#11366](https://github.com/NVIDIA/spark-rapids/pull/11366)|Enable parquet suites from Spark UT|
|[#11477](https://github.com/NVIDIA/spark-rapids/pull/11477)|Install cuDF-py against python 3.10 on Databricks|
|[#11462](https://github.com/NVIDIA/spark-rapids/pull/11462)|Support non-UTC timezone for casting from date type to timestamp type|
|[#11449](https://github.com/NVIDIA/spark-rapids/pull/11449)|Support yyyyMMdd in GetTimestamp operator for LEGACY mode|
|[#11456](https://github.com/NVIDIA/spark-rapids/pull/11456)|Enable tests for all JSON white space normalization|
|[#11483](https://github.com/NVIDIA/spark-rapids/pull/11483)|Use reusable auto-merge workflow [skip ci]|
|[#11482](https://github.com/NVIDIA/spark-rapids/pull/11482)|Fix a json test for non utc time zone|
|[#11464](https://github.com/NVIDIA/spark-rapids/pull/11464)|Use improved CUDF JSON validation|
|[#11474](https://github.com/NVIDIA/spark-rapids/pull/11474)|Enable tests after string_split was fixed|
|[#11473](https://github.com/NVIDIA/spark-rapids/pull/11473)|Revert "Skip test_hash_groupby_approx_percentile byte and double test…|
|[#11466](https://github.com/NVIDIA/spark-rapids/pull/11466)|Replace scala.util.Try with a try statement in the DBR buildinfo|
|[#11469](https://github.com/NVIDIA/spark-rapids/pull/11469)|Skip test_hash_groupby_approx_percentile byte and double tests tempor…|
|[#11429](https://github.com/NVIDIA/spark-rapids/pull/11429)|Fixed some of the failing parquet_tests|
|[#11455](https://github.com/NVIDIA/spark-rapids/pull/11455)|Log DBR BuildInfo|
|[#11451](https://github.com/NVIDIA/spark-rapids/pull/11451)|xfail array and map cast to string tests|
|[#11331](https://github.com/NVIDIA/spark-rapids/pull/11331)|Add companion metrics for all nsTiming metrics without semaphore|
|[#11421](https://github.com/NVIDIA/spark-rapids/pull/11421)|[DOC] remove the redundant archive link [skip ci]|
|[#11308](https://github.com/NVIDIA/spark-rapids/pull/11308)|Dynamic Shim Detection for `build` Process|
|[#11427](https://github.com/NVIDIA/spark-rapids/pull/11427)|Update CI scripts to work with the "Dynamic Shim Detection" change [skip ci]|
|[#11425](https://github.com/NVIDIA/spark-rapids/pull/11425)|Update signoff usage [skip ci]|
|[#11420](https://github.com/NVIDIA/spark-rapids/pull/11420)|Add in array_join support|
|[#11418](https://github.com/NVIDIA/spark-rapids/pull/11418)|stop using copyWithBooleanColumnAsValidity|
|[#11411](https://github.com/NVIDIA/spark-rapids/pull/11411)|Fix asymmetric join crash when stream side is empty|
|[#11395](https://github.com/NVIDIA/spark-rapids/pull/11395)|Fix a Pandas UDF slowness issue|
|[#11371](https://github.com/NVIDIA/spark-rapids/pull/11371)|Support MinBy and MaxBy for non-float ordering|
|[#11399](https://github.com/NVIDIA/spark-rapids/pull/11399)|stop using copyWithBooleanColumnAsValidity|
|[#11389](https://github.com/NVIDIA/spark-rapids/pull/11389)|prevent duplicate queueing in the prio semaphore|
|[#11291](https://github.com/NVIDIA/spark-rapids/pull/11291)|Add distinct join support for right outer joins|
|[#11396](https://github.com/NVIDIA/spark-rapids/pull/11396)|Drop cudf-py python 3.9 support [skip ci]|
|[#11393](https://github.com/NVIDIA/spark-rapids/pull/11393)|Revert work-around for empty split-string|
|[#11334](https://github.com/NVIDIA/spark-rapids/pull/11334)|Add support for Spark 3.5.2|
|[#11388](https://github.com/NVIDIA/spark-rapids/pull/11388)|JSON tests for corrected date, timestamp, and mixed types|
|[#11375](https://github.com/NVIDIA/spark-rapids/pull/11375)|Fix spark400 build in datagen and tests|
|[#11376](https://github.com/NVIDIA/spark-rapids/pull/11376)|Create a PrioritySemaphore to back the GpuSemaphore|
|[#11383](https://github.com/NVIDIA/spark-rapids/pull/11383)|Fix nightly snapshots being downloaded in premerge build|
|[#11368](https://github.com/NVIDIA/spark-rapids/pull/11368)|Move SparkRapidsBuildInfoEvent to its own file|
|[#11329](https://github.com/NVIDIA/spark-rapids/pull/11329)|Change reference to `MapUtils` into `JSONUtils`|
|[#11365](https://github.com/NVIDIA/spark-rapids/pull/11365)|Set numRows for the ColumnBatch created in GpuBringBackToHost|
|[#11363](https://github.com/NVIDIA/spark-rapids/pull/11363)|Fix failing test compile for Spark 4.0.0|
|[#11362](https://github.com/NVIDIA/spark-rapids/pull/11362)|Add tests for repeated JSON columns/keys|
|[#11321](https://github.com/NVIDIA/spark-rapids/pull/11321)|conform dependency list in 341db to previous versions style|
|[#10604](https://github.com/NVIDIA/spark-rapids/pull/10604)|Add string escaping JSON tests to the test_json_matrix|
|[#11328](https://github.com/NVIDIA/spark-rapids/pull/11328)|Swap build side for outer joins when natural build side is explosive|
|[#11358](https://github.com/NVIDIA/spark-rapids/pull/11358)|Fix download doc [skip ci]|
|[#11357](https://github.com/NVIDIA/spark-rapids/pull/11357)|Fix auto merge conflict 11354 [skip ci]|
|[#11347](https://github.com/NVIDIA/spark-rapids/pull/11347)|Revert "Fix the mismatching default configs in integration tests  (#11283)"|
|[#11323](https://github.com/NVIDIA/spark-rapids/pull/11323)|replace inputFiles with location.rootPaths.toString|
|[#11340](https://github.com/NVIDIA/spark-rapids/pull/11340)|Audit script - Check commits from sql-hive directory [skip ci]|
|[#11283](https://github.com/NVIDIA/spark-rapids/pull/11283)|Fix the mismatching default configs in integration tests |
|[#11327](https://github.com/NVIDIA/spark-rapids/pull/11327)|Make hive column matches not case-sensitive|
|[#11324](https://github.com/NVIDIA/spark-rapids/pull/11324)|Append ustcfy to blossom-ci whitelist [skip ci]|
|[#11325](https://github.com/NVIDIA/spark-rapids/pull/11325)|Fix auto merge conflict 11317 [skip ci]|
|[#11319](https://github.com/NVIDIA/spark-rapids/pull/11319)|Update passing JSON tests after list support added in CUDF|
|[#11307](https://github.com/NVIDIA/spark-rapids/pull/11307)|Safely close multiple resources in RapidsBufferCatalog|
|[#11313](https://github.com/NVIDIA/spark-rapids/pull/11313)|Fix auto merge conflict 10845 11310 [skip ci]|
|[#11312](https://github.com/NVIDIA/spark-rapids/pull/11312)|Add jihoonson as an authorized user for blossom-ci [skip ci]|
|[#11302](https://github.com/NVIDIA/spark-rapids/pull/11302)|Fix display issue of lore.md|
|[#11301](https://github.com/NVIDIA/spark-rapids/pull/11301)|Skip deploying non-critical intermediate artifacts [skip ci]|
|[#11299](https://github.com/NVIDIA/spark-rapids/pull/11299)|Enable get_json_object by default and remove legacy version|
|[#11289](https://github.com/NVIDIA/spark-rapids/pull/11289)|Use the new chunked API from multi-get_json_object|
|[#11295](https://github.com/NVIDIA/spark-rapids/pull/11295)|Remove redundant classes from the dist jar and unshimmed list|
|[#11284](https://github.com/NVIDIA/spark-rapids/pull/11284)|Use distinct count to estimate join magnification factor|
|[#11288](https://github.com/NVIDIA/spark-rapids/pull/11288)|Move easy unshimmed classes to sql-plugin-api|
|[#11285](https://github.com/NVIDIA/spark-rapids/pull/11285)|Remove files under tools/generated_files/spark31* [skip ci]|
|[#11280](https://github.com/NVIDIA/spark-rapids/pull/11280)|Asynchronously copy table data to the host during shuffle|
|[#11258](https://github.com/NVIDIA/spark-rapids/pull/11258)|Explicitly disable ANSI mode for ast_test.py|
|[#11267](https://github.com/NVIDIA/spark-rapids/pull/11267)|Update the rapids JNI and private dependency version to 24.10.0-SNAPSHOT|

## Release 24.08

### Features
|||
|:---|:---|
|[#9259](https://github.com/NVIDIA/spark-rapids/issues/9259)|[FEA] Create Spark 4.0.0 shim and build env|
|[#10366](https://github.com/NVIDIA/spark-rapids/issues/10366)|[FEA] It would be nice if we could support Hive-style write bucketing table|
|[#10987](https://github.com/NVIDIA/spark-rapids/issues/10987)|[FEA] Implement lore framework to support all operators.|
|[#11087](https://github.com/NVIDIA/spark-rapids/issues/11087)|[FEA] Support regex pattern with brackets when rewrite to PrefixRange patten in rlike|
|[#22](https://github.com/NVIDIA/spark-rapids/issues/22)|[FEA] Add support for bucketed writes|
|[#9939](https://github.com/NVIDIA/spark-rapids/issues/9939)|[FEA] `GpuInsertIntoHiveTable` supports parquet format|

### Performance
|||
|:---|:---|
|[#8750](https://github.com/NVIDIA/spark-rapids/issues/8750)|[FEA] Rework GpuSubstringIndex to use cudf::slice_strings|
|[#7404](https://github.com/NVIDIA/spark-rapids/issues/7404)|[FEA] explore a hash agg passthrough on partial aggregates|
|[#10976](https://github.com/NVIDIA/spark-rapids/issues/10976)|Rewrite `pattern1|pattern2|pattern3` to multiple contains in `rlike`|

### Bugs Fixed
|||
|:---|:---|
|[#11287](https://github.com/NVIDIA/spark-rapids/issues/11287)|[BUG] String split APIs on empty string produce incorrect result|
|[#11270](https://github.com/NVIDIA/spark-rapids/issues/11270)|[BUG] test_regexp_replace[DATAGEN_SEED=1722297411, TZ=UTC] hanging there forever in pre-merge CI intermittently|
|[#9682](https://github.com/NVIDIA/spark-rapids/issues/9682)|[BUG] Casting FLOAT64 to DECIMAL(12,7) produces different rows from Apache Spark CPU|
|[#10809](https://github.com/NVIDIA/spark-rapids/issues/10809)|[BUG] cast(9.95 as decimal(3,1)), actual: 9.9, expected: 10.0|
|[#11266](https://github.com/NVIDIA/spark-rapids/issues/11266)|[BUG] test_broadcast_hash_join_constant_keys failed in databricks runtimes|
|[#11243](https://github.com/NVIDIA/spark-rapids/issues/11243)|[BUG] ArrayIndexOutOfBoundsException on a left outer join|
|[#11030](https://github.com/NVIDIA/spark-rapids/issues/11030)|Fix tests failures in string_test.py|
|[#11245](https://github.com/NVIDIA/spark-rapids/issues/11245)|[BUG] mvn verify for the source-javadoc fails and no pre-merge check catches it|
|[#11223](https://github.com/NVIDIA/spark-rapids/issues/11223)|[BUG] Remove unreferenced `CUDF_VER=xxx` in the CI script|
|[#11114](https://github.com/NVIDIA/spark-rapids/issues/11114)|[BUG] Update nightly tests for Scala 2.13 to use JDK 17 only|
|[#11229](https://github.com/NVIDIA/spark-rapids/issues/11229)|[BUG] test_delta_name_column_mapping_no_field_ids fails on Spark |
|[#11031](https://github.com/NVIDIA/spark-rapids/issues/11031)|Fix tests failures in multiple files |
|[#10948](https://github.com/NVIDIA/spark-rapids/issues/10948)|Figure out why `MapFromArrays ` appears in the tests for hive parquet write|
|[#11018](https://github.com/NVIDIA/spark-rapids/issues/11018)|Fix tests failures in hash_aggregate_test.py|
|[#11173](https://github.com/NVIDIA/spark-rapids/issues/11173)|[BUG] The `rs. serialization time` metric is misleading|
|[#11017](https://github.com/NVIDIA/spark-rapids/issues/11017)|Fix tests failures in url_test.py|
|[#11201](https://github.com/NVIDIA/spark-rapids/issues/11201)|[BUG] Delta Lake tables with name mapping can throw exceptions on read|
|[#11175](https://github.com/NVIDIA/spark-rapids/issues/11175)|[BUG] Clean up unused and duplicated 'org/roaringbitmap' folder in the spark3xx shims|
|[#11196](https://github.com/NVIDIA/spark-rapids/issues/11196)|[BUG] pipeline failed due to class not found exception: NoClassDefFoundError: com/nvidia/spark/rapids/GpuScalar|
|[#11189](https://github.com/NVIDIA/spark-rapids/issues/11189)|[BUG] regression in NDS after PR #11170|
|[#11167](https://github.com/NVIDIA/spark-rapids/issues/11167)|[BUG] UnsupportedOperationException during delta write with `optimize()`|
|[#11172](https://github.com/NVIDIA/spark-rapids/issues/11172)|[BUG] `get_json_object` returns wrong output with wildcard path|
|[#11148](https://github.com/NVIDIA/spark-rapids/issues/11148)|[BUG] Integration test `test_write_hive_bucketed_table` fails|
|[#11155](https://github.com/NVIDIA/spark-rapids/issues/11155)|[BUG] ArrayIndexOutOfBoundsException in BatchWithPartitionData.splitColumnarBatch|
|[#11152](https://github.com/NVIDIA/spark-rapids/issues/11152)|[BUG] LORE dumping consumes too much memory.|
|[#11029](https://github.com/NVIDIA/spark-rapids/issues/11029)|Fix tests failures in subquery_test.py|
|[#11150](https://github.com/NVIDIA/spark-rapids/issues/11150)|[BUG] hive_parquet_write_test.py::test_insert_hive_bucketed_table failure|
|[#11070](https://github.com/NVIDIA/spark-rapids/issues/11070)|[BUG] numpy2 fail fastparquet cases: numpy.dtype size changed|
|[#11136](https://github.com/NVIDIA/spark-rapids/issues/11136)|UnaryPositive expression doesn't extend UnaryExpression|
|[#11122](https://github.com/NVIDIA/spark-rapids/issues/11122)|[BUG] UT MetricRange failed 651070526 was not less than 1.5E8 in spark313|
|[#11119](https://github.com/NVIDIA/spark-rapids/issues/11119)|[BUG] window_function_test.py::test_window_group_limits_fallback_for_row_number fails in a distributed environment|
|[#11023](https://github.com/NVIDIA/spark-rapids/issues/11023)|Fix tests failures in dpp_test.py|
|[#11026](https://github.com/NVIDIA/spark-rapids/issues/11026)|Fix tests failures in map_test.py|
|[#11020](https://github.com/NVIDIA/spark-rapids/issues/11020)|Fix tests failures in grouping_sets_test.py|
|[#11113](https://github.com/NVIDIA/spark-rapids/issues/11113)|[BUG] Update premerge tests for Scala 2.13 to use JDK 17 only|
|[#11027](https://github.com/NVIDIA/spark-rapids/issues/11027)|Fix tests failures in sort_test.py|
|[#10775](https://github.com/NVIDIA/spark-rapids/issues/10775)|[BUG] Issues found by Spark UT Framework on RapidsStringExpressionsSuite|
|[#11033](https://github.com/NVIDIA/spark-rapids/issues/11033)|[BUG] CICD failed a case: cmp_test.py::test_empty_filter[>]|
|[#11103](https://github.com/NVIDIA/spark-rapids/issues/11103)|[BUG] UCX Shuffle With scala.MatchError |
|[#11007](https://github.com/NVIDIA/spark-rapids/issues/11007)|Fix tests failures in array_test.py|
|[#10801](https://github.com/NVIDIA/spark-rapids/issues/10801)|[BUG] JDK17 nightly build after Spark UT Framework is merged|
|[#11019](https://github.com/NVIDIA/spark-rapids/issues/11019)|Fix tests failures in window_function_test.py|
|[#11063](https://github.com/NVIDIA/spark-rapids/issues/11063)|[BUG] op time for GpuCoalesceBatches is more than actual|
|[#11006](https://github.com/NVIDIA/spark-rapids/issues/11006)|Fix test failures in arithmetic_ops_test.py|
|[#10995](https://github.com/NVIDIA/spark-rapids/issues/10995)|Fallback TimeZoneAwareExpression that only support UTC with zoneId instead of timeZone config|
|[#8652](https://github.com/NVIDIA/spark-rapids/issues/8652)|[BUG] array_item test failures on Spark 3.3.x|
|[#11053](https://github.com/NVIDIA/spark-rapids/issues/11053)|[BUG] Build on Databricks 330 fails|
|[#10925](https://github.com/NVIDIA/spark-rapids/issues/10925)| Concat cannot accept no parameter|
|[#10975](https://github.com/NVIDIA/spark-rapids/issues/10975)|[BUG] regex `^.*literal` cannot be rewritten as `contains(literal)` for multiline strings|
|[#10956](https://github.com/NVIDIA/spark-rapids/issues/10956)|[BUG] hive_parquet_write_test.py: test_write_compressed_parquet_into_hive_table integration test failures|
|[#10772](https://github.com/NVIDIA/spark-rapids/issues/10772)|[BUG] Issues found by Spark UT Framework on RapidsDataFrameAggregateSuite|
|[#10986](https://github.com/NVIDIA/spark-rapids/issues/10986)|[BUG]Cast from string to float using hand-picked values failed in CastOpSuite|
|[#10972](https://github.com/NVIDIA/spark-rapids/issues/10972)|Spark 4.0 compile errors |
|[#10794](https://github.com/NVIDIA/spark-rapids/issues/10794)|[BUG] Incorrect cast of string columns containing various infinity notations with trailing spaces |
|[#10964](https://github.com/NVIDIA/spark-rapids/issues/10964)|[BUG] Improve stability of pre-merge jenkinsfile|
|[#10714](https://github.com/NVIDIA/spark-rapids/issues/10714)|Signature changed for `PythonUDFRunner.writeUDFs` |
|[#10712](https://github.com/NVIDIA/spark-rapids/issues/10712)|[AUDIT] BatchScanExec/DataSourceV2Relation to group splits by join keys if they differ from partition keys|
|[#10673](https://github.com/NVIDIA/spark-rapids/issues/10673)|[AUDIT] Rename plan nodes for PythonMapInArrowExec|
|[#10710](https://github.com/NVIDIA/spark-rapids/issues/10710)|[AUDIT] `uncacheTableOrView` changed in CommandUtils |
|[#10711](https://github.com/NVIDIA/spark-rapids/issues/10711)|[AUDIT] Match DataSourceV2ScanExecBase changes to groupPartitions method |
|[#10669](https://github.com/NVIDIA/spark-rapids/issues/10669)|Supporting broadcast of multiple filtering keys in DynamicPruning |

### PRs
|||
|:---|:---|
|[#11400](https://github.com/NVIDIA/spark-rapids/pull/11400)|[DOC] update notes in download page for the decompressing gzip issue [skip ci]|
|[#11355](https://github.com/NVIDIA/spark-rapids/pull/11355)|Update changelog for the v24.08 release [skip ci]|
|[#11353](https://github.com/NVIDIA/spark-rapids/pull/11353)|Update download doc for v24.08.1 [skip ci]|
|[#11352](https://github.com/NVIDIA/spark-rapids/pull/11352)|Update version to 24.08.1-SNAPSHOT [skip ci]|
|[#11337](https://github.com/NVIDIA/spark-rapids/pull/11337)|Update changelog for the v24.08 release [skip ci]|
|[#11335](https://github.com/NVIDIA/spark-rapids/pull/11335)|Fix Delta Lake truncation of min/max string values|
|[#11304](https://github.com/NVIDIA/spark-rapids/pull/11304)|Update changelog for v24.08.0 release [skip ci]|
|[#11303](https://github.com/NVIDIA/spark-rapids/pull/11303)|Update rapids JNI and private dependency to 24.08.0|
|[#11296](https://github.com/NVIDIA/spark-rapids/pull/11296)|[DOC] update doc for 2408 release [skip CI]|
|[#11309](https://github.com/NVIDIA/spark-rapids/pull/11309)|[Doc ]Update lore doc about the range [skip ci]|
|[#11292](https://github.com/NVIDIA/spark-rapids/pull/11292)|Add work around for string split with empty input.|
|[#11278](https://github.com/NVIDIA/spark-rapids/pull/11278)|Fix formatting of advanced configs doc|
|[#10917](https://github.com/NVIDIA/spark-rapids/pull/10917)|Adopt changes from JNI for casting from float to decimal|
|[#11269](https://github.com/NVIDIA/spark-rapids/pull/11269)|Revert "upgrade ucx to 1.17.0"|
|[#11260](https://github.com/NVIDIA/spark-rapids/pull/11260)|Mitigate intermittent test_buckets and shuffle_smoke_test OOM issue|
|[#11268](https://github.com/NVIDIA/spark-rapids/pull/11268)|Fix degenerate conditional nested loop join detection|
|[#11244](https://github.com/NVIDIA/spark-rapids/pull/11244)|Fix ArrayIndexOutOfBoundsException on join counts with constant join keys|
|[#11259](https://github.com/NVIDIA/spark-rapids/pull/11259)|CI Docker to support integration tests with Rocky OS + jdk17 [skip ci]|
|[#11247](https://github.com/NVIDIA/spark-rapids/pull/11247)|Fix `string_test.py` errors on Spark 4.0|
|[#11246](https://github.com/NVIDIA/spark-rapids/pull/11246)|Rework Maven Source Plugin Skip|
|[#11149](https://github.com/NVIDIA/spark-rapids/pull/11149)|Rework on substring index|
|[#11236](https://github.com/NVIDIA/spark-rapids/pull/11236)|Remove the unused vars from the version-def CI script|
|[#11237](https://github.com/NVIDIA/spark-rapids/pull/11237)|Fork jvm for maven-source-plugin|
|[#11200](https://github.com/NVIDIA/spark-rapids/pull/11200)|Multi-get_json_object|
|[#11230](https://github.com/NVIDIA/spark-rapids/pull/11230)|Skip test where Delta Lake may not be fully compatible with Spark|
|[#11220](https://github.com/NVIDIA/spark-rapids/pull/11220)|Avoid failing spark bug SPARK-44242 while generate run_dir|
|[#11226](https://github.com/NVIDIA/spark-rapids/pull/11226)|Fix auto merge conflict 11212|
|[#11129](https://github.com/NVIDIA/spark-rapids/pull/11129)|Spark 4: Fix miscellaneous tests including logic, repart, hive_delimited.|
|[#11163](https://github.com/NVIDIA/spark-rapids/pull/11163)|Support `MapFromArrays` on GPU|
|[#11219](https://github.com/NVIDIA/spark-rapids/pull/11219)|Fix hash_aggregate_test.py to run with ANSI enabled|
|[#11186](https://github.com/NVIDIA/spark-rapids/pull/11186)|from_json Json to Struct Exception Logging|
|[#11180](https://github.com/NVIDIA/spark-rapids/pull/11180)|More accurate estimation for the result serialization time in RapidsShuffleThreadedWriterBase|
|[#11194](https://github.com/NVIDIA/spark-rapids/pull/11194)|Fix ANSI mode test failures in url_test.py|
|[#11202](https://github.com/NVIDIA/spark-rapids/pull/11202)|Fix read from Delta Lake table with name column mapping and missing Parquet IDs|
|[#11185](https://github.com/NVIDIA/spark-rapids/pull/11185)|Fix multi-release jar problem|
|[#11144](https://github.com/NVIDIA/spark-rapids/pull/11144)|Build the Scala2.13 dist jar with JDK17|
|[#11197](https://github.com/NVIDIA/spark-rapids/pull/11197)|Fix class not found error: com/nvidia/spark/rapids/GpuScalar|
|[#11191](https://github.com/NVIDIA/spark-rapids/pull/11191)|Fix dynamic pruning regression in GpuFileSourceScanExec|
|[#10994](https://github.com/NVIDIA/spark-rapids/pull/10994)|Add Spark 4.0.0 Build Profile and Other Supporting Changes|
|[#11192](https://github.com/NVIDIA/spark-rapids/pull/11192)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#11179](https://github.com/NVIDIA/spark-rapids/pull/11179)|Allow more expressions to be tiered|
|[#11141](https://github.com/NVIDIA/spark-rapids/pull/11141)|Enable some Rapids config in RapidsSQLTestsBaseTrait for Spark UT|
|[#11170](https://github.com/NVIDIA/spark-rapids/pull/11170)|Avoid listFiles or inputFiles on relations with static partitioning|
|[#11159](https://github.com/NVIDIA/spark-rapids/pull/11159)|Drop spark31x shims|
|[#10951](https://github.com/NVIDIA/spark-rapids/pull/10951)|Case when performance improvement: reduce the `copy_if_else`|
|[#11165](https://github.com/NVIDIA/spark-rapids/pull/11165)|Fix some GpuBroadcastToRowExec by not dropping columns|
|[#11126](https://github.com/NVIDIA/spark-rapids/pull/11126)|Coalesce batches after a logical coalesce operation|
|[#11164](https://github.com/NVIDIA/spark-rapids/pull/11164)|fix the bucketed write error for non-utc cases|
|[#11132](https://github.com/NVIDIA/spark-rapids/pull/11132)|Add deletion vector metrics for low shuffle merge.|
|[#11156](https://github.com/NVIDIA/spark-rapids/pull/11156)|Fix batch splitting for partition column size on row-count-only batches|
|[#11153](https://github.com/NVIDIA/spark-rapids/pull/11153)|Fix LORE dump oom.|
|[#11102](https://github.com/NVIDIA/spark-rapids/pull/11102)|Fix ANSI mode failures in subquery_test.py|
|[#11151](https://github.com/NVIDIA/spark-rapids/pull/11151)|Fix the test error of the bucketed write for the non-utc case|
|[#11147](https://github.com/NVIDIA/spark-rapids/pull/11147)|upgrade ucx to 1.17.0|
|[#11138](https://github.com/NVIDIA/spark-rapids/pull/11138)|Update fastparquet to 2024.5.0 for numpy2 compatibility|
|[#11137](https://github.com/NVIDIA/spark-rapids/pull/11137)|Handle the change for UnaryPositive now extending RuntimeReplaceable|
|[#11094](https://github.com/NVIDIA/spark-rapids/pull/11094)|Add `HiveHash` support on GPU|
|[#11139](https://github.com/NVIDIA/spark-rapids/pull/11139)|Improve MetricsSuite to allow more gc jitter|
|[#11133](https://github.com/NVIDIA/spark-rapids/pull/11133)|Fix `test_window_group_limits_fallback`|
|[#11097](https://github.com/NVIDIA/spark-rapids/pull/11097)|Fix miscellaneous integ tests for Spark 4|
|[#11118](https://github.com/NVIDIA/spark-rapids/pull/11118)|Fix issue with DPP and AQE on reused broadcast exchanges|
|[#11043](https://github.com/NVIDIA/spark-rapids/pull/11043)|Dataproc serverless test fixes|
|[#10965](https://github.com/NVIDIA/spark-rapids/pull/10965)|Profiler: Disable collecting async allocation events by default|
|[#11117](https://github.com/NVIDIA/spark-rapids/pull/11117)|Update Scala2.13 premerge CI against JDK17|
|[#11084](https://github.com/NVIDIA/spark-rapids/pull/11084)|Introduce LORE framework.|
|[#11099](https://github.com/NVIDIA/spark-rapids/pull/11099)|Spark 4: Handle ANSI mode in sort_test.py|
|[#11115](https://github.com/NVIDIA/spark-rapids/pull/11115)|Fix match error in RapidsShuffleIterator.scala [scala2.13]|
|[#11088](https://github.com/NVIDIA/spark-rapids/pull/11088)|Support regex patterns with brackets when rewriting to PrefixRange pattern in rlike.|
|[#10950](https://github.com/NVIDIA/spark-rapids/pull/10950)|Add a heuristic to skip second or third agg pass|
|[#11048](https://github.com/NVIDIA/spark-rapids/pull/11048)|Fixed array_tests for Spark 4.0.0|
|[#11049](https://github.com/NVIDIA/spark-rapids/pull/11049)|Fix some cast_tests for Spark 4.0.0|
|[#11066](https://github.com/NVIDIA/spark-rapids/pull/11066)|Replaced spark3xx-common references to spark-shared|
|[#11083](https://github.com/NVIDIA/spark-rapids/pull/11083)|Exclude a case based on JDK version in Spark UT|
|[#10997](https://github.com/NVIDIA/spark-rapids/pull/10997)|Fix some test issues in Spark UT and keep RapidsTestSettings update-to-date|
|[#11073](https://github.com/NVIDIA/spark-rapids/pull/11073)|Disable ANSI mode for window function tests|
|[#11076](https://github.com/NVIDIA/spark-rapids/pull/11076)|Improve the diagnostics for 'conv' fallback explain|
|[#11092](https://github.com/NVIDIA/spark-rapids/pull/11092)|Add GpuBucketingUtils shim to Spark 4.0.0|
|[#11062](https://github.com/NVIDIA/spark-rapids/pull/11062)|fix duplicate counted metrics like op time for GpuCoalesceBatches|
|[#11044](https://github.com/NVIDIA/spark-rapids/pull/11044)|Fixed Failing tests in arithmetic_ops_tests for Spark 4.0.0|
|[#11086](https://github.com/NVIDIA/spark-rapids/pull/11086)|upgrade blossom-ci actions version [skip ci]|
|[#10957](https://github.com/NVIDIA/spark-rapids/pull/10957)|Support bucketing write for GPU|
|[#10979](https://github.com/NVIDIA/spark-rapids/pull/10979)|[FEA] Introduce low shuffle merge.|
|[#10996](https://github.com/NVIDIA/spark-rapids/pull/10996)|Fallback non-UTC TimeZoneAwareExpression with zoneId|
|[#11072](https://github.com/NVIDIA/spark-rapids/pull/11072)|Workaround numpy2 failed fastparquet compatibility tests|
|[#11046](https://github.com/NVIDIA/spark-rapids/pull/11046)|Calculate parallelism to speed up pre-merge CI|
|[#11054](https://github.com/NVIDIA/spark-rapids/pull/11054)|fix flaky array_item test failures|
|[#11051](https://github.com/NVIDIA/spark-rapids/pull/11051)|[FEA] Increase parallelism of deltalake test on databricks|
|[#10993](https://github.com/NVIDIA/spark-rapids/pull/10993)|`binary-dedupe` changes for Spark 4.0.0|
|[#11060](https://github.com/NVIDIA/spark-rapids/pull/11060)|Add in the ability to fingerprint JSON columns|
|[#11059](https://github.com/NVIDIA/spark-rapids/pull/11059)|Revert "Add in the ability to fingerprint JSON columns (#11002)" [skip ci]|
|[#11039](https://github.com/NVIDIA/spark-rapids/pull/11039)|Concat() Exception bug fix|
|[#11002](https://github.com/NVIDIA/spark-rapids/pull/11002)|Add in the ability to fingerprint JSON columns|
|[#10977](https://github.com/NVIDIA/spark-rapids/pull/10977)|Rewrite multiple literal choice regex to multiple contains in rlike|
|[#11035](https://github.com/NVIDIA/spark-rapids/pull/11035)|Fix auto merge conflict 11034 [skip ci]|
|[#11040](https://github.com/NVIDIA/spark-rapids/pull/11040)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#11036](https://github.com/NVIDIA/spark-rapids/pull/11036)|Update blossom-ci ACL to secure format [skip ci]|
|[#11032](https://github.com/NVIDIA/spark-rapids/pull/11032)|Fix a hive write test failure for Spark 350|
|[#10998](https://github.com/NVIDIA/spark-rapids/pull/10998)|Improve log to print more lines in build [skip ci]|
|[#10992](https://github.com/NVIDIA/spark-rapids/pull/10992)|Addressing the Named Parameter change in Spark 4.0.0|
|[#10943](https://github.com/NVIDIA/spark-rapids/pull/10943)|Fix Spark UT issues in RapidsDataFrameAggregateSuite|
|[#10963](https://github.com/NVIDIA/spark-rapids/pull/10963)|Add rapids configs to enable GPU running in Spark UT|
|[#10978](https://github.com/NVIDIA/spark-rapids/pull/10978)|More compilation fixes for Spark 4.0.0|
|[#10953](https://github.com/NVIDIA/spark-rapids/pull/10953)|Speed up the integration tests by running them in parallel on the Databricks cluster|
|[#10958](https://github.com/NVIDIA/spark-rapids/pull/10958)|Fix a hive write test failure|
|[#10970](https://github.com/NVIDIA/spark-rapids/pull/10970)|Move Support for `RaiseError` to a Shim Excluding Spark 4.0.0|
|[#10966](https://github.com/NVIDIA/spark-rapids/pull/10966)|Add default value for REF of premerge jenkinsfile to avoid bad overwritten [skip ci]|
|[#10959](https://github.com/NVIDIA/spark-rapids/pull/10959)|Add new ID to blossom-ci allow list [skip ci]|
|[#10952](https://github.com/NVIDIA/spark-rapids/pull/10952)|Add shims to take care of the signature change for writeUDFs in PythonUDFRunner|
|[#10931](https://github.com/NVIDIA/spark-rapids/pull/10931)|Add Support for Renaming of PythonMapInArrow|
|[#10949](https://github.com/NVIDIA/spark-rapids/pull/10949)|Change dependency version to 24.08.0-SNAPSHOT|
|[#10857](https://github.com/NVIDIA/spark-rapids/pull/10857)|[Spark 4.0] Account for `PartitionedFileUtil.splitFiles` signature change.|
|[#10912](https://github.com/NVIDIA/spark-rapids/pull/10912)|GpuInsertIntoHiveTable supports parquet format|
|[#10863](https://github.com/NVIDIA/spark-rapids/pull/10863)|[Spark 4.0] Account for `CommandUtils.uncacheTableOrView` signature change.|
|[#10944](https://github.com/NVIDIA/spark-rapids/pull/10944)|Added Shim for BatchScanExec to Support Spark 4.0|
|[#10946](https://github.com/NVIDIA/spark-rapids/pull/10946)|Unarchive Spark test jar for spark.read(ability)|
|[#10945](https://github.com/NVIDIA/spark-rapids/pull/10945)|Add Support for Multiple Filtering Keys for Subquery Broadcast|
|[#10871](https://github.com/NVIDIA/spark-rapids/pull/10871)|Add classloader diagnostics to initShuffleManager error message|
|[#10933](https://github.com/NVIDIA/spark-rapids/pull/10933)|Fixed Databricks build|
|[#10929](https://github.com/NVIDIA/spark-rapids/pull/10929)|Append new authorized user to blossom-ci whitelist [skip ci]|

## Release 24.06

### Features
|||
|:---|:---|
|[#10850](https://github.com/NVIDIA/spark-rapids/issues/10850)|[FEA] Refine the test framework introduced in #10745|
|[#6969](https://github.com/NVIDIA/spark-rapids/issues/6969)|[FEA] Support parse_url |
|[#10496](https://github.com/NVIDIA/spark-rapids/issues/10496)|[FEA] Drop support for CentOS7|
|[#10760](https://github.com/NVIDIA/spark-rapids/issues/10760)|[FEA]Support ArrayFilter|
|[#10721](https://github.com/NVIDIA/spark-rapids/issues/10721)|[FEA] Dump the complete set of build-info properties to the Spark eventLog|
|[#10666](https://github.com/NVIDIA/spark-rapids/issues/10666)|[FEA]  Create Spark 3.4.3 shim|

### Performance
|||
|:---|:---|
|[#8963](https://github.com/NVIDIA/spark-rapids/issues/8963)|[FEA] Use custom kernel for parse_url|
|[#10817](https://github.com/NVIDIA/spark-rapids/issues/10817)|[FOLLOW ON] Combining regex parsing in transpiling and regex rewrite in `rlike`|
|[#10821](https://github.com/NVIDIA/spark-rapids/issues/10821)|Rewrite `pattern[A-B]{X,Y}` (a pattern string followed by X to Y chars in range A - B) in `RLIKE` to a custom kernel|

### Bugs Fixed
|||
|:---|:---|
|[#10928](https://github.com/NVIDIA/spark-rapids/issues/10928)|[BUG] 24.06 test_conditional_with_side_effects_case_when test failed on Scala 2.13 with DATAGEN_SEED=1716656294|
|[#10941](https://github.com/NVIDIA/spark-rapids/issues/10941)|[BUG] Failed to build on databricks due to GpuOverrides.scala:4264: not found: type GpuSubqueryBroadcastMeta|
|[#10902](https://github.com/NVIDIA/spark-rapids/issues/10902)|Spark UT failed: SPARK-37360: Timestamp type inference for a mix of TIMESTAMP_NTZ and TIMESTAMP_LTZ|
|[#10899](https://github.com/NVIDIA/spark-rapids/issues/10899)|[BUG] format_number Spark UT failed because Type conversion is not allowed|
|[#10913](https://github.com/NVIDIA/spark-rapids/issues/10913)|[BUG] rlike with empty pattern failed with 'NoSuchElementException' when enabling regex rewrite|
|[#10774](https://github.com/NVIDIA/spark-rapids/issues/10774)|[BUG] Issues found by Spark UT Framework on RapidsRegexpExpressionsSuite|
|[#10606](https://github.com/NVIDIA/spark-rapids/issues/10606)|[BUG] Update Plugin to use the new `getPartitionedFile` method|
|[#10806](https://github.com/NVIDIA/spark-rapids/issues/10806)|[BUG] orc_write_test.py::test_write_round_trip_corner failed with DATAGEN_SEED=1715517863|
|[#10831](https://github.com/NVIDIA/spark-rapids/issues/10831)|[BUG] Failed to read data from iceberg|
|[#10810](https://github.com/NVIDIA/spark-rapids/issues/10810)|[BUG] NPE when running `ParseUrl` tests in `RapidsStringExpressionsSuite`|
|[#10797](https://github.com/NVIDIA/spark-rapids/issues/10797)|[BUG] udf_test test_single_aggregate_udf, test_group_aggregate_udf and test_group_apply_udf_more_types failed on DB 13.3|
|[#10719](https://github.com/NVIDIA/spark-rapids/issues/10719)|[BUG] test_exact_percentile_groupby FAILED: hash_aggregate_test.py::test_exact_percentile_groupby with DATAGEN seed 1713362217|
|[#10738](https://github.com/NVIDIA/spark-rapids/issues/10738)|[BUG] test_exact_percentile_groupby_partial_fallback_to_cpu failed with DATAGEN_SEED=1713928179|
|[#10768](https://github.com/NVIDIA/spark-rapids/issues/10768)|[DOC] Dead links with tools pages|
|[#10751](https://github.com/NVIDIA/spark-rapids/issues/10751)|[BUG] Cascaded Pandas UDFs not working as expected on Databricks when plugin is enabled|
|[#10318](https://github.com/NVIDIA/spark-rapids/issues/10318)|[BUG] `fs.azure.account.keyInvalid` configuration issue while reading from Unity Catalog Tables on Azure DB|
|[#10722](https://github.com/NVIDIA/spark-rapids/issues/10722)|[BUG] "Could not find any rapids-4-spark jars in classpath" error when debugging UT in IDEA|
|[#10724](https://github.com/NVIDIA/spark-rapids/issues/10724)|[BUG] Failed to convert string with invisible characters to float|
|[#10633](https://github.com/NVIDIA/spark-rapids/issues/10633)|[BUG] ScanJson and JsonToStructs can give almost random errors|
|[#10659](https://github.com/NVIDIA/spark-rapids/issues/10659)|[BUG] from_json ArrayIndexOutOfBoundsException in 24.02|
|[#10656](https://github.com/NVIDIA/spark-rapids/issues/10656)|[BUG] Databricks cache tests failing with host memory OOM|

### PRs
|||
|:---|:---|
|[#11222](https://github.com/NVIDIA/spark-rapids/pull/11222)|Update change log for v24.06.1 release [skip ci]|
|[#11221](https://github.com/NVIDIA/spark-rapids/pull/11221)|Change cudf version back to 24.06.0-SNAPSHOT [skip ci]|
|[#11217](https://github.com/NVIDIA/spark-rapids/pull/11217)|Update latest changelog [skip ci]|
|[#11211](https://github.com/NVIDIA/spark-rapids/pull/11211)|Use fixed seed for test_from_json_struct_decimal|
|[#11203](https://github.com/NVIDIA/spark-rapids/pull/11203)|Update version to 24.06.1-SNAPSHOT|
|[#11205](https://github.com/NVIDIA/spark-rapids/pull/11205)|Update docs for 24.06.1 release [skip ci]|
|[#11056](https://github.com/NVIDIA/spark-rapids/pull/11056)|Update latest changelog [skip ci]|
|[#11052](https://github.com/NVIDIA/spark-rapids/pull/11052)|Add spark343 shim for scala2.13 dist jar|
|[#10981](https://github.com/NVIDIA/spark-rapids/pull/10981)|Update latest changelog [skip ci]|
|[#10984](https://github.com/NVIDIA/spark-rapids/pull/10984)|[DOC] Update docs for 24.06.0 release [skip ci]|
|[#10974](https://github.com/NVIDIA/spark-rapids/pull/10974)|Update rapids JNI and private dependency to 24.06.0|
|[#10830](https://github.com/NVIDIA/spark-rapids/pull/10830)|Use ErrorClass to Throw AnalysisException|
|[#10947](https://github.com/NVIDIA/spark-rapids/pull/10947)|Prevent contains-PrefixRange optimization if not preceded by wildcards|
|[#10934](https://github.com/NVIDIA/spark-rapids/pull/10934)|Revert "Add Support for Multiple Filtering Keys for Subquery Broadcast "|
|[#10870](https://github.com/NVIDIA/spark-rapids/pull/10870)|Add support for self-contained profiling|
|[#10903](https://github.com/NVIDIA/spark-rapids/pull/10903)|Use upper case for LEGACY_TIME_PARSER_POLICY to fix a spark UT|
|[#10900](https://github.com/NVIDIA/spark-rapids/pull/10900)|Fix type convert error in format_number scalar input|
|[#10868](https://github.com/NVIDIA/spark-rapids/pull/10868)|Disable default cuDF pinned pool|
|[#10914](https://github.com/NVIDIA/spark-rapids/pull/10914)|Fix NoSuchElementException when rlike with empty pattern|
|[#10858](https://github.com/NVIDIA/spark-rapids/pull/10858)|Add Support for Multiple Filtering Keys for Subquery Broadcast |
|[#10861](https://github.com/NVIDIA/spark-rapids/pull/10861)|refine ut framework including Part 1 and Part 2|
|[#10872](https://github.com/NVIDIA/spark-rapids/pull/10872)|[DOC] ignore released plugin links to reduce the bother info [skip ci]|
|[#10839](https://github.com/NVIDIA/spark-rapids/pull/10839)|Replace anonymous classes for SortOrder and FIlterExec overrides|
|[#10873](https://github.com/NVIDIA/spark-rapids/pull/10873)|Auto merge PRs to branch-24.08 from branch-24.06 [skip ci]|
|[#10860](https://github.com/NVIDIA/spark-rapids/pull/10860)|[Spark 4.0] Account for `PartitionedFileUtil.getPartitionedFile` signature change.|
|[#10822](https://github.com/NVIDIA/spark-rapids/pull/10822)|Rewrite regex pattern `literal[a-b]{x}` to custom kernel in rlike|
|[#10833](https://github.com/NVIDIA/spark-rapids/pull/10833)|Filter out unused json_path tokens|
|[#10855](https://github.com/NVIDIA/spark-rapids/pull/10855)|Fix auto merge conflict 10845 [[skip ci]]|
|[#10826](https://github.com/NVIDIA/spark-rapids/pull/10826)|Add NVTX ranges to identify Spark stages and tasks|
|[#10836](https://github.com/NVIDIA/spark-rapids/pull/10836)|Catch exceptions when trying to examine Iceberg scan for metadata queries|
|[#10824](https://github.com/NVIDIA/spark-rapids/pull/10824)|Support zstd for GPU shuffle compression|
|[#10828](https://github.com/NVIDIA/spark-rapids/pull/10828)|Added DateTimeUtilsShims [Databricks]|
|[#10829](https://github.com/NVIDIA/spark-rapids/pull/10829)|Fix `Inheritance Shadowing` to add support for Spark 4.0.0|
|[#10811](https://github.com/NVIDIA/spark-rapids/pull/10811)|Fix NPE in GpuParseUrl for null keys.|
|[#10723](https://github.com/NVIDIA/spark-rapids/pull/10723)|Implement chunked ORC reader|
|[#10715](https://github.com/NVIDIA/spark-rapids/pull/10715)|Rewrite some rlike expression to StartsWith/Contains|
|[#10820](https://github.com/NVIDIA/spark-rapids/pull/10820)|workaround #10801 temporally|
|[#10812](https://github.com/NVIDIA/spark-rapids/pull/10812)|Replace ThreadPoolExecutor creation with ThreadUtils API|
|[#10813](https://github.com/NVIDIA/spark-rapids/pull/10813)|Fix the errors for Pandas UDF tests on DB13.3|
|[#10795](https://github.com/NVIDIA/spark-rapids/pull/10795)|Remove fixed seed for exact `percentile` integration tests|
|[#10805](https://github.com/NVIDIA/spark-rapids/pull/10805)|Drop Support for CentOS 7|
|[#10800](https://github.com/NVIDIA/spark-rapids/pull/10800)|Add number normalization test and address followup for getJsonObject|
|[#10796](https://github.com/NVIDIA/spark-rapids/pull/10796)|fixing build break on DBR|
|[#10791](https://github.com/NVIDIA/spark-rapids/pull/10791)|Fix auto merge conflict 10779 [skip ci]|
|[#10636](https://github.com/NVIDIA/spark-rapids/pull/10636)|Update actions version [skip ci]|
|[#10743](https://github.com/NVIDIA/spark-rapids/pull/10743)|initial PR for the framework reusing Vanilla Spark's unit tests|
|[#10767](https://github.com/NVIDIA/spark-rapids/pull/10767)|Add rows-only batches support to RebatchingRoundoffIterator|
|[#10763](https://github.com/NVIDIA/spark-rapids/pull/10763)|Add in the GpuArrayFilter command|
|[#10766](https://github.com/NVIDIA/spark-rapids/pull/10766)|Fix dead links related to tools documentation [skip ci]|
|[#10644](https://github.com/NVIDIA/spark-rapids/pull/10644)|Add logging to Integration test runs in local and local-cluster mode|
|[#10756](https://github.com/NVIDIA/spark-rapids/pull/10756)|Fix Authorization Failure While Reading Tables From Unity Catalog|
|[#10752](https://github.com/NVIDIA/spark-rapids/pull/10752)|Add SparkRapidsBuildInfoEvent to the event log|
|[#10754](https://github.com/NVIDIA/spark-rapids/pull/10754)|Substitute whoami for $USER|
|[#10755](https://github.com/NVIDIA/spark-rapids/pull/10755)|[DOC] Update README for prioritize-commits script [skip ci]|
|[#10728](https://github.com/NVIDIA/spark-rapids/pull/10728)|Let big data gen set nullability recursively|
|[#10740](https://github.com/NVIDIA/spark-rapids/pull/10740)|Use parse_url kernel for PATH parsing|
|[#10734](https://github.com/NVIDIA/spark-rapids/pull/10734)|Add short circuit path for get-json-object when there is separate wildcard path|
|[#10725](https://github.com/NVIDIA/spark-rapids/pull/10725)|Initial definition for Spark 4.0.0 shim|
|[#10635](https://github.com/NVIDIA/spark-rapids/pull/10635)|Use new getJsonObject kernel for json_tuple|
|[#10739](https://github.com/NVIDIA/spark-rapids/pull/10739)|Use fixed seed for some random failed tests|
|[#10720](https://github.com/NVIDIA/spark-rapids/pull/10720)|Add Shims for Spark 3.4.3|
|[#10716](https://github.com/NVIDIA/spark-rapids/pull/10716)|Remove the mixedType config for JSON as it has no downsides any longer|
|[#10733](https://github.com/NVIDIA/spark-rapids/pull/10733)|Fix "Could not find any rapids-4-spark jars in classpath" error when debugging UT in IDEA|
|[#10718](https://github.com/NVIDIA/spark-rapids/pull/10718)|Change parameters for memory limit in Parquet chunked reader|
|[#10292](https://github.com/NVIDIA/spark-rapids/pull/10292)|Upgrade to UCX 1.16.0|
|[#10709](https://github.com/NVIDIA/spark-rapids/pull/10709)|Removing some authorizations for departed users [skip ci]|
|[#10726](https://github.com/NVIDIA/spark-rapids/pull/10726)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#10708](https://github.com/NVIDIA/spark-rapids/pull/10708)|Updated dump tool to verify get_json_object|
|[#10706](https://github.com/NVIDIA/spark-rapids/pull/10706)|Fix auto merge conflict 10704 [skip ci]|
|[#10675](https://github.com/NVIDIA/spark-rapids/pull/10675)|Fix merge conflict with branch-24.04 [skip ci]|
|[#10678](https://github.com/NVIDIA/spark-rapids/pull/10678)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#10662](https://github.com/NVIDIA/spark-rapids/pull/10662)|Audit script - Check commits from shuffle and storage directories [skip ci]|
|[#10655](https://github.com/NVIDIA/spark-rapids/pull/10655)|Update rapids jni/private dependency to 24.06|
|[#10652](https://github.com/NVIDIA/spark-rapids/pull/10652)|Substitute murmurHash32 for spark32BitMurmurHash3|

## Release 24.04

### Features
|||
|:---|:---|
|[#10263](https://github.com/NVIDIA/spark-rapids/issues/10263)|[FEA] Add support for reading JSON containing structs where rows are not consistent|
|[#10436](https://github.com/NVIDIA/spark-rapids/issues/10436)|[FEA] Move Spark 3.5.1 out of snapshot once released|
|[#10430](https://github.com/NVIDIA/spark-rapids/issues/10430)|[FEA] Error out when running on an unsupported GPU architecture|
|[#9750](https://github.com/NVIDIA/spark-rapids/issues/9750)|[FEA] Review `JsonToStruct` and `JsonScan` and consolidate some testing and implementation|
|[#8680](https://github.com/NVIDIA/spark-rapids/issues/8680)|[AUDIT][SPARK-42779][SQL] Allow V2 writes to indicate advisory shuffle partition size|
|[#10429](https://github.com/NVIDIA/spark-rapids/issues/10429)|[FEA] Drop support for Databricks 10.4 ML LTS|
|[#10334](https://github.com/NVIDIA/spark-rapids/issues/10334)|[FEA] Turn on memory limits for parquet reader|
|[#10344](https://github.com/NVIDIA/spark-rapids/issues/10344)|[FEA] support barrier mode for mapInPandas/mapInArrow|

### Performance
|||
|:---|:---|
|[#10578](https://github.com/NVIDIA/spark-rapids/issues/10578)|[FEA] Support project expression rewrite for the case ```stringinstr(str_col, substr) > 0``` to ```contains(str_col, substr)```|
|[#10570](https://github.com/NVIDIA/spark-rapids/issues/10570)|[FEA] See if we can optimize sort for a single batch|
|[#10531](https://github.com/NVIDIA/spark-rapids/issues/10531)|[FEA] Support "WindowGroupLimit" optimization on GPU for Databricks 13.3 ML LTS+|
|[#5553](https://github.com/NVIDIA/spark-rapids/issues/5553)|[FEA][Audit] - Push down StringEndsWith/Contains to Parquet |
|[#8208](https://github.com/NVIDIA/spark-rapids/issues/8208)|[FEA][AUDIT][SPARK-37099][SQL] Introduce the group limit of Window for rank-based filter to optimize top-k computation|
|[#10249](https://github.com/NVIDIA/spark-rapids/issues/10249)|[FEA] Support common subexpression elimination for expand operator|
|[#10301](https://github.com/NVIDIA/spark-rapids/issues/10301)|[FEA] Improve performance of from_json|

### Bugs Fixed
|||
|:---|:---|
|[#10700](https://github.com/NVIDIA/spark-rapids/issues/10700)|[BUG] get_json_object cannot handle ints or boolean values|
|[#10645](https://github.com/NVIDIA/spark-rapids/issues/10645)|[BUG] java.lang.IllegalStateException: Expected to only receive a single batch|
|[#10665](https://github.com/NVIDIA/spark-rapids/issues/10665)|[BUG] Need to update private jar's version to v24.04.1 for spark-rapids v24.04.0 release|
|[#10589](https://github.com/NVIDIA/spark-rapids/issues/10589)|[BUG] ZSTD version mismatch in integration tests|
|[#10255](https://github.com/NVIDIA/spark-rapids/issues/10255)|[BUG] parquet_tests are skipped on Dataproc CI|
|[#10624](https://github.com/NVIDIA/spark-rapids/issues/10624)|[BUG] Deploy script "gpg:sign-and-deploy-file failed: 401 Unauthorized|
|[#10631](https://github.com/NVIDIA/spark-rapids/issues/10631)|[BUG] pending `BlockState` leaks blocks if the shuffle read doesn't finish successfully|
|[#10349](https://github.com/NVIDIA/spark-rapids/issues/10349)|[BUG]Test in json_test.py failed: test_from_json_struct_decimal|
|[#9033](https://github.com/NVIDIA/spark-rapids/issues/9033)|[BUG] GpuGetJsonObject does not expand escaped characters|
|[#10216](https://github.com/NVIDIA/spark-rapids/issues/10216)|[BUG] GetJsonObject fails at spark unit test $.store.book[*].reader|
|[#10217](https://github.com/NVIDIA/spark-rapids/issues/10217)|[BUG] GetJsonObject fails at spark unit test $.store.basket[0][*].b|
|[#10537](https://github.com/NVIDIA/spark-rapids/issues/10537)|[BUG] GetJsonObject throws exception when json path contains a name starting with `'`|
|[#10194](https://github.com/NVIDIA/spark-rapids/issues/10194)|[BUG] GetJsonObject does not validate the input is JSON in the same way as Spark|
|[#10196](https://github.com/NVIDIA/spark-rapids/issues/10196)|[BUG] GetJsonObject does not process escape sequences in returned strings or queries|
|[#10212](https://github.com/NVIDIA/spark-rapids/issues/10212)|[BUG] GetJsonObject should return null for invalid query instead of throwing an exception|
|[#10218](https://github.com/NVIDIA/spark-rapids/issues/10218)|[BUG] GetJsonObject does not normalize non-string output|
|[#10591](https://github.com/NVIDIA/spark-rapids/issues/10591)|[BUG] `test_column_add_after_partition` failed on EGX Standalone cluster|
|[#10277](https://github.com/NVIDIA/spark-rapids/issues/10277)|Add monitoring for GH action deprecations|
|[#10627](https://github.com/NVIDIA/spark-rapids/issues/10627)|[BUG] Integration tests FAILED on: "nvCOMP 2.3/2.4 or newer is required for Zstandard compression"|
|[#10585](https://github.com/NVIDIA/spark-rapids/issues/10585)|[BUG]Test simple pinned blocking alloc Failed nightly tests|
|[#10586](https://github.com/NVIDIA/spark-rapids/issues/10586)|[BUG]  YARN EGX IT build failing parquet_testing_test can't find file|
|[#10133](https://github.com/NVIDIA/spark-rapids/issues/10133)|[BUG] test_hash_reduction_collect_set_on_nested_array_type failed in a distributed environment|
|[#10378](https://github.com/NVIDIA/spark-rapids/issues/10378)|[BUG] `test_range_running_window_float_decimal_sum_runs_batched` fails intermittently|
|[#10486](https://github.com/NVIDIA/spark-rapids/issues/10486)|[BUG] StructsToJson does not fall back to the CPU for unsupported timeZone options|
|[#10484](https://github.com/NVIDIA/spark-rapids/issues/10484)|[BUG] JsonToStructs does not fallback when columnNameOfCorruptRecord is set|
|[#10460](https://github.com/NVIDIA/spark-rapids/issues/10460)|[BUG] JsonToStructs should reject float numbers for integer types|
|[#10468](https://github.com/NVIDIA/spark-rapids/issues/10468)|[BUG] JsonToStructs and ScanJson should not treat quoted strings as valid integers|
|[#10470](https://github.com/NVIDIA/spark-rapids/issues/10470)|[BUG] ScanJson and JsonToStructs should support parsing quoted decimal strings that are formatted by local (at least for en-US)|
|[#10494](https://github.com/NVIDIA/spark-rapids/issues/10494)|[BUG] JsonToStructs parses INF wrong when nonNumericNumbers is enabled|
|[#10456](https://github.com/NVIDIA/spark-rapids/issues/10456)|[BUG] allowNonNumericNumbers OFF supported for JSON Scan, but not JsonToStructs|
|[#10467](https://github.com/NVIDIA/spark-rapids/issues/10467)|[BUG] JsonToStructs should reject 1. as a valid number|
|[#10469](https://github.com/NVIDIA/spark-rapids/issues/10469)|[BUG] ScanJson should accept "1." as a valid Decimal|
|[#10559](https://github.com/NVIDIA/spark-rapids/issues/10559)|[BUG] test_spark_from_json_date_with_format FAILED on : Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec|
|[#10209](https://github.com/NVIDIA/spark-rapids/issues/10209)|[BUG] Test failure hash_aggregate_test.py::test_hash_reduction_collect_set_on_nested_array_type DATAGEN_SEED=1705515231|
|[#10319](https://github.com/NVIDIA/spark-rapids/issues/10319)|[BUG] Shuffled join OOM with 4GB of GPU memory|
|[#10507](https://github.com/NVIDIA/spark-rapids/issues/10507)|[BUG] regexp_test.py FAILED test_regexp_extract_all_idx_positive[DATAGEN_SEED=1709054829, INJECT_OOM]|
|[#10527](https://github.com/NVIDIA/spark-rapids/issues/10527)|[BUG] Build on Databricks failed with GpuGetJsonObject.scala:19: object parsing is not a member of package util|
|[#10509](https://github.com/NVIDIA/spark-rapids/issues/10509)|[BUG] scalar leaks when running nds query51|
|[#10214](https://github.com/NVIDIA/spark-rapids/issues/10214)|[BUG] GetJsonObject does not support unquoted array like notation|
|[#10215](https://github.com/NVIDIA/spark-rapids/issues/10215)|[BUG] GetJsonObject removes leading space characters|
|[#10213](https://github.com/NVIDIA/spark-rapids/issues/10213)|[BUG] GetJsonObject supports array index notation without a root|
|[#10452](https://github.com/NVIDIA/spark-rapids/issues/10452)|[BUG] JsonScan and from_json share fallback checks, but have hard coded names in the results|
|[#10455](https://github.com/NVIDIA/spark-rapids/issues/10455)|[BUG] JsonToStructs and ScanJson do not fall back/support it properly if single quotes are disabled|
|[#10219](https://github.com/NVIDIA/spark-rapids/issues/10219)|[BUG] GetJsonObject sees a double quote in a single quoted string as invalid|
|[#10431](https://github.com/NVIDIA/spark-rapids/issues/10431)|[BUG] test_casting_from_overflow_double_to_timestamp `DID NOT RAISE <class 'Exception'>`|
|[#10499](https://github.com/NVIDIA/spark-rapids/issues/10499)|[BUG] Unit tests core dump as below|
|[#9325](https://github.com/NVIDIA/spark-rapids/issues/9325)|[BUG] test_csv_infer_schema_timestamp_ntz fails|
|[#10422](https://github.com/NVIDIA/spark-rapids/issues/10422)|[BUG] test_get_json_object_single_quotes failure|
|[#10411](https://github.com/NVIDIA/spark-rapids/issues/10411)|[BUG] Some fast parquet tests fail if the time zone is not UTC|
|[#10410](https://github.com/NVIDIA/spark-rapids/issues/10410)|[BUG]delta_lake_update_test.py::test_delta_update_partitions[['a', 'b']-False] failed by DATAGEN_SEED=1707683137|
|[#10404](https://github.com/NVIDIA/spark-rapids/issues/10404)|[BUG] GpuJsonTuple memory leak|
|[#10382](https://github.com/NVIDIA/spark-rapids/issues/10382)|[BUG] Complile failed on branch-24.04 :  literals.scala:32: object codec is not a member of package org.apache.commons|

### PRs
|||
|:---|:---|
|[#10844](https://github.com/NVIDIA/spark-rapids/pull/10844)|Update rapids private dependency to 24.04.3|
|[#10788](https://github.com/NVIDIA/spark-rapids/pull/10788)|[DOC] Update archive page for v24.04.1 [skip ci]|
|[#10784](https://github.com/NVIDIA/spark-rapids/pull/10784)|Update latest changelog [skip ci]|
|[#10782](https://github.com/NVIDIA/spark-rapids/pull/10782)|Update latest changelog [skip ci]|
|[#10780](https://github.com/NVIDIA/spark-rapids/pull/10780)|[DOC]Update download page for v24.04.1 [skip ci]|
|[#10778](https://github.com/NVIDIA/spark-rapids/pull/10778)|Update version to 24.04.1-SNAPSHOT|
|[#10777](https://github.com/NVIDIA/spark-rapids/pull/10777)|Update rapids JNI dependency: private to 24.04.2|
|[#10683](https://github.com/NVIDIA/spark-rapids/pull/10683)|Update latest changelog [skip ci]|
|[#10681](https://github.com/NVIDIA/spark-rapids/pull/10681)|Update rapids JNI dependency to 24.04.0, private to 24.04.1|
|[#10660](https://github.com/NVIDIA/spark-rapids/pull/10660)|Ensure an executor broadcast is in a single batch|
|[#10676](https://github.com/NVIDIA/spark-rapids/pull/10676)|[DOC] Update docs for 24.04.0 release [skip ci]|
|[#10654](https://github.com/NVIDIA/spark-rapids/pull/10654)|Add a config to switch back to old impl for getJsonObject|
|[#10667](https://github.com/NVIDIA/spark-rapids/pull/10667)|Update rapids private dependency to 24.04.1|
|[#10664](https://github.com/NVIDIA/spark-rapids/pull/10664)|Remove build link from the premerge-CI workflow|
|[#10657](https://github.com/NVIDIA/spark-rapids/pull/10657)|Revert "Host Memory OOM handling for RowToColumnarIterator (#10617)"|
|[#10625](https://github.com/NVIDIA/spark-rapids/pull/10625)|Pin to 3.1.0 maven-gpg-plugin in deploy script [skip ci]|
|[#10637](https://github.com/NVIDIA/spark-rapids/pull/10637)|Cleanup async state when multi-threaded shuffle readers fail|
|[#10617](https://github.com/NVIDIA/spark-rapids/pull/10617)|Host Memory OOM handling for RowToColumnarIterator|
|[#10614](https://github.com/NVIDIA/spark-rapids/pull/10614)|Use random seed for `test_from_json_struct_decimal`|
|[#10581](https://github.com/NVIDIA/spark-rapids/pull/10581)|Use new jni kernel for getJsonObject|
|[#10630](https://github.com/NVIDIA/spark-rapids/pull/10630)|Fix removal of internal metadata information in 350 shim|
|[#10623](https://github.com/NVIDIA/spark-rapids/pull/10623)|Auto merge PRs to branch-24.06 from branch-24.04 [skip ci]|
|[#10616](https://github.com/NVIDIA/spark-rapids/pull/10616)|Pass metadata extractors to FileScanRDD|
|[#10620](https://github.com/NVIDIA/spark-rapids/pull/10620)|Remove unused shared lib in Jenkins files|
|[#10615](https://github.com/NVIDIA/spark-rapids/pull/10615)|Turn off state logging in HostAllocSuite|
|[#10610](https://github.com/NVIDIA/spark-rapids/pull/10610)|Do not replace TableCacheQueryStageExec|
|[#10599](https://github.com/NVIDIA/spark-rapids/pull/10599)|Call globStatus directly via PY4J in hdfs_glob to avoid calling hadoop command|
|[#10602](https://github.com/NVIDIA/spark-rapids/pull/10602)|Remove InMemoryTableScanExec support for Spark 3.5+|
|[#10608](https://github.com/NVIDIA/spark-rapids/pull/10608)|Update perfio.s3.enabled doc to fix build failure [skip ci]|
|[#10598](https://github.com/NVIDIA/spark-rapids/pull/10598)|Update CI script to build and deploy using the same CUDA classifier[skip ci]|
|[#10575](https://github.com/NVIDIA/spark-rapids/pull/10575)|Update JsonToStructs and ScanJson to have white space normalization|
|[#10597](https://github.com/NVIDIA/spark-rapids/pull/10597)|add guardword to hide cloud info|
|[#10540](https://github.com/NVIDIA/spark-rapids/pull/10540)|Handle minimum GPU architecture supported|
|[#10584](https://github.com/NVIDIA/spark-rapids/pull/10584)|Add in small optimization for instr comparison|
|[#10590](https://github.com/NVIDIA/spark-rapids/pull/10590)|Turn on transition logging in HostAllocSuite|
|[#10572](https://github.com/NVIDIA/spark-rapids/pull/10572)|Improve performance of Sort for the common single batch use case|
|[#10568](https://github.com/NVIDIA/spark-rapids/pull/10568)|Add configuration to share JNI pinned pool with cuIO|
|[#10550](https://github.com/NVIDIA/spark-rapids/pull/10550)|Enable window-group-limit optimization on|
|[#10542](https://github.com/NVIDIA/spark-rapids/pull/10542)|Make JSON parsing common between JsonToStructs and ScanJson|
|[#10562](https://github.com/NVIDIA/spark-rapids/pull/10562)|Fix test_spark_from_json_date_with_format when run in a non-UTC TZ|
|[#10564](https://github.com/NVIDIA/spark-rapids/pull/10564)|Enable specifying specific integration test methods via TESTS environment|
|[#10563](https://github.com/NVIDIA/spark-rapids/pull/10563)|Append new authorized user to blossom-ci safelist [skip ci]|
|[#10520](https://github.com/NVIDIA/spark-rapids/pull/10520)|Distinct left join|
|[#10538](https://github.com/NVIDIA/spark-rapids/pull/10538)|Move K8s cloud name into common lib for Jenkins CI|
|[#10552](https://github.com/NVIDIA/spark-rapids/pull/10552)|Fix issues when no value can be extracted from a regular expression|
|[#10522](https://github.com/NVIDIA/spark-rapids/pull/10522)|Fix missing scala-parser-combinators dependency on Databricks|
|[#10549](https://github.com/NVIDIA/spark-rapids/pull/10549)|Update to latest branch-24.02 [skip ci]|
|[#10544](https://github.com/NVIDIA/spark-rapids/pull/10544)|Fix merge conflict from branch-24.02|
|[#10503](https://github.com/NVIDIA/spark-rapids/pull/10503)|Distinct inner join|
|[#10512](https://github.com/NVIDIA/spark-rapids/pull/10512)|Move to parsing from_json input preserving quoted strings.|
|[#10528](https://github.com/NVIDIA/spark-rapids/pull/10528)|Fix auto merge conflict 10523|
|[#10519](https://github.com/NVIDIA/spark-rapids/pull/10519)|Replicate HostColumnVector.ColumnBuilder in plugin to enable host memory oom work|
|[#10521](https://github.com/NVIDIA/spark-rapids/pull/10521)|Fix Spark 3.5.1 build|
|[#10516](https://github.com/NVIDIA/spark-rapids/pull/10516)|One more metric for expand|
|[#10500](https://github.com/NVIDIA/spark-rapids/pull/10500)|Support "WindowGroupLimit" optimization on GPU|
|[#10508](https://github.com/NVIDIA/spark-rapids/pull/10508)|Move 351 shims into noSnapshot buildvers|
|[#10510](https://github.com/NVIDIA/spark-rapids/pull/10510)|Fix scalar leak in SumBinaryFixer|
|[#10466](https://github.com/NVIDIA/spark-rapids/pull/10466)|Use parser from spark to normalize json path in GetJsonObject|
|[#10490](https://github.com/NVIDIA/spark-rapids/pull/10490)|Start working on a more complete json test matrix json|
|[#10497](https://github.com/NVIDIA/spark-rapids/pull/10497)|Add minValue overflow check in ORC double-to-timestamp cast|
|[#10501](https://github.com/NVIDIA/spark-rapids/pull/10501)|Fix scalar leak in WindowRetrySuite|
|[#10474](https://github.com/NVIDIA/spark-rapids/pull/10474)|Remove Support for Databricks 10.4|
|[#10418](https://github.com/NVIDIA/spark-rapids/pull/10418)|Enable GpuShuffledSymmetricHashJoin by default|
|[#10450](https://github.com/NVIDIA/spark-rapids/pull/10450)|Improve internal row to columnar host memory by using a combined spillable buffer|
|[#10440](https://github.com/NVIDIA/spark-rapids/pull/10440)|Generate CSV data per Spark version for tools|
|[#10449](https://github.com/NVIDIA/spark-rapids/pull/10449)|[DOC] Fix table rendering issue in github.io download UI page [skip ci]|
|[#10438](https://github.com/NVIDIA/spark-rapids/pull/10438)|Integrate perfio.s3 reader|
|[#10423](https://github.com/NVIDIA/spark-rapids/pull/10423)|Disable Integration Test:`test_get_json_object_single_quotes`  on DB 10.4|
|[#10419](https://github.com/NVIDIA/spark-rapids/pull/10419)|Export TZ in tests when default TZ is used|
|[#10426](https://github.com/NVIDIA/spark-rapids/pull/10426)|Fix auto merge conflict 10425 [skip ci]|
|[#10427](https://github.com/NVIDIA/spark-rapids/pull/10427)|Update test doc for 24.04 [skip ci]|
|[#10396](https://github.com/NVIDIA/spark-rapids/pull/10396)|Remove inactive user from github workflow [skip ci]|
|[#10421](https://github.com/NVIDIA/spark-rapids/pull/10421)|Use withRetry when manifesting spillable batch in GpuShuffledHashJoinExec|
|[#10420](https://github.com/NVIDIA/spark-rapids/pull/10420)|Disable JsonTuple by default|
|[#10407](https://github.com/NVIDIA/spark-rapids/pull/10407)|Enable Single Quote Support in getJSONObject API with GetJsonObjectOptions|
|[#10415](https://github.com/NVIDIA/spark-rapids/pull/10415)|Avoid comparing Delta logs when writing partitioned tables|
|[#10247](https://github.com/NVIDIA/spark-rapids/pull/10247)|Improve `GpuExpand` by pre-projecting some columns|
|[#10248](https://github.com/NVIDIA/spark-rapids/pull/10248)|Group-by aggregation based optimization for UNBOUNDED `collect_set` window function|
|[#10406](https://github.com/NVIDIA/spark-rapids/pull/10406)|Enabled subPage chunking by default|
|[#10361](https://github.com/NVIDIA/spark-rapids/pull/10361)|Add in basic support for JSON generation in BigDataGen and improve performance of from_json|
|[#10158](https://github.com/NVIDIA/spark-rapids/pull/10158)|Add in framework for unbounded to unbounded window agg optimization|
|[#10394](https://github.com/NVIDIA/spark-rapids/pull/10394)|Fix auto merge conflict 10393 [skip ci]|
|[#10375](https://github.com/NVIDIA/spark-rapids/pull/10375)|Support barrier mode for mapInPandas/mapInArrow|
|[#10356](https://github.com/NVIDIA/spark-rapids/pull/10356)|Update locate_parquet_testing_files function to support hdfs input path for dataproc CI|
|[#10369](https://github.com/NVIDIA/spark-rapids/pull/10369)|Revert "Support barrier mode for mapInPandas/mapInArrow (#10364)"|
|[#10358](https://github.com/NVIDIA/spark-rapids/pull/10358)|Disable Spark UI by default for integration tests|
|[#10360](https://github.com/NVIDIA/spark-rapids/pull/10360)|Fix a memory leak in json tuple|
|[#10364](https://github.com/NVIDIA/spark-rapids/pull/10364)|Support barrier mode for mapInPandas/mapInArrow|
|[#10348](https://github.com/NVIDIA/spark-rapids/pull/10348)|Remove redundant joinOutputRows metric|
|[#10321](https://github.com/NVIDIA/spark-rapids/pull/10321)|Bump up dependency version to 24.04.0-SNAPSHOT|
|[#10330](https://github.com/NVIDIA/spark-rapids/pull/10330)|Add tryAcquire to GpuSemaphore|
|[#10258](https://github.com/NVIDIA/spark-rapids/pull/10258)|Init project version 24.04.0-SNAPSHOT|

## Release 24.02

### Features
|||
|:---|:---|
|[#9926](https://github.com/NVIDIA/spark-rapids/issues/9926)|[FEA] Add config option for the parquet reader input read limit.|
|[#10270](https://github.com/NVIDIA/spark-rapids/issues/10270)|[FEA] Add support for single quotes when reading JSON|
|[#10253](https://github.com/NVIDIA/spark-rapids/issues/10253)|[FEA] Enable mixed types as string in GpuJsonToStruct|
|[#9692](https://github.com/NVIDIA/spark-rapids/issues/9692)|[FEA] Remove Pascal support|
|[#8806](https://github.com/NVIDIA/spark-rapids/issues/8806)|[FEA] Support lazy quantifier and specified group index in regexp_extract function|
|[#10079](https://github.com/NVIDIA/spark-rapids/issues/10079)|[FEA] Add string parameter support for `unix_timestamp` for non-UTC time zones|
|[#9667](https://github.com/NVIDIA/spark-rapids/issues/9667)|[FEA][JSON] Add support for non default `dateFormat` in `from_json`|
|[#9173](https://github.com/NVIDIA/spark-rapids/issues/9173)|[FEA] Support format_number |
|[#10145](https://github.com/NVIDIA/spark-rapids/issues/10145)|[FEA] Support to_utc_timestamp|
|[#9927](https://github.com/NVIDIA/spark-rapids/issues/9927)|[FEA] Support to_date with non-UTC timezones without DST|
|[#10006](https://github.com/NVIDIA/spark-rapids/issues/10006)|[FEA] Support ```ParseToTimestamp``` for non-UTC time zones|
|[#9096](https://github.com/NVIDIA/spark-rapids/issues/9096)|[FEA] Add Spark 3.3.4 support|
|[#9585](https://github.com/NVIDIA/spark-rapids/issues/9585)|[FEA] support ascii function|
|[#9260](https://github.com/NVIDIA/spark-rapids/issues/9260)|[FEA] Create Spark 3.4.2 shim and build env|
|[#10076](https://github.com/NVIDIA/spark-rapids/issues/10076)|[FEA] Add performance test framework for non-UTC time zone features.|
|[#9881](https://github.com/NVIDIA/spark-rapids/issues/9881)|[TASK] Remove `spark.rapids.sql.nonUTC.enabled` configuration option|
|[#9801](https://github.com/NVIDIA/spark-rapids/issues/9801)|[FEA] Support DateFormat on GPU with a non-UTC timezone|
|[#6834](https://github.com/NVIDIA/spark-rapids/issues/6834)|[FEA] Support GpuHour expression for timezones other than UTC|
|[#6842](https://github.com/NVIDIA/spark-rapids/issues/6842)|[FEA] Support TimeZone aware operations for value extraction|
|[#1860](https://github.com/NVIDIA/spark-rapids/issues/1860)|[FEA] Optimize row based window operations for BOUNDED ranges|
|[#9606](https://github.com/NVIDIA/spark-rapids/issues/9606)|[FEA] Support unix_timestamp with CST(China Time Zone) support|
|[#9815](https://github.com/NVIDIA/spark-rapids/issues/9815)|[FEA] Support ```unix_timestamp``` for non-DST timezones|
|[#8807](https://github.com/NVIDIA/spark-rapids/issues/8807)|[FEA] support ‘yyyyMMdd’ format in from_unixtime function|
|[#9605](https://github.com/NVIDIA/spark-rapids/issues/9605)|[FEA] Support from_unixtime with CST(China Time Zone) support|
|[#6836](https://github.com/NVIDIA/spark-rapids/issues/6836)|[FEA] Support FromUnixTime for non UTC timezones|
|[#9175](https://github.com/NVIDIA/spark-rapids/issues/9175)|[FEA] Support Databricks 13.3|
|[#6881](https://github.com/NVIDIA/spark-rapids/issues/6881)|[FEA] Support RAPIDS Spark plugin on ARM|
|[#9274](https://github.com/NVIDIA/spark-rapids/issues/9274)|[FEA] Regular deploy process to include arm artifacts|
|[#9844](https://github.com/NVIDIA/spark-rapids/issues/9844)|[FEA] Let Gpu arrow python runners support writing one batch one time for the single threaded model.|
|[#7309](https://github.com/NVIDIA/spark-rapids/issues/7309)|[FEA] Detect multiple versions of the RAPIDS jar on the classpath at the same time|

### Performance
|||
|:---|:---|
|[#9442](https://github.com/NVIDIA/spark-rapids/issues/9442)|[FEA] For hash joins where the build side can change use the smaller table for the build side|
|[#10142](https://github.com/NVIDIA/spark-rapids/issues/10142)|[TASK] Benchmark existing timestamp functions that work in non-UTC time zone (non-DST)|

### Bugs Fixed
|||
|:---|:---|
|[#10548](https://github.com/NVIDIA/spark-rapids/issues/10548)|[BUG] test_dpp_bypass / test_dpp_via_aggregate_subquery failures in CI Databricks 13.3|
|[#10530](https://github.com/NVIDIA/spark-rapids/issues/10530)|test_delta_merge_match_delete_only java.lang.OutOfMemoryError: GC overhead limit exceeded|
|[#10464](https://github.com/NVIDIA/spark-rapids/issues/10464)|[BUG] spark334 and spark342 shims missed in scala2.13 dist jar|
|[#10473](https://github.com/NVIDIA/spark-rapids/issues/10473)|[BUG] Leak when running RANK query|
|[#10432](https://github.com/NVIDIA/spark-rapids/issues/10432)|Plug-in Build Failing for Databricks 11.3 |
|[#9974](https://github.com/NVIDIA/spark-rapids/issues/9974)|[BUG] host memory Leak in MultiFileCoalescingPartitionReaderBase in UTC time zone|
|[#10359](https://github.com/NVIDIA/spark-rapids/issues/10359)|[BUG] Build failure on Databricks nightly run with `GpuMapInPandasExecMeta`|
|[#10327](https://github.com/NVIDIA/spark-rapids/issues/10327)|[BUG] Unit test FAILED against : SPARK-24957: average with decimal followed by aggregation returning wrong result |
|[#10324](https://github.com/NVIDIA/spark-rapids/issues/10324)|[BUG] hash_aggregate_test.py test FAILED:  Type conversion is not allowed from Table {...}|
|[#10291](https://github.com/NVIDIA/spark-rapids/issues/10291)|[BUG] SIGSEGV in libucp.so|
|[#9212](https://github.com/NVIDIA/spark-rapids/issues/9212)|[BUG] `from_json` fails with cuDF error `Invalid list size computation error`|
|[#10264](https://github.com/NVIDIA/spark-rapids/issues/10264)|[BUG] hash aggregate test failures due to type conversion errors|
|[#10262](https://github.com/NVIDIA/spark-rapids/issues/10262)|[BUG] Test "SPARK-24957: average with decimal followed by aggregation returning wrong result" failed.|
|[#9353](https://github.com/NVIDIA/spark-rapids/issues/9353)|[BUG] [JSON] A mix of lists and structs within the same column is not supported|
|[#10099](https://github.com/NVIDIA/spark-rapids/issues/10099)|[BUG] orc_test.py::test_orc_scan_with_aggregate_pushdown fails with a standalone cluster on spark 3.3.0|
|[#10047](https://github.com/NVIDIA/spark-rapids/issues/10047)|[BUG] CudfException during conditional hash join while running nds query64|
|[#9779](https://github.com/NVIDIA/spark-rapids/issues/9779)|[BUG] 330cdh failed test_hash_reduction_sum_full_decimal on CI|
|[#10197](https://github.com/NVIDIA/spark-rapids/issues/10197)|[BUG] Disable GetJsonObject by default and update docs|
|[#10165](https://github.com/NVIDIA/spark-rapids/issues/10165)|[BUG] Databricks 13.3 executor side broadcast failure|
|[#10224](https://github.com/NVIDIA/spark-rapids/issues/10224)|[BUG] DBR builds fails when installing Maven|
|[#10222](https://github.com/NVIDIA/spark-rapids/issues/10222)|[BUG] to_utc_timestamp and from_utc_timestamp fallback when TZ is supported time zone|
|[#10195](https://github.com/NVIDIA/spark-rapids/issues/10195)|[BUG] test_window_aggs_for_negative_rows_partitioned failure in CI|
|[#10182](https://github.com/NVIDIA/spark-rapids/issues/10182)|[BUG] test_dpp_bypass / test_dpp_via_aggregate_subquery failures in CI (databricks)|
|[#10169](https://github.com/NVIDIA/spark-rapids/issues/10169)|[BUG] Host column vector leaks when running `test_cast_timestamp_to_date`|
|[#10050](https://github.com/NVIDIA/spark-rapids/issues/10050)|[BUG] test_cast_decimal_to_decimal[to:DecimalType(1,-1)-from:Decimal(5,-3)] fails with DATAGEN_SEED=1702439569|
|[#10088](https://github.com/NVIDIA/spark-rapids/issues/10088)|[BUG] GpuExplode single row split to fit cuDF limits|
|[#10174](https://github.com/NVIDIA/spark-rapids/issues/10174)|[BUG]  json_test.py::test_from_json_struct_timestamp failed on: Part of the plan is not columnar |
|[#10186](https://github.com/NVIDIA/spark-rapids/issues/10186)|[BUG] test_to_date_with_window_functions failed in non-UTC nightly CI|
|[#10154](https://github.com/NVIDIA/spark-rapids/issues/10154)|[BUG] 'spark-test.sh' integration tests FAILED on 'ps: command not found" in Rocky Docker environment|
|[#10175](https://github.com/NVIDIA/spark-rapids/issues/10175)|[BUG] string_test.py::test_format_number_float_special FAILED : AssertionError 'NaN' == |
|[#10166](https://github.com/NVIDIA/spark-rapids/issues/10166)|Detect Undeclared Shim in POM.xml|
|[#10170](https://github.com/NVIDIA/spark-rapids/issues/10170)|[BUG] `test_cast_timestamp_to_date` fails with `TZ=Asia/Hebron`|
|[#10149](https://github.com/NVIDIA/spark-rapids/issues/10149)|[BUG] GPU illegal access detected during delta_byte_array.parquet read|
|[#9905](https://github.com/NVIDIA/spark-rapids/issues/9905)|[BUG] GpuJsonScan incorrect behavior when parsing dates|
|[#10163](https://github.com/NVIDIA/spark-rapids/issues/10163)|Spark 3.3.4 Shim Build Failure|
|[#10105](https://github.com/NVIDIA/spark-rapids/issues/10105)|[BUG] scala:compile is not thread safe unless compiler bridge already exists |
|[#10026](https://github.com/NVIDIA/spark-rapids/issues/10026)|[BUG] test_hash_agg_with_nan_keys failed with a DATAGEN_SEED=1702335559|
|[#10075](https://github.com/NVIDIA/spark-rapids/issues/10075)|[BUG] `non-pinned blocking alloc with spill` unit test failed in HostAllocSuite|
|[#10134](https://github.com/NVIDIA/spark-rapids/issues/10134)|[BUG] test_window_aggs_for_batched_finite_row_windows_partitioned failed on Scala 2.13 with DATAGEN_SEED=1704033145|
|[#10118](https://github.com/NVIDIA/spark-rapids/issues/10118)|[BUG] non-UTC Nightly CI failed|
|[#10136](https://github.com/NVIDIA/spark-rapids/issues/10136)|[BUG] The canonicalized version of `GpuFileSourceScanExec`s that suppose to be semantic-equal can be different |
|[#10110](https://github.com/NVIDIA/spark-rapids/issues/10110)|[BUG] disable collect_list and collect_set for window operations by default.|
|[#10129](https://github.com/NVIDIA/spark-rapids/issues/10129)|[BUG] Unit test suite fails with `Null data pointer` in GpuTimeZoneDB|
|[#10089](https://github.com/NVIDIA/spark-rapids/issues/10089)|[BUG] DATAGEN_SEED=<seed> environment does not override the marker datagen_overrides|
|[#10108](https://github.com/NVIDIA/spark-rapids/issues/10108)|[BUG] @datagen_overrides seed is sticky when it shouldn't be|
|[#10064](https://github.com/NVIDIA/spark-rapids/issues/10064)|[BUG] test_unsupported_fallback_regexp_replace failed with DATAGEN_SEED=1702662063|
|[#10117](https://github.com/NVIDIA/spark-rapids/issues/10117)|[BUG] test_from_utc_timestamp failed on Cloudera Env when TZ is Iran|
|[#9914](https://github.com/NVIDIA/spark-rapids/issues/9914)|[BUG] Report GPU OOM on recent passed CI premerges.|
|[#10094](https://github.com/NVIDIA/spark-rapids/issues/10094)|[BUG] spark351 PR check failure MockTaskContext method isFailed in class TaskContext of type ()Boolean is not defined|
|[#10017](https://github.com/NVIDIA/spark-rapids/issues/10017)|[BUG] test_casting_from_double_to_timestamp failed for DATAGEN_SEED=1702329497|
|[#9992](https://github.com/NVIDIA/spark-rapids/issues/9992)|[BUG] conditionals_test.py::test_conditional_with_side_effects_cast[String] failed with DATAGEN_SEED=1701976979|
|[#9743](https://github.com/NVIDIA/spark-rapids/issues/9743)|[BUG][AUDIT] SPARK-45652 - SPJ: Handle empty input partitions after dynamic filtering|
|[#9859](https://github.com/NVIDIA/spark-rapids/issues/9859)|[AUDIT] [SPARK-45786] Inaccurate Decimal multiplication and division results|
|[#9555](https://github.com/NVIDIA/spark-rapids/issues/9555)|[BUG] Scala 2.13 build with JDK 11 or 17 fails OpcodeSuite tests|
|[#10073](https://github.com/NVIDIA/spark-rapids/issues/10073)|[BUG] test_csv_prefer_date_with_infer_schema failed with DATAGEN_SEED=1702847907|
|[#10004](https://github.com/NVIDIA/spark-rapids/issues/10004)|[BUG] If a host memory buffer is spilled, it cannot be unspilled|
|[#10063](https://github.com/NVIDIA/spark-rapids/issues/10063)|[BUG] CI build failure with 341db: method getKillReason has weaker access privileges; it should be public|
|[#10055](https://github.com/NVIDIA/spark-rapids/issues/10055)|[BUG]  array_test.py::test_array_transform_non_deterministic failed with non-UTC time zone|
|[#10056](https://github.com/NVIDIA/spark-rapids/issues/10056)|[BUG] Unit tests ToPrettyStringSuite FAILED on spark-3.5.0|
|[#10048](https://github.com/NVIDIA/spark-rapids/issues/10048)|[BUG] Fix ```out of range``` error from ```pySpark``` in ```test_timestamp_millis``` and other two integration test cases|
|[#4204](https://github.com/NVIDIA/spark-rapids/issues/4204)|casting double to string does not match Spark|
|[#9938](https://github.com/NVIDIA/spark-rapids/issues/9938)|Better to do some refactor for the Python UDF code|
|[#10018](https://github.com/NVIDIA/spark-rapids/issues/10018)|[BUG] `GpuToUnixTimestampImproved` off by 1 on GPU when handling timestamp before epoch|
|[#10012](https://github.com/NVIDIA/spark-rapids/issues/10012)|[BUG] test_str_to_map_expr_random_delimiters with DATAGEN_SEED=1702166057 hangs|
|[#10029](https://github.com/NVIDIA/spark-rapids/issues/10029)|[BUG] doc links fail with 404 for shims.md|
|[#9472](https://github.com/NVIDIA/spark-rapids/issues/9472)|[BUG] Non-Deterministic expressions in an array_transform can cause errors|
|[#9884](https://github.com/NVIDIA/spark-rapids/issues/9884)|[BUG] delta_lake_delete_test.py failed assertion [DATAGEN_SEED=1701225104, IGNORE_ORDER...|
|[#9977](https://github.com/NVIDIA/spark-rapids/issues/9977)|[BUG] test_cast_date_integral fails on databricks 3.4.1|
|[#9936](https://github.com/NVIDIA/spark-rapids/issues/9936)|[BUG] Nightly CI of non-UTC time zone reports 'year 0 is out of range' error|
|[#9941](https://github.com/NVIDIA/spark-rapids/issues/9941)|[BUG] A potential data corruption in Pandas UDFs|
|[#9897](https://github.com/NVIDIA/spark-rapids/issues/9897)|[BUG] Error message for multiple jars on classpath is wrong|
|[#9916](https://github.com/NVIDIA/spark-rapids/issues/9916)|[BUG] ```test_cast_string_ts_valid_format``` failed at ```seed = 1701362564```|
|[#9559](https://github.com/NVIDIA/spark-rapids/issues/9559)|[BUG] precommit regularly fails with error trying to download a dependency|
|[#9708](https://github.com/NVIDIA/spark-rapids/issues/9708)|[BUG] test_cast_string_ts_valid_format fails with DATAGEN_SEED=1699978422|

### PRs
|||
|:---|:---|
|[#10555](https://github.com/NVIDIA/spark-rapids/pull/10555)|Update change log [skip ci]|
|[#10551](https://github.com/NVIDIA/spark-rapids/pull/10551)|Try to make degenerative joins here impossible for these tests|
|[#10546](https://github.com/NVIDIA/spark-rapids/pull/10546)|Update changelog [skip ci]|
|[#10541](https://github.com/NVIDIA/spark-rapids/pull/10541)|Fix Delta log cache size settings during integration tests|
|[#10525](https://github.com/NVIDIA/spark-rapids/pull/10525)|Update changelog for v24.02.0 release [skip ci]|
|[#10465](https://github.com/NVIDIA/spark-rapids/pull/10465)|Add missed shims for scala2.13|
|[#10511](https://github.com/NVIDIA/spark-rapids/pull/10511)|Update rapids jni and private dependency version to 24.02.1|
|[#10513](https://github.com/NVIDIA/spark-rapids/pull/10513)|Fix scalar leak in SumBinaryFixer (#10510)|
|[#10475](https://github.com/NVIDIA/spark-rapids/pull/10475)|Fix scalar leak in RankFixer|
|[#10461](https://github.com/NVIDIA/spark-rapids/pull/10461)|Preserve tags on FileSourceScanExec|
|[#10459](https://github.com/NVIDIA/spark-rapids/pull/10459)|[DOC] Fix table rendering issue in github.io download UI page on branch-24.02 [skip ci] |
|[#10443](https://github.com/NVIDIA/spark-rapids/pull/10443)|Update change log for v24.02.0 release [skip ci]|
|[#10439](https://github.com/NVIDIA/spark-rapids/pull/10439)|Reverts NVIDIA/spark-rapids#10232 and fixes the plugin build on Databricks 11.3|
|[#10380](https://github.com/NVIDIA/spark-rapids/pull/10380)|Init changelog 24.02 [skip ci]|
|[#10367](https://github.com/NVIDIA/spark-rapids/pull/10367)|Update rapids JNI and private version to release 24.02.0|
|[#10414](https://github.com/NVIDIA/spark-rapids/pull/10414)|[DOC] Fix 24.02.0 documentation errors [skip ci]|
|[#10403](https://github.com/NVIDIA/spark-rapids/pull/10403)|Cherry-pick: Fix a memory leak in json tuple (#10360)|
|[#10387](https://github.com/NVIDIA/spark-rapids/pull/10387)|[DOC] Update docs for 24.02.0 release [skip ci]|
|[#10399](https://github.com/NVIDIA/spark-rapids/pull/10399)|Update NOTICE-binary|
|[#10389](https://github.com/NVIDIA/spark-rapids/pull/10389)|Change version and branch to 24.02 in docs [skip ci]|
|[#10384](https://github.com/NVIDIA/spark-rapids/pull/10384)|[DOC] Update docs for 23.12.2 release [skip ci] |
|[#10309](https://github.com/NVIDIA/spark-rapids/pull/10309)|[DOC] add custom 404 page and fix some document issue [skip ci]|
|[#10352](https://github.com/NVIDIA/spark-rapids/pull/10352)|xfail mixed type test|
|[#10355](https://github.com/NVIDIA/spark-rapids/pull/10355)|Revert "Support barrier mode for mapInPandas/mapInArrow (#10343)"|
|[#10353](https://github.com/NVIDIA/spark-rapids/pull/10353)|Use fixed seed for test_from_json_struct_decimal|
|[#10343](https://github.com/NVIDIA/spark-rapids/pull/10343)|Support barrier mode for mapInPandas/mapInArrow|
|[#10345](https://github.com/NVIDIA/spark-rapids/pull/10345)|Fix auto merge conflict 10339 [skip ci]|
|[#9991](https://github.com/NVIDIA/spark-rapids/pull/9991)|Start to use explicit memory limits in the parquet chunked reader|
|[#10328](https://github.com/NVIDIA/spark-rapids/pull/10328)|Fix typo in spark-tests.sh [skip ci]|
|[#10279](https://github.com/NVIDIA/spark-rapids/pull/10279)|Run '--packages' only with default cuda11 jar|
|[#10273](https://github.com/NVIDIA/spark-rapids/pull/10273)|Support reading JSON data with single quotes around attribute names and values|
|[#10306](https://github.com/NVIDIA/spark-rapids/pull/10306)|Fix performance regression in from_json|
|[#10272](https://github.com/NVIDIA/spark-rapids/pull/10272)|Add FullOuter support to GpuShuffledSymmetricHashJoinExec|
|[#10260](https://github.com/NVIDIA/spark-rapids/pull/10260)|Add perf test for time zone operators|
|[#10275](https://github.com/NVIDIA/spark-rapids/pull/10275)|Add tests for window Python udf with array input|
|[#10278](https://github.com/NVIDIA/spark-rapids/pull/10278)|Clean up $M2_CACHE to avoid side-effect of previous dependency:get [skip ci]|
|[#10268](https://github.com/NVIDIA/spark-rapids/pull/10268)|Add config to enable mixed types as string in GpuJsonToStruct & GpuJsonScan|
|[#10297](https://github.com/NVIDIA/spark-rapids/pull/10297)|Revert "UCX 1.16.0 upgrade (#10190)"|
|[#10289](https://github.com/NVIDIA/spark-rapids/pull/10289)|Add gerashegalov to CODEOWNERS [skip ci]|
|[#10290](https://github.com/NVIDIA/spark-rapids/pull/10290)|Fix merge conflict with 23.12 [skip ci]|
|[#10190](https://github.com/NVIDIA/spark-rapids/pull/10190)|UCX 1.16.0 upgrade|
|[#10211](https://github.com/NVIDIA/spark-rapids/pull/10211)|Use parse_url kernel for QUERY literal and column key|
|[#10267](https://github.com/NVIDIA/spark-rapids/pull/10267)|Update to libcudf unsigned sum aggregation types change|
|[#10208](https://github.com/NVIDIA/spark-rapids/pull/10208)|Added Support for Lazy Quantifier|
|[#9993](https://github.com/NVIDIA/spark-rapids/pull/9993)|Enable mixed types as string in GpuJsonScan|
|[#10246](https://github.com/NVIDIA/spark-rapids/pull/10246)|Refactor full join iterator to allow access to build tracker|
|[#10257](https://github.com/NVIDIA/spark-rapids/pull/10257)|Enable auto-merge from branch-24.02 to branch-24.04 [skip CI]|
|[#10178](https://github.com/NVIDIA/spark-rapids/pull/10178)|Mark hash reduction decimal overflow test as a permanent seed override|
|[#10244](https://github.com/NVIDIA/spark-rapids/pull/10244)|Use POSIX mode in assembly plugin to avoid issues with large UID/GID|
|[#10238](https://github.com/NVIDIA/spark-rapids/pull/10238)|Smoke test with '--package' to fetch the plugin jar|
|[#10201](https://github.com/NVIDIA/spark-rapids/pull/10201)|Deploy release candidates to local maven repo for dependency check[skip ci]|
|[#10240](https://github.com/NVIDIA/spark-rapids/pull/10240)|Improved inner joins with large build side|
|[#10220](https://github.com/NVIDIA/spark-rapids/pull/10220)|Disable GetJsonObject by default and add tests for as many issues with it as possible|
|[#10230](https://github.com/NVIDIA/spark-rapids/pull/10230)|Fix Databricks 13.3 BroadcastHashJoin using executor side broadcast fed by ColumnarToRow [Databricks]|
|[#10232](https://github.com/NVIDIA/spark-rapids/pull/10232)|Fixed 330db Shims to Adopt the PythonRunner Changes|
|[#10225](https://github.com/NVIDIA/spark-rapids/pull/10225)|Download Maven from apache.org archives [skip ci]|
|[#10210](https://github.com/NVIDIA/spark-rapids/pull/10210)|Add string parameter support for unix_timestamp for non-UTC time zones|
|[#10223](https://github.com/NVIDIA/spark-rapids/pull/10223)|Fix to_utc_timestamp and from_utc_timestamp fallback when TZ is supported time zone|
|[#10205](https://github.com/NVIDIA/spark-rapids/pull/10205)|Deterministic ordering in window tests|
|[#10204](https://github.com/NVIDIA/spark-rapids/pull/10204)|Further prevent degenerative joins in dpp_test|
|[#10156](https://github.com/NVIDIA/spark-rapids/pull/10156)|Update string to float compatibility doc[skip ci]|
|[#10193](https://github.com/NVIDIA/spark-rapids/pull/10193)|Fix explode with carry-along columns on GpuExplode single row retry handling|
|[#10191](https://github.com/NVIDIA/spark-rapids/pull/10191)|Updating the config documentation for filecache configs [skip ci]|
|[#10131](https://github.com/NVIDIA/spark-rapids/pull/10131)|With a single row GpuExplode tries to split the generator array|
|[#10179](https://github.com/NVIDIA/spark-rapids/pull/10179)|Fix build regression against Spark 3.2.x|
|[#10189](https://github.com/NVIDIA/spark-rapids/pull/10189)|test needs marks for non-UTC and for non_supported timezones|
|[#10176](https://github.com/NVIDIA/spark-rapids/pull/10176)|Fix format_number NaN symbol in high jdk version|
|[#10074](https://github.com/NVIDIA/spark-rapids/pull/10074)|Update the legacy mode check: only take effect when reading date/timestamp column|
|[#10167](https://github.com/NVIDIA/spark-rapids/pull/10167)|Defined Shims Should Be Declared In POM |
|[#10168](https://github.com/NVIDIA/spark-rapids/pull/10168)|Prevent a degenerative join in test_dpp_reuse_broadcast_exchange|
|[#10171](https://github.com/NVIDIA/spark-rapids/pull/10171)|Fix `test_cast_timestamp_to_date` when running in a DST time zone|
|[#9975](https://github.com/NVIDIA/spark-rapids/pull/9975)|Improve dateFormat support in GpuJsonScan and make tests consistent with GpuStructsToJson|
|[#9790](https://github.com/NVIDIA/spark-rapids/pull/9790)|Support float case of format_number with format_float kernel|
|[#10144](https://github.com/NVIDIA/spark-rapids/pull/10144)|Support to_utc_timestamp|
|[#10162](https://github.com/NVIDIA/spark-rapids/pull/10162)|Fix Spark 334 Build|
|[#10146](https://github.com/NVIDIA/spark-rapids/pull/10146)|Refactor the window code so it is not mostly kept in a few very large files|
|[#10155](https://github.com/NVIDIA/spark-rapids/pull/10155)|Install procps tools for rocky docker images [skip ci]|
|[#10153](https://github.com/NVIDIA/spark-rapids/pull/10153)|Disable multi-threaded Maven |
|[#10100](https://github.com/NVIDIA/spark-rapids/pull/10100)|Enable to_date (via gettimestamp and casting timestamp to date) for non-UTC time zones|
|[#10140](https://github.com/NVIDIA/spark-rapids/pull/10140)|Removed Unnecessary Whitespaces From Spark 3.3.4 Shim [skip ci]|
|[#10148](https://github.com/NVIDIA/spark-rapids/pull/10148)|fix test_hash_agg_with_nan_keys floating point sum failure|
|[#10150](https://github.com/NVIDIA/spark-rapids/pull/10150)|Increase timeouts in HostAllocSuite to avoid timeout failures on slow machines|
|[#10143](https://github.com/NVIDIA/spark-rapids/pull/10143)|Fix `test_window_aggs_for_batched_finite_row_windows_partitioned` fail|
|[#9887](https://github.com/NVIDIA/spark-rapids/pull/9887)|Reduce time-consuming of pre-merge|
|[#10130](https://github.com/NVIDIA/spark-rapids/pull/10130)|Change unit tests that force ooms to specify the oom type (gpu|cpu)|
|[#10138](https://github.com/NVIDIA/spark-rapids/pull/10138)|Update copyright dates in NOTICE files [skip ci]|
|[#10139](https://github.com/NVIDIA/spark-rapids/pull/10139)|Add Delta Lake 2.3.0 to list of versions to test for Spark 3.3.x|
|[#10135](https://github.com/NVIDIA/spark-rapids/pull/10135)|Fix CI: can't find script when there is pushd in script [skip ci]|
|[#10137](https://github.com/NVIDIA/spark-rapids/pull/10137)|Fix the canonicalizing for GPU file scan|
|[#10132](https://github.com/NVIDIA/spark-rapids/pull/10132)|Disable collect_list and collect_set for window by default|
|[#10084](https://github.com/NVIDIA/spark-rapids/pull/10084)|Refactor GpuJsonToStruct to reduce code duplication and manage resources more efficiently|
|[#10087](https://github.com/NVIDIA/spark-rapids/pull/10087)|Additional unit tests for GeneratedInternalRowToCudfRowIterator|
|[#10082](https://github.com/NVIDIA/spark-rapids/pull/10082)|Add Spark 3.3.4 Shim|
|[#10054](https://github.com/NVIDIA/spark-rapids/pull/10054)|Support Ascii function for ascii and latin-1|
|[#10127](https://github.com/NVIDIA/spark-rapids/pull/10127)|Fix merge conflict with branch-23.12|
|[#10097](https://github.com/NVIDIA/spark-rapids/pull/10097)|[DOC] Update docs for 23.12.1 release [skip ci]|
|[#10109](https://github.com/NVIDIA/spark-rapids/pull/10109)|Fixes a bug where datagen seed overrides were sticky and adds datagen_seed_override_disabled|
|[#10093](https://github.com/NVIDIA/spark-rapids/pull/10093)|Fix test_unsupported_fallback_regexp_replace|
|[#10119](https://github.com/NVIDIA/spark-rapids/pull/10119)|Fix from_utc_timestamp case failure on Cloudera when TZ is Iran|
|[#10106](https://github.com/NVIDIA/spark-rapids/pull/10106)|Add `isFailed()` to MockTaskContext and Remove MockTaskContextBase.scala|
|[#10112](https://github.com/NVIDIA/spark-rapids/pull/10112)|Remove datagen seed override for test_conditional_with_side_effects_cast|
|[#10104](https://github.com/NVIDIA/spark-rapids/pull/10104)|[DOC] Add in docs about memory debugging [skip ci]|
|[#9925](https://github.com/NVIDIA/spark-rapids/pull/9925)|Use threads, cache Scala compiler in GH mvn workflow|
|[#9967](https://github.com/NVIDIA/spark-rapids/pull/9967)|Added Spark-3.4.2 Shims|
|[#10061](https://github.com/NVIDIA/spark-rapids/pull/10061)|Use parse_url kernel for QUERY parsing|
|[#10101](https://github.com/NVIDIA/spark-rapids/pull/10101)|[DOC] Add column order error docs [skip ci]|
|[#10078](https://github.com/NVIDIA/spark-rapids/pull/10078)|Add perf test for non-UTC operators|
|[#10096](https://github.com/NVIDIA/spark-rapids/pull/10096)|Shim MockTaskContext to fix Spark 3.5.1 build|
|[#10092](https://github.com/NVIDIA/spark-rapids/pull/10092)|Implement Math.round using floor on GPU|
|[#10085](https://github.com/NVIDIA/spark-rapids/pull/10085)|Update tests that originally restricted the Spark timestamp range|
|[#10090](https://github.com/NVIDIA/spark-rapids/pull/10090)|Replace GPU-unsupported `\z` with an alternative RLIKE expression|
|[#10095](https://github.com/NVIDIA/spark-rapids/pull/10095)|Temporarily fix date format failed cases for non-UTC time zone.|
|[#9999](https://github.com/NVIDIA/spark-rapids/pull/9999)|Add some odd time zones for timezone transition tests|
|[#9962](https://github.com/NVIDIA/spark-rapids/pull/9962)|Add 3.5.1-SNAPSHOT Shim|
|[#10071](https://github.com/NVIDIA/spark-rapids/pull/10071)|Cleanup usage of non-utc configuration here|
|[#10057](https://github.com/NVIDIA/spark-rapids/pull/10057)|Add support for StringConcatFactory.makeConcatWithConstants (#9555)|
|[#9996](https://github.com/NVIDIA/spark-rapids/pull/9996)|Test full timestamp output range in PySpark|
|[#10081](https://github.com/NVIDIA/spark-rapids/pull/10081)|Add a fallback Cloudera Maven repo URL [skip ci]|
|[#10065](https://github.com/NVIDIA/spark-rapids/pull/10065)|Improve host memory spill interfaces|
|[#10069](https://github.com/NVIDIA/spark-rapids/pull/10069)|Revert "Support split broadcast join condition into ast and non-ast […|
|[#10070](https://github.com/NVIDIA/spark-rapids/pull/10070)|Fix 332db build failure|
|[#10060](https://github.com/NVIDIA/spark-rapids/pull/10060)|Fix failed cases for non-utc time zone|
|[#10038](https://github.com/NVIDIA/spark-rapids/pull/10038)|Remove spark.rapids.sql.nonUTC.enabled configuration option|
|[#10059](https://github.com/NVIDIA/spark-rapids/pull/10059)|Fixed Failing ToPrettyStringSuite Test for 3.5.0|
|[#10013](https://github.com/NVIDIA/spark-rapids/pull/10013)|Extended configuration of OOM injection mode|
|[#10052](https://github.com/NVIDIA/spark-rapids/pull/10052)|Set seed=0 for some integration test cases|
|[#10053](https://github.com/NVIDIA/spark-rapids/pull/10053)|Remove invalid user from CODEOWNER file [skip ci]|
|[#10049](https://github.com/NVIDIA/spark-rapids/pull/10049)|Fix out of range error from pySpark in test_timestamp_millis and other two integration test cases|
|[#9721](https://github.com/NVIDIA/spark-rapids/pull/9721)|Support date_format via Gpu for non-UTC time zone|
|[#9470](https://github.com/NVIDIA/spark-rapids/pull/9470)|Use float to string kernel|
|[#9845](https://github.com/NVIDIA/spark-rapids/pull/9845)|Use parse_url kernel for HOST parsing|
|[#10024](https://github.com/NVIDIA/spark-rapids/pull/10024)|Support hour minute second for non-UTC time zone|
|[#9973](https://github.com/NVIDIA/spark-rapids/pull/9973)|Batching support for row-based bounded window functions |
|[#10042](https://github.com/NVIDIA/spark-rapids/pull/10042)|Update tests to not have hard coded fallback when not needed|
|[#9816](https://github.com/NVIDIA/spark-rapids/pull/9816)|Support unix_timestamp and to_unix_timestamp with non-UTC timezones (non-DST)|
|[#9902](https://github.com/NVIDIA/spark-rapids/pull/9902)|Some refactor for the Python UDF code|
|[#10023](https://github.com/NVIDIA/spark-rapids/pull/10023)|GPU supports `yyyyMMdd` format by post process for the `from_unixtime` function|
|[#10033](https://github.com/NVIDIA/spark-rapids/pull/10033)|Remove GpuToTimestampImproved and spark.rapids.sql.improvedTimeOps.enabled|
|[#10016](https://github.com/NVIDIA/spark-rapids/pull/10016)|Fix infinite loop in test_str_to_map_expr_random_delimiters|
|[#9481](https://github.com/NVIDIA/spark-rapids/pull/9481)|Use parse_url kernel for PROTOCOL parsing|
|[#10030](https://github.com/NVIDIA/spark-rapids/pull/10030)|Update links in shims.md|
|[#10015](https://github.com/NVIDIA/spark-rapids/pull/10015)|Fix array_transform to not recompute the argument|
|[#10011](https://github.com/NVIDIA/spark-rapids/pull/10011)|Add cpu oom retry split handling to InternalRowToColumnarBatchIterator|
|[#10019](https://github.com/NVIDIA/spark-rapids/pull/10019)|Fix auto merge conflict 10010 [skip ci]|
|[#9760](https://github.com/NVIDIA/spark-rapids/pull/9760)|Support split broadcast join condition into ast and non-ast|
|[#9827](https://github.com/NVIDIA/spark-rapids/pull/9827)|Enable ORC timestamp and decimal predicate push down tests|
|[#10002](https://github.com/NVIDIA/spark-rapids/pull/10002)|Use Spark 3.3.3 instead of 3.3.2 for Scala 2.13 premerge builds|
|[#10000](https://github.com/NVIDIA/spark-rapids/pull/10000)|Optimize from_unixtime|
|[#10003](https://github.com/NVIDIA/spark-rapids/pull/10003)|Fix merge conflict with branch-23.12|
|[#9984](https://github.com/NVIDIA/spark-rapids/pull/9984)|Fix 340+(including DB341+) does not support casting date to integral/float|
|[#9972](https://github.com/NVIDIA/spark-rapids/pull/9972)|Fix year 0 is out of range in test_from_json_struct_timestamp |
|[#9814](https://github.com/NVIDIA/spark-rapids/pull/9814)|Support from_unixtime via Gpu for non-UTC time zone|
|[#9929](https://github.com/NVIDIA/spark-rapids/pull/9929)|Add host memory retries for GeneratedInternalRowToCudfRowIterator|
|[#9957](https://github.com/NVIDIA/spark-rapids/pull/9957)|Update cases for cast between integral and (date/time)|
|[#9959](https://github.com/NVIDIA/spark-rapids/pull/9959)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#9942](https://github.com/NVIDIA/spark-rapids/pull/9942)|Fix a potential data corruption for Pandas UDF|
|[#9922](https://github.com/NVIDIA/spark-rapids/pull/9922)|Fix `allowMultipleJars` recommend setting message|
|[#9947](https://github.com/NVIDIA/spark-rapids/pull/9947)|Fix merge conflict with branch-23.12|
|[#9908](https://github.com/NVIDIA/spark-rapids/pull/9908)|Register default allocator for host memory|
|[#9944](https://github.com/NVIDIA/spark-rapids/pull/9944)|Fix Java OOM caused by incorrect state of shouldCapture when exception occurred|
|[#9937](https://github.com/NVIDIA/spark-rapids/pull/9937)|Refactor to use CLASSIFIER instead of CUDA_CLASSIFIER [skip ci]|
|[#9904](https://github.com/NVIDIA/spark-rapids/pull/9904)|Params for build and test CI scripts on Databricks|
|[#9719](https://github.com/NVIDIA/spark-rapids/pull/9719)|Support fine grained timezone checker instead of type based|
|[#9918](https://github.com/NVIDIA/spark-rapids/pull/9918)|Prevent generation of 'year 0 is out of range' strings in IT|
|[#9852](https://github.com/NVIDIA/spark-rapids/pull/9852)|Avoid generating duplicate nan keys with MapGen(FloatGen)|
|[#9674](https://github.com/NVIDIA/spark-rapids/pull/9674)|Add cache action to speed up mvn workflow [skip ci]|
|[#9900](https://github.com/NVIDIA/spark-rapids/pull/9900)|Revert "Remove Databricks 13.3 from release 23.12  (#9890)"|
|[#9889](https://github.com/NVIDIA/spark-rapids/pull/9889)|Fix test_cast_string_ts_valid_format test|
|[#9888](https://github.com/NVIDIA/spark-rapids/pull/9888)|Update nightly build and deploy script for arm artifacts [skip ci]|
|[#9833](https://github.com/NVIDIA/spark-rapids/pull/9833)|Fix a hang for Pandas UDFs on DB 13.3|
|[#9656](https://github.com/NVIDIA/spark-rapids/pull/9656)|Update for new retry state machine JNI APIs|
|[#9654](https://github.com/NVIDIA/spark-rapids/pull/9654)|Detect multiple jars on the classpath when init plugin|
|[#9857](https://github.com/NVIDIA/spark-rapids/pull/9857)|Skip redundant steps in nightly build [skip ci]|
|[#9812](https://github.com/NVIDIA/spark-rapids/pull/9812)|Update JNI and private dep version to 24.02.0-SNAPSHOT|
|[#9716](https://github.com/NVIDIA/spark-rapids/pull/9716)|Initiate project version 24.02.0-SNAPSHOT|

