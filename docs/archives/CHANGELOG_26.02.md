# Change log
Generated on 2026-06-09

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

