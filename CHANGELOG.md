# Change log
Generated on 2026-08-11

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
|[#14868](https://github.com/NVIDIA/cudf-spark/issues/14868)|[FEA][Follow-up] Emit multiple batches from GpuProjectExec split-retry instead of concatenating|
|[#13649](https://github.com/NVIDIA/cudf-spark/issues/13649)|[FEA] BinaryType support for HostColumnarToGpu|
|[#15065](https://github.com/NVIDIA/cudf-spark/issues/15065)|[FEA] Add support for Apache Spark 4.0.3|
|[#14832](https://github.com/NVIDIA/cudf-spark/issues/14832)|[FEA] Add support for Spark 4.1.2|

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

## Release 26.06

### Features
|||
|:---|:---|
|[#13927](https://github.com/NVIDIA/cudf-spark/issues/13927)|[FEA] Support GpuMergeIntoCommand notMatchedBySourceClauses on GPU for OSS Delta|
|[#14601](https://github.com/NVIDIA/cudf-spark/issues/14601)|[FEA] Delta Lake DB-17.3: Enable Delta Lake tests in CI|
|[#14598](https://github.com/NVIDIA/cudf-spark/issues/14598)|[FEA] Delta Lake DB-17.3: Enable GPU MERGE INTO|
|[#14054](https://github.com/NVIDIA/cudf-spark/issues/14054)|[FEA] splitTargetSizeInHalfGpu should split the sequence by elements if splitting by byte size is not possible|
|[#14597](https://github.com/NVIDIA/cudf-spark/issues/14597)|[FEA] Delta Lake DB-17.3: Enable GPU DELETE + UPDATE|
|[#14600](https://github.com/NVIDIA/cudf-spark/issues/14600)|[FEA] Enable GPU-accelerated Deletion Vector (DV) reads for Delta Lake on Databricks 17.3.|
|[#14561](https://github.com/NVIDIA/cudf-spark/issues/14561)|[FEA] Support `replace( strCol, searchCol, replCol )`|
|[#14596](https://github.com/NVIDIA/cudf-spark/issues/14596)|[FEA] Delta Lake DB-17.3: Build system setup and write path|
|[#14461](https://github.com/NVIDIA/cudf-spark/issues/14461)|[FEA] Add support for Delta Lake 4.1.x|
|[#14539](https://github.com/NVIDIA/cudf-spark/issues/14539)|[FEA] Support `contains( strCol, expr )`|
|[#14613](https://github.com/NVIDIA/cudf-spark/issues/14613)|[FEA] Support binary type in higher-order functions|
|[#12550](https://github.com/NVIDIA/cudf-spark/issues/12550)|[FEA] Support `org.apache.spark.sql.catalyst.expressions.Hex`|

### Performance
|||
|:---|:---|
|[#15163](https://github.com/NVIDIA/cudf-spark/issues/15163)|Parquet Hadoop fallback readVectored uses 128 KiB copies and can regress remote scans|
|[#14283](https://github.com/NVIDIA/cudf-spark/issues/14283)|[FEA] Support join condition which has "cast to bigint"|
|[#14068](https://github.com/NVIDIA/cudf-spark/issues/14068)|[FEA] Iceberg planning overhead is larger than parquet planning.|
|[#14591](https://github.com/NVIDIA/cudf-spark/issues/14591)|[FEA] Support perf io in iceberg.|
|[#14064](https://github.com/NVIDIA/cudf-spark/issues/14064)|[FEA] Iceberg parquet reader should use file cache for parquet footers.|
|[#14063](https://github.com/NVIDIA/cudf-spark/issues/14063)|[FEA] Iceberg parquet reader should not blindly disable small combination.|

### Bugs Fixed
|||
|:---|:---|
|[#12495](https://github.com/NVIDIA/cudf-spark/issues/12495)|[BUG] java.lang.UnsupportedOperationException: Type NullType not supported|
|[#15120](https://github.com/NVIDIA/cudf-spark/issues/15120)|[BUG] Databricks 17.3 SNAPSHOT (Spark 4.0.0) integration tests fail with NoSuchMethodError CatalogTable.copy in GpuCreateDataSourceTableAsSelectCommand|
|[#14285](https://github.com/NVIDIA/cudf-spark/issues/14285)|[BUG] KudoGpuSerializer can hang during `assembleFromDeviceRawNative`|
|[#15235](https://github.com/NVIDIA/cudf-spark/issues/15235)|[BUG] cudaErrorIllegalAddress while writing into the Delta table|
|[#15183](https://github.com/NVIDIA/cudf-spark/issues/15183)|[BUG] DBR 17.3 build fails after SessionCatalog listPartitions APIs added resolvedCatalogTable|
|[#14864](https://github.com/NVIDIA/cudf-spark/issues/14864)|[BUG] test_parquet_interleaved_file_splits_partition_value_alignment fails on Dataproc Serverless: os.walk cannot see GCS-backed spark_tmp_path|
|[#14981](https://github.com/NVIDIA/cudf-spark/issues/14981)|[BUG] DBR 14.3 CPU fallback MERGE with NOT MATCHED BY SOURCE can fail with GpuUnionExec|
|[#14986](https://github.com/NVIDIA/cudf-spark/issues/14986)|[BUG] Delta DELETE on Databricks 14.3 returns num_affected_rows = -1 for metadata-only (partition/whole-table) deletes|
|[#14976](https://github.com/NVIDIA/cudf-spark/issues/14976)|[BUG] Delta DV predicate pushdown crashes when DV predicate filter remains CPU FilterExec|
|[#14949](https://github.com/NVIDIA/cudf-spark/issues/14949)|[BUG] Delta DV zero-column PERFILE scans ignore deleted rows and return incorrect counts|
|[#14944](https://github.com/NVIDIA/cudf-spark/issues/14944)|[BUG] [v26.06.0-SNAPSHOT][DB 17.3] Delta OPTIMIZE DV fallback fails with NPE in GpuOverrides.isDeltaLakeMetadataQuery|
|[#14807](https://github.com/NVIDIA/cudf-spark/issues/14807)|[BUG] Iceberg _pos is task-local on split data files, causing silent data corruption on MoR reads with positional deletes|
|[#14726](https://github.com/NVIDIA/cudf-spark/issues/14726)|[BUG] Iceberg 1.10.1 SparkWrite class loader issue when jars in $SPARK_HOME/jars|
|[#14895](https://github.com/NVIDIA/cudf-spark/issues/14895)|[BUG] DBR 17.3 Delta no-DV read fails with DELTA_SKIP_ROW_COLUMN_NOT_FILLED on GPU|
|[#14861](https://github.com/NVIDIA/cudf-spark/issues/14861)|[BUG] test_comprehensive_from_utc_timestamp fails on Databricks 14.3 for timezone SystemV/EST5EDT (1-hour GPU/CPU diff) intermittently|
|[#14813](https://github.com/NVIDIA/cudf-spark/issues/14813)|GpuJsonToStructs fails with token count assertion on multiple malformed open-brace rows|
|[#14689](https://github.com/NVIDIA/cudf-spark/issues/14689)|[BUG] Replace cuDF regex chain in GpuToTimestamp with a fused JNI kernel|
|[#14815](https://github.com/NVIDIA/cudf-spark/issues/14815)|[BUG] Dataproc Serverless 2.2 IT: string_test.py Spark job exceeded 3600s timeout, batch FAILED|
|[#14831](https://github.com/NVIDIA/cudf-spark/issues/14831)|[BUG] Parquet COALESCING reader can return invalid results from partitioned tables|
|[#11653](https://github.com/NVIDIA/cudf-spark/issues/11653)|[BUG] Spark UT framework: select explode of nested field of array of struct: Encountered an exception applying GPU overrides|
|[#14790](https://github.com/NVIDIA/cudf-spark/issues/14790)|[BUG] MetricsEventLogValidationSuite parquet write operator time ratio test fails near 10% threshold on Spark 4.1.1 / Scala 2.13|
|[#14696](https://github.com/NVIDIA/cudf-spark/issues/14696)|[BUG] Queries against Delta tables with deletion vectors may not reuse plan parts that should be reusable|
|[#14800](https://github.com/NVIDIA/cudf-spark/issues/14800)|[BUG] iceberg parquet shim class-name collision in dist jar: cache-aware 1.10.x shim silently dropped|
|[#14763](https://github.com/NVIDIA/cudf-spark/issues/14763)|[BUG] [CI] Spark Connect smoke test fails to start server on 127.0.0.1:15002 for multiple jobs|
|[#14767](https://github.com/NVIDIA/cudf-spark/issues/14767)|[BUG] GitHub mvn verify docgen check no longer validates Spark 330 generated docs|
|[#14681](https://github.com/NVIDIA/cudf-spark/issues/14681)|[BUG] test_std_variance fails with GPU nan vs CPU inf on Double data with small batchSizeBytes intermittently|
|[#14758](https://github.com/NVIDIA/cudf-spark/issues/14758)|[BUG] Unsafe close of HostColumnVectors in `GpuColumnVector::extractHostColumns()`|
|[#14765](https://github.com/NVIDIA/cudf-spark/issues/14765)|[BUG] Nightly IT matrix: delta-core 2.1.1 Ivy resolution fails on Spark 3.3.4 and Spark Connect server fails to start on Spark 3.5.8|
|[#14766](https://github.com/NVIDIA/cudf-spark/issues/14766)|[BUG] Iceberg S3Tables IT fails: ivy unresolved dependencies (iceberg/AWS SDK v2/netty) -> JAVA_GATEWAY_EXITED, no tests ran|
|[#14755](https://github.com/NVIDIA/cudf-spark/issues/14755)|[BUG] Nightly dependency-check fails: Non-resolvable parent POM for rapids-4-spark-parent SNAPSHOT|
|[#14712](https://github.com/NVIDIA/cudf-spark/issues/14712)|[BUG] spark.rapids.sql.optimizer.enabled=true throws NoClassDefFoundError: com/nvidia/spark/rapids/Optimizer|
|[#14630](https://github.com/NVIDIA/cudf-spark/issues/14630)|[BUG] Fatal cudaErrorIllegalAddress error occurred in CI test job|
|[#14701](https://github.com/NVIDIA/cudf-spark/issues/14701)|[BUG] Spark 411 unit test fails with NoSuchMethodError RowDeltaUtils.REINSERT_OPERATION in RapidsShuffleIntegrationSuite|
|[#14705](https://github.com/NVIDIA/cudf-spark/issues/14705)|[BUG] FileCache metrics is missing for iceberg.|
|[#14699](https://github.com/NVIDIA/cudf-spark/issues/14699)|[BUG] cudf_udf nightly fails: `No module named pip` in newly created conda env (python=3.12, cudf=26.06)|
|[#14567](https://github.com/NVIDIA/cudf-spark/issues/14567)|[BUG] hash_aggregate_test.py::test_hash_grpby_pivot failed java.lang.ArithmeticException: BigInteger out of long range in DB 17.3 runtime intermittently|
|[#14614](https://github.com/NVIDIA/cudf-spark/issues/14614)|[BUG] Build failure: `object sketch is not a member of package org.apache.spark.util` in GpuBloomFilterAggregate on Databricks runtimes|
|[#13816](https://github.com/NVIDIA/cudf-spark/issues/13816)|[AutoSparkUT]MakeDecimal test failed in DecimalExpressSuite from Spark UT|
|[#14532](https://github.com/NVIDIA/cudf-spark/issues/14532)|[BUG] GPU JSON reader incorrectly returns null/drops rows for non-timestamp values after isTimestamp validation change in incompatible date formats path|
|[#14581](https://github.com/NVIDIA/cudf-spark/issues/14581)|[BUG] JVM crash (SIGSEGV) in native cuDF code during RapidsDataFrameFunctionsSuite array_repeat (Spark 3.3.0, cuda12)|
|[#11416](https://github.com/NVIDIA/cudf-spark/issues/11416)|[BUG] Create parquet table with compression|
|[#14592](https://github.com/NVIDIA/cudf-spark/issues/14592)|arrays_zip crashes with 'Range is out of bounds' when input batch has 0 rows|
|[#13759](https://github.com/NVIDIA/cudf-spark/issues/13759)|[AutoSparkUT] GetTimestamp Parses Invalid Format Instead of Returning Null|
|[#14109](https://github.com/NVIDIA/cudf-spark/issues/14109)|[AutoSparkUT]"SPARK-17515: CollectLimit.execute() should perform per-partition limits" in SQLQuerySuite failed|
|[#12452](https://github.com/NVIDIA/cudf-spark/issues/12452)|[BUG] hyper_log_log_plus_plus_test.test_hllpp_precisions_groupby[0.3] failed in mismatch cpu and gpu result|
|[#14122](https://github.com/NVIDIA/cudf-spark/issues/14122)|[AutoSparkUT]"SPARK-33482: Fix FileScan canonicalization" in SQLQuerySuite failed|

### PRs
|||
|:---|:---|
|[#15300](https://github.com/NVIDIA/cudf-spark/pull/15300)|Update changelog for the v26.06.1 release [skip ci]|
|[#15299](https://github.com/NVIDIA/cudf-spark/pull/15299)|Update dependency version JNI to 26.06.1|
|[#15298](https://github.com/NVIDIA/cudf-spark/pull/15298)|[DOC] update download page for 26.06.1 release [skip ci]|
|[#15164](https://github.com/NVIDIA/cudf-spark/pull/15164)|Use configured copy buffer for Hadoop vectored reads|
|[#15228](https://github.com/NVIDIA/cudf-spark/pull/15228)|Update dependency version JNI to 26.06.1-SNAPSHOT|
|[#15211](https://github.com/NVIDIA/cudf-spark/pull/15211)|Fix DBR 17.3 build after SessionCatalog partition API change|
|[#15085](https://github.com/NVIDIA/cudf-spark/pull/15085)|Fix auto merge conflict 15081 [skip ci]|
|[#15084](https://github.com/NVIDIA/cudf-spark/pull/15084)|Update changelog for the v26.06.0 release [skip ci]|
|[#15079](https://github.com/NVIDIA/cudf-spark/pull/15079)|Update dependency version private to 26.06.1|
|[#15024](https://github.com/NVIDIA/cudf-spark/pull/15024)|Fall back when Iceberg S3 PerfIO is unsupported|
|[#15067](https://github.com/NVIDIA/cudf-spark/pull/15067)|Fix auto merge conflict 15009 [skip ci]|
|[#15066](https://github.com/NVIDIA/cudf-spark/pull/15066)|Use current repository name in GitHub workflows [skip ci]|
|[#14943](https://github.com/NVIDIA/cudf-spark/pull/14943)|Update changelog for the v26.06.0 release|
|[#14941](https://github.com/NVIDIA/cudf-spark/pull/14941)|Update dependency version JNI, private, hybrid to 26.06.0|
|[#14998](https://github.com/NVIDIA/cudf-spark/pull/14998)|Add the missing case for GpuShuffleCoalesceExec for the broadcast hash join on DBR 14.3 [Databricks]|
|[#14990](https://github.com/NVIDIA/cudf-spark/pull/14990)|[BUG] Keep DBR 14.3 local union source on CPU|
|[#14987](https://github.com/NVIDIA/cudf-spark/pull/14987)|Fix Delta DELETE num_affected_rows on DBR 13.3 and 14.3 for metadata-only deletes|
|[#14988](https://github.com/NVIDIA/cudf-spark/pull/14988)|Fix Delta DV predicate pushdown with CPU FilterExec|
|[#14952](https://github.com/NVIDIA/cudf-spark/pull/14952)|Fix Delta DV zero-column scan row counts|
|[#14945](https://github.com/NVIDIA/cudf-spark/pull/14945)|Fix Delta OPTIMIZE DV fallback NPE on DBR 17.3|
|[#14808](https://github.com/NVIDIA/cudf-spark/pull/14808)|Fix Iceberg _pos to be file-global instead of task-local on split files|
|[#14937](https://github.com/NVIDIA/cudf-spark/pull/14937)|Iceberg integration tests: trim redundant coverage matrices|
|[#14925](https://github.com/NVIDIA/cudf-spark/pull/14925)|Reduce bucketed parquet test scale|
|[#14922](https://github.com/NVIDIA/cudf-spark/pull/14922)|[DOC] update download page for 26.06 release [skip ci]|
|[#14920](https://github.com/NVIDIA/cudf-spark/pull/14920)|Add regression tests for to_timestamp bug fixes|
|[#14866](https://github.com/NVIDIA/cudf-spark/pull/14866)|Fix Iceberg package-private access after shim isolation|
|[#14903](https://github.com/NVIDIA/cudf-spark/pull/14903)|[CHERRYPICK] Shuffle bytes double count metric fix + tests to cover shuffle removal at unregister|
|[#14914](https://github.com/NVIDIA/cudf-spark/pull/14914)|Keep row transition for final AQE exchanges|
|[#14847](https://github.com/NVIDIA/cudf-spark/pull/14847)|Add DBR 17.3 Delta OPTIMIZE and auto compaction support|
|[#14904](https://github.com/NVIDIA/cudf-spark/pull/14904)|Fix for delta skip row  exception|
|[#14880](https://github.com/NVIDIA/cudf-spark/pull/14880)|[BUG FIX] Iceberg: fix Missing required field for newly-added nested MAP/LIST|
|[#14820](https://github.com/NVIDIA/cudf-spark/pull/14820)|[DeltaLake] Enable GPU Delta MERGE on DBR 17.3|
|[#14804](https://github.com/NVIDIA/cudf-spark/pull/14804)|ci: declare workflow-level `contents: read` on 4 workflows [skip ci]|
|[#14859](https://github.com/NVIDIA/cudf-spark/pull/14859)|Fall back to CPU for Iceberg partition transforms sourcing nested fields|
|[#14839](https://github.com/NVIDIA/cudf-spark/pull/14839)|[AutoSparkUT] CSV: support decimal grouping separator parsing (Locale.US)|
|[#14706](https://github.com/NVIDIA/cudf-spark/pull/14706)|Replace cuDF regex chain in GpuToTimestamp with fused JNI parser|
|[#14865](https://github.com/NVIDIA/cudf-spark/pull/14865)|Fix async output write with pipe-backed cloud streams|
|[#14850](https://github.com/NVIDIA/cudf-spark/pull/14850)|Revert "Skip GBK decode test on Dataproc Serverless (#14816)"|
|[#14851](https://github.com/NVIDIA/cudf-spark/pull/14851)|Fix concurrent writer fallback with empty caches|
|[#14845](https://github.com/NVIDIA/cudf-spark/pull/14845)|Optimize null-restore sequence in cast struct to json|
|[#14835](https://github.com/NVIDIA/cudf-spark/pull/14835)|[AutoSparkUT] Fix invalid numSplits 0 in single-column explode (#11653)|
|[#14849](https://github.com/NVIDIA/cudf-spark/pull/14849)|[AutoSparkUT] Fix regex complexity estimator overflow|
|[#14852](https://github.com/NVIDIA/cudf-spark/pull/14852)|Relax parquet write operator time lower bound|
|[#14841](https://github.com/NVIDIA/cudf-spark/pull/14841)|Fix parquet coalescing reader file grouping alignment|
|[#14843](https://github.com/NVIDIA/cudf-spark/pull/14843)|Use fused replaceNulls to compute per row repetition in explode|
|[#14842](https://github.com/NVIDIA/cudf-spark/pull/14842)|Use null-propagating stringConcatenate in cast complex-type-to-string|
|[#14684](https://github.com/NVIDIA/cudf-spark/pull/14684)|splitTargetSizeInHalfGpu by data size if not target size|
|[#14844](https://github.com/NVIDIA/cudf-spark/pull/14844)|Port Delta DV predicate pruning fix to DBR 17.3|
|[#14825](https://github.com/NVIDIA/cudf-spark/pull/14825)|TimeoutSparkListener: dump executor threads in addition to driver threads|
|[#14830](https://github.com/NVIDIA/cudf-spark/pull/14830)|Use fused mergeAndSetValidity kernel in hypot|
|[#14817](https://github.com/NVIDIA/cudf-spark/pull/14817)|Use fused replaceNulls for non-nested types in GpuNvl|
|[#14818](https://github.com/NVIDIA/cudf-spark/pull/14818)|Use fused mergeAndSetValidity kernel in mergeNulls|
|[#14819](https://github.com/NVIDIA/cudf-spark/pull/14819)|Use scalar extractListElement index instead of column in substring where length is fixed|
|[#14647](https://github.com/NVIDIA/cudf-spark/pull/14647)|Use scalar extractListElement index instead of column in regex extract|
|[#14826](https://github.com/NVIDIA/cudf-spark/pull/14826)|Move former shim sources to conventional source code roots|
|[#14810](https://github.com/NVIDIA/cudf-spark/pull/14810)|Enable Delta DELETE and UPDATE for DBR 17.3|
|[#14824](https://github.com/NVIDIA/cudf-spark/pull/14824)|[AutoSparkUT] Fix CSV maxCharsPerColumn fallback|
|[#14770](https://github.com/NVIDIA/cudf-spark/pull/14770)|[DOC] update download page for 26.04.2 hot release [skip ci]|
|[#14761](https://github.com/NVIDIA/cudf-spark/pull/14761)|Remove deletion vector predicate from dataFilters of scan|
|[#14787](https://github.com/NVIDIA/cudf-spark/pull/14787)|Enable native Delta DV reads for DBR 17.3|
|[#14793](https://github.com/NVIDIA/cudf-spark/pull/14793)|Support join condition which has cast|
|[#14809](https://github.com/NVIDIA/cudf-spark/pull/14809)|[Perf] [bugfix] Fix a Iceberg class collision between Iceberg versions to improve perf|
|[#14795](https://github.com/NVIDIA/cudf-spark/pull/14795)|Fix flaky parquet write operator time validation|
|[#14792](https://github.com/NVIDIA/cudf-spark/pull/14792)|[AutoSparkUT] Preserve non-deterministic expression values across coalesce/union (#14156)|
|[#14778](https://github.com/NVIDIA/cudf-spark/pull/14778)|[AutoSparkUT] URI-decode JSON/CSV file path in GpuTextBasedPartitionReader (#11158, #13898)|
|[#14754](https://github.com/NVIDIA/cudf-spark/pull/14754)|Add per-table session-level Iceberg scan-option overrides|
|[#14783](https://github.com/NVIDIA/cudf-spark/pull/14783)|Expose cuDF Parquet writer row group size configs|
|[#14816](https://github.com/NVIDIA/cudf-spark/pull/14816)|Skip GBK decode test on Dataproc Serverless|
|[#14814](https://github.com/NVIDIA/cudf-spark/pull/14814)|[integration tests]: extend spark connect startup wait [skip ci]|
|[#14545](https://github.com/NVIDIA/cudf-spark/pull/14545)|Support StringDecode for GBK encoding|
|[#14803](https://github.com/NVIDIA/cudf-spark/pull/14803)|Use Spark330 for generated docs [skip ci]|
|[#14652](https://github.com/NVIDIA/cudf-spark/pull/14652)|Add GPU ArrayAggregate for SUM/PRODUCT/MAX/MIN/ALL/ANY|
|[#14801](https://github.com/NVIDIA/cudf-spark/pull/14801)|[integration tests]: pass ivy settings to spark connect smoke test [skip ci]|
|[#14799](https://github.com/NVIDIA/cudf-spark/pull/14799)|[AutoSparkUT] Exclude flaky SPARK-33084 Add jar Ivy URI SQL test (#14777)|
|[#14772](https://github.com/NVIDIA/cudf-spark/pull/14772)|Avoid to_json fallback for JSON without timestamps on unsupported timezones|
|[#14796](https://github.com/NVIDIA/cudf-spark/pull/14796)|[AutoSparkUT] Re-enable parquet vectorized schema mismatch test|
|[#14660](https://github.com/NVIDIA/cudf-spark/pull/14660)|[AutoSparkUT] Add RapidsInjectRuntimeFilterSuite|
|[#14762](https://github.com/NVIDIA/cudf-spark/pull/14762)|[AutoSparkUT] Fix std variance floating overflow coverage|
|[#14789](https://github.com/NVIDIA/cudf-spark/pull/14789)|Update premerge CI m2 cache restore [skip ci]|
|[#14674](https://github.com/NVIDIA/cudf-spark/pull/14674)|optimize iceberg read|
|[#14798](https://github.com/NVIDIA/cudf-spark/pull/14798)|Fix docs [skip ci]|
|[#14791](https://github.com/NVIDIA/cudf-spark/pull/14791)|[DeltaLake] Address Delta 4.x follow-up nits and add type widening tests|
|[#14637](https://github.com/NVIDIA/cudf-spark/pull/14637)|[AutoSparkUT] Fix SPARK-39175 Cast ANSI error query context (#14123)|
|[#14779](https://github.com/NVIDIA/cudf-spark/pull/14779)|[AutoSparkUT] Reclassify #14106 (Common subexpression elimination) as WONT_FIX_ISSUE|
|[#14694](https://github.com/NVIDIA/cudf-spark/pull/14694)|[AutoSparkUT] Recover ParquetEncodingSuite v2 tests via ADJUST_UT testRapids (#13745, #13746)|
|[#14611](https://github.com/NVIDIA/cudf-spark/pull/14611)|Support Iceberg nested and binary GPU writes|
|[#14759](https://github.com/NVIDIA/cudf-spark/pull/14759)|Fix unsafe close of HostColumnVectors in `GpuColumnVector::extractHostColumns()`|
|[#14623](https://github.com/NVIDIA/cudf-spark/pull/14623)|Support `replace(col, targetExpr, replExpr)` for strings. (Include  for testing.)|
|[#14692](https://github.com/NVIDIA/cudf-spark/pull/14692)|[AutoSparkUT] Fix #14172: relax dynamicallySelectedPartitions visibility + recover SPARK-26893 subquery pushdown test|
|[#14610](https://github.com/NVIDIA/cudf-spark/pull/14610)|[AutoSparkUT] Re-enable Flatten test after cuDF fix (rapidsai/cudf#22147)|
|[#14724](https://github.com/NVIDIA/cudf-spark/pull/14724)|Add split-and-retry path to GpuProjectExec|
|[#14716](https://github.com/NVIDIA/cudf-spark/pull/14716)|Add initial Delta lake write support for Databricks-17.3|
|[#14586](https://github.com/NVIDIA/cudf-spark/pull/14586)|Optimize format number implementation|
|[#14646](https://github.com/NVIDIA/cudf-spark/pull/14646)|Support Delta Lake 4.1 on Spark 4.1|
|[#14774](https://github.com/NVIDIA/cudf-spark/pull/14774)|Use ivysettings for spark packages resolution [skip ci]|
|[#14612](https://github.com/NVIDIA/cudf-spark/pull/14612)|[AutoSparkUT] Fix null struct entry handling in GpuMapFromEntries (issue #14128)|
|[#14764](https://github.com/NVIDIA/cudf-spark/pull/14764)|Run dependency checks without Jenkins Maven settings [skip ci]|
|[#14693](https://github.com/NVIDIA/cudf-spark/pull/14693)|[AutoSparkUT] Fix GpuCast decimal-overflow error to match CPU's CheckOverflow message (#14143)|
|[#14691](https://github.com/NVIDIA/cudf-spark/pull/14691)|[AutoSparkUT] Reclassify #11434 as WONT_FIX_ISSUE: parquet non-vectorized error path is unreachable on GPU|
|[#14654](https://github.com/NVIDIA/cudf-spark/pull/14654)|[AutoSparkUT] Add RapidsDataFrameJoinSuite + RapidsBloomFilterAggregateQuerySuite|
|[#14632](https://github.com/NVIDIA/cudf-spark/pull/14632)|[AutoSparkUT] Fix SPARK-39177 map ANSI error query context (#14123)|
|[#14752](https://github.com/NVIDIA/cudf-spark/pull/14752)|Quick fix of the cannot find settings file issue [skip ci]|
|[#14702](https://github.com/NVIDIA/cudf-spark/pull/14702)|[AutoSparkUT] Fix binary host columnar copy for SPARK-33593|
|[#14688](https://github.com/NVIDIA/cudf-spark/pull/14688)|Remove non-Kudo test from integration test|
|[#14727](https://github.com/NVIDIA/cudf-spark/pull/14727)|Use mirror for internal usage to avoid 429 from maven central [skip ci]|
|[#14713](https://github.com/NVIDIA/cudf-spark/pull/14713)|Publish Optimizer trait at JAR root to fix NoClassDefFoundError|
|[#14690](https://github.com/NVIDIA/cudf-spark/pull/14690)|[AutoSparkUT] Fix GpuParquetScan schema-mismatch error message format (#11446)|
|[#14669](https://github.com/NVIDIA/cudf-spark/pull/14669)|[AutoSparkUT] Fix GpuCreateMap empty-map eval; RowToColumnar NullType (#14140, #14108)|
|[#14678](https://github.com/NVIDIA/cudf-spark/pull/14678)|Rename URM helpers and credentials to Artifactory naming in CI and build config|
|[#14538](https://github.com/NVIDIA/cudf-spark/pull/14538)|Support `contains(col, expr)` for strings|
|[#14723](https://github.com/NVIDIA/cudf-spark/pull/14723)|Fix async profiler output copy to s3 [skip ci]|
|[#14719](https://github.com/NVIDIA/cudf-spark/pull/14719)|[DOC] update download page for 26.04.1 hot release [skip ci]|
|[#14717](https://github.com/NVIDIA/cudf-spark/pull/14717)|Update POM files to include Iceberg artifact properties|
|[#14722](https://github.com/NVIDIA/cudf-spark/pull/14722)|Xfail quoted get_json_object test on Dataproc Serverless|
|[#14718](https://github.com/NVIDIA/cudf-spark/pull/14718)|Use GpuShuffleBlockResolverBase.wrapped in unregisterShuffle|
|[#14708](https://github.com/NVIDIA/cudf-spark/pull/14708)|Add file cache metrics for iceberg|
|[#14707](https://github.com/NVIDIA/cudf-spark/pull/14707)|Temporarily xfail std variance edge case|
|[#14687](https://github.com/NVIDIA/cudf-spark/pull/14687)|[CHERRY-PICK] Fix unregister/remove path for wrapped shuffle resolver|
|[#14700](https://github.com/NVIDIA/cudf-spark/pull/14700)|Explicitly install pip for cudf_udf cases [skip ci]|
|[#14645](https://github.com/NVIDIA/cudf-spark/pull/14645)|Add footer cache for iceberg|
|[#14520](https://github.com/NVIDIA/cudf-spark/pull/14520)|Reduce pom bloat for easier shim management|
|[#14639](https://github.com/NVIDIA/cudf-spark/pull/14639)|Add FLOAT/DECIMAL coverage for asinh/atanh/cbrt math functions (#14638)|
|[#14642](https://github.com/NVIDIA/cudf-spark/pull/14642)|Add DECIMAL value coverage for transform_values (#14641)|
|[#14672](https://github.com/NVIDIA/cudf-spark/pull/14672)|[AutoSparkUT] Add 7 all-pass Spark suites (batch: SelfJoin / WindowFrames / TimeWindow / SessionWindow / Stat / TypedImperativeAgg / DatasetAggregator)|
|[#14633](https://github.com/NVIDIA/cudf-spark/pull/14633)|Add FP corner-case coverage for stddev/variance aggregates (#14631)|
|[#14676](https://github.com/NVIDIA/cudf-spark/pull/14676)|Increase Databricks cluster create wait from 60 to 150 iterations [skip ci]|
|[#14640](https://github.com/NVIDIA/cudf-spark/pull/14640)|Fix buffer leak in KudoGpuTableOperator.concat under OOM|
|[#14593](https://github.com/NVIDIA/cudf-spark/pull/14593)|Allow combining of small files in iceberg parquet reader.|
|[#14636](https://github.com/NVIDIA/cudf-spark/pull/14636)|[AutoSparkUT] Add RapidsApproximatePercentileQuerySuite|
|[#14605](https://github.com/NVIDIA/cudf-spark/pull/14605)|Fix GpuArrayRemove to fallback for unsupported element types|
|[#14570](https://github.com/NVIDIA/cudf-spark/pull/14570)|Add aggregate reduction path coverage tests|
|[#14625](https://github.com/NVIDIA/cudf-spark/pull/14625)|Drop _V2 suffix from URM/Artifactory symbols [skip ci]|
|[#14580](https://github.com/NVIDIA/cudf-spark/pull/14580)|Add named accumulators to track PerfIO S3 backend usage per executor|
|[#14618](https://github.com/NVIDIA/cudf-spark/pull/14618)|Support binary type in higher-order functions|
|[#14617](https://github.com/NVIDIA/cudf-spark/pull/14617)|Fix BloomFilterAggregate buffer conversion on DB runtimes|
|[#14526](https://github.com/NVIDIA/cudf-spark/pull/14526)|[AutoSparkUT] Fix GpuMakeDecimal bitcast crash for low-precision decimals|
|[#14575](https://github.com/NVIDIA/cudf-spark/pull/14575)|Support Hex expression|
|[#14573](https://github.com/NVIDIA/cudf-spark/pull/14573)|Fix BloomFilterAggregate buffer conversion across CPU/GPU stages|
|[#14604](https://github.com/NVIDIA/cudf-spark/pull/14604)|Enable array_repeat test case|
|[#14528](https://github.com/NVIDIA/cudf-spark/pull/14528)|[AutoSparkUT] Fix legacy 2-level Parquet LIST schema evolution crash (issue #11454)|
|[#14527](https://github.com/NVIDIA/cudf-spark/pull/14527)|[AutoSparkUT] Add testRapids for parquet compression codec test (issue #11416)|
|[#14594](https://github.com/NVIDIA/cudf-spark/pull/14594)|Fix "Range is out of bounds" crash from GpuArraysZip when receiving a 0-row batch|
|[#14590](https://github.com/NVIDIA/cudf-spark/pull/14590)|Fix auto merge conflict 14589 [skip ci]|
|[#14587](https://github.com/NVIDIA/cudf-spark/pull/14587)|Append rishic3 to blossom-ci allowlist [skip ci]|
|[#14584](https://github.com/NVIDIA/cudf-spark/pull/14584)|Exclude array_repeat from auto unit tests while debugging|
|[#14458](https://github.com/NVIDIA/cudf-spark/pull/14458)|Add AI code review configuration and enhanced PR template [skip ci]|
|[#14524](https://github.com/NVIDIA/cudf-spark/pull/14524)|Add code review guidelines [skip ci]|
|[#14550](https://github.com/NVIDIA/cudf-spark/pull/14550)|Explicit check boxes for non-applicable PR checklist items [skip ci]|
|[#14552](https://github.com/NVIDIA/cudf-spark/pull/14552)|Map snapshots-repo through URM for mirror profile|
|[#14507](https://github.com/NVIDIA/cudf-spark/pull/14507)|Fix MT read memory limit defaulting to wrong size when off-heap limit is disabled|
|[#14531](https://github.com/NVIDIA/cudf-spark/pull/14531)|Append patilkishorv to authorized user to blossom-ci whitelist[skip ci]|
|[#14517](https://github.com/NVIDIA/cudf-spark/pull/14517)|[CI] Use Artifactory v2 for URM and expand Maven settings|
|[#14529](https://github.com/NVIDIA/cudf-spark/pull/14529)|Fix batched window passthrough for mixed ROWS windows|
|[#14502](https://github.com/NVIDIA/cudf-spark/pull/14502)|[AutoSparkUT] Add yyyy-MM-dd HH:mm:ss.SSS to CORRECTED_COMPATIBLE_FORMATS (issue #13759)|
|[#14392](https://github.com/NVIDIA/cudf-spark/pull/14392)|[AutoSparkUT] Fix GpuCollectLimitExec per-partition row-level limits (issue #14109)|
|[#14440](https://github.com/NVIDIA/cudf-spark/pull/14440)|[AutoSparkUT] Propagate SQL query context to GPU arithmetic overflow exceptions (issue #14123)|
|[#14500](https://github.com/NVIDIA/cudf-spark/pull/14500)|Remove deprecated GpuTimeZoneDB cache overload usage|
|[#14496](https://github.com/NVIDIA/cudf-spark/pull/14496)|Update dependency version JNI, private, hybrid to 26.06.0-SNAPSHOT|
|[#14430](https://github.com/NVIDIA/cudf-spark/pull/14430)|Enable precision 4 for HLLPP|
|[#14493](https://github.com/NVIDIA/cudf-spark/pull/14493)|Update shared actions to Node 24 for GitHub Actions Node 20 deprecation [skip ci]|
|[#14479](https://github.com/NVIDIA/cudf-spark/pull/14479)|[AutoSparkUT] Fix AM-PM timestamp parsing when hour field is missing (issue #13758)|
|[#14463](https://github.com/NVIDIA/cudf-spark/pull/14463)|Delay Parquet reader resource collection until close.|
|[#14453](https://github.com/NVIDIA/cudf-spark/pull/14453)|Add IcebergProvider$ to dist/unshimmed-from-each-spark3xx.txt to fix classloader issue.|
|[#14478](https://github.com/NVIDIA/cudf-spark/pull/14478)|Bump up version to 26.06|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
