# Change log
Generated on 2026-04-08
## Release 25.12

### Features
|||
|:---|:---|
|[#13520](https://github.com/NVIDIA/spark-rapids/issues/13520)|[FEA] DML Support for iceberg|
|[#12229](https://github.com/NVIDIA/spark-rapids/issues/12229)|[FEA] Support insert statement for iceberg table.|
|[#13419](https://github.com/NVIDIA/spark-rapids/issues/13419)|[FEA] Enable iceberg insert support by default.|
|[#13383](https://github.com/NVIDIA/spark-rapids/issues/13383)|[FEA] Add support for iceberg truncate transform.|
|[#12719](https://github.com/NVIDIA/spark-rapids/issues/12719)|[FEA] Run iceberg nds power run tests.|
|[#13548](https://github.com/NVIDIA/spark-rapids/issues/13548)|[FEA] Accelerate deletes on clustered tables|
|[#13547](https://github.com/NVIDIA/spark-rapids/issues/13547)|[FEA] Accelerate updating clustered tables|
|[#13386](https://github.com/NVIDIA/spark-rapids/issues/13386)|[FEA] Add support for iceberg day transform.|
|[#13387](https://github.com/NVIDIA/spark-rapids/issues/13387)|[FEA] Add support for iceberg hour transform.|
|[#13384](https://github.com/NVIDIA/spark-rapids/issues/13384)|[FEA] Add support for iceberg year transform.|
|[#13385](https://github.com/NVIDIA/spark-rapids/issues/13385)|[FEA] Add support for iceberg month transform.|
|[#13546](https://github.com/NVIDIA/spark-rapids/issues/13546)|[FEA] Accelerate merging clustered tables|
|[#9079](https://github.com/NVIDIA/spark-rapids/issues/9079)|[FEA] Support map_from_entries|
|[#13636](https://github.com/NVIDIA/spark-rapids/issues/13636)|[FEA] Support merge command for iceberg's merge on read mode.|
|[#13635](https://github.com/NVIDIA/spark-rapids/issues/13635)|[FEA] Support update command for iceberg's merge on read mode.|
|[#13634](https://github.com/NVIDIA/spark-rapids/issues/13634)|[FEA] Support delete command for iceberg's merge on read mode.|
|[#13705](https://github.com/NVIDIA/spark-rapids/issues/13705)|[FEA] Support GBK encoded data|
|[#13470](https://github.com/NVIDIA/spark-rapids/issues/13470)|[FEA] Support Spark 3.5.7|
|[#13468](https://github.com/NVIDIA/spark-rapids/issues/13468)|[FEA] Update rapids JNI, private and hybrid dependency version to 25.12.0-SNAPSHOT|
|[#13339](https://github.com/NVIDIA/spark-rapids/issues/13339)|[FEA] Support Delta Lake 4.0.0|
|[#13525](https://github.com/NVIDIA/spark-rapids/issues/13525)|[FEA] Support merge statement for iceberg's copy on write mode.|
|[#13524](https://github.com/NVIDIA/spark-rapids/issues/13524)|[FEA] Support Update statement for iceberg's copy on write mode.|
|[#13523](https://github.com/NVIDIA/spark-rapids/issues/13523)|[FEA] Support Delete statement for iceberg's copy on write mode.|
|[#13522](https://github.com/NVIDIA/spark-rapids/issues/13522)|[FEA] Support RTAS for iceberg.|
|[#13605](https://github.com/NVIDIA/spark-rapids/issues/13605)|[FEA] Support insert into overwrite static for iceberg.|
|[#13604](https://github.com/NVIDIA/spark-rapids/issues/13604)|[FEA] Support insert into dynamic overwrite for iceberg.|
|[#13521](https://github.com/NVIDIA/spark-rapids/issues/13521)|[FEA] Support CTAS in for iceberg.|
|[#13110](https://github.com/NVIDIA/spark-rapids/issues/13110)|[FEA] Support DeltaDynamicPartitionOverwriteCommand for deltalake 3.3.x|

### Performance
|||
|:---|:---|
|[#13266](https://github.com/NVIDIA/spark-rapids/issues/13266)|[FEA] Upgrade to UCX 1.19|
|[#13462](https://github.com/NVIDIA/spark-rapids/issues/13462)|[TASK] Add microbenchmarks for Delta|
|[#12968](https://github.com/NVIDIA/spark-rapids/issues/12968)|[FEA] Use kudo kernels in the plugin|
|[#12919](https://github.com/NVIDIA/spark-rapids/issues/12919)|[FEA] Triple buffering: traffic control by a customized thread pool|
|[#13568](https://github.com/NVIDIA/spark-rapids/issues/13568)|[TASK] Analyze Deletion Vector read performance against CPU FileSourceScanExec with Predicate Pushdowns|

### Bugs Fixed
|||
|:---|:---|
|[#14009](https://github.com/NVIDIA/spark-rapids/issues/14009)|[BUG] Delta Lake deletion vector tests failing on Spark != 3.5 due to version incompatibilities|
|[#14004](https://github.com/NVIDIA/spark-rapids/issues/14004)|[BUG] test_delta_deletion_vector_read_drop_row_group fails with https://github.com/NVIDIA/spark-rapids/pull/13980|
|[#13950](https://github.com/NVIDIA/spark-rapids/issues/13950)|[BUG] Delta parquet files are not split when they can be|
|[#13873](https://github.com/NVIDIA/spark-rapids/issues/13873)|[BUG]  `join` an empty table brings errors on the GPU engines|
|[#13917](https://github.com/NVIDIA/spark-rapids/issues/13917)|[BUG] NDS2 Delta Microbenchmark Consistent Test Failures on SPARK2A Cluster|
|[#13881](https://github.com/NVIDIA/spark-rapids/issues/13881)|[BUG] Iceberg REST catalog integration tests timeout after 5 hours|
|[#13956](https://github.com/NVIDIA/spark-rapids/issues/13956)|[BUG] iceberg AWS S3 Tables test timeout 24 hours|
|[#13940](https://github.com/NVIDIA/spark-rapids/issues/13940)|[BUG] [Iceberg] After update MoR run select collect raise column not match|
|[#13854](https://github.com/NVIDIA/spark-rapids/issues/13854)|[BUG] run_other_join_modes_tests Integration tests: 1121 broadcast join failures and timeout the CI|
|[#13885](https://github.com/NVIDIA/spark-rapids/issues/13885)|[BUG] New added iceberg cases causes nightly integration tests timeout after 12 hours|
|[#13788](https://github.com/NVIDIA/spark-rapids/issues/13788)|[BUG] Scala test 'AsyncCpuTask task priority' failed UT|
|[#13798](https://github.com/NVIDIA/spark-rapids/issues/13798)|[BUG] GpuLoreSuite test failure: GpuInsertIntoHiveTable LoRE replay produces no data (Spark 3.5.7)|
|[#13848](https://github.com/NVIDIA/spark-rapids/issues/13848)|[BUG] test_delta_merge_sql_liquid_clustering fails with Delta 4|
|[#13860](https://github.com/NVIDIA/spark-rapids/issues/13860)|[Test] ERROR: Could not find a version that satisfies the requirement pyspark-client==3.5.6  and 3.5.7|
|[#13852](https://github.com/NVIDIA/spark-rapids/issues/13852)|[BUG] Iceberg merge tests failing after Delta clustered table update support|
|[#13855](https://github.com/NVIDIA/spark-rapids/issues/13855)|[BUG] Scala test GpuInsertIntoHiveTable with LoRE dump and replay failed in CI|
|[#13718](https://github.com/NVIDIA/spark-rapids/issues/13718)|[BUG] Spark Connect smoke test uses a  pip package `pyspark[connect]` with jars|
|[#13731](https://github.com/NVIDIA/spark-rapids/issues/13731)|[BUG]  `.select()` query with `left_outer join()` brings errors when using the GPU engines|
|[#13824](https://github.com/NVIDIA/spark-rapids/issues/13824)|[BUG] ArrayIndexOutOfBounds when reading iceberg table.|
|[#13826](https://github.com/NVIDIA/spark-rapids/issues/13826)|[BUG] markdown link check fails 404 of logging_resource_adaptor|
|[#13660](https://github.com/NVIDIA/spark-rapids/issues/13660)|[BUG] dataproc serverless failed: AssertionError: Assertion should have executed on driver|
|[#13737](https://github.com/NVIDIA/spark-rapids/issues/13737)|[AutoSparkUT] conv - Base conversion function fails on GPU|
|[#13738](https://github.com/NVIDIA/spark-rapids/issues/13738)|[AutoSparkUT] log10 - Function has different output on GPU|
|[#13740](https://github.com/NVIDIA/spark-rapids/issues/13740)|[AutoSparkUT] log2 - Function has different output on GPU|
|[#13741](https://github.com/NVIDIA/spark-rapids/issues/13741)|[AutoSparkUT] binary log - GPU returns NaN instead of null for negative base|
|[#13689](https://github.com/NVIDIA/spark-rapids/issues/13689)|[BUG] Failed to authenticate in iceberg rest catalog tests|
|[#13800](https://github.com/NVIDIA/spark-rapids/issues/13800)|[BUG] test_csv_read_gbk_encoded_data fail Part of the plan is not columnar class of DB 14.3 runtime|
|[#13779](https://github.com/NVIDIA/spark-rapids/issues/13779)|[BUG] Divide-by-0 exception in Delta optimized write|
|[#13771](https://github.com/NVIDIA/spark-rapids/issues/13771)|[BUG] RapidsMathExpressionsSuite fails on Spark 330: atanh FAILED|
|[#13774](https://github.com/NVIDIA/spark-rapids/issues/13774)|[BUG] pool tasks can mistakenly be registered as dedicated|
|[#13732](https://github.com/NVIDIA/spark-rapids/issues/13732)|[BUG] Small per task leak in RmmSpark task metrics|
|[#13716](https://github.com/NVIDIA/spark-rapids/issues/13716)|[BUG] A special `select` query with the `where` expression brings inconsistency when using the CPU and GPU engines|
|[#13708](https://github.com/NVIDIA/spark-rapids/issues/13708)|[BUG] A special `select` query with the `join on` expression brings inconsistency when using the CPU and GPU engines|
|[#13690](https://github.com/NVIDIA/spark-rapids/issues/13690)|[BUG] java.io.NotSerializableException: com.nvidia.spark.rapids.RapidsConf occurred in iceberg rest catalog tests|
|[#13664](https://github.com/NVIDIA/spark-rapids/issues/13664)|[BUG] some hash_aggregate integration tests failed with kudo.serializer.mode=GPU|
|[#13651](https://github.com/NVIDIA/spark-rapids/issues/13651)|[BUG] plugin generate different output from CPU run|
|[#13694](https://github.com/NVIDIA/spark-rapids/issues/13694)|[BUG] databricks shims failed compile: not found: value StatsExprShim|
|[#13662](https://github.com/NVIDIA/spark-rapids/issues/13662)|[BUG] GpuRetryOOM in KudoGpuSerializer splitAndSerializeToDevice|
|[#13661](https://github.com/NVIDIA/spark-rapids/issues/13661)|[BUG] Multiple NDS queries failed due to unmatched results on Dataproc|
|[#13612](https://github.com/NVIDIA/spark-rapids/issues/13612)|[BUG] Kudo CPU serializer overflows with columns in a customer join query|
|[#13641](https://github.com/NVIDIA/spark-rapids/issues/13641)|[BUG]  test_delta_overwrite_dynamic_missing_clauses fails with "RapidsDeltaWrite is not found in any captured plan"|
|[#13648](https://github.com/NVIDIA/spark-rapids/issues/13648)|[BUG] delta_lake_merge_test:test_delta_merge_match_delete_only failed mismatch cpu and gpu outputs|
|[#13652](https://github.com/NVIDIA/spark-rapids/issues/13652)|[BUG] mortgate_test and dpp_test failure in CI|
|[#12246](https://github.com/NVIDIA/spark-rapids/issues/12246)|[BUG] The plugin can use a different GPU to validate the GPU architecture when multiple GPUs are equipped|
|[#13659](https://github.com/NVIDIA/spark-rapids/issues/13659)|[BUG] validateGpuArchitecture sometimes looks at the wrong GPU|
|[#13647](https://github.com/NVIDIA/spark-rapids/issues/13647)|[BUG] AssertUtils is not a member of package com.nvidia.spark.rapids|
|[#13623](https://github.com/NVIDIA/spark-rapids/issues/13623)|[BUG] Iceberg s3table cases failed multiple different errors|
|[#13059](https://github.com/NVIDIA/spark-rapids/issues/13059)|[BUG] Delta Lake writes are not being checked for CPU fallback in integration tests|
|[#13607](https://github.com/NVIDIA/spark-rapids/issues/13607)|[BUG] DB builds are failing in premerge.|
|[#13583](https://github.com/NVIDIA/spark-rapids/issues/13583)|[BUG] shuffle dockerfile build failed: UCX has no cuda13 release for rocky/centos|
|[#12891](https://github.com/NVIDIA/spark-rapids/issues/12891)|[BUG] NullPointerException at ai.rapids.cudf.HostColumnVectorCore.getByte|

### PRs
|||
|:---|:---|
|[#14077](https://github.com/NVIDIA/spark-rapids/pull/14077)|[DOC] Update document issue [skip ci]|
|[#14028](https://github.com/NVIDIA/spark-rapids/pull/14028)|[DOC] update cuda13 related jars in download doc [skip ci]|
|[#14045](https://github.com/NVIDIA/spark-rapids/pull/14045)|Update version to 25.12.1-SNAPSHOT|
|[#13968](https://github.com/NVIDIA/spark-rapids/pull/13968)|Update changelog for the v25.12 release [skip ci]|
|[#14011](https://github.com/NVIDIA/spark-rapids/pull/14011)|[DOC] update for download page 2512 release [skip ci]|
|[#14013](https://github.com/NVIDIA/spark-rapids/pull/14013)|Fixes various issues in 25.12 release branch|
|[#13988](https://github.com/NVIDIA/spark-rapids/pull/13988)|Update dependency version JNI, private, hybrid to 25.12.0|
|[#13980](https://github.com/NVIDIA/spark-rapids/pull/13980)|Delta table scan should be optimized when deletion vectors don't exist|
|[#14002](https://github.com/NVIDIA/spark-rapids/pull/14002)|Skip test_delta_filter_out_metadata_col on|
|[#13991](https://github.com/NVIDIA/spark-rapids/pull/13991)|Drop _tmp_metadata_row_index column from the output of Delta Scan on GPU|
|[#13903](https://github.com/NVIDIA/spark-rapids/pull/13903)|Fix join bug on csv datasources|
|[#13971](https://github.com/NVIDIA/spark-rapids/pull/13971)|Fix a special case in limit where it could return an empty batch with the wrong number of columns (#13938)|
|[#13962](https://github.com/NVIDIA/spark-rapids/pull/13962)|Cut iceberg test cases for remote catalogs.|
|[#13941](https://github.com/NVIDIA/spark-rapids/pull/13941)|Fix Iceberg column not match error when select count after cor update|
|[#13942](https://github.com/NVIDIA/spark-rapids/pull/13942)|Add null safety guards to the conversion methods in FromIcebergShaded|
|[#13926](https://github.com/NVIDIA/spark-rapids/pull/13926)|Fix columnar mismatch bug in iceberg dml when aqe enabled.|
|[#13929](https://github.com/NVIDIA/spark-rapids/pull/13929)|Fix the ordering of join gather maps, and make it harder to break|
|[#13922](https://github.com/NVIDIA/spark-rapids/pull/13922)|Remove low priority iceberg it test cases to save test time [skip ci]|
|[#13919](https://github.com/NVIDIA/spark-rapids/pull/13919)|fix race condition due to premature disk handle exposure|
|[#13891](https://github.com/NVIDIA/spark-rapids/pull/13891)|Enable iceberg write by default, and disable dml iceberg operation on mor table by default.|
|[#13910](https://github.com/NVIDIA/spark-rapids/pull/13910)|Fix the unstable unit test case: ResourceBoundedExecutorSuite|
|[#13797](https://github.com/NVIDIA/spark-rapids/pull/13797)|Supports Iceberg truncate transform|
|[#13886](https://github.com/NVIDIA/spark-rapids/pull/13886)|[WAR] Remove ICEBERG_ONLY tests from default mode [skip ci]|
|[#13872](https://github.com/NVIDIA/spark-rapids/pull/13872)|[Backport to 25.12] add missing spark 357 to GpuWriteFilesUnsupportedVersions|
|[#13849](https://github.com/NVIDIA/spark-rapids/pull/13849)|Fix test_delta_merge_sql_liquid_clustering for Delta 4|
|[#13867](https://github.com/NVIDIA/spark-rapids/pull/13867)|Use pyspark-client only for Spark 4.x [skip ci]|
|[#13864](https://github.com/NVIDIA/spark-rapids/pull/13864)|[WAR] Move run_other_join_modes_tests out of DEFAULT mode [skip ci]|
|[#13859](https://github.com/NVIDIA/spark-rapids/pull/13859)|Delete support for clustered tables|
|[#13856](https://github.com/NVIDIA/spark-rapids/pull/13856)|Fix test cases: Iceberg datetime transforms|
|[#13778](https://github.com/NVIDIA/spark-rapids/pull/13778)|Use pyspark-client for the Spark Connect smoke test|
|[#13822](https://github.com/NVIDIA/spark-rapids/pull/13822)|Support update for Delta clustered tables|
|[#13726](https://github.com/NVIDIA/spark-rapids/pull/13726)|Supports year/month/day/hour partition transforms for Iceberg|
|[#13830](https://github.com/NVIDIA/spark-rapids/pull/13830)|Fix ArrayIndexOutOfBounds in iceberg reader.|
|[#13719](https://github.com/NVIDIA/spark-rapids/pull/13719)|Upgrade ucx to 1.19.1-rc2|
|[#13814](https://github.com/NVIDIA/spark-rapids/pull/13814)|Support merging clustered tables for Delta IO|
|[#13717](https://github.com/NVIDIA/spark-rapids/pull/13717)|Add different join strategies, join logging, and heuristic confs|
|[#13806](https://github.com/NVIDIA/spark-rapids/pull/13806)|Support GpuMapFromEntries to prevent fallback|
|[#13755](https://github.com/NVIDIA/spark-rapids/pull/13755)|Add update/merge command support for iceberg's merge on read mode.|
|[#13827](https://github.com/NVIDIA/spark-rapids/pull/13827)|Fix broken link to rmm logging_resource_adaptor source code [skip ci]|
|[#13818](https://github.com/NVIDIA/spark-rapids/pull/13818)|Fixes an issue were we were not properly checking log params and nullable output|
|[#13803](https://github.com/NVIDIA/spark-rapids/pull/13803)|Fix a GBK test failure on DB 143+|
|[#13725](https://github.com/NVIDIA/spark-rapids/pull/13725)|Add delete support for iceberg's merge on read mode.|
|[#13792](https://github.com/NVIDIA/spark-rapids/pull/13792)|Add doc for RANDOM_SELECT and oom_injection_mode in integration tests readme [skip ci]|
|[#13780](https://github.com/NVIDIA/spark-rapids/pull/13780)|Add missing handling for the 0 input partition count in computing partition count in GpuOptimizeWriteExchangeExec|
|[#13742](https://github.com/NVIDIA/spark-rapids/pull/13742)|Add Spark 3.5.7 support|
|[#13773](https://github.com/NVIDIA/spark-rapids/pull/13773)|Add an option to run a random subset of integration tests|
|[#13786](https://github.com/NVIDIA/spark-rapids/pull/13786)|Return true when comparing 2 variables if both  isNaN in Unit Test|
|[#13761](https://github.com/NVIDIA/spark-rapids/pull/13761)|[AutoSparkUT] Migrate DataFramePivotSuite tests to RAPIDS|
|[#13775](https://github.com/NVIDIA/spark-rapids/pull/13775)|Ensure that task threads are the only ones registered as dedicated|
|[#13733](https://github.com/NVIDIA/spark-rapids/pull/13733)|Calls removeTaskMetrics to remove task metrics in RmmSpark after task completion|
|[#13688](https://github.com/NVIDIA/spark-rapids/pull/13688)|Use new API to do Iceberg partition.|
|[#13768](https://github.com/NVIDIA/spark-rapids/pull/13768)|Refactor RapidsTestSettings imports to one suite per line|
|[#13753](https://github.com/NVIDIA/spark-rapids/pull/13753)|[AutoSparkUT] Add RapidsParquetEncodingSuite migration|
|[#13748](https://github.com/NVIDIA/spark-rapids/pull/13748)|[AutoSparkUT]Enable Spark UT RapidsMathExpressionsSuite, RapidsMiscFunctionsSuite on 330|
|[#13735](https://github.com/NVIDIA/spark-rapids/pull/13735)|Align with CPU behavior for the null handling in GpuInSet|
|[#13656](https://github.com/NVIDIA/spark-rapids/pull/13656)|Add op time metric to Hybrid Scan|
|[#13721](https://github.com/NVIDIA/spark-rapids/pull/13721)|Enable Delta Lake 4.0.x integration tests in nightly CI/CD jobs|
|[#13710](https://github.com/NVIDIA/spark-rapids/pull/13710)|Enable Delta Lake 4.0.x support for Spark 4.0.1|
|[#13489](https://github.com/NVIDIA/spark-rapids/pull/13489)|Support kudo GPU shuffle reads in the plugin|
|[#13711](https://github.com/NVIDIA/spark-rapids/pull/13711)|define usesKudoGPUSlicing override got GpuSinglePartitioning|
|[#13691](https://github.com/NVIDIA/spark-rapids/pull/13691)|Support the null-aware anti join in GPU broadcast hash join|
|[#13413](https://github.com/NVIDIA/spark-rapids/pull/13413)|Fix core dump in MemoryCleaner|
|[#13707](https://github.com/NVIDIA/spark-rapids/pull/13707)|Refactor DeltaDynamicPartitionOverwriteCommand and enable Delta 4.0 integration tests|
|[#13701](https://github.com/NVIDIA/spark-rapids/pull/13701)|Add StatsExprShim in databricks shim|
|[#13700](https://github.com/NVIDIA/spark-rapids/pull/13700)|Support ChangeLog for the new branch model [skip ci]|
|[#13695](https://github.com/NVIDIA/spark-rapids/pull/13695)|[DOC] update the download doc [skip ci]|
|[#13692](https://github.com/NVIDIA/spark-rapids/pull/13692)|Add missing javadocs in delta-33x-40x refactored code [skip ci]|
|[#13666](https://github.com/NVIDIA/spark-rapids/pull/13666)|Add common infrastructure and Spark 4.0 compatibility for Delta Lake 4.0.0 support|
|[#13663](https://github.com/NVIDIA/spark-rapids/pull/13663)|Add withRetry for gpuSplitAndSerialize in GPU kudo|
|[#13643](https://github.com/NVIDIA/spark-rapids/pull/13643)|Add allow_non_gpu_delta_write_if to conditionally allow CPU fallback for delta writes|
|[#13130](https://github.com/NVIDIA/spark-rapids/pull/13130)|Resource-bounded MultiFileCloudPartitionReader|
|[#13657](https://github.com/NVIDIA/spark-rapids/pull/13657)|Add support for update iceberg table with copy on write mode.|
|[#13670](https://github.com/NVIDIA/spark-rapids/pull/13670)|Revert "Temporarily disable test_dpp_reuse_broadcast_exchange and mor…|
|[#13639](https://github.com/NVIDIA/spark-rapids/pull/13639)|Support delete command for iceberg's copy on write mode|
|[#13658](https://github.com/NVIDIA/spark-rapids/pull/13658)|move validateGpuArch check to after initializeGpuAndMemory|
|[#13644](https://github.com/NVIDIA/spark-rapids/pull/13644)|Add FileFormat, Catalog, Provider, and RowIndexFilters base classes and refactor delta-33x|
|[#13654](https://github.com/NVIDIA/spark-rapids/pull/13654)|Move AssertUtils to the api submodule|
|[#13655](https://github.com/NVIDIA/spark-rapids/pull/13655)|Temporarily disable test_dpp_reuse_broadcast_exchange and mortgage_test given cuDF issue|
|[#13650](https://github.com/NVIDIA/spark-rapids/pull/13650)|Mitigate mamba hang on Rocky linux [skip ci]|
|[#13642](https://github.com/NVIDIA/spark-rapids/pull/13642)| Add Create/Optimize table command base classes and refactor delta-33x|
|[#13584](https://github.com/NVIDIA/spark-rapids/pull/13584)|Use assertInTests for cases with potential side effects, or expensive calls|
|[#13637](https://github.com/NVIDIA/spark-rapids/pull/13637)|Update automerge pattern to release/* [skip ci]|
|[#13619](https://github.com/NVIDIA/spark-rapids/pull/13619)|Add replace table as select for iceberg.|
|[#13614](https://github.com/NVIDIA/spark-rapids/pull/13614)|Migrate most remaining NvtxRanges to use NvtxId/NvtxRangeWithDoc|
|[#13626](https://github.com/NVIDIA/spark-rapids/pull/13626)|Add Update/Merge command base classes and  refactor delta-33x|
|[#13620](https://github.com/NVIDIA/spark-rapids/pull/13620)|Add shims layer, Delete command base classes and refactor delta-33x Delete|
|[#13586](https://github.com/NVIDIA/spark-rapids/pull/13586)|Assert RapidsDeltaWrite in integration tests|
|[#13613](https://github.com/NVIDIA/spark-rapids/pull/13613)|Add Delta Lake 4.0.x module skeleton and build infrastructure|
|[#13611](https://github.com/NVIDIA/spark-rapids/pull/13611)|Add insert static overwrite support for iceberg.|
|[#13606](https://github.com/NVIDIA/spark-rapids/pull/13606)|Add insert overwrite dynamic support for iceberg.|
|[#13595](https://github.com/NVIDIA/spark-rapids/pull/13595)|Add create table as select support for iceberg.|
|[#13573](https://github.com/NVIDIA/spark-rapids/pull/13573)|Accelerate DeltaDynamicPartitionOverwriteCommand|
|[#13608](https://github.com/NVIDIA/spark-rapids/pull/13608)|fix db GpuBroadcastHashJoinExec CollectTimeIterator args|
|[#13594](https://github.com/NVIDIA/spark-rapids/pull/13594)|Use Cuda 11 ucx build for rocky dockerfiles|
|[#12900](https://github.com/NVIDIA/spark-rapids/pull/12900)|Migrate CollectTimeIterator to use new NvtxIdWithMetrics class|
|[#13599](https://github.com/NVIDIA/spark-rapids/pull/13599)|Fix auto merge conflict 13598 [skip ci]|
|[#13501](https://github.com/NVIDIA/spark-rapids/pull/13501)|Use `RapidsFileIO` for writing data.|
|[#13572](https://github.com/NVIDIA/spark-rapids/pull/13572)|Upgrade UCX to 1.19-rc1|
|[#13544](https://github.com/NVIDIA/spark-rapids/pull/13544)|GpuBubbleTime: A new metric recording GPU underutilization wall time|
|[#13500](https://github.com/NVIDIA/spark-rapids/pull/13500)|Add Spark Connect smoke test in nightly integration tests|
|[#13399](https://github.com/NVIDIA/spark-rapids/pull/13399)|Add two metrics for the sized join|
|[#13454](https://github.com/NVIDIA/spark-rapids/pull/13454)|log if maxMem is called twice in a task|
|[#13475](https://github.com/NVIDIA/spark-rapids/pull/13475)|Update dependency version JNI, private, hybrid to 25.12.0-SNAPSHOT|

## Release 25.10

### Features
|||
|:---|:---|
|[#12821](https://github.com/NVIDIA/spark-rapids/issues/12821)|[FEA] Zero Conf - set spark.rapids.memory.gpu.allocSize based on memory type|
|[#13503](https://github.com/NVIDIA/spark-rapids/issues/13503)|[FEA] Add integration test case to verify that falling back to cpu when insert into partitioned iceberg table with unsupported partition.|
|[#13400](https://github.com/NVIDIA/spark-rapids/issues/13400)|[FEA] Add gpu iceberg rolling file writer|
|[#13380](https://github.com/NVIDIA/spark-rapids/issues/13380)|[FEA] Add iceberg append data integration tests.|
|[#12575](https://github.com/NVIDIA/spark-rapids/issues/12575)|[FEA][DV]  Support Parquet+DV Scan using PERFILE GpuParquetReader|
|[#13418](https://github.com/NVIDIA/spark-rapids/issues/13418)|[FEA] Support Spark 4.0.1|
|[#13378](https://github.com/NVIDIA/spark-rapids/issues/13378)|[FEA] Add support for iceberg data writer factory.|
|[#13440](https://github.com/NVIDIA/spark-rapids/issues/13440)|[FEA] Use `RapidsFileIO` interface from `spark-rapids-jni`|
|[#13376](https://github.com/NVIDIA/spark-rapids/issues/13376)|[FEA] Add support for iceberg spark write|
|[#13394](https://github.com/NVIDIA/spark-rapids/issues/13394)|[FEA] Accelerate optimizing clustered tables|
|[#13377](https://github.com/NVIDIA/spark-rapids/issues/13377)|[FEA] Add support for iceberg partitioner|
|[#12553](https://github.com/NVIDIA/spark-rapids/issues/12553)|[FEA][Liquid] Accelerate writes to Clustered Tables|
|[#12882](https://github.com/NVIDIA/spark-rapids/issues/12882)|[FEA] It would be nice if we support Asia/Shanghai timezone for orc file scan|
|[#13379](https://github.com/NVIDIA/spark-rapids/issues/13379)|[FEA] Add support for iceberg parquet file writer.|
|[#13403](https://github.com/NVIDIA/spark-rapids/issues/13403)|[FEA] Add DV read support without support for combining files|
|[#12982](https://github.com/NVIDIA/spark-rapids/issues/12982)|[FEA] Support uuid|
|[#12552](https://github.com/NVIDIA/spark-rapids/issues/12552)|[FEA] [Liquid] Accelerate reads from Clustered Tables|
|[#13301](https://github.com/NVIDIA/spark-rapids/issues/13301)|[FEA] Setup integration tests against rest catalog for iceberg.|
|[#11193](https://github.com/NVIDIA/spark-rapids/issues/11193)|[FEA] Support "Fail on error" for `parse_url`|
|[#13012](https://github.com/NVIDIA/spark-rapids/issues/13012)|[FEA] Optimize command support for Delta IO|
|[#7008](https://github.com/NVIDIA/spark-rapids/issues/7008)|[FEA] Validate ORC deflate (zlib) compression|

### Performance
|||
|:---|:---|
|[#13311](https://github.com/NVIDIA/spark-rapids/issues/13311)|[FEA] Explore alternative way to calculate M2 for stddev* and variance*|
|[#13321](https://github.com/NVIDIA/spark-rapids/issues/13321)|Run NDS-H (TPC-H) SF100 locally on Delta 3.3.x|
|[#12471](https://github.com/NVIDIA/spark-rapids/issues/12471)|[FEA] Zero Config - Figure out a good default for CPU memory limits|

### Bugs Fixed
|||
|:---|:---|
|[#13478](https://github.com/NVIDIA/spark-rapids/issues/13478)|[BUG] databricks test to download from archive.apache.org consume too much time|
|[#13628](https://github.com/NVIDIA/spark-rapids/issues/13628)|[BUG] spark.rapids.memory.host.offHeapLimit doesn't play well with spark.memory.offHeap.size in Databricks|
|[#13578](https://github.com/NVIDIA/spark-rapids/issues/13578)|[BUG] Query failure in InMemoryTableScan with AQE enabled for aggregation|
|[#13554](https://github.com/NVIDIA/spark-rapids/issues/13554)|[BUG] iceberg-rest-catalog CI failed: Missing "org.apache.iceberg.rest.RESTCatalog"|
|[#13312](https://github.com/NVIDIA/spark-rapids/issues/13312)|[BUG] zip_with test failures|
|[#13513](https://github.com/NVIDIA/spark-rapids/issues/13513)|[BUG] Data mismatch when running a query with a filter against Delta table with DV|
|[#13562](https://github.com/NVIDIA/spark-rapids/issues/13562)|[BUG] cuda13 spark-rapids fails executors when profiler is enabled|
|[#12796](https://github.com/NVIDIA/spark-rapids/issues/12796)|[BUG] Delta CDF tests potentially use an incorrect read option|
|[#13519](https://github.com/NVIDIA/spark-rapids/issues/13519)|[BUG] NDS run failed java.lang.NoClassDefFoundError: org/apache/spark/sql/delta/DeltaParquetFileFormat|
|[#13553](https://github.com/NVIDIA/spark-rapids/issues/13553)|[BUG] iceberg_append_test failures due to IllegalArgumentException in pre_release|
|[#13542](https://github.com/NVIDIA/spark-rapids/issues/13542)|[BUG] new op time collection has huge overhead when op fallback happens|
|[#13535](https://github.com/NVIDIA/spark-rapids/issues/13535)|[BUG] iceberg_append_test failed Part of the plan is not columnar class org.apache.spark.sql.execution.SortExec|
|[#13531](https://github.com/NVIDIA/spark-rapids/issues/13531)|[BUG] IncrementMetric expression causes ColumnarToRow fallback|
|[#13514](https://github.com/NVIDIA/spark-rapids/issues/13514)|[BUG] MetricsEventLogValidationSuite failed: operator time metrics are less|
|[#13505](https://github.com/NVIDIA/spark-rapids/issues/13505)|[BUG] iceberg_append_test case failed: BasicColumnarWriteJobStatsTracker has no safety check for Active SparkContext|
|[#13445](https://github.com/NVIDIA/spark-rapids/issues/13445)|[BUG] com.nvidia.spark.rapids.jni.CpuRetryOOM: CPU OutOfMemory in performance test|
|[#13449](https://github.com/NVIDIA/spark-rapids/issues/13449)|[BUG] Device memory allocation failure when running the delete command for Delta tables with deletion vector enabled|
|[#13358](https://github.com/NVIDIA/spark-rapids/issues/13358)|[BUG] Failed to run IT on dataproc serverless with spark.executor.memoryOverhead|
|[#13477](https://github.com/NVIDIA/spark-rapids/issues/13477)|[BUG] MetricsEventLogValidationSuite failed Total operator time exceed total executor run time intermittently|
|[#13465](https://github.com/NVIDIA/spark-rapids/issues/13465)|[BUG] MetricsEventLogValidationSuite failed in spark400|
|[#13458](https://github.com/NVIDIA/spark-rapids/issues/13458)|[BUG] [TEST] Optimization for unit test: Reduce TimeZonePerfSuite test time|
|[#13457](https://github.com/NVIDIA/spark-rapids/issues/13457)|[BUG] delta_lake_liquid_clustering_test.test_delta_rtas_sql_liquid_clustering failed Part of the plan is not columnar class|
|[#12883](https://github.com/NVIDIA/spark-rapids/issues/12883)|[BUG] HostAllocSuite failed split should not happen immediately after fallback on memory contention failed|
|[#13455](https://github.com/NVIDIA/spark-rapids/issues/13455)|[BUG] CI integration test images fails build: Secondary flag is not valid for non-boolean flag|
|[#13443](https://github.com/NVIDIA/spark-rapids/issues/13443)|[BUG] CI failed: could not initialize class com.databricks.sql.transaction.tahoe.DeltaValidation$|
|[#13360](https://github.com/NVIDIA/spark-rapids/issues/13360)|[BUG] Column Support mismatch error with collect_list + JOIN when AQE is enabled|
|[#13396](https://github.com/NVIDIA/spark-rapids/issues/13396)|[BUG] markdown link check failed dead link loan-performance-data.html|
|[#13416](https://github.com/NVIDIA/spark-rapids/issues/13416)|[BUG] All IT join test and NDS test failed due to cudaErrorIllegalAddress an illegal memory access was encountered|
|[#13198](https://github.com/NVIDIA/spark-rapids/issues/13198)|[BUG] all tasks are bigger than the total memory limit FAILED|
|[#13370](https://github.com/NVIDIA/spark-rapids/issues/13370)|[BUG] liquid clustering should be triggered after enabled for testing|
|[#13361](https://github.com/NVIDIA/spark-rapids/issues/13361)|[BUG] Array index out of bounds when running iceberg query|
|[#10988](https://github.com/NVIDIA/spark-rapids/issues/10988)|[AUDIT][SPARK-48019] Fix incorrect behavior in ColumnVector/ColumnarArray with dictionary and nulls|
|[#13224](https://github.com/NVIDIA/spark-rapids/issues/13224)|[BUG] slightly deep expression planning/processing is slow|
|[#11921](https://github.com/NVIDIA/spark-rapids/issues/11921)|[BUG][AUDIT][SPARK-50258][SQL] Fix output column order changed issue after AQE optimization|
|[#13348](https://github.com/NVIDIA/spark-rapids/issues/13348)|[BUG] init_cudf_udf.sh failed: Script exit status is non-zero for databricks runtime since 25.10|
|[#13353](https://github.com/NVIDIA/spark-rapids/issues/13353)|[BUG] Regression with RapidsFileIO change|
|[#13326](https://github.com/NVIDIA/spark-rapids/issues/13326)|[BUG] hive_write_test.py::test_optimized_hive_ctas_configs_orc failure|
|[#13342](https://github.com/NVIDIA/spark-rapids/issues/13342)|[BUG] Build failure due to deprecation of some methods in ParseURI class from spark-rapids-jni|
|[#13337](https://github.com/NVIDIA/spark-rapids/issues/13337)|[BUG] `ParseURI` method deprecations causing pre-merge CI jobs failures|
|[#11106](https://github.com/NVIDIA/spark-rapids/issues/11106)|[BUG] Revisit integration tests with @disable_ansi_mode label|
|[#13319](https://github.com/NVIDIA/spark-rapids/issues/13319)|[BUG] some nvtx ranges pop out of order or have the wrong lifecycle|
|[#13138](https://github.com/NVIDIA/spark-rapids/issues/13138)|[FEA] Use iceberg FileIO to access parquet files.|
|[#13331](https://github.com/NVIDIA/spark-rapids/issues/13331)|[BUG] test_delta_optimize_fallback_on_clustered_table failed does not support truncate in batch mode|
|[#13270](https://github.com/NVIDIA/spark-rapids/issues/13270)|[BUG] Iceberg integration failed under spark connect|
|[#13145](https://github.com/NVIDIA/spark-rapids/issues/13145)|[BUG] NDS queries fail with sufficiently low offHeap limits in kudo deserializer|

### PRs
|||
|:---|:---|
|[#13831](https://github.com/NVIDIA/spark-rapids/pull/13831)|Support GpuMapFromEntries to prevent fallback 25.10|
|[#13782](https://github.com/NVIDIA/spark-rapids/pull/13782)|Ensure that task threads are the only ones registered as dedicated|
|[#13804](https://github.com/NVIDIA/spark-rapids/pull/13804)|[25.10] Add missing handling for the 0 input partition count in computing partition count in GpuOptimizeWriteExchangeExec|
|[#13631](https://github.com/NVIDIA/spark-rapids/pull/13631)|Update changelog for the v25.10 release [skip ci]|
|[#13627](https://github.com/NVIDIA/spark-rapids/pull/13627)|disable offHeapLimit by default|
|[#13588](https://github.com/NVIDIA/spark-rapids/pull/13588)|Update changelog for the v25.10 release [skip ci]|
|[#13593](https://github.com/NVIDIA/spark-rapids/pull/13593)|[DOC] update the download doc for 2510 release [skip ci]|
|[#13587](https://github.com/NVIDIA/spark-rapids/pull/13587)|Update dependency version JNI, private, hybrid to 25.10.0|
|[#13581](https://github.com/NVIDIA/spark-rapids/pull/13581)|Avoid host-vector access under TableCache by replacing CPU ColumnarToRow with GPU variant|
|[#13580](https://github.com/NVIDIA/spark-rapids/pull/13580)|Remove nested quotes and double quotes [skip ci]|
|[#13569](https://github.com/NVIDIA/spark-rapids/pull/13569)|Disable predicate pushdown filters in MULTITHREADED Delta Scan when reading Deletion Vectors|
|[#13563](https://github.com/NVIDIA/spark-rapids/pull/13563)|Fallback gracefully if libprofiler cannot be loaded|
|[#13532](https://github.com/NVIDIA/spark-rapids/pull/13532)|Fix read option name for change data feed for Delta|
|[#13545](https://github.com/NVIDIA/spark-rapids/pull/13545)|Add iceberg tests of insert into with values|
|[#13558](https://github.com/NVIDIA/spark-rapids/pull/13558)|Avoid throwing ClassNotFoundExceptions when checking isSupportedFormat with Delta Lake versions < 2.0.1|
|[#13541](https://github.com/NVIDIA/spark-rapids/pull/13541)|fix overhead of new op time collection upon c2r|
|[#13539](https://github.com/NVIDIA/spark-rapids/pull/13539)|Add env header for iceberg test variable [skip ci]|
|[#13537](https://github.com/NVIDIA/spark-rapids/pull/13537)|Fix iceberg integration test test_insert_into_partitioned_table_unsupported_partition_fallback [skip ci]|
|[#13335](https://github.com/NVIDIA/spark-rapids/pull/13335)|Add a GpuOverride for IncrementMetric |
|[#12967](https://github.com/NVIDIA/spark-rapids/pull/12967)|limit gpu alloc on integrated cards|
|[#13497](https://github.com/NVIDIA/spark-rapids/pull/13497)|Switch rounding calls to using new API|
|[#13515](https://github.com/NVIDIA/spark-rapids/pull/13515)|Fallback to cpu when inserting into partitioned iceberg table with unsupported partition.|
|[#13517](https://github.com/NVIDIA/spark-rapids/pull/13517)|more relaxed margin for MetricsEventLogValidationSuite|
|[#13516](https://github.com/NVIDIA/spark-rapids/pull/13516)|Fix wrong usage of GpuWriteJobStatsTracker in iceberg write|
|[#13499](https://github.com/NVIDIA/spark-rapids/pull/13499)|Refine column size estimation based on previous batch for Host2Gpu|
|[#13491](https://github.com/NVIDIA/spark-rapids/pull/13491)|Add Deletion Vector Read Support to Multithreaded Parquet Reader|
|[#13476](https://github.com/NVIDIA/spark-rapids/pull/13476)|Add GpuRollingFileWriter for iceberg.|
|[#13490](https://github.com/NVIDIA/spark-rapids/pull/13490)|Fix a Memory leak introduced as part of adding Deletion Vector support |
|[#13460](https://github.com/NVIDIA/spark-rapids/pull/13460)|Add support for Iceberg insert integration tests.|
|[#13492](https://github.com/NVIDIA/spark-rapids/pull/13492)|Update archive.apache usage in dockerfile [skip ci]|
|[#13439](https://github.com/NVIDIA/spark-rapids/pull/13439)|Add Partial support for Deletion Vector for PERFILE Reader Type|
|[#13481](https://github.com/NVIDIA/spark-rapids/pull/13481)|allow some margin for maxExpectedOperatorTime|
|[#13474](https://github.com/NVIDIA/spark-rapids/pull/13474)|Revert "insert coalesce after join" [skip ci]|
|[#13480](https://github.com/NVIDIA/spark-rapids/pull/13480)|avoid using Noop as default excludeMetric|
|[#13466](https://github.com/NVIDIA/spark-rapids/pull/13466)|fix evenlog by default zstd compression in spark 400|
|[#13459](https://github.com/NVIDIA/spark-rapids/pull/13459)|Optimization for unit test: Reduce TimeZonePerfSuite test time|
|[#13429](https://github.com/NVIDIA/spark-rapids/pull/13429)|refactor optime collection for correctness|
|[#13461](https://github.com/NVIDIA/spark-rapids/pull/13461)|Add a missing allow-non-gpu tag for CreateTableExec for test_delta_rtas_sql_liquid_clustering|
|[#13447](https://github.com/NVIDIA/spark-rapids/pull/13447)|Add Spark 4.0.1 shim|
|[#13444](https://github.com/NVIDIA/spark-rapids/pull/13444)|insert coalesce after join because join can cause row reductions|
|[#13456](https://github.com/NVIDIA/spark-rapids/pull/13456)|Remove legacy spacy pkg [skip ci]|
|[#13434](https://github.com/NVIDIA/spark-rapids/pull/13434)|Support InMemoryTableScan with AQE for Spark 3.5.2+|
|[#13369](https://github.com/NVIDIA/spark-rapids/pull/13369)|[DOC] Update memory debugging docs with scalar support [skip ci]|
|[#13441](https://github.com/NVIDIA/spark-rapids/pull/13441)|Substitue RapidsFileIO interface from spark-rapids-jni for RapidsFileIO in spark-rapids|
|[#13446](https://github.com/NVIDIA/spark-rapids/pull/13446)|Add support for iceberg GpuSparkWrite|
|[#13414](https://github.com/NVIDIA/spark-rapids/pull/13414)|Add support for the Delta optimize table command for clustered tables|
|[#13415](https://github.com/NVIDIA/spark-rapids/pull/13415)|Add gpu iceberg partitioner|
|[#13438](https://github.com/NVIDIA/spark-rapids/pull/13438)|Add rule to override StaticInvoke for iceberg|
|[#13417](https://github.com/NVIDIA/spark-rapids/pull/13417)|Add write support for Delta clustered tables|
|[#13367](https://github.com/NVIDIA/spark-rapids/pull/13367)|Allows AQE to fall back to the CPU for non-reused broadcast|
|[#13246](https://github.com/NVIDIA/spark-rapids/pull/13246)|Fix docstring typo in collect_data_or_exception [skip ci]|
|[#13052](https://github.com/NVIDIA/spark-rapids/pull/13052)|Orc supports non-UTC timezone when reader/writer timezones are the same|
|[#13409](https://github.com/NVIDIA/spark-rapids/pull/13409)|[DOC] update the download doc to add download plugin from maven [skip ci]|
|[#13407](https://github.com/NVIDIA/spark-rapids/pull/13407)|Add iceberg gpu parquet file appender and file writer factory|
|[#13366](https://github.com/NVIDIA/spark-rapids/pull/13366)|Fix the flaky test in `TrafficControllerSuite`|
|[#13359](https://github.com/NVIDIA/spark-rapids/pull/13359)|limit concurrent gpu task by a config|
|[#13392](https://github.com/NVIDIA/spark-rapids/pull/13392)|Add GpuAppendDataExec as prerequisite to support iceberg append|
|[#13364](https://github.com/NVIDIA/spark-rapids/pull/13364)|fix wrong timing to sleep(yield) in HostAllocSuite|
|[#13397](https://github.com/NVIDIA/spark-rapids/pull/13397)|Add support for iceberg bucket expression of int and long types.|
|[#12960](https://github.com/NVIDIA/spark-rapids/pull/12960)|Use async profiler to create per stage flame graph|
|[#13373](https://github.com/NVIDIA/spark-rapids/pull/13373)|Add an executor cache class|
|[#13371](https://github.com/NVIDIA/spark-rapids/pull/13371)|Optimize table to trigger liquid clustering before test|
|[#13372](https://github.com/NVIDIA/spark-rapids/pull/13372)|Fix index out of bound bug in iceberg parquet reader.|
|[#13289](https://github.com/NVIDIA/spark-rapids/pull/13289)|Filter out timezones not supported by Python|
|[#13308](https://github.com/NVIDIA/spark-rapids/pull/13308)|Add UUID function support|
|[#13225](https://github.com/NVIDIA/spark-rapids/pull/13225)|Fix performance issue with nested add|
|[#13318](https://github.com/NVIDIA/spark-rapids/pull/13318)|Add a missing numbers import in PySpark RAPIDS daemons|
|[#13329](https://github.com/NVIDIA/spark-rapids/pull/13329)|Add rest catalog integration tests for iceberg|
|[#13327](https://github.com/NVIDIA/spark-rapids/pull/13327)|Add read tests for the delta clustered table|
|[#13349](https://github.com/NVIDIA/spark-rapids/pull/13349)|Make the iceberg bug easier to produce.|
|[#13356](https://github.com/NVIDIA/spark-rapids/pull/13356)|CUDF Conda package has dropped cuda11 python packages from 25.10 [skip ci]|
|[#13354](https://github.com/NVIDIA/spark-rapids/pull/13354)|This fixes a big regression due to serialization of the hadoop configuration|
|[#13352](https://github.com/NVIDIA/spark-rapids/pull/13352)|Add ANSI mode support for parse_url function|
|[#13328](https://github.com/NVIDIA/spark-rapids/pull/13328)|Fix test_optimized_hive_ctas_configs_orc for non_utc timezones|
|[#13346](https://github.com/NVIDIA/spark-rapids/pull/13346)|Merge branch-25.08 to branch-25.10 [skip ci]|
|[#13338](https://github.com/NVIDIA/spark-rapids/pull/13338)|Suppress deprecation warnings for `ParseURI`|
|[#13317](https://github.com/NVIDIA/spark-rapids/pull/13317)|nvtx fixes in shuffle manager|
|[#13177](https://github.com/NVIDIA/spark-rapids/pull/13177)|enable more logging for BUFN_PLUS related issues|
|[#13252](https://github.com/NVIDIA/spark-rapids/pull/13252)|Add Rapids file io layer.|
|[#13333](https://github.com/NVIDIA/spark-rapids/pull/13333)|Fix test_delta_optimize_fallback_on_clustered_table test failure with Spark 3.5.6|
|[#13250](https://github.com/NVIDIA/spark-rapids/pull/13250)|add nvtx, fix row count, add data size metric|
|[#13299](https://github.com/NVIDIA/spark-rapids/pull/13299)|LORE: Add support to dump parquet with exact column names|
|[#13330](https://github.com/NVIDIA/spark-rapids/pull/13330)|Enable shellcheck: Fix nits [skip ci]|
|[#13323](https://github.com/NVIDIA/spark-rapids/pull/13323)|Update stringrepr for collation for StringGen|
|[#13309](https://github.com/NVIDIA/spark-rapids/pull/13309)|Enable shellcheck|
|[#13313](https://github.com/NVIDIA/spark-rapids/pull/13313)|Add support for optimize table command for Detla IO 3.3|
|[#13320](https://github.com/NVIDIA/spark-rapids/pull/13320)|Fix the checklists in the PR template to reduce confusion [skip test]|
|[#13316](https://github.com/NVIDIA/spark-rapids/pull/13316)|Fixed bug with arithmetic overflow on some map_zip_with tests|
|[#13322](https://github.com/NVIDIA/spark-rapids/pull/13322)|Fix testing.md to use fixed relative paths [skip ci]|
|[#13053](https://github.com/NVIDIA/spark-rapids/pull/13053)|Allowlist zlib for Orc writes|
|[#13220](https://github.com/NVIDIA/spark-rapids/pull/13220)|Add test cases for Collate, CharType and VarcharType|
|[#13295](https://github.com/NVIDIA/spark-rapids/pull/13295)|Avoid thread safety issues on input iterator in AsymmetricJoinSizer|
|[#13087](https://github.com/NVIDIA/spark-rapids/pull/13087)|Add Support for map_zip_with|
|[#13303](https://github.com/NVIDIA/spark-rapids/pull/13303)|wrap shuffle read's underlying input stream with timing logic|
|[#13288](https://github.com/NVIDIA/spark-rapids/pull/13288)|Replace 'get' with `getOrElse` to avoid None.get exception in GpuBatchScanExec|
|[#13287](https://github.com/NVIDIA/spark-rapids/pull/13287)|Revert "enable shellcheck" [skip ci]|
|[#13273](https://github.com/NVIDIA/spark-rapids/pull/13273)|enable offheap limits by default|
|[#13277](https://github.com/NVIDIA/spark-rapids/pull/13277)|Fix auto merge conflict 13276 [skip ci]|
|[#13122](https://github.com/NVIDIA/spark-rapids/pull/13122)|enable shellcheck|
|[#13262](https://github.com/NVIDIA/spark-rapids/pull/13262)|Run test_decimal_precision_over_max for all Spark versions|
|[#13255](https://github.com/NVIDIA/spark-rapids/pull/13255)|concat metrics should not contain iter.hasNext|
|[#13241](https://github.com/NVIDIA/spark-rapids/pull/13241)|Fix ThrottlingExecutor not closed bug for PR 12674|
|[#12674](https://github.com/NVIDIA/spark-rapids/pull/12674)|async threads for shuffle read|
|[#13229](https://github.com/NVIDIA/spark-rapids/pull/13229)|Update dependency version JNI, private, hybrid to 25.10.0-SNAPSHOT|

## Release 25.08

### Features
|||
|:---|:---|
|[#12720](https://github.com/NVIDIA/spark-rapids/issues/12720)|[FEA] Enable qa iceberg daily tests.|
|[#13054](https://github.com/NVIDIA/spark-rapids/issues/13054)|[FEA] Support GpuMultiply with ANSI mode|
|[#11101](https://github.com/NVIDIA/spark-rapids/issues/11101)|[audit] [SPARK-48649][SQL] Add "ignoreInvalidPartitionPaths" and "spark.sql.files.ignoreInvalidPartitionPaths|
|[#5114](https://github.com/NVIDIA/spark-rapids/issues/5114)|[FEA] Support aggregates when ANSI mode is enabled|
|[#12574](https://github.com/NVIDIA/spark-rapids/issues/12574)|[FEA] Add delta-33x module with base implementation|
|[#12714](https://github.com/NVIDIA/spark-rapids/issues/12714)|[FEA] Throw ExtendedAnalysisException while casting when ansi is enabled|
|[#12991](https://github.com/NVIDIA/spark-rapids/issues/12991)|[FEA] Implement GpuCreateDeltaTableCommand for Delta 3.3.x|
|[#12580](https://github.com/NVIDIA/spark-rapids/issues/12580)|[FEA] Accelerate autoOptimize.autoCompact|
|[#12988](https://github.com/NVIDIA/spark-rapids/issues/12988)|[FEA] Implement GpuUpdateCommand for Delta 3.3.x|
|[#12989](https://github.com/NVIDIA/spark-rapids/issues/12989)|[FEA] Implement GpuDeleteCommand for Delta 3.3.x|
|[#12785](https://github.com/NVIDIA/spark-rapids/issues/12785)|[FEA] Update iceberg doc.|
|[#12232](https://github.com/NVIDIA/spark-rapids/issues/12232)|[FEA] Support s3 table for iceberg.|
|[#12679](https://github.com/NVIDIA/spark-rapids/issues/12679)|[FEA] Update to CUDA 12.9|
|[#12578](https://github.com/NVIDIA/spark-rapids/issues/12578)|[FEA] Accelerate autoOptimize.optimizeWrite adaptive shuffled writes|
|[#12793](https://github.com/NVIDIA/spark-rapids/issues/12793)|[FEA] Add GpuOptimisticTransaction to Delta 3.3.0 Shim|
|[#12966](https://github.com/NVIDIA/spark-rapids/issues/12966)|[FEA] Diff between CPU and GPU for casting from Timestamp to other type|
|[#6846](https://github.com/NVIDIA/spark-rapids/issues/6846)|[FEA] Support parsing dates and timestamps with timezone IDs in the string|
|[#10035](https://github.com/NVIDIA/spark-rapids/issues/10035)|[FEA] Support Cast for String to Timestamps for non-UTC timezones|
|[#12738](https://github.com/NVIDIA/spark-rapids/issues/12738)|[FEA] better task.resource.gpu.amount experience|

### Performance
|||
|:---|:---|
|[#12903](https://github.com/NVIDIA/spark-rapids/issues/12903)|[FEA] Run NDS benchmark for Spark-4.0|
|[#12626](https://github.com/NVIDIA/spark-rapids/issues/12626)|[TASK] Run NDS benchmarks once basic Delta 3.3.0 Read is supported|
|[#10571](https://github.com/NVIDIA/spark-rapids/issues/10571)|[AUDIT] [SPARK-46366] Implement `Between` expression GpuOverrides|
|[#13007](https://github.com/NVIDIA/spark-rapids/issues/13007)|[FEA] Look into better performance for ANSI mode SUM reduction/group by aggregation|
|[#13035](https://github.com/NVIDIA/spark-rapids/issues/13035)|[FEA] Add Debug Metrics measuring the actual parallelism of multithreaded reader|
|[#12927](https://github.com/NVIDIA/spark-rapids/issues/12927)|[FEA] optimize casting string to date|

### Bugs Fixed
|||
|:---|:---|
|[#13112](https://github.com/NVIDIA/spark-rapids/issues/13112)|[BUG] GpuRegExpextract exception num_blocks must be > 0|
|[#13242](https://github.com/NVIDIA/spark-rapids/issues/13242)|[BUG] Integration test Intermittently failed Method GpuShuffleExchangeExec.ensReqDPMetricTag is abstract in DB 14.3 runtime|
|[#13131](https://github.com/NVIDIA/spark-rapids/issues/13131)|[BUG] Find a way to disable false ERROR messages for pinned memory|
|[#13279](https://github.com/NVIDIA/spark-rapids/issues/13279)|[BUG] Integration test failures with Delta|
|[#13265](https://github.com/NVIDIA/spark-rapids/issues/13265)|[BUG] test failed Expected error IllegalArgumentException/ArrayIndexOutOfBoundsException|
|[#13254](https://github.com/NVIDIA/spark-rapids/issues/13254)|[BUG] delta_lake_time_travel_test.py::test_time_travel_on_non_existing_table failed Expected error 'AnalysisException' did not appear|
|[#13049](https://github.com/NVIDIA/spark-rapids/issues/13049)|[BUG] hash_aggregate_test tests FAILED on [DATABRICKS]|
|[#13164](https://github.com/NVIDIA/spark-rapids/issues/13164)|[BUG][DATAGEN_SEED=1753338612] test_array_slice_with_negative_length failed in scala 2.13 integration|
|[#13204](https://github.com/NVIDIA/spark-rapids/issues/13204)|[BUG] multi-* expressions does not handle partial replacement within a single expression tree|
|[#13179](https://github.com/NVIDIA/spark-rapids/issues/13179)|[BUG] arithmetic_ops_test.py::test_try_sum_groupby_fallback_to_cpu[Long][DATAGEN_SEED=1753421319] failed in rapids_it-MT-shuffle-standalone|
|[#12949](https://github.com/NVIDIA/spark-rapids/issues/12949)|[BUG] cudaErrorIllegalAddress and cudaErrorIllegalInstruction error on NDS-H parquet3k snappy on Spark 3.3.3|
|[#13217](https://github.com/NVIDIA/spark-rapids/issues/13217)|[BUG] integration test src.main.python.ast_test.test_refer_to_non_fixed_width_column failed on scala 2.13|
|[#13101](https://github.com/NVIDIA/spark-rapids/issues/13101)|[BUG] conditionals_test.py::test_case_when failed due to com.nvidia.spark.rapids.jni.CpuRetryOOM: CPU OutOfMemory in dataproc serverless IT job|
|[#13183](https://github.com/NVIDIA/spark-rapids/issues/13183)|[BUG] json_test.py failure test_arrays_to_json with time stamp failure|
|[#12505](https://github.com/NVIDIA/spark-rapids/issues/12505)|[BUG] AST report ai.rapids.cudf.CudfException:  Invalid, non-fixed-width type|
|[#13188](https://github.com/NVIDIA/spark-rapids/issues/13188)|[BUG] [Spark-4.0] NDS query-18 is failing due to Divide_By_Zero error|
|[#13197](https://github.com/NVIDIA/spark-rapids/issues/13197)|[BUG] databricks 12.2 test failed to install required packages for fastparquet|
|[#13154](https://github.com/NVIDIA/spark-rapids/issues/13154)|[BUG] nds run illegal access seen last night|
|[#11512](https://github.com/NVIDIA/spark-rapids/issues/11512)|[BUG] Support for wider types in read schemas for Parquet Reads|
|[#13040](https://github.com/NVIDIA/spark-rapids/issues/13040)|[BUG] tests fail on systems with more than one GPU|
|[#11931](https://github.com/NVIDIA/spark-rapids/issues/11931)|[BUG][AUDIT][SPARK-50480][SQL] Extend CharType and VarcharType from StringType|
|[#13146](https://github.com/NVIDIA/spark-rapids/issues/13146)|[BUG] CPU off heap limit default subtracts spark.offHeap size from memory overhead|
|[#13152](https://github.com/NVIDIA/spark-rapids/issues/13152)|[BUG] json_test.py::test_structs_to_json_timestamp/arrays_to_json/maps_to_json fails|
|[#13137](https://github.com/NVIDIA/spark-rapids/issues/13137)|[BUG] SQL CASE condition is bypassed leading to error in ELSE branch|
|[#13153](https://github.com/NVIDIA/spark-rapids/issues/13153)|[BUG] arithmetic_ops_test.py::test_try_avg_fallback_to_cpu - GPU and CPU float values are different|
|[#11987](https://github.com/NVIDIA/spark-rapids/issues/11987)|[AUDIT 4.0] [SPARK-50446][PYTHON] Concurrent level in Arrow-optimized Python UDF|
|[#13133](https://github.com/NVIDIA/spark-rapids/issues/13133)|[BUG] test_delta_rtas_sql_liquid_clustering_fallback fails with Spark 3.5.6 pipeline|
|[#13129](https://github.com/NVIDIA/spark-rapids/issues/13129)|[BUG] delta_lake_write_test.py::test_delta_write_optimized_supported_types_partitioned FAILED ON "Part of the plan is not columnar class org.apache.spark.sql.execution.command.ExecutedCommandExec"|
|[#12992](https://github.com/NVIDIA/spark-rapids/issues/12992)|[TASK] Run all integration tests that were xfailed for Spark 3.5.3+|
|[#13062](https://github.com/NVIDIA/spark-rapids/issues/13062)|[BUG] Fallback writes to cpu when liquid clustering is enabled for delta 3.3.x|
|[#13118](https://github.com/NVIDIA/spark-rapids/issues/13118)|[TASK] Avoid skipping tests for optimize writes|
|[#13065](https://github.com/NVIDIA/spark-rapids/issues/13065)|[BUG] cast_test.py::test_ansi_cast_failures_decimal_to_decimal test failed|
|[#13102](https://github.com/NVIDIA/spark-rapids/issues/13102)|[BUG] delta_lake_write_test.py::test_delta_write_round_trip_managed failed due to IllegalArgumentException in databricks nightly test with 350db|
|[#11130](https://github.com/NVIDIA/spark-rapids/issues/11130)|[BUG] `get_json_test.py::test_get_json_object_quoted_question` fails on Spark 4 with mismatched output|
|[#12806](https://github.com/NVIDIA/spark-rapids/issues/12806)|[BUG] Shimplify.py skips shims that are excluded in pom.xml |
|[#13045](https://github.com/NVIDIA/spark-rapids/issues/13045)|[BUG] Add check in `OptimizeExecutor` to ensure that no deletion vector is enabled in delta 3.3.x|
|[#10908](https://github.com/NVIDIA/spark-rapids/issues/10908)|[BUG] Cast string to decimal won't return null for out of range floats|
|[#12969](https://github.com/NVIDIA/spark-rapids/issues/12969)|[BUG] Multiple test failures in `src.main.python.hash_aggregate_test` across various CI jobs|
|[#11047](https://github.com/NVIDIA/spark-rapids/issues/11047)|Revisit the ANSI tests which is enabled by default in Spark 4.0.0|
|[#13096](https://github.com/NVIDIA/spark-rapids/issues/13096)|[BUG] [Spark-4.0] NDS query_94 and query_95 are failing with IllegalArgumentException|
|[#13018](https://github.com/NVIDIA/spark-rapids/issues/13018)|[BUG] AppendDataExecV1 falls back when running CTAS/RTAS on Delta 3.3.x|
|[#13022](https://github.com/NVIDIA/spark-rapids/issues/13022)|[BUG] GpuRowToColumnarExec with RequireSingleBatch allocates a large amount of memory|
|[#12857](https://github.com/NVIDIA/spark-rapids/issues/12857)|[BUG] GPU generate different output as CPU|
|[#13063](https://github.com/NVIDIA/spark-rapids/issues/13063)|[BUG] rapids_it-matrix-dev-github delta_lake_time_travel_test.py test failures DATAGEN_SEED=1751797993|
|[#13070](https://github.com/NVIDIA/spark-rapids/issues/13070)|[BUG] Integration tests failure in json_test.py|
|[#13064](https://github.com/NVIDIA/spark-rapids/issues/13064)|[BUG] rapids_it-matrix-dev-github test_delta_update_fallback_with_deletion_vectors failing DATAGEN_SEED=1751798742|
|[#13037](https://github.com/NVIDIA/spark-rapids/issues/13037)|[BUG] NDS query39_part1  and and query39_part2 are failing due to SparkArithmeticException thrown from the plugin.|
|[#12879](https://github.com/NVIDIA/spark-rapids/issues/12879)|[BUG] Implement MergeIntoCommand for Delta Lake 3.3.0|
|[#13051](https://github.com/NVIDIA/spark-rapids/issues/13051)|[BUG] jason_test.py::test_structs_to_json FAILED on _assert_gpu_and_cpu_are_equal|
|[#13019](https://github.com/NVIDIA/spark-rapids/issues/13019)|[BUG] `GpuOptimisticTransaction` should add the identity column stats tracker as a statsTracker for Delta IO 3.3|
|[#11552](https://github.com/NVIDIA/spark-rapids/issues/11552)|[BUG] [Spark 4] Raw Cudf/JNI exception encountered during casting with ANSI enabled|
|[#12950](https://github.com/NVIDIA/spark-rapids/issues/12950)|[BUG] CREATE VIEW does not function|
|[#13027](https://github.com/NVIDIA/spark-rapids/issues/13027)|[BUG] get-deps-sha1 to support new central.sonatype.com|
|[#11550](https://github.com/NVIDIA/spark-rapids/issues/11550)|[BUG] [Spark 4] Decimal casting errors raised from the plugin do not match those from Spark 4.0 in ANSI mode|
|[#13002](https://github.com/NVIDIA/spark-rapids/issues/13002)|[BUG] Multiple GpuLoreSuite tests failed in nightly UT|
|[#12999](https://github.com/NVIDIA/spark-rapids/issues/12999)|[BUG] delta lake case failed NoClassDefFoundError: org/apache/spark/sql/catalyst/plans/logical/SupportsNonDeterministicExpression in spark350|
|[#12924](https://github.com/NVIDIA/spark-rapids/issues/12924)|[BUG] Gpu statistics on Delta Lake 3.3.0 don't match the expected value in the `delta_lake_write_test.py::test_delta_write_stat_column_limits`|
|[#12930](https://github.com/NVIDIA/spark-rapids/issues/12930)|[BUG] Implement `GpuAppendDataExecV1` for Delta Lake 3.3.x|
|[#12931](https://github.com/NVIDIA/spark-rapids/issues/12931)|[BUG] Implement `GpuAtomicCreateTableAsSelectExec` and `GpuAtomicReplaceTableAsSelectExec` for Delta Lake 3.3.x|
|[#12932](https://github.com/NVIDIA/spark-rapids/issues/12932)|[BUG] Implement `GpuOverwriteByExpressionExecV1` for Delta Lake 3.3.x|
|[#11004](https://github.com/NVIDIA/spark-rapids/issues/11004)|Fix integration test failures for Spark 4.0.0 |
|[#12929](https://github.com/NVIDIA/spark-rapids/issues/12929)|[BUG] Add support for CreatableRelationProvider for Delta Lake 3.3.x|
|[#11555](https://github.com/NVIDIA/spark-rapids/issues/11555)|[BUG] [Spark 4] Invalid results from Casting timestamps to integral types|
|[#12836](https://github.com/NVIDIA/spark-rapids/issues/12836)|[BUG] XFAIL failing tests for Delta 3.3.0|
|[#12001](https://github.com/NVIDIA/spark-rapids/issues/12001)|[BUG] Fix unit test failures in Spark-4.0|
|[#12841](https://github.com/NVIDIA/spark-rapids/issues/12841)|XFAIL the failing tests and assert fallback in delta_zorder_test.py|
|[#12019](https://github.com/NVIDIA/spark-rapids/issues/12019)|[BUG] Spark-4.0: Tests failures in CastOpSuite and conditional_test.py|
|[#11018](https://github.com/NVIDIA/spark-rapids/issues/11018)|Fix tests failures in hash_aggregate_test.py|
|[#11007](https://github.com/NVIDIA/spark-rapids/issues/11007)|Fix tests failures in array_test.py|
|[#12840](https://github.com/NVIDIA/spark-rapids/issues/12840)|XFAIL the failing tests and assert fallback in delta_lake_write_test.py|
|[#12933](https://github.com/NVIDIA/spark-rapids/issues/12933)|Fix integration test failures in dpp_test.py|
|[#11028](https://github.com/NVIDIA/spark-rapids/issues/11028)|Fix tests failures in qa_nightly_select_test.py|
|[#11022](https://github.com/NVIDIA/spark-rapids/issues/11022)|Fix tests failures in join_test.py|
|[#12954](https://github.com/NVIDIA/spark-rapids/issues/12954)|[BUG] GpuDeleteFilterSuite failed in nightly UT|
|[#11017](https://github.com/NVIDIA/spark-rapids/issues/11017)|Fix tests failures in url_test.py|
|[#11030](https://github.com/NVIDIA/spark-rapids/issues/11030)|Fix tests failures in string_test.py|
|[#11016](https://github.com/NVIDIA/spark-rapids/issues/11016)|Fix tests failures in csv_test.py|
|[#11009](https://github.com/NVIDIA/spark-rapids/issues/11009)|Fix tests failures in cast_test.py|
|[#12772](https://github.com/NVIDIA/spark-rapids/issues/12772)|[BUG] Drop the `materializeSourceTimeMs` from some delta_lake_merge_tests |
|[#11014](https://github.com/NVIDIA/spark-rapids/issues/11014)|Fix tests failures in json_test.py|
|[#12926](https://github.com/NVIDIA/spark-rapids/issues/12926)|[BUG] A diff issue which could only be reproduce on Ampere GPU|
|[#11015](https://github.com/NVIDIA/spark-rapids/issues/11015)|Fix tests failures in parquet_test.py|
|[#11012](https://github.com/NVIDIA/spark-rapids/issues/11012)|Fix tests failures in conditionals_test.py|
|[#11010](https://github.com/NVIDIA/spark-rapids/issues/11010)|Fix tests failures in cache_test.py|
|[#11005](https://github.com/NVIDIA/spark-rapids/issues/11005)|Fix test failures in aqe_test.py|
|[#11013](https://github.com/NVIDIA/spark-rapids/issues/11013)|Fix tests failures in orc_test.py|
|[#12915](https://github.com/NVIDIA/spark-rapids/issues/12915)|[BUG] Delta Lake nightly tests failing due to a regression|
|[#12838](https://github.com/NVIDIA/spark-rapids/issues/12838)|XFAIL the failing tests and assert fallback in delta_lake_merge_test.py|
|[#12884](https://github.com/NVIDIA/spark-rapids/issues/12884)|[BUG] [Spark-4.0] Scala tests in integration_tests module are failing|
|[#12870](https://github.com/NVIDIA/spark-rapids/issues/12870)|[BUG] 25.08 ansi_cast string to bool (invalid values) failed: java.lang.AssertionError: Column has non-empty nulls|

### PRs
|||
|:---|:---|
|[#13344](https://github.com/NVIDIA/spark-rapids/pull/13344)|[DOC] update the wrong links in download doc [skip ci]|
|[#13292](https://github.com/NVIDIA/spark-rapids/pull/13292)|Update changelog for the v25.08 release [skip ci]|
|[#13286](https://github.com/NVIDIA/spark-rapids/pull/13286)|Temporarily disable timezone America/Coyhaique to unblock branch-25.08 release|
|[#13282](https://github.com/NVIDIA/spark-rapids/pull/13282)|Update changelog for the v25.08 release [skip ci]|
|[#13280](https://github.com/NVIDIA/spark-rapids/pull/13280)|Fix fallback test params for Delta MergeCommand, UpdateCommand, and DeleteCommand|
|[#13258](https://github.com/NVIDIA/spark-rapids/pull/13258)|Update changelog for the v25.08 release [skip ci]|
|[#13257](https://github.com/NVIDIA/spark-rapids/pull/13257)|Update dependency version JNI, private, hybrid to 25.08.0|
|[#13123](https://github.com/NVIDIA/spark-rapids/pull/13123)|Enable MERGE, UPDATE, DELETE by default for Delta 3.3.0|
|[#13267](https://github.com/NVIDIA/spark-rapids/pull/13267)|Use pytest.ExceptionInfo to create the same exception text as pytest raises|
|[#13244](https://github.com/NVIDIA/spark-rapids/pull/13244)|[DOC] update the download doc for 2508 release [skip ci]|
|[#13261](https://github.com/NVIDIA/spark-rapids/pull/13261)|Print DB runtime version details for debugging purpose [skip ci]|
|[#13253](https://github.com/NVIDIA/spark-rapids/pull/13253)|Add override tag to the 'ensReqDPMetricTag' and change it to a 'val'|
|[#13248](https://github.com/NVIDIA/spark-rapids/pull/13248)|Close shared buffers at batch read completion in kudo deser stream iter|
|[#13243](https://github.com/NVIDIA/spark-rapids/pull/13243)|Define the DB required method named 'ensReqDPMetricTag' in GpuShuffleExechangeExec.|
|[#13236](https://github.com/NVIDIA/spark-rapids/pull/13236)|Fix case where null start index or length would throw invalid parameter, instead of returning null rows|
|[#13211](https://github.com/NVIDIA/spark-rapids/pull/13211)|Improve the pull request template [skip ci]|
|[#13231](https://github.com/NVIDIA/spark-rapids/pull/13231)|Implements scalar * scalar overload function|
|[#13213](https://github.com/NVIDIA/spark-rapids/pull/13213)|Fix expression combining when only some can combine|
|[#13196](https://github.com/NVIDIA/spark-rapids/pull/13196)|Run try_ functions aggregation arithmetic tests on single partition|
|[#13199](https://github.com/NVIDIA/spark-rapids/pull/13199)|Fallback to CPU for unsupported binary formats|
|[#13218](https://github.com/NVIDIA/spark-rapids/pull/13218)|Use non-ANSI mode to fix overflow error on Spark 400|
|[#13207](https://github.com/NVIDIA/spark-rapids/pull/13207)|Specify timestamp format for to_json for Spark 400|
|[#12506](https://github.com/NVIDIA/spark-rapids/pull/12506)|Fix AST report can not create non-fixed-width column error|
|[#13192](https://github.com/NVIDIA/spark-rapids/pull/13192)|Fix bug with divide by zero in ANSI avg on decimal128 output|
|[#13108](https://github.com/NVIDIA/spark-rapids/pull/13108)|Support ANSI multiply|
|[#13206](https://github.com/NVIDIA/spark-rapids/pull/13206)|Add the project name for jdk-profiles [skip ci]|
|[#13202](https://github.com/NVIDIA/spark-rapids/pull/13202)|Disable fastparquet tests for  versions < 13.3|
|[#13180](https://github.com/NVIDIA/spark-rapids/pull/13180)|Add tests for the cases of decimal precision going beyond 38|
|[#13195](https://github.com/NVIDIA/spark-rapids/pull/13195)|disable offheap limit by default|
|[#13194](https://github.com/NVIDIA/spark-rapids/pull/13194)|Fix a small memory leak that can show up in some error cases|
|[#13165](https://github.com/NVIDIA/spark-rapids/pull/13165)|Allow for better parquet type conversion|
|[#13163](https://github.com/NVIDIA/spark-rapids/pull/13163)|xfail mixed read for 25.08 release [skip ci]|
|[#13143](https://github.com/NVIDIA/spark-rapids/pull/13143)|Add in ANSI AVG support|
|[#13178](https://github.com/NVIDIA/spark-rapids/pull/13178)|Add config to skip dumping lore plan.|
|[#13078](https://github.com/NVIDIA/spark-rapids/pull/13078)|kudo gpu kernel usage|
|[#13158](https://github.com/NVIDIA/spark-rapids/pull/13158)|Host limits fix device count|
|[#13151](https://github.com/NVIDIA/spark-rapids/pull/13151)|Use the "StringType" object for the string type comparison in `case-match` statements|
|[#13157](https://github.com/NVIDIA/spark-rapids/pull/13157)|dont subtract spark offheap from spark memoryOverhead|
|[#13150](https://github.com/NVIDIA/spark-rapids/pull/13150)|Adds ctas/rtas sql test with managed tables for deltalake 3.3.x|
|[#13127](https://github.com/NVIDIA/spark-rapids/pull/13127)|Add tests for between sql and between expr|
|[#13162](https://github.com/NVIDIA/spark-rapids/pull/13162)|Fix failed to_json test cases|
|[#13149](https://github.com/NVIDIA/spark-rapids/pull/13149)|Support side-effect check for the "else" path in GpuCaseWhen|
|[#13155](https://github.com/NVIDIA/spark-rapids/pull/13155)|Add approximate_float decorator to try_avg_fallback_to_cpu  integration test|
|[#13095](https://github.com/NVIDIA/spark-rapids/pull/13095)|Fallback to CPU for try_ functions.|
|[#13126](https://github.com/NVIDIA/spark-rapids/pull/13126)|Supports `Invoke` generated by `RuntimeReplaceable` expressions.|
|[#13117](https://github.com/NVIDIA/spark-rapids/pull/13117)|Improve performance of ANSI SUM integer aggregations|
|[#13139](https://github.com/NVIDIA/spark-rapids/pull/13139)|Allow override mvn command in version-def [skip ci]|
|[#13031](https://github.com/NVIDIA/spark-rapids/pull/13031)|Add in code to halt on shutdown timeouts|
|[#13120](https://github.com/NVIDIA/spark-rapids/pull/13120)|[Spark-4.0] Throw RuntimeException for invalid string to boolean cast in ansi mode|
|[#13136](https://github.com/NVIDIA/spark-rapids/pull/13136)|Set CONDA_PLUGINS_AUTO_ACCEPT_TOS=yes for CI docker build and support python 3.12+ IT test [skip ci]|
|[#13135](https://github.com/NVIDIA/spark-rapids/pull/13135)|Skip test_delta_rtas_sql_liquid_clustering_fallback on Spark 3.5.6|
|[#13132](https://github.com/NVIDIA/spark-rapids/pull/13132)|Fix skip condition for test_delta_write_optimized_supported_types_partitioned|
|[#13111](https://github.com/NVIDIA/spark-rapids/pull/13111)|Ensure deltalake 3.3.x writes fallback when liquid clustering is enabled|
|[#13119](https://github.com/NVIDIA/spark-rapids/pull/13119)|Avoid skipping optimizewrite tests on Spark 3.5.3+|
|[#13113](https://github.com/NVIDIA/spark-rapids/pull/13113)|Add test to verify materialization while merging|
|[#13109](https://github.com/NVIDIA/spark-rapids/pull/13109)|Use the original input column to get the overflow decimal values|
|[#13107](https://github.com/NVIDIA/spark-rapids/pull/13107)|Xfail `test_delta_write_round_trip_managed` on Databricks 14.3|
|[#13114](https://github.com/NVIDIA/spark-rapids/pull/13114)|Fix conda build CondaToSNonInteractiveError issue [skip ci]|
|[#13104](https://github.com/NVIDIA/spark-rapids/pull/13104)|Add Spark 400 shim for get_json_object to handle named string with '?'|
|[#12807](https://github.com/NVIDIA/spark-rapids/pull/12807)|Ignore `dyn.shim.excluded.releases` when shimplifying |
|[#13094](https://github.com/NVIDIA/spark-rapids/pull/13094)|Remove `AppendDataExecV1` from the allow_non_gpu list in `delta_lake_write_test.py` |
|[#13068](https://github.com/NVIDIA/spark-rapids/pull/13068)|Add some debug info for iceberg test.|
|[#13103](https://github.com/NVIDIA/spark-rapids/pull/13103)|Add sanity check in `GpuOptimizeExecutor` to ensure deletion vector is disabled.|
|[#13089](https://github.com/NVIDIA/spark-rapids/pull/13089)|Fix up the exception type for casting strings to decimals.|
|[#13056](https://github.com/NVIDIA/spark-rapids/pull/13056)|Revert "declare shuffle thread for threaded writer and reader"|
|[#13069](https://github.com/NVIDIA/spark-rapids/pull/13069)|avoid using Thread.sleep in the flaky test case|
|[#13082](https://github.com/NVIDIA/spark-rapids/pull/13082)|Add a write round trip test for managed delta tables|
|[#13083](https://github.com/NVIDIA/spark-rapids/pull/13083)|Handle StagedTableCatalog when creating AppendDataExecV1|
|[#12948](https://github.com/NVIDIA/spark-rapids/pull/12948)|enable off heap limit by default|
|[#13085](https://github.com/NVIDIA/spark-rapids/pull/13085)|[DOC] fix broken link in download page [skip ci]|
|[#13032](https://github.com/NVIDIA/spark-rapids/pull/13032)|Update tests/aggregations for ANSI mode, when they don't care|
|[#13084](https://github.com/NVIDIA/spark-rapids/pull/13084)|Skip `test_time_travel_on_non_existing_table` with 'AS OF' before Spark 3.3.0|
|[#13017](https://github.com/NVIDIA/spark-rapids/pull/13017)|Add Create Delta Table command support on GPU for Delta 3.3.x |
|[#13079](https://github.com/NVIDIA/spark-rapids/pull/13079)|Add $ in front of bash variable IS_SPARK_35X in spark-test.sh|
|[#13075](https://github.com/NVIDIA/spark-rapids/pull/13075)|Revert "Support RuntimeReplaceable version of StructsToJson for Spark 400  (#13029)"|
|[#13043](https://github.com/NVIDIA/spark-rapids/pull/13043)|Spark-4.0: Enable ansi mode for some of the integration tests|
|[#13066](https://github.com/NVIDIA/spark-rapids/pull/13066)|Fix delta time travel test|
|[#13067](https://github.com/NVIDIA/spark-rapids/pull/13067)|Fix incorrect deltalake update fallback test|
|[#13046](https://github.com/NVIDIA/spark-rapids/pull/13046)|Replace the `copyToHost` with `getScalarElement` when dealing with some exception cases.|
|[#13050](https://github.com/NVIDIA/spark-rapids/pull/13050)|Merge nulls for left and right operands before performing a div|
|[#13038](https://github.com/NVIDIA/spark-rapids/pull/13038)|Add iceberg s3tables test in integration test [skip ci]|
|[#13034](https://github.com/NVIDIA/spark-rapids/pull/13034)|Add GpuMergeIntoCommand for Delta Lake 3.3.x |
|[#13048](https://github.com/NVIDIA/spark-rapids/pull/13048)|Add tests for time travel in deltalake|
|[#13060](https://github.com/NVIDIA/spark-rapids/pull/13060)|Fix failed to_json cases on non-UTC timezones|
|[#13039](https://github.com/NVIDIA/spark-rapids/pull/13039)|Fix failed cases in ParseDateTimeSuite for Spark 400|
|[#13058](https://github.com/NVIDIA/spark-rapids/pull/13058)|Disable approx_percentile tests temporarily to unblock CI|
|[#13033](https://github.com/NVIDIA/spark-rapids/pull/13033)|Add support for identity column for Delta IO|
|[#13042](https://github.com/NVIDIA/spark-rapids/pull/13042)|Re-xfail test_delta_update_fallback_with_deletion_vectors for databricks|
|[#13036](https://github.com/NVIDIA/spark-rapids/pull/13036)|Add Debug Metrics tracking the parallelism of multithreaded reader|
|[#13023](https://github.com/NVIDIA/spark-rapids/pull/13023)|Auto compact|
|[#13029](https://github.com/NVIDIA/spark-rapids/pull/13029)|Support RuntimeReplaceable version of StructsToJson for Spark 400|
|[#13014](https://github.com/NVIDIA/spark-rapids/pull/13014)|Implement update command for deltalake 3.3.x|
|[#13024](https://github.com/NVIDIA/spark-rapids/pull/13024)|Align the exception type with Spark for casting to numbers|
|[#13010](https://github.com/NVIDIA/spark-rapids/pull/13010)|[BUG] Auto compact tests should use the confs with auto compact enabled|
|[#12986](https://github.com/NVIDIA/spark-rapids/pull/12986)|Add in ANSI support for sum aggregation|
|[#13030](https://github.com/NVIDIA/spark-rapids/pull/13030)|mvn verify cache: replace xmllint with ubuntu built-in commands [skip ci]|
|[#13026](https://github.com/NVIDIA/spark-rapids/pull/13026)|Update snapshot repo to central.sonatype.com|
|[#13003](https://github.com/NVIDIA/spark-rapids/pull/13003)|Introduce delete command for deltalake 3.3.0|
|[#13013](https://github.com/NVIDIA/spark-rapids/pull/13013)|Remove redundant tests to accelerate premerge CI|
|[#12940](https://github.com/NVIDIA/spark-rapids/pull/12940)|Add iceberg aws s3table support.|
|[#13000](https://github.com/NVIDIA/spark-rapids/pull/13000)|Align the exception message for casting floats to decimals|
|[#12938](https://github.com/NVIDIA/spark-rapids/pull/12938)|Add support for optimizeWrite for Delta IO|
|[#12998](https://github.com/NVIDIA/spark-rapids/pull/12998)|Some missing changes for CTAS and RTAS for Delta Lake 3.3.x|
|[#13004](https://github.com/NVIDIA/spark-rapids/pull/13004)|Fix lore GpuInsertIntoHiveTableunit test failures in spark332,330cdh,334|
|[#12526](https://github.com/NVIDIA/spark-rapids/pull/12526)|Add sort time metrics for GPU write exec|
|[#13001](https://github.com/NVIDIA/spark-rapids/pull/13001)|Fix spark-test for delta 3.3.0 [skip ci]|
|[#12985](https://github.com/NVIDIA/spark-rapids/pull/12985)|Align the exception message with that of Spark for casting to Decimals|
|[#12901](https://github.com/NVIDIA/spark-rapids/pull/12901)|fix GpuDeviceManager off heap memory log line|
|[#12987](https://github.com/NVIDIA/spark-rapids/pull/12987)|Remove user|
|[#12990](https://github.com/NVIDIA/spark-rapids/pull/12990)|Add ctas, rtas, append, overwrite for deltalake 3.3.x.|
|[#12923](https://github.com/NVIDIA/spark-rapids/pull/12923)|LoRE: support GpuInsertIntoHiveTable command|
|[#12983](https://github.com/NVIDIA/spark-rapids/pull/12983)|Revert signature changes of executeTasks in GpuFileFormatWriter.|
|[#12981](https://github.com/NVIDIA/spark-rapids/pull/12981)|Enable delta lake tests when TEST_MODE is DEFAULT |
|[#12984](https://github.com/NVIDIA/spark-rapids/pull/12984)|enable rmm state log for troubleshooting 12883|
|[#12977](https://github.com/NVIDIA/spark-rapids/pull/12977)|Support writing to table in deltalake 33x|
|[#12970](https://github.com/NVIDIA/spark-rapids/pull/12970)|Update exception message for spark400 integration tests|
|[#12978](https://github.com/NVIDIA/spark-rapids/pull/12978)|Set overflow rows to nulls when casting timestamps to integrals for Spark 400|
|[#12973](https://github.com/NVIDIA/spark-rapids/pull/12973)|declare shuffle thread for threaded writer and reader|
|[#12971](https://github.com/NVIDIA/spark-rapids/pull/12971)|Ignore .mvn [skip ci]|
|[#12965](https://github.com/NVIDIA/spark-rapids/pull/12965)|Fix Spark400 failed cases in CastOpSuite|
|[#12962](https://github.com/NVIDIA/spark-rapids/pull/12962)|optimize shuffle read perf after getting a cpu wall time flame graph from customer|
|[#12964](https://github.com/NVIDIA/spark-rapids/pull/12964)|Fix failures in hash aggregate tests|
|[#12893](https://github.com/NVIDIA/spark-rapids/pull/12893)|change a critical log's log level|
|[#12955](https://github.com/NVIDIA/spark-rapids/pull/12955)|Fix the failures in array_test.py for Spark 400|
|[#12935](https://github.com/NVIDIA/spark-rapids/pull/12935)|XFAIL failing tests in `delta_lake_write_test.py` and assert fallback for Delta Lake 3.3.x|
|[#12958](https://github.com/NVIDIA/spark-rapids/pull/12958)|Fix Spark 400 IT failures in date_time_test.py|
|[#12956](https://github.com/NVIDIA/spark-rapids/pull/12956)|Disable ansi mode for the kudo dump tests.|
|[#12957](https://github.com/NVIDIA/spark-rapids/pull/12957)|Xfail some tests in json_matrix_test.py for Spark 400+|
|[#12959](https://github.com/NVIDIA/spark-rapids/pull/12959)|Allow the `EmptyRelationExec` on CPU for dpp empty relation tests|
|[#12953](https://github.com/NVIDIA/spark-rapids/pull/12953)|Disable ansi mode for some tests in qa nightly|
|[#12939](https://github.com/NVIDIA/spark-rapids/pull/12939)|[Spark-4.0] Resolve integration test failures in join_test.py|
|[#12961](https://github.com/NVIDIA/spark-rapids/pull/12961)|Fix get string in GpuDeleteFilter test|
|[#12952](https://github.com/NVIDIA/spark-rapids/pull/12952)|xfail the url tests for Spark 400+|
|[#12944](https://github.com/NVIDIA/spark-rapids/pull/12944)|[follow-up] Refactor: rename castStringToDateAnsi to castStringToDate|
|[#12947](https://github.com/NVIDIA/spark-rapids/pull/12947)|Fix Spark 4.0 IT failures: test_conv|
|[#12946](https://github.com/NVIDIA/spark-rapids/pull/12946)|Fix Spark 4.0 IT failures: csv_test.py|
|[#12942](https://github.com/NVIDIA/spark-rapids/pull/12942)|Fix IT: cast_test.py|
|[#12945](https://github.com/NVIDIA/spark-rapids/pull/12945)|Fix JSON test failures in for 400|
|[#12784](https://github.com/NVIDIA/spark-rapids/pull/12784)|Add GpuDeleteFilterSuite|
|[#12824](https://github.com/NVIDIA/spark-rapids/pull/12824)|Fix bug in casting string to timestamp: Spark400+ and DB35 do not support pattern: spaces + Thh:mm:ss|
|[#12669](https://github.com/NVIDIA/spark-rapids/pull/12669)|Fully support casting string to date via new kernel|
|[#12843](https://github.com/NVIDIA/spark-rapids/pull/12843)|Add Spark 3.5.3 and Delta 3.3.0 to `spark-tests.sh`|
|[#12925](https://github.com/NVIDIA/spark-rapids/pull/12925)|[Spark-4.0]: Resolve integration tests in parquet_test|
|[#12904](https://github.com/NVIDIA/spark-rapids/pull/12904)|Spark-4.0 - Resolve/skip integration test failures|
|[#12914](https://github.com/NVIDIA/spark-rapids/pull/12914)|Skip Ansi check for Count agg.|
|[#12869](https://github.com/NVIDIA/spark-rapids/pull/12869)|Revert "Remove Delta Lake 3.3.0 support from the plugin for Spark 3.5.3, 3.5.4 and 3.5.5 (#12853)"|
|[#12908](https://github.com/NVIDIA/spark-rapids/pull/12908)|Fix test_orc_fallback for Spark400|
|[#12916](https://github.com/NVIDIA/spark-rapids/pull/12916)|Make `assert_func` an optional parameter with a default value|
|[#12910](https://github.com/NVIDIA/spark-rapids/pull/12910)|Revert iceberg changes for 25.06 release|
|[#12912](https://github.com/NVIDIA/spark-rapids/pull/12912)|Fix auto merge conflict 12911 [skip ci]|
|[#12898](https://github.com/NVIDIA/spark-rapids/pull/12898)|Add more sanity check for iceberg multi thread reader|
|[#12892](https://github.com/NVIDIA/spark-rapids/pull/12892)|Fix Column to Expression conversion for Spark 4.0|
|[#12880](https://github.com/NVIDIA/spark-rapids/pull/12880)|XFAIL'd Delta Lake 3.3.0 merge tests |
|[#12680](https://github.com/NVIDIA/spark-rapids/pull/12680)|cpu mem defaults|
|[#12890](https://github.com/NVIDIA/spark-rapids/pull/12890)|Add a new metric to indicate the IO time in GPU write.|
|[#12875](https://github.com/NVIDIA/spark-rapids/pull/12875)|Support spark400 shim's integration tests|
|[#12831](https://github.com/NVIDIA/spark-rapids/pull/12831)|Support eager I/O prefetch for scan as two-sides of bucket ShuffleSizedHashJoin|
|[#12854](https://github.com/NVIDIA/spark-rapids/pull/12854)|Drop the new time based metric in commitInfo while comparing Delta Logs|
|[#12847](https://github.com/NVIDIA/spark-rapids/pull/12847)|Update spark400 to released version|
|[#12777](https://github.com/NVIDIA/spark-rapids/pull/12777)|Fix auto merge conflict 12774 [skip ci]|
|[#12761](https://github.com/NVIDIA/spark-rapids/pull/12761)|Use cuda12 as default, drop cuda11 support|
|[#12721](https://github.com/NVIDIA/spark-rapids/pull/12721)|Fix auto merge conflict 12710 [skip ci]|

## Release 25.06

### Features
|||
|:---|:---|
|[#12754](https://github.com/NVIDIA/spark-rapids/issues/12754)|[FEA] Introduce gpu delete filter test suite.|
|[#12210](https://github.com/NVIDIA/spark-rapids/issues/12210)|[FEA] Support merge on read of iceberg table.|
|[#12779](https://github.com/NVIDIA/spark-rapids/issues/12779)|[FEA] Support Spark 3.5.6|
|[#12757](https://github.com/NVIDIA/spark-rapids/issues/12757)|[FEA] Accelerated Delta Scan without deletionvector support|
|[#12740](https://github.com/NVIDIA/spark-rapids/issues/12740)|[FEA] Introduce gpu delete filter for iceberg merge on read scan.|
|[#12755](https://github.com/NVIDIA/spark-rapids/issues/12755)|[FEA] Add iceberg merge on read integration tests.|
|[#12790](https://github.com/NVIDIA/spark-rapids/issues/12790)|[FEA] Support `unix_timestamp` for `America/Los_Angeles` timezone|
|[#12209](https://github.com/NVIDIA/spark-rapids/issues/12209)|[FEA] Supports reading iceberg table without deletions|
|[#9077](https://github.com/NVIDIA/spark-rapids/issues/9077)|[FEA] Support bit_or|
|[#12571](https://github.com/NVIDIA/spark-rapids/issues/12571)|[FEA] bit_count|
|[#12623](https://github.com/NVIDIA/spark-rapids/issues/12623)|[FEA] Add a minimum delta-33x module that compiles common code|
|[#10221](https://github.com/NVIDIA/spark-rapids/issues/10221)|[FEA] [follow-up] Parse just time to timestamp|
|[#12024](https://github.com/NVIDIA/spark-rapids/issues/12024)|[FEA] Support from_utc_timestamp for PST/EST/CST|
|[#11639](https://github.com/NVIDIA/spark-rapids/issues/11639)|[FEA] Support to_utc_timestamp for US/Central timezone|
|[#12537](https://github.com/NVIDIA/spark-rapids/issues/12537)|[FEA] Do we need a timezone for Date to String and String to Date|
|[#12519](https://github.com/NVIDIA/spark-rapids/issues/12519)|[FEA] CICD Ubuntu dockerfiles to support 24.04 LTS|
|[#6840](https://github.com/NVIDIA/spark-rapids/issues/6840)|[FEA] Support timestamp transitions to and from UTC for repeating rules (daylight savings time zones)|
|[#7035](https://github.com/NVIDIA/spark-rapids/issues/7035)|[FEA]Support function sha1|
|[#8511](https://github.com/NVIDIA/spark-rapids/issues/8511)|[FEA] Support conv function|
|[#12368](https://github.com/NVIDIA/spark-rapids/issues/12368)|[FEA] HybridScan: Add a specific SQL Hint to enable HYBRID_SCAN on certain tables|
|[#5221](https://github.com/NVIDIA/spark-rapids/issues/5221)|[FEA] Support function array_distinct|

### Performance
|||
|:---|:---|
|[#12472](https://github.com/NVIDIA/spark-rapids/issues/12472)|[FEA] Zero Config - Centralized Priority Assignment|
|[#12557](https://github.com/NVIDIA/spark-rapids/issues/12557)|[FEA] by default enable skip agg when the inputs of agg cannot reduce|
|[#12457](https://github.com/NVIDIA/spark-rapids/issues/12457)|[FEA] Add a specific physical rule for LocalAggregate|

### Bugs Fixed
|||
|:---|:---|
|[#12527](https://github.com/NVIDIA/spark-rapids/issues/12527)|[BUG][metric] Enhance the metric for GPU write|
|[#12666](https://github.com/NVIDIA/spark-rapids/issues/12666)|[BUG] Kudo serialization error on limit test|
|[#12619](https://github.com/NVIDIA/spark-rapids/issues/12619)|[BUG] delta_lake_test.py::test_delta_read integration test failure|
|[#12839](https://github.com/NVIDIA/spark-rapids/issues/12839)|XFAIL the failing tests and assert fallback in delta_lake_test.py|
|[#12855](https://github.com/NVIDIA/spark-rapids/issues/12855)|[BUG] delta_lake_test.py::test_delta_read_column_mapping fails with delta 3.3.0|
|[#12845](https://github.com/NVIDIA/spark-rapids/issues/12845)|[BUG] [test] Specify timezones in pytest according to JVM version|
|[#12852](https://github.com/NVIDIA/spark-rapids/issues/12852)|Remove Delta-3.3.0 support from the plugin for release 25.06|
|[#12856](https://github.com/NVIDIA/spark-rapids/issues/12856)|[BUG] 400+ iceberg test cases failed DATAGEN_SEED=1748978166, INJECT_OOM with spark356|
|[#12816](https://github.com/NVIDIA/spark-rapids/issues/12816)|[BUG] Iceberg nightly test failed code5 with spark355 "No tests collected"|
|[#11235](https://github.com/NVIDIA/spark-rapids/issues/11235)|[BUG] 229 Unit tests failed in Spark 400 against cuda 11 and scala 2.13|
|[#11556](https://github.com/NVIDIA/spark-rapids/issues/11556)|[BUG] [Spark 4] Exceptions from casting to `DATE`/`TIMESTAMP` do not match the Spark's exceptions, with ANSI enabled|
|[#12808](https://github.com/NVIDIA/spark-rapids/issues/12808)|[BUG] Integration test fails with undefined use_cdf|
|[#12742](https://github.com/NVIDIA/spark-rapids/issues/12742)|[BUG]  `hybrid_parquet_test.py` test fails on Spark 3.3|
|[#12015](https://github.com/NVIDIA/spark-rapids/issues/12015)|[BUG] Spark-4.0: Tests failures in WindowFunctionSuite|
|[#12022](https://github.com/NVIDIA/spark-rapids/issues/12022)|[BUG] Spark-4.0: Tests failures in HashAggregatesSuite|
|[#12013](https://github.com/NVIDIA/spark-rapids/issues/12013)|[BUG] Spark-4.0: Test failure in JoinsSuite|
|[#12009](https://github.com/NVIDIA/spark-rapids/issues/12009)|[BUG] Spark-4.0: Tests failures in CostBasedOptimizerSuite|
|[#12006](https://github.com/NVIDIA/spark-rapids/issues/12006)|[BUG] Spark-4.0: Tests failures in AdaptiveQueryExecSuite|
|[#12794](https://github.com/NVIDIA/spark-rapids/issues/12794)|[BUG] Iceberg 3.5.2 failed|
|[#12802](https://github.com/NVIDIA/spark-rapids/issues/12802)|[BUG] iceberg integration tests failed on python 3.8.|
|[#12778](https://github.com/NVIDIA/spark-rapids/issues/12778)|[BUG] getjsonobj generate different output as cpu|
|[#12012](https://github.com/NVIDIA/spark-rapids/issues/12012)|[BUG] Spark-4.0: Tests failures in ParquetWriterSuite|
|[#12758](https://github.com/NVIDIA/spark-rapids/issues/12758)|[BUG] dataframe write option for optimizeWrite for delta is ignored on databricks|
|[#12770](https://github.com/NVIDIA/spark-rapids/issues/12770)|[BUG] scala2.13 iceberg nightly test failed ClassNotFoundException: com.nvidia.spark.rapids.iceberg.IcebergProviderImpl|
|[#12004](https://github.com/NVIDIA/spark-rapids/issues/12004)|[BUG] Spark-4.0: Tests failures in StringOperatorsSuite|
|[#12014](https://github.com/NVIDIA/spark-rapids/issues/12014)|[BUG] Spark-4.0: Tests failures in TimeOperatorsSuite|
|[#12723](https://github.com/NVIDIA/spark-rapids/issues/12723)|[BUG] spark.rapids.sql.mode=explainOnly fails without GPU present|
|[#12608](https://github.com/NVIDIA/spark-rapids/issues/12608)|[BUG] Unhandled exceptions in Python worker|
|[#12711](https://github.com/NVIDIA/spark-rapids/issues/12711)|[BUG] Bad StringGen In test_formats_for_legacy_mode_other_formats_tz_rules|
|[#12703](https://github.com/NVIDIA/spark-rapids/issues/12703)|[BUG] Spilling state not cleaned properly when a task is killed by spark speculation|
|[#12682](https://github.com/NVIDIA/spark-rapids/issues/12682)|[BUG] Multiple Delta Lake tests failing|
|[#12677](https://github.com/NVIDIA/spark-rapids/issues/12677)|[BUG] `iceberg-common` produced scala classes can't be interpreted by scala compiler.|
|[#12702](https://github.com/NVIDIA/spark-rapids/issues/12702)|[BUG]  key not found: maxWritersNumber in databricks runtimes|
|[#12659](https://github.com/NVIDIA/spark-rapids/issues/12659)|[BUG] spark350 build failed GpuMultiFileBatchReader.java: error: not found: type ParquetFileInfo*|
|[#12003](https://github.com/NVIDIA/spark-rapids/issues/12003)|[BUG] Spark-4.0: Tests failures in ParseDateTimeSuite|
|[#12637](https://github.com/NVIDIA/spark-rapids/issues/12637)|[BUG] date_time_test.py failed to import zoneinfo for python 3.8 (databricks 11 and 12 runtimes)|
|[#12529](https://github.com/NVIDIA/spark-rapids/issues/12529)|[BUG][metric] build time total should exclude parent operator time.|
|[#12633](https://github.com/NVIDIA/spark-rapids/issues/12633)|[BUG] Integration Tests Fail For Months_Between|
|[#12625](https://github.com/NVIDIA/spark-rapids/issues/12625)|[BUG] UnsupportedOperationException When Reading Boolean Fields From Bigquery|
|[#12016](https://github.com/NVIDIA/spark-rapids/issues/12016)|[BUG] Spark-4.0: Tests failures in GpuLoreSuite|
|[#12017](https://github.com/NVIDIA/spark-rapids/issues/12017)|[BUG]Spark-4.0: Tests failures in ExpandExecSuite|
|[#12018](https://github.com/NVIDIA/spark-rapids/issues/12018)|[BUG] Spark-4.0: Tests failures in OrcScanSuite|
|[#12008](https://github.com/NVIDIA/spark-rapids/issues/12008)|[BUG] Spark-4.0: Test failure in  HashSortOptimizeSuite|
|[#12010](https://github.com/NVIDIA/spark-rapids/issues/12010)|[BUG] Spark-4.0: Test failure in GpuBringBackToHostSuite|
|[#12011](https://github.com/NVIDIA/spark-rapids/issues/12011)|[BUG] Spark-4.0: Test failure in ParquetFilterSuite|
|[#12002](https://github.com/NVIDIA/spark-rapids/issues/12002)|[BUG] Spark-4.0: Tests failures in OrcFilterSuite|
|[#12005](https://github.com/NVIDIA/spark-rapids/issues/12005)|[BUG] Spark-4.0: Tests failures in UnaryOperatorsSuite|
|[#12007](https://github.com/NVIDIA/spark-rapids/issues/12007)|[BUG] Spark-4.0: Test failure in DecimalBinaryOpSuite|
|[#12586](https://github.com/NVIDIA/spark-rapids/issues/12586)|[BUG] Integration tests  AssertionError: CPU and GPU list have different lengths at [] CPU: xxx GPU: xxx-1|
|[#12539](https://github.com/NVIDIA/spark-rapids/issues/12539)|[BUG] Unable to display rows from a Delta Table on Databricks 13.3 with Deletion Vectors enabled|
|[#12611](https://github.com/NVIDIA/spark-rapids/issues/12611)|[BUG] spark400 build failed: GpuAtomicCreateTableAsSelectExec.scala:77: not enough arguments for method writeToTable|
|[#12587](https://github.com/NVIDIA/spark-rapids/issues/12587)|[BUG] Installing pip on Databricks 11.3/12.2 failed due to `get-pip.py` is archived for python 3.8 an below|
|[#12583](https://github.com/NVIDIA/spark-rapids/issues/12583)|[BUG] test_std_variance and test_std_variance_nulls failed Input to `group_merge_m2` has invalid children type.|
|[#12468](https://github.com/NVIDIA/spark-rapids/issues/12468)|[BUG]GpuStddevPop produces different values than Spark when a group number is larger than Int.MaxValue|
|[#12566](https://github.com/NVIDIA/spark-rapids/issues/12566)|[BUG] Python UDF test failure|
|[#9784](https://github.com/NVIDIA/spark-rapids/issues/9784)|[BUG] test_conv_dec_to_from_hex for different datagen_seeds|
|[#12528](https://github.com/NVIDIA/spark-rapids/issues/12528)|[BUG][metric] time in heuristic (excl. SemWait)|
|[#12538](https://github.com/NVIDIA/spark-rapids/issues/12538)|[BUG] rapids_scala213_nightly-dev-github build failure spark400 |
|[#12532](https://github.com/NVIDIA/spark-rapids/issues/12532)|[BUG] missing companion metrics for Gpu Scan scan time|
|[#11326](https://github.com/NVIDIA/spark-rapids/issues/11326)|[BUG] Dynamic partitions metric for insert into hive appears to be off|
|[#12523](https://github.com/NVIDIA/spark-rapids/issues/12523)|[BUG] shuffle example dockerfile build fails rdma-core compilation with ubuntu22|
|[#12454](https://github.com/NVIDIA/spark-rapids/issues/12454)|[BUG] Crashing pyspark pytests lead to misreported errors|
|[#12453](https://github.com/NVIDIA/spark-rapids/issues/12453)|[BUG] loading LZ4 Parquet files SIGSEGs|
|[#12412](https://github.com/NVIDIA/spark-rapids/issues/12412)|[BUG] fastparquet_compatibility_test failed TypeError: 'type' object is not subscriptable with python3.8|
|[#12369](https://github.com/NVIDIA/spark-rapids/issues/12369)|[BUG] Kudo raised IllegalArgumentException when using with celeborn|

### PRs
|||
|:---|:---|
|[#12846](https://github.com/NVIDIA/spark-rapids/pull/12846)|[DOC] update the download doc for 2506 release [skip ci]|
|[#12861](https://github.com/NVIDIA/spark-rapids/pull/12861)|Update changelog for the v25.06.0 release [skip ci]|
|[#12909](https://github.com/NVIDIA/spark-rapids/pull/12909)|Update dependency version JNI, hybrid to 25.06.0; private to 25.06.1|
|[#12905](https://github.com/NVIDIA/spark-rapids/pull/12905)|xfail iceberg test_iceberg_v2_mixed_deletes [skip ci]|
|[#12894](https://github.com/NVIDIA/spark-rapids/pull/12894)|Disable multi thread reader with deletions in iceberg|
|[#12896](https://github.com/NVIDIA/spark-rapids/pull/12896)|Fix mem_debug rmm deadlink [skip ci]|
|[#12842](https://github.com/NVIDIA/spark-rapids/pull/12842)|Add the NV license to relevant py files[skip ci]|
|[#12868](https://github.com/NVIDIA/spark-rapids/pull/12868)|Fix double column mapping in delta 3.3 parquet reader|
|[#12862](https://github.com/NVIDIA/spark-rapids/pull/12862)|Fix a potential issue that could cause a NPE when dumping kudo tables|
|[#12851](https://github.com/NVIDIA/spark-rapids/pull/12851)|fix shebang in prioritize-commits.sh|
|[#12853](https://github.com/NVIDIA/spark-rapids/pull/12853)|Remove Delta Lake 3.3.0 support from the plugin for Spark 3.5.3, 3.5.4 and 3.5.5|
|[#12864](https://github.com/NVIDIA/spark-rapids/pull/12864)|[HybridScan] hotfix: mark the async thread preloading the HybridScan as pool thread|
|[#12859](https://github.com/NVIDIA/spark-rapids/pull/12859)|Fix iceberg npe during read.|
|[#12849](https://github.com/NVIDIA/spark-rapids/pull/12849)|Fix iceberg test missing problem|
|[#12825](https://github.com/NVIDIA/spark-rapids/pull/12825)|[Doc] Update doc description for castStringToTimestamp config|
|[#12813](https://github.com/NVIDIA/spark-rapids/pull/12813)|Spark-4.0: Skip scala tests when ansi mode is enabled|
|[#12792](https://github.com/NVIDIA/spark-rapids/pull/12792)|Added support for Spark 3.5.6|
|[#12598](https://github.com/NVIDIA/spark-rapids/pull/12598)|Fully support casting string with timezone to timestamp|
|[#12812](https://github.com/NVIDIA/spark-rapids/pull/12812)|Replace stale use_cdf parameter reference in test_delta_scan_read pytest|
|[#12817](https://github.com/NVIDIA/spark-rapids/pull/12817)|Fix the dead links issue for Pandas UDFs doc[skip ci]|
|[#12809](https://github.com/NVIDIA/spark-rapids/pull/12809)|Temporarily disable test case test_delta_scan_read|
|[#12810](https://github.com/NVIDIA/spark-rapids/pull/12810)|Fix ca cerificate shuffle [skip ci]|
|[#12783](https://github.com/NVIDIA/spark-rapids/pull/12783)|Spark-4.0: Skip ScalaTests when ANSI mode is enabled until implemented|
|[#12767](https://github.com/NVIDIA/spark-rapids/pull/12767)|Add support for DeltaScan without DV for Delta Lake 3.3.0|
|[#12795](https://github.com/NVIDIA/spark-rapids/pull/12795)|Fix spark 3.5.2 for iceberg|
|[#12744](https://github.com/NVIDIA/spark-rapids/pull/12744)|Add gpu delete filter for merge on read scan.|
|[#12798](https://github.com/NVIDIA/spark-rapids/pull/12798)|Update dockerfile to use HTTPS service for ubuntu repos [skip ci]|
|[#12759](https://github.com/NVIDIA/spark-rapids/pull/12759)|Fix dataframe writer option for delta optimizeWrite on Databricks|
|[#12766](https://github.com/NVIDIA/spark-rapids/pull/12766)|Use task priority for spill priority|
|[#12756](https://github.com/NVIDIA/spark-rapids/pull/12756)|GPUSemaphore now uses unified priority|
|[#12765](https://github.com/NVIDIA/spark-rapids/pull/12765)|Fix spill metrics for disk|
|[#12776](https://github.com/NVIDIA/spark-rapids/pull/12776)|Enable pr desc check [skip ci]|
|[#12760](https://github.com/NVIDIA/spark-rapids/pull/12760)|Accelerate premerge CI with non-PVC workspace|
|[#12771](https://github.com/NVIDIA/spark-rapids/pull/12771)|Fix iceberg 2.13 build|
|[#12684](https://github.com/NVIDIA/spark-rapids/pull/12684)|Spark-4.0: Fix/Skip non-ansi unit test failures|
|[#12732](https://github.com/NVIDIA/spark-rapids/pull/12732)|Enable iceberg reader without deletions.|
|[#12701](https://github.com/NVIDIA/spark-rapids/pull/12701)|migrate RapidsShuffleInternalManagerBase NvtxRanges to NvtxRangeWithDoc|
|[#12746](https://github.com/NVIDIA/spark-rapids/pull/12746)|Enable the companion metric in the static "ns" method|
|[#12724](https://github.com/NVIDIA/spark-rapids/pull/12724)|Fix explainMode to work when GPU isn't present|
|[#12688](https://github.com/NVIDIA/spark-rapids/pull/12688)|Propagate errors to JVM while initializing GPU memory in python worker|
|[#12606](https://github.com/NVIDIA/spark-rapids/pull/12606)|Support `bit_count` SQL function|
|[#12745](https://github.com/NVIDIA/spark-rapids/pull/12745)|Disable ArrayIntersect in hybrid scan filter pushdown|
|[#12725](https://github.com/NVIDIA/spark-rapids/pull/12725)|Unified priority task life-cycle|
|[#12739](https://github.com/NVIDIA/spark-rapids/pull/12739)|Output more information of the current GPU before an executor exits due to fatal cuda exceptions|
|[#12716](https://github.com/NVIDIA/spark-rapids/pull/12716)|Add Delta 3.3.0 module to Maven|
|[#12726](https://github.com/NVIDIA/spark-rapids/pull/12726)|Gpu iceberg multithread and coeleacing reader.|
|[#12731](https://github.com/NVIDIA/spark-rapids/pull/12731)|Update SHIM_M2DIR for all shims build if DEV_MODE is enabled [skip ci]|
|[#12722](https://github.com/NVIDIA/spark-rapids/pull/12722)|Add a new debug-level metric for MultiFileCloudPartitionReader: bufferTime with semaphore|
|[#12672](https://github.com/NVIDIA/spark-rapids/pull/12672)|Introduce iceberg parquet reader interface and perfile reader|
|[#12607](https://github.com/NVIDIA/spark-rapids/pull/12607)|Support bitwise aggregate functions (`bit_and`, `bit_or` and `bit_xor`) in groupby and reduction|
|[#12693](https://github.com/NVIDIA/spark-rapids/pull/12693)|Disable Databricks 11.3 shim build|
|[#12712](https://github.com/NVIDIA/spark-rapids/pull/12712)|Fix test_formats_for_legacy_mode_other_formats_tz_rules Test|
|[#12705](https://github.com/NVIDIA/spark-rapids/pull/12705)|fix spill framework bug causing gpu oom|
|[#12622](https://github.com/NVIDIA/spark-rapids/pull/12622)|Fix the failing delta log query test test_delta_read_with_deletion_vectors_enabled|
|[#12695](https://github.com/NVIDIA/spark-rapids/pull/12695)|Add parquet converter layer for shading.|
|[#12699](https://github.com/NVIDIA/spark-rapids/pull/12699)|Disable accelerated columnar to row to avoid data corruption|
|[#12704](https://github.com/NVIDIA/spark-rapids/pull/12704)|Update 'maxNumWriters' metric only when it exists|
|[#12681](https://github.com/NVIDIA/spark-rapids/pull/12681)|AsyncOutputStream should copy the buffer before scheduling write task|
|[#12374](https://github.com/NVIDIA/spark-rapids/pull/12374)|Use metrics about memory usage to adjust the number of concurrent tasks|
|[#12656](https://github.com/NVIDIA/spark-rapids/pull/12656)|Add a metric to represent the max number of open writers|
|[#12658](https://github.com/NVIDIA/spark-rapids/pull/12658)|Add gpu spark scan related classes for iceberg.|
|[#12687](https://github.com/NVIDIA/spark-rapids/pull/12687)|Revert "Add Delta 3.3.0 module to Maven  (#12624)"|
|[#12590](https://github.com/NVIDIA/spark-rapids/pull/12590)|Introduce GpuParquetReaderPostProcessor|
|[#12647](https://github.com/NVIDIA/spark-rapids/pull/12647)|refine bookkeeping logs to ease OOM related troubleshooting|
|[#12667](https://github.com/NVIDIA/spark-rapids/pull/12667)|Allow 0 for some footer fields [Databricks]|
|[#12613](https://github.com/NVIDIA/spark-rapids/pull/12613)|Supports ToPrettyString for date type in non-UTC timezone|
|[#12670](https://github.com/NVIDIA/spark-rapids/pull/12670)|Add deepwiki badge [skip ci]|
|[#12661](https://github.com/NVIDIA/spark-rapids/pull/12661)|Skip generating scaladoc for iceberg module [skip ci]|
|[#12624](https://github.com/NVIDIA/spark-rapids/pull/12624)|Add Delta 3.3.0 module to Maven|
|[#12636](https://github.com/NVIDIA/spark-rapids/pull/12636)|[Spark-4.0] ParseDateDateTimeSuite- Skip some of the unit tests when ansi is enabled|
|[#12594](https://github.com/NVIDIA/spark-rapids/pull/12594)|Move `SparkSchemaConverter` to top class|
|[#12602](https://github.com/NVIDIA/spark-rapids/pull/12602)|Update UCX default ver to 1.18.1 to support ubuntu24.04|
|[#12643](https://github.com/NVIDIA/spark-rapids/pull/12643)|Remove copied classes from iceberg library|
|[#12651](https://github.com/NVIDIA/spark-rapids/pull/12651)|xfail delta_lake_test.py::test_delta_read |
|[#12641](https://github.com/NVIDIA/spark-rapids/pull/12641)|Remove Usage of ZoneInfo|
|[#12558](https://github.com/NVIDIA/spark-rapids/pull/12558)|by default enable skip agg|
|[#12634](https://github.com/NVIDIA/spark-rapids/pull/12634)|Allow NonGPU for MonthsBetween Tests|
|[#12618](https://github.com/NVIDIA/spark-rapids/pull/12618)|Restrict the pyarrow version to 19.0.1 in requirements.txt|
|[#12627](https://github.com/NVIDIA/spark-rapids/pull/12627)|Always use boolean columnar to columnar conversion|
|[#12502](https://github.com/NVIDIA/spark-rapids/pull/12502)|Add Support For Timezone Rules|
|[#12592](https://github.com/NVIDIA/spark-rapids/pull/12592)|Add trampoline access to utility classes from iceberg-spark-runtime|
|[#12582](https://github.com/NVIDIA/spark-rapids/pull/12582)|Add iceberg-common module|
|[#12433](https://github.com/NVIDIA/spark-rapids/pull/12433)|Run unit tests with both ANSI modes enabled and disabled containing HashAggregates for Spark 4.0|
|[#12572](https://github.com/NVIDIA/spark-rapids/pull/12572)|Add sha1|
|[#12554](https://github.com/NVIDIA/spark-rapids/pull/12554)|Fallback Delta scan if deletion vectors are being read in Databricks 13.3|
|[#12616](https://github.com/NVIDIA/spark-rapids/pull/12616)|Fix Spark-4.0 build error - Add overwrite argument to writeToTable [skip ci]|
|[#12601](https://github.com/NVIDIA/spark-rapids/pull/12601)|Add DEV_MODE in nightly build script for dev build job [skip ci]|
|[#12588](https://github.com/NVIDIA/spark-rapids/pull/12588)|Fix the Python pip installation failure using 'get-pip.py'|
|[#12569](https://github.com/NVIDIA/spark-rapids/pull/12569)|Fix overflow for `stddev` and `variance` SQL functions|
|[#12394](https://github.com/NVIDIA/spark-rapids/pull/12394)|Add a way to override the PLUGIN_JARS|
|[#12414](https://github.com/NVIDIA/spark-rapids/pull/12414)|HybridScan: Fix supported expressions list in filter pushdown|
|[#12568](https://github.com/NVIDIA/spark-rapids/pull/12568)|A quick fix for the rmm import issue|
|[#12565](https://github.com/NVIDIA/spark-rapids/pull/12565)|fix broken nvtx_ranges link [skip ci]|
|[#12548](https://github.com/NVIDIA/spark-rapids/pull/12548)|Move parquet classes to parquet namespace|
|[#12559](https://github.com/NVIDIA/spark-rapids/pull/12559)|Update Databricks CICD default Ubuntu Docker image to 22.04|
|[#12437](https://github.com/NVIDIA/spark-rapids/pull/12437)|Fully support Conv expression|
|[#12541](https://github.com/NVIDIA/spark-rapids/pull/12541)|Exclude parent op time in Hash Aggregate heuristic time|
|[#12540](https://github.com/NVIDIA/spark-rapids/pull/12540)|HybridScan SQL HINT: adapt the spec change of LogicalRelation in spark400 [dataBricks]|
|[#12534](https://github.com/NVIDIA/spark-rapids/pull/12534)|companion metrics for all timing metrics|
|[#12496](https://github.com/NVIDIA/spark-rapids/pull/12496)|Align the semantics of the `dynamic partitions` metric for GPU write with Spark|
|[#12458](https://github.com/NVIDIA/spark-rapids/pull/12458)|Add a physical rule to fold two-stages LocalAggregate|
|[#12035](https://github.com/NVIDIA/spark-rapids/pull/12035)|Add class for documented Nvtx Ranges|
|[#12531](https://github.com/NVIDIA/spark-rapids/pull/12531)|Remove inactive user from github workflow[skip ci]|
|[#12525](https://github.com/NVIDIA/spark-rapids/pull/12525)|Explicitly disable LTO in shuffle example dockerfiles [skip ci]|
|[#12370](https://github.com/NVIDIA/spark-rapids/pull/12370)|HybridScan: Add a specific SQL Hint|
|[#12455](https://github.com/NVIDIA/spark-rapids/pull/12455)|Crash pytest worker if the Pyspark child JVM is unusable|
|[#12517](https://github.com/NVIDIA/spark-rapids/pull/12517)|Add TaskMetrics about the watermark of PinnedMemory and PageableMemory |
|[#12513](https://github.com/NVIDIA/spark-rapids/pull/12513)|Update CICD defaults to ubuntu 22.04|
|[#12512](https://github.com/NVIDIA/spark-rapids/pull/12512)|Fix auto merge conflict 12511 [skip ci]|
|[#12467](https://github.com/NVIDIA/spark-rapids/pull/12467)|LoRE: support dump data from shuffle related nodes|
|[#12306](https://github.com/NVIDIA/spark-rapids/pull/12306)|Accelerate ArrayDistinct|
|[#12463](https://github.com/NVIDIA/spark-rapids/pull/12463)|Fix the flaky ThrottlingExecutor task metrics test|
|[#12416](https://github.com/NVIDIA/spark-rapids/pull/12416)|Enable kudo by default.|
|[#12482](https://github.com/NVIDIA/spark-rapids/pull/12482)|Remove Lz4 xfail tests|
|[#12483](https://github.com/NVIDIA/spark-rapids/pull/12483)|Xfail json_matrix_test for Databricks 14.3|
|[#12459](https://github.com/NVIDIA/spark-rapids/pull/12459)|Unblock CI due to #12452 and #12453|
|[#12373](https://github.com/NVIDIA/spark-rapids/pull/12373)|Update dependency version JNI, private, hybrid to 25.06.0-SNAPSHOT|

## Release 25.04

### Features
|||
|:---|:---|
|[#5221](https://github.com/NVIDIA/spark-rapids/issues/5221)|[FEA] Support function array_distinct|
|[#5199](https://github.com/NVIDIA/spark-rapids/issues/5199)|[FEA]Support function approx_count_distinct|
|[#12367](https://github.com/NVIDIA/spark-rapids/issues/12367)|[FEA] Allow BigSizedJoinIterator#buildPartitioner to produce more subparittions to avoid CudfColumnSizeOverflowException|
|[#5224](https://github.com/NVIDIA/spark-rapids/issues/5224)|[FEA]Support array_position|
|[#12261](https://github.com/NVIDIA/spark-rapids/issues/12261)|[FEA] Enable Hive text writer by default|
|[#12297](https://github.com/NVIDIA/spark-rapids/issues/12297)|[FEA] Enable bucketed scan in Hybrid Scan|
|[#9161](https://github.com/NVIDIA/spark-rapids/issues/9161)|[AUDIT][SPARK-36612][SQL] Support left outer join build left or right outer join build right in shuffled hash join|
|[#4411](https://github.com/NVIDIA/spark-rapids/issues/4411)|[FEA] Add AST expression support for IsNull and IsNotNull|
|[#12187](https://github.com/NVIDIA/spark-rapids/issues/12187)|[FEA] Support AST with isNull function|
|[#12234](https://github.com/NVIDIA/spark-rapids/issues/12234)|[FEA] Support legacy mode for yyyy-mm-dd format|
|[#11607](https://github.com/NVIDIA/spark-rapids/issues/11607)|[FEA] it would be nice if we could support org.apache.spark.sql.catalyst.expressions.Slice|

### Performance
|||
|:---|:---|
|[#12256](https://github.com/NVIDIA/spark-rapids/issues/12256)|[FEA] BroadcastNestedLoopJoin does not support stream-side first adjustment|
|[#11386](https://github.com/NVIDIA/spark-rapids/issues/11386)|[FEA] move to multi-get_json_object for json_tuple|
|[#11999](https://github.com/NVIDIA/spark-rapids/issues/11999)|[FEA] HybridParquetScan: support asynchronous prefetching|
|[#11985](https://github.com/NVIDIA/spark-rapids/issues/11985)|[FEA] upgrade to ucx 1.18|
|[#11888](https://github.com/NVIDIA/spark-rapids/issues/11888)|[FEA] spill bounce buffer on disk->device and device->disk needs to be a pool|

### Bugs Fixed
|||
|:---|:---|
|[#12530](https://github.com/NVIDIA/spark-rapids/issues/12530)|[BUG] Outer join result is incorrect when Spark is 3.5.x, join side is outer side,  join on struct column and there is null|
|[#12410](https://github.com/NVIDIA/spark-rapids/issues/12410)|[BUG] ThrottlingExecutorSuite: test task metrics failed intermittently|
|[#12435](https://github.com/NVIDIA/spark-rapids/issues/12435)|[BUG] Running integration tests with `PERFILE` results in failed tests|
|[#12438](https://github.com/NVIDIA/spark-rapids/issues/12438)|[BUG] Incorrect supported ops CSV file generation|
|[#12360](https://github.com/NVIDIA/spark-rapids/issues/12360)|[BUG] delta_lake_test test_delta_deletion_vector cases failed in databricks 14.3 runtime|
|[#12123](https://github.com/NVIDIA/spark-rapids/issues/12123)|[BUG] delta_lake_update_test.test_delta_update_fallback_with_deletion_vectors failed assertion failed: Could not find RapidsDeltaWriteExec in the GPU plans with spark34Xshims|
|[#12405](https://github.com/NVIDIA/spark-rapids/issues/12405)|[BUG] test_delta_deletion_vector_fallback fails on [databricks] 14.3 CI|
|[#12460](https://github.com/NVIDIA/spark-rapids/issues/12460)|[BUG] Fallback to the CPU when FileSourceScan is reading Deletion Vectors on Databricks 14.3|
|[#12027](https://github.com/NVIDIA/spark-rapids/issues/12027)|[BUG] [DB 14.3] `tightBounds` stat in Delta Lake tables is set incorrectly|
|[#12379](https://github.com/NVIDIA/spark-rapids/issues/12379)|[BUG] test_parse_url_supported fails on [databricks] 14.3|
|[#12428](https://github.com/NVIDIA/spark-rapids/issues/12428)|[BUG] Multiple python udf integration test cases failed in DB 14.3|
|[#12408](https://github.com/NVIDIA/spark-rapids/issues/12408)|[BUG] Job timeout registration pathologically fails in some [databricks] CI_PART1 pipelines|
|[#12413](https://github.com/NVIDIA/spark-rapids/issues/12413)|[BUG] nightly shuffle multi-thread/UCX CI failed possibly out of memory or process/resource limits reached|
|[#12376](https://github.com/NVIDIA/spark-rapids/issues/12376)|[BUG] test_col_size_exceeding_cudf_limit fails on [databricks]|
|[#12378](https://github.com/NVIDIA/spark-rapids/issues/12378)|[BUG] test_parquet_partition_batch_row_count_only_splitting fails on [databricks] 14.3|
|[#12398](https://github.com/NVIDIA/spark-rapids/issues/12398)|[BUG] test_window_aggregate_udf_array_from_python fails on Spark 3.2.0|
|[#12158](https://github.com/NVIDIA/spark-rapids/issues/12158)|[BUG] split should not happen immediately after fallback on memory contention (OOM state machine bug) |
|[#12333](https://github.com/NVIDIA/spark-rapids/issues/12333)|[BUG]  Pandas UDF tests hang on Databricks 14.3|
|[#12331](https://github.com/NVIDIA/spark-rapids/issues/12331)|[BUG] Add timeouts to prevent hanging Pytests|
|[#12351](https://github.com/NVIDIA/spark-rapids/issues/12351)|[BUG] CudfColumnSizeOverflowException could cause misleading logs|
|[#12345](https://github.com/NVIDIA/spark-rapids/issues/12345)|[BUG] CoalescedHostResult should be spillable to avoid CPU OOM|
|[#12361](https://github.com/NVIDIA/spark-rapids/issues/12361)|[BUG] HybridExecutionUtils.useHybridScan fail to do type check correctly|
|[#12339](https://github.com/NVIDIA/spark-rapids/issues/12339)|[BUG] test_array_slice_with_negative_length and  test_array_slice_with_zero_start fail on Databricks 14.3|
|[#12332](https://github.com/NVIDIA/spark-rapids/issues/12332)|[BUG] Scala2.13 binary-dedupe fails with buildver 400 included|
|[#12228](https://github.com/NVIDIA/spark-rapids/issues/12228)|[BUG] spark.rapids.memory.gpu.pool=NONE is not respected with spark.rapids.sql.python.gpu.enabled=true|
|[#12314](https://github.com/NVIDIA/spark-rapids/issues/12314)|[BUG] Deprecated `spark.rapids.memory.gpu.pooling.enabled` overrules `spark.rapids.memory.gpu.pool`|
|[#11215](https://github.com/NVIDIA/spark-rapids/issues/11215)|[BUG] ai.rapids.cudf.CudfException: parallel_for failed: cudaErrorInvalidDevice: invalid device ordinal in ParquetChunkedReader|
|[#12326](https://github.com/NVIDIA/spark-rapids/issues/12326)|[BUG] orc_write_test.py::test_write_with_stripe_size_rows failed in HDFS env|
|[#12335](https://github.com/NVIDIA/spark-rapids/issues/12335)|[BUG] Iceberg loading should be disabled temporarily.|
|[#12062](https://github.com/NVIDIA/spark-rapids/issues/12062)|[BUG] Spark-4.0 build failure due to update in package name to org.apache.spark.sql.classic |
|[#12309](https://github.com/NVIDIA/spark-rapids/issues/12309)|[BUG] spark400 nightly failed multiple compilation issues|
|[#8268](https://github.com/NVIDIA/spark-rapids/issues/8268)|[BUG] NDS query 16 fails on EMR 6.10 with java.lang.ClassCastException|
|[#12319](https://github.com/NVIDIA/spark-rapids/issues/12319)|[BUG] leak in GpuSubPartitionHashJoin|
|[#12316](https://github.com/NVIDIA/spark-rapids/issues/12316)|[BUG] build fail TrampolineUtil is not a member of package ...sql.rapids.execution of integration_tests module|
|[#12245](https://github.com/NVIDIA/spark-rapids/issues/12245)|[BUG] Bloop project import for VSCode/Metals fails for Databricks Maven profiles|
|[#11735](https://github.com/NVIDIA/spark-rapids/issues/11735)|[BUG] GPU file writes only test writing a single row group or stripe|
|[#12277](https://github.com/NVIDIA/spark-rapids/issues/12277)|[BUG] QueryExecutionErrors.unexpectedValueForLengthInFunctionError being changed after Spark400+|
|[#12274](https://github.com/NVIDIA/spark-rapids/issues/12274)|[BUG] Databricks350+ has different interface on unexpectedValue-like SparkException|
|[#12267](https://github.com/NVIDIA/spark-rapids/issues/12267)|[BUG] HybridScan: Filter elimination may fail when AQE is enabled|
|[#12260](https://github.com/NVIDIA/spark-rapids/issues/12260)|[BUG] Databricks nightly build fails with "object roaringbitmap is not a member"|
|[#11385](https://github.com/NVIDIA/spark-rapids/issues/11385)|[BUG] get_json_object and json_tuple do not match on escaped characters in name|
|[#12164](https://github.com/NVIDIA/spark-rapids/issues/12164)|[BUG] Method mergeOnHost is deprecated in KudoSerializer|
|[#12047](https://github.com/NVIDIA/spark-rapids/issues/12047)|[BUG]  [DB 14.3] `numRecords` stat in Delta Lake tables is not set on GPU|
|[#12211](https://github.com/NVIDIA/spark-rapids/issues/12211)|[BUG] Deadlock during spill|
|[#12207](https://github.com/NVIDIA/spark-rapids/issues/12207)|[BUG] Build error in premerge on Spark 400 due to `Strategy` being not found|
|[#12147](https://github.com/NVIDIA/spark-rapids/issues/12147)|[BUG] test_min_max_in_groupby_and_reduction failed End address is too high for getInt when kudo_enabled:true intermittently|
|[#12056](https://github.com/NVIDIA/spark-rapids/issues/12056)|[TEST BUG] CollectLimit falling off of GPU on Databricks 14.3|
|[#12137](https://github.com/NVIDIA/spark-rapids/issues/12137)|[BUG] rapids_databricks_nightly-dev-github fails on DB 14.3 with RapidsShuffleManager class not found|
|[#12100](https://github.com/NVIDIA/spark-rapids/issues/12100)|[BUG] Potential duplicate call to the `close` method of a SpillableHostConcatResult in sized hash join.|
|[#12087](https://github.com/NVIDIA/spark-rapids/issues/12087)|[BUG] Nightly tests fail for Databricks 14.3 on branch-25.04|
|[#12020](https://github.com/NVIDIA/spark-rapids/issues/12020)|[BUG] mortgage_test.py failing on Databricks 14.3|
|[#11990](https://github.com/NVIDIA/spark-rapids/issues/11990)|[BUG] row-based_udf_test.py::test_hive_empty_* fail for Databricks 14.3|
|[#11988](https://github.com/NVIDIA/spark-rapids/issues/11988)|[BUG] datasourcev2_read_test failure on Databricks 14.3|
|[#12074](https://github.com/NVIDIA/spark-rapids/issues/12074)|[BUG] Build broken on Databricks 14.3 after #11996|

### PRs
|||
|:---|:---|
|[#12543](https://github.com/NVIDIA/spark-rapids/pull/12543)|Update changelog for the v25.04 release [skip ci]|
|[#12535](https://github.com/NVIDIA/spark-rapids/pull/12535)|Fix bug when join side is outer side|
|[#12494](https://github.com/NVIDIA/spark-rapids/pull/12494)|Update changelog for v25.04.0 release [skip ci]|
|[#12497](https://github.com/NVIDIA/spark-rapids/pull/12497)|[DOC] update the download page for 2504 release [skip ci]|
|[#12473](https://github.com/NVIDIA/spark-rapids/pull/12473)|Update dependency version JNI, private, hybrid to 25.04.0|
|[#12485](https://github.com/NVIDIA/spark-rapids/pull/12485)|Enable the  14.3 Shim|
|[#12490](https://github.com/NVIDIA/spark-rapids/pull/12490)|Fix bug where rows dropped if partitioned column size is too large|
|[#12481](https://github.com/NVIDIA/spark-rapids/pull/12481)|Fix incorrect supported DataSources in generated tools generated files|
|[#12445](https://github.com/NVIDIA/spark-rapids/pull/12445)|Tag UpdateCommand to fallback to the CPU when deletion vectors are enabled on Databricks 14.3|
|[#12488](https://github.com/NVIDIA/spark-rapids/pull/12488)|xfail json_matrix test on Databricks 14.3|
|[#12444](https://github.com/NVIDIA/spark-rapids/pull/12444)|Tag DeleteCommand to fallback to the CPU when deletion vectors are enabled on Databricks 14.3|
|[#12462](https://github.com/NVIDIA/spark-rapids/pull/12462)|Fallback GpuDeltaParquetFileFormat to CPU in presence of deletion vectors|
|[#12451](https://github.com/NVIDIA/spark-rapids/pull/12451)|Match logic of DV enabling in Gpu Stats Collection|
|[#12456](https://github.com/NVIDIA/spark-rapids/pull/12456)|Fix the Pandas UDF test failures on DB14.3|
|[#12464](https://github.com/NVIDIA/spark-rapids/pull/12464)|Disable HLLPP precision 4 due to cuCollection bug|
|[#12448](https://github.com/NVIDIA/spark-rapids/pull/12448)|Ignore flaky "test task metrics" from ThrottlingExecutorSuite|
|[#12426](https://github.com/NVIDIA/spark-rapids/pull/12426)|Use a single listener for all timeout fixture invocations|
|[#12397](https://github.com/NVIDIA/spark-rapids/pull/12397)|Set the `spark.rapids.sql.format.parquet.reader.type` to `AUTO` even on Databricks 14.3|
|[#12415](https://github.com/NVIDIA/spark-rapids/pull/12415)|Use 1hr as the default Spark action timeout|
|[#12419](https://github.com/NVIDIA/spark-rapids/pull/12419)|WAR: hardcode fsspec==2025.3.0 [skip ci]|
|[#12312](https://github.com/NVIDIA/spark-rapids/pull/12312)|fix premature concensus on "all blocked" in OOM state machine|
|[#12404](https://github.com/NVIDIA/spark-rapids/pull/12404)|Remove special timeout for udf_test|
|[#12372](https://github.com/NVIDIA/spark-rapids/pull/12372)|Allow BigSizedJoinIterator#buildPartitioner to produce more subparittions|
|[#12224](https://github.com/NVIDIA/spark-rapids/pull/12224)|add test case for issue 12158|
|[#12383](https://github.com/NVIDIA/spark-rapids/pull/12383)|Fix a hanging issue for Python UDF|
|[#11638](https://github.com/NVIDIA/spark-rapids/pull/11638)|Add support for Hyper Log Log PLus Plus(HLL++)|
|[#12346](https://github.com/NVIDIA/spark-rapids/pull/12346)|Implement Job-level timeout for pytests to avoid forever hangs|
|[#12380](https://github.com/NVIDIA/spark-rapids/pull/12380)|add doc for ai.rapids.memory.bookkeep|
|[#12352](https://github.com/NVIDIA/spark-rapids/pull/12352)|Refine split-retry logs to expose the real reason|
|[#12338](https://github.com/NVIDIA/spark-rapids/pull/12338)|print spillable summary on bookkeep|
|[#12375](https://github.com/NVIDIA/spark-rapids/pull/12375)|Updating link to RapidsUDF.java|
|[#12349](https://github.com/NVIDIA/spark-rapids/pull/12349)|Make KudoHostMergeResultWrapper spillable|
|[#12362](https://github.com/NVIDIA/spark-rapids/pull/12362)|HybridScan: a hotfix over previous changes on HybridExecutionUtils.useHybridScan|
|[#12363](https://github.com/NVIDIA/spark-rapids/pull/12363)|Added Databricks 14.3 Specific Error Messages|
|[#12357](https://github.com/NVIDIA/spark-rapids/pull/12357)|Add lore replay doc|
|[#12356](https://github.com/NVIDIA/spark-rapids/pull/12356)|Add 350db143 as supported by spark320 SparkSessionUtils|
|[#12325](https://github.com/NVIDIA/spark-rapids/pull/12325)|Add option to dump kudo tables for debugging|
|[#12342](https://github.com/NVIDIA/spark-rapids/pull/12342)|Shim functions to fix binary-dedupe failures on Spark-4.0|
|[#12238](https://github.com/NVIDIA/spark-rapids/pull/12238)|Add Limited Read Support for Deletion Vectors on Databricks 14.3|
|[#12344](https://github.com/NVIDIA/spark-rapids/pull/12344)|HybridScan: Log fallback reason when fallbacks to GpuScan|
|[#12308](https://github.com/NVIDIA/spark-rapids/pull/12308)|Add support for `org.apache.spark.sql.catalyst.expressions.ArrayPosition`|
|[#12317](https://github.com/NVIDIA/spark-rapids/pull/12317)|Always respect the new conf for RMM allocation mode|
|[#12334](https://github.com/NVIDIA/spark-rapids/pull/12334)|Run `test_write_with_stripe_size_rows` only on apache runtime and databricks runtime|
|[#12336](https://github.com/NVIDIA/spark-rapids/pull/12336)|Disable iceberg load temporarily|
|[#12313](https://github.com/NVIDIA/spark-rapids/pull/12313)|Fix Spark-4.0 build errors for tests module.|
|[#12248](https://github.com/NVIDIA/spark-rapids/pull/12248)|Reenable JsonTuple|
|[#12328](https://github.com/NVIDIA/spark-rapids/pull/12328)|Fix empty broadcast conversion|
|[#12324](https://github.com/NVIDIA/spark-rapids/pull/12324)|print the count of threads being blocked when OOM is thrown from retr…|
|[#12330](https://github.com/NVIDIA/spark-rapids/pull/12330)|[Doc] Update doc: Hybrid execution limitations [skip ci]|
|[#12303](https://github.com/NVIDIA/spark-rapids/pull/12303)|Support group by binary|
|[#12320](https://github.com/NVIDIA/spark-rapids/pull/12320)|Fix leaks when we re-partition in GpuSubPartitionHashJoin|
|[#12318](https://github.com/NVIDIA/spark-rapids/pull/12318)|Fix integration tests compilation due to missing function in TrampolineUtil|
|[#12249](https://github.com/NVIDIA/spark-rapids/pull/12249)|Fix bloop project generation on|
|[#12286](https://github.com/NVIDIA/spark-rapids/pull/12286)|short term solution for PinnedMemoryPool issue|
|[#12310](https://github.com/NVIDIA/spark-rapids/pull/12310)|Support more types for a Seq to dump the total size when an OOM happens|
|[#12300](https://github.com/NVIDIA/spark-rapids/pull/12300)|Fix Spark-4.0 build errors for sql-plugin, datagen and integration_tests module|
|[#11743](https://github.com/NVIDIA/spark-rapids/pull/11743)|Add test for ORC write with two stripes|
|[#12262](https://github.com/NVIDIA/spark-rapids/pull/12262)|Enable GPU acceleration for Hive delimited text write|
|[#12288](https://github.com/NVIDIA/spark-rapids/pull/12288)|Update spark400 version to 4.0.1-SNAPSHOT|
|[#12292](https://github.com/NVIDIA/spark-rapids/pull/12292)|HybridScan: Enable bucketed read|
|[#12285](https://github.com/NVIDIA/spark-rapids/pull/12285)|Support build side is join side for outer join|
|[#12296](https://github.com/NVIDIA/spark-rapids/pull/12296)|Fix auto merge conflict 12293 [skip ci]|
|[#12290](https://github.com/NVIDIA/spark-rapids/pull/12290)|Update supported exprs and docs to fix build error [skip ci]|
|[#12287](https://github.com/NVIDIA/spark-rapids/pull/12287)|Disable kudo by default|
|[#12178](https://github.com/NVIDIA/spark-rapids/pull/12178)|Update ExprChecks for RowNumber.|
|[#12221](https://github.com/NVIDIA/spark-rapids/pull/12221)|Add support for `ORC` stripe cudf size configs|
|[#12257](https://github.com/NVIDIA/spark-rapids/pull/12257)|Materialize the stream side first for BroadcastNestedLoopJoins|
|[#12283](https://github.com/NVIDIA/spark-rapids/pull/12283)|Fix an API change issue for DB350+|
|[#12192](https://github.com/NVIDIA/spark-rapids/pull/12192)|Support HiveHash in GPU partitioning|
|[#12268](https://github.com/NVIDIA/spark-rapids/pull/12268)|HybridScan: fix Filter Elimination under AQE|
|[#12270](https://github.com/NVIDIA/spark-rapids/pull/12270)|Add AST expression support for `IsNull` and `IsNotNull`|
|[#12269](https://github.com/NVIDIA/spark-rapids/pull/12269)|Fix auto merge conflict 12265 [skip ci]|
|[#12264](https://github.com/NVIDIA/spark-rapids/pull/12264)|Fix missing roaring bitmap dependency|
|[#12241](https://github.com/NVIDIA/spark-rapids/pull/12241)|Add validation for `start` and `length` parameters for `slice`|
|[#12251](https://github.com/NVIDIA/spark-rapids/pull/12251)|Support to retry the host allocation for hybrid converters|
|[#12263](https://github.com/NVIDIA/spark-rapids/pull/12263)|Fix auto merge conflict 12258 [skip ci]|
|[#12247](https://github.com/NVIDIA/spark-rapids/pull/12247)|Remove Multiget Testing Config|
|[#12208](https://github.com/NVIDIA/spark-rapids/pull/12208)|Upgrade iceberg support to 1.6.1|
|[#12244](https://github.com/NVIDIA/spark-rapids/pull/12244)|Add retry to cardinality estimation|
|[#12214](https://github.com/NVIDIA/spark-rapids/pull/12214)|Change JsonTuple Implemenation to MultiGetJSONObject|
|[#12233](https://github.com/NVIDIA/spark-rapids/pull/12233)|Add retry with SpillableHostBuffer to GPU scans|
|[#12236](https://github.com/NVIDIA/spark-rapids/pull/12236)|make kudo shuffle read retryable and spillable|
|[#12237](https://github.com/NVIDIA/spark-rapids/pull/12237)|Simplify mvn verify cache key [skip ci]|
|[#12227](https://github.com/NVIDIA/spark-rapids/pull/12227)|Refactor SparkSession to fix Spark-4.0 build|
|[#12181](https://github.com/NVIDIA/spark-rapids/pull/12181)|add memory bookkeeping for CPU Memory|
|[#12235](https://github.com/NVIDIA/spark-rapids/pull/12235)|Support legacy mode for yyyy-mm-dd format|
|[#12222](https://github.com/NVIDIA/spark-rapids/pull/12222)|Enable kudo serializer by default|
|[#12198](https://github.com/NVIDIA/spark-rapids/pull/12198)|Refactor SparkStrategy to fix Spark-4.0 build|
|[#12223](https://github.com/NVIDIA/spark-rapids/pull/12223)|Fix auto merge conflict 12220 [skip ci]|
|[#12148](https://github.com/NVIDIA/spark-rapids/pull/12148)|Add the pre-split support to GPU project|
|[#12183](https://github.com/NVIDIA/spark-rapids/pull/12183)|Add GPU support for `spark.sql.catalyst.expressions.Slice`|
|[#12196](https://github.com/NVIDIA/spark-rapids/pull/12196)|Write `numRecords` in CDC files regardless of whether we are using  deletion vectors|
|[#11998](https://github.com/NVIDIA/spark-rapids/pull/11998)|HybridParquetScan: support asynchronous prefetching|
|[#12201](https://github.com/NVIDIA/spark-rapids/pull/12201)|Try Revert #12068 [skip ci]|
|[#12191](https://github.com/NVIDIA/spark-rapids/pull/12191)|Fix auto merge conflict 12190 [skip ci]|
|[#12180](https://github.com/NVIDIA/spark-rapids/pull/12180)|Add Tests to JSON Tuples|
|[#12175](https://github.com/NVIDIA/spark-rapids/pull/12175)|Remove unnecessary output stream wrapper in kudo serializer.|
|[#12156](https://github.com/NVIDIA/spark-rapids/pull/12156)|Remove usage of deprecated kudo api|
|[#12161](https://github.com/NVIDIA/spark-rapids/pull/12161)|Make Partitioning shim-able like getExprs, getExecs and getScans|
|[#12146](https://github.com/NVIDIA/spark-rapids/pull/12146)|Remove unnecessary kudo debug metrics|
|[#12155](https://github.com/NVIDIA/spark-rapids/pull/12155)|Fix auto merge conflict 12153 [skip ci]|
|[#12141](https://github.com/NVIDIA/spark-rapids/pull/12141)|Xfail test_delta_update_fallback_with_deletion_vectors on|
|[#12138](https://github.com/NVIDIA/spark-rapids/pull/12138)|Use SPARK_SHIM_VER in RapidsShuffleManager package|
|[#12110](https://github.com/NVIDIA/spark-rapids/pull/12110)|Upgrade ucx to 1.18|
|[#12101](https://github.com/NVIDIA/spark-rapids/pull/12101)|Fix a potential NPE error in the sized hash join|
|[#12117](https://github.com/NVIDIA/spark-rapids/pull/12117)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#12105](https://github.com/NVIDIA/spark-rapids/pull/12105)|revert ucx 1.18 upgrade|
|[#12094](https://github.com/NVIDIA/spark-rapids/pull/12094)|Fix the flaky test in ThrottlingExecutorSuite|
|[#12048](https://github.com/NVIDIA/spark-rapids/pull/12048)|Fix failing nightly-build on branch-25.04 for Databricks 14.3|
|[#12081](https://github.com/NVIDIA/spark-rapids/pull/12081)|Actually enable/disable measuring kudo buffer copy according to config|
|[#12082](https://github.com/NVIDIA/spark-rapids/pull/12082)|Use 350db143 jars in integration tests on  14.3|
|[#12075](https://github.com/NVIDIA/spark-rapids/pull/12075)|Use SHUFFLE_KUDO_SERIALIZER_MEASURE_BUFFER_COPY_ENABLED for|
|[#11996](https://github.com/NVIDIA/spark-rapids/pull/11996)|Add option to disable measuring buffer copy|
|[#12030](https://github.com/NVIDIA/spark-rapids/pull/12030)|Update dependency version JNI, private, hybrid to 25.04.0-SNAPSHOT|

## Release 25.02

### Features
|||
|:---|:---|
|[#12225](https://github.com/NVIDIA/spark-rapids/issues/12225)|[FEA] Support Spark 3.5.5|
|[#11648](https://github.com/NVIDIA/spark-rapids/issues/11648)|[FEA] it would be nice if we could support org.apache.spark.sql.catalyst.expressions.Bin|
|[#11891](https://github.com/NVIDIA/spark-rapids/issues/11891)|[FEA] Support Spark 3.5.4 release|
|[#11928](https://github.com/NVIDIA/spark-rapids/issues/11928)|[FEA] make maxCpuBatchSize in GpuPartitioning configurable|
|[#10505](https://github.com/NVIDIA/spark-rapids/issues/10505)|[FOLLOW UP] Support `row_number()` filters for `GpuWindowGroupLimitExec`|
|[#11853](https://github.com/NVIDIA/spark-rapids/issues/11853)|[FEA] Ability to dump tables on a write|
|[#11804](https://github.com/NVIDIA/spark-rapids/issues/11804)|[FEA] Support TruncDate expression|
|[#11674](https://github.com/NVIDIA/spark-rapids/issues/11674)|[FEA] HiveHash supports nested types|

### Performance
|||
|:---|:---|
|[#11342](https://github.com/NVIDIA/spark-rapids/issues/11342)|[FEA] Put file writes in a background thread|
|[#11729](https://github.com/NVIDIA/spark-rapids/issues/11729)|[FEA] optimize the multi-contains generated by rlike|
|[#11860](https://github.com/NVIDIA/spark-rapids/issues/11860)|[FEA] kernel for date_trunc and trunc that has a scalar format|
|[#11812](https://github.com/NVIDIA/spark-rapids/issues/11812)|[FEA] Support escape characters in search list when rewrite `regexp_replace` to string replace|

### Bugs Fixed
|||
|:---|:---|
|[#12230](https://github.com/NVIDIA/spark-rapids/issues/12230)|[BUG] parquet_test.py::test_many_column_project test fails on GB100 and cuda12.8|
|[#12231](https://github.com/NVIDIA/spark-rapids/issues/12231)|[BUG] hash_aggregate_test.py::test_hash_multiple_grpby_pivot and row_conversion_test.py::test_row_conversions_fixed_width_wide fails on GB100 and cuda12.8|
|[#12152](https://github.com/NVIDIA/spark-rapids/issues/12152)|[BUG] mvn-verify-check failed cache non-snapshot deps|
|[#11835](https://github.com/NVIDIA/spark-rapids/issues/11835)|[BUG] Intermittent result discrepancy for NDS SF3K query86 on L40S|
|[#12111](https://github.com/NVIDIA/spark-rapids/issues/12111)|[BUG] Hit velox runtime error when filtering df with timestamp column inside when enabling hybrid|
|[#12091](https://github.com/NVIDIA/spark-rapids/issues/12091)|[BUG]An assertion error in the sized hash join|
|[#12113](https://github.com/NVIDIA/spark-rapids/issues/12113)|[BUG] HybridParquetScan fails on the `select count(1)` case|
|[#12096](https://github.com/NVIDIA/spark-rapids/issues/12096)|[BUG] CI_PART1 for DBR 14.3 hangs in the nightly pre-release pipeline|
|[#12076](https://github.com/NVIDIA/spark-rapids/issues/12076)|[BUG] ExtraPlugins might be loaded duplicated|
|[#11433](https://github.com/NVIDIA/spark-rapids/issues/11433)|[BUG] Spark UT framework: SPARK-34212 Parquet should read decimals correctly|
|[#12038](https://github.com/NVIDIA/spark-rapids/issues/12038)|[BUG] spark321 failed core dump in nightly|
|[#12046](https://github.com/NVIDIA/spark-rapids/issues/12046)|[BUG] orc_test fail non-UTC cases with Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec|
|[#12036](https://github.com/NVIDIA/spark-rapids/issues/12036)|[BUG] The check in assertIsOnTheGpu method to test if a plan is on the GPU is not accurate|
|[#11989](https://github.com/NVIDIA/spark-rapids/issues/11989)|[BUG] ParquetCachedBatchSerializer does not grab the GPU semaphore and does not have retry blocks|
|[#11651](https://github.com/NVIDIA/spark-rapids/issues/11651)|[BUG] Parse regular expressions using JDK to make error behavior more consistent between CPU and GPU|
|[#11628](https://github.com/NVIDIA/spark-rapids/issues/11628)|[BUG] Spark UT framework: select one deep nested complex field after join, IOException parsing parquet|
|[#11629](https://github.com/NVIDIA/spark-rapids/issues/11629)|[BUG] Spark UT framework: select one deep nested complex field after outer join, IOException parsing parquet|
|[#11620](https://github.com/NVIDIA/spark-rapids/issues/11620)|[BUG] Spark UT framework: "select a single complex field and partition column" causes java.lang.IndexOutOfBoundsException|
|[#11621](https://github.com/NVIDIA/spark-rapids/issues/11621)|[BUG] Spark UT framework: partial schema intersection - select missing subfield causes java.lang.IndexOutOfBoundsException|
|[#11619](https://github.com/NVIDIA/spark-rapids/issues/11619)|[BUG] Spark UT framework: "select a single complex field" causes java.lang.IndexOutOfBoundsException|
|[#11975](https://github.com/NVIDIA/spark-rapids/issues/11975)|[FOLLOWUP] We should have a separate version definition for the  rapids-4-spark-hybrid dependency|
|[#11976](https://github.com/NVIDIA/spark-rapids/issues/11976)|[BUG] scala 2.13 rapids_integration test failed|
|[#11971](https://github.com/NVIDIA/spark-rapids/issues/11971)|[BUG] scala213 nightly build failed rapids-4-spark-tests_2.13 of spark400|
|[#11903](https://github.com/NVIDIA/spark-rapids/issues/11903)|[BUG] Unexpected large output batches due to implementation defects|
|[#11914](https://github.com/NVIDIA/spark-rapids/issues/11914)|[BUG] Nightly CI does not upload sources and Javadoc JARs as the release script does|
|[#11896](https://github.com/NVIDIA/spark-rapids/issues/11896)|[BUG] [BUILD] CI passes without checking for `operatorsScore.csv`, `supportedExprs.csv` update|
|[#11895](https://github.com/NVIDIA/spark-rapids/issues/11895)|[BUG] BasePythonRunner has a new parameter metrics in Spark 4.0|
|[#11107](https://github.com/NVIDIA/spark-rapids/issues/11107)|[BUG] Rework RapidsShuffleManager initialization for Apache Spark 4.0.0|
|[#11897](https://github.com/NVIDIA/spark-rapids/issues/11897)|[BUG] JsonScanRetrySuite is failing in the CI.|
|[#11885](https://github.com/NVIDIA/spark-rapids/issues/11885)|[BUG] data corruption with spill framework changes|
|[#11762](https://github.com/NVIDIA/spark-rapids/issues/11762)|[BUG] Non-nullable bools in a nullable struct fails|
|[#11526](https://github.com/NVIDIA/spark-rapids/issues/11526)|Fix Arithmetic tests on Databricks 14.3|
|[#11866](https://github.com/NVIDIA/spark-rapids/issues/11866)|[BUG]The CHANGELOG is generated based on the project's roadmap rather than the target branch.|
|[#11749](https://github.com/NVIDIA/spark-rapids/issues/11749)|[BUG] Include Databricks 14.3 shim into the dist jar|
|[#11822](https://github.com/NVIDIA/spark-rapids/issues/11822)|[BUG] [Spark 4] Type mismatch Exceptions from DFUDFShims.scala with Spark-4.0.0 expressions.Expression|
|[#11760](https://github.com/NVIDIA/spark-rapids/issues/11760)|[BUG] isTimestamp leaks a Scalar|
|[#11796](https://github.com/NVIDIA/spark-rapids/issues/11796)|[BUG] populate-daily-cache action masks errors|
|[#10901](https://github.com/NVIDIA/spark-rapids/issues/10901)|from_json throws exception when the json's structure only partially matches the provided schema|
|[#11736](https://github.com/NVIDIA/spark-rapids/issues/11736)|[BUG] Orc writes don't fully support Booleans with nulls|

### PRs
|||
|:---|:---|
|[#12294](https://github.com/NVIDIA/spark-rapids/pull/12294)|Update changelog for the v25.02 release [skip ci]|
|[#12289](https://github.com/NVIDIA/spark-rapids/pull/12289)|Update dependency version JNI, private to 25.02.1|
|[#12282](https://github.com/NVIDIA/spark-rapids/pull/12282)|[DOC] update the download page for 2502.1 hot release [skip ci]|
|[#12280](https://github.com/NVIDIA/spark-rapids/pull/12280)|Disable hybrid cases for spark35X [skip ci]|
|[#12259](https://github.com/NVIDIA/spark-rapids/pull/12259)|Add Spark 3.5.5 shim|
|[#12252](https://github.com/NVIDIA/spark-rapids/pull/12252)|Update dependency version private to 25.02.1-SNAPSHOT|
|[#12253](https://github.com/NVIDIA/spark-rapids/pull/12253)|Accelerate nightly build [skip ci]|
|[#12239](https://github.com/NVIDIA/spark-rapids/pull/12239)|Mirror the Cloudera Maven repository|
|[#12189](https://github.com/NVIDIA/spark-rapids/pull/12189)|Update version to 25.02.1-SNAPSHOT|
|[#12204](https://github.com/NVIDIA/spark-rapids/pull/12204)|[DOC] address ghpage pr comments [skip ci]|
|[#12212](https://github.com/NVIDIA/spark-rapids/pull/12212)|Fix deadlock in BounceBufferPool|
|[#12205](https://github.com/NVIDIA/spark-rapids/pull/12205)|Workaround: tmply disable the HostAllocSuite to unblock CI [skip ci]|
|[#12188](https://github.com/NVIDIA/spark-rapids/pull/12188)|Update dependency version JNI to 25.02.1-SNAPSHOT|
|[#12157](https://github.com/NVIDIA/spark-rapids/pull/12157)|Add hybrid sha1 to mvn verify cache key [skip ci]|
|[#12154](https://github.com/NVIDIA/spark-rapids/pull/12154)|Update latest changelog [skip ci]|
|[#12129](https://github.com/NVIDIA/spark-rapids/pull/12129)|Update dependency version JNI, private, hybrid to 25.02.0 [skip ci]|
|[#12102](https://github.com/NVIDIA/spark-rapids/pull/12102)|[DOC] update the download page for 2502 release [skip ci]|
|[#12112](https://github.com/NVIDIA/spark-rapids/pull/12112)|HybridParquetScan: Fix velox runtime error in hybrid scan when filter timestamp|
|[#12092](https://github.com/NVIDIA/spark-rapids/pull/12092)|Fix an assertion error in the sized hash join|
|[#12114](https://github.com/NVIDIA/spark-rapids/pull/12114)|Fix HybridParquetScan over select(1)|
|[#12109](https://github.com/NVIDIA/spark-rapids/pull/12109)|revert ucx 1.18 upgrade|
|[#12103](https://github.com/NVIDIA/spark-rapids/pull/12103)|Revert "Enable event log for qualification & profiling tools testing …|
|[#12058](https://github.com/NVIDIA/spark-rapids/pull/12058)|upgrade jucx to 1.18|
|[#12077](https://github.com/NVIDIA/spark-rapids/pull/12077)|Fix the issue of ExtraPlugins loading multiple times|
|[#12080](https://github.com/NVIDIA/spark-rapids/pull/12080)|Quick fix for hybrid tests without git information.|
|[#12068](https://github.com/NVIDIA/spark-rapids/pull/12068)|Do not build Spark-4.0.0-SNAPSHOT [skip ci]|
|[#12064](https://github.com/NVIDIA/spark-rapids/pull/12064)|Run mvn with the project's pom.xml in hybrid_execution.sh|
|[#12060](https://github.com/NVIDIA/spark-rapids/pull/12060)|Relax decimal metadata checks for mismatched precision/scale|
|[#12054](https://github.com/NVIDIA/spark-rapids/pull/12054)|Update the version of the rapids-hybrid-execution dependency.|
|[#11970](https://github.com/NVIDIA/spark-rapids/pull/11970)|Explicitly set Delta table props to accommodate for different defaults|
|[#12044](https://github.com/NVIDIA/spark-rapids/pull/12044)|Set CI=true for complete failure reason in summary|
|[#12050](https://github.com/NVIDIA/spark-rapids/pull/12050)|Fixed `FileSourceScanExec` and `BatchScanExec` inadvertently falling to the CPU in non-utc orc tests |
|[#12000](https://github.com/NVIDIA/spark-rapids/pull/12000)|HybridParquetScan: Refine filter push down to avoid double evaluation|
|[#12037](https://github.com/NVIDIA/spark-rapids/pull/12037)|Removed the assumption if a plan is Columnar it probably is on the GPU|
|[#11991](https://github.com/NVIDIA/spark-rapids/pull/11991)|Grab the GPU Semaphore when reading cached batch data with the GPU|
|[#11880](https://github.com/NVIDIA/spark-rapids/pull/11880)|Perform handle spill IO outside of locked section in SpillFramework|
|[#11997](https://github.com/NVIDIA/spark-rapids/pull/11997)|Configure 14.3 support at runtime|
|[#11977](https://github.com/NVIDIA/spark-rapids/pull/11977)|Use bounce buffer pools in the Spill Framework|
|[#11912](https://github.com/NVIDIA/spark-rapids/pull/11912)|Ensure Java Compatibility Check for Regex Patterns|
|[#11984](https://github.com/NVIDIA/spark-rapids/pull/11984)|Include the size information when printing a SCB|
|[#11889](https://github.com/NVIDIA/spark-rapids/pull/11889)|Change order of initialization so pinned pool is available for spill framework buffers|
|[#11956](https://github.com/NVIDIA/spark-rapids/pull/11956)|Enable tests in RapidsParquetSchemaPruningSuite|
|[#11981](https://github.com/NVIDIA/spark-rapids/pull/11981)|Protect the batch read by a retry block in agg|
|[#11967](https://github.com/NVIDIA/spark-rapids/pull/11967)|Add support for `org.apache.spark.sql.catalyst.expressions.Bin`|
|[#11982](https://github.com/NVIDIA/spark-rapids/pull/11982)|Use common add-to-project action [skip ci]|
|[#11978](https://github.com/NVIDIA/spark-rapids/pull/11978)|Try to fix Scala 2.13 nightly failure: can not find version-def.sh|
|[#11973](https://github.com/NVIDIA/spark-rapids/pull/11973)|Minor change: Make Hybrid version a separate config like priviate repo|
|[#11969](https://github.com/NVIDIA/spark-rapids/pull/11969)|Support `raise_error()` on  14.3, Spark 4.|
|[#11972](https://github.com/NVIDIA/spark-rapids/pull/11972)|Update MockTaskContext to support new functions added in Spark-4.0|
|[#11906](https://github.com/NVIDIA/spark-rapids/pull/11906)|Enable Hybrid test cases in premerge/nightly CIs|
|[#11720](https://github.com/NVIDIA/spark-rapids/pull/11720)|Introduce hybrid (CPU) scan for Parquet read|
|[#11911](https://github.com/NVIDIA/spark-rapids/pull/11911)|Avoid concatentating multiple host buffers when reading Parquet|
|[#11960](https://github.com/NVIDIA/spark-rapids/pull/11960)|Remove jlowe as committer since he retired|
|[#11958](https://github.com/NVIDIA/spark-rapids/pull/11958)|Update to use vulnerability-scan runner [skip ci]|
|[#11955](https://github.com/NVIDIA/spark-rapids/pull/11955)|Add Spark 3.5.4 shim|
|[#11959](https://github.com/NVIDIA/spark-rapids/pull/11959)|Remove inactive user from github workflow[skip ci]|
|[#11952](https://github.com/NVIDIA/spark-rapids/pull/11952)|Fix auto merge conflict 11948 [skip ci]|
|[#11908](https://github.com/NVIDIA/spark-rapids/pull/11908)|Fix two potential OOM issues in GPU aggregate.|
|[#11936](https://github.com/NVIDIA/spark-rapids/pull/11936)|Add throttle time metrics for async write|
|[#11929](https://github.com/NVIDIA/spark-rapids/pull/11929)|make maxCpuBatchSize in GpuPartitioning configurable|
|[#11939](https://github.com/NVIDIA/spark-rapids/pull/11939)|[DOC] update release note to add spark 353 support [skip ci]|
|[#11920](https://github.com/NVIDIA/spark-rapids/pull/11920)|Remove Alluxio support|
|[#11938](https://github.com/NVIDIA/spark-rapids/pull/11938)|Update codeowners file to use team [skip ci]|
|[#11915](https://github.com/NVIDIA/spark-rapids/pull/11915)|Deploy the sources and Javadoc JARs in the nightly CICD [skip ci]|
|[#11917](https://github.com/NVIDIA/spark-rapids/pull/11917)|Fix issue with CustomerShuffleReaderExec metadata copy|
|[#11910](https://github.com/NVIDIA/spark-rapids/pull/11910)|fix bug: enable if_modified_files check for all shims in github actions [skip ci]|
|[#11909](https://github.com/NVIDIA/spark-rapids/pull/11909)|Update copyright year in NOTICE [skip ci]|
|[#11907](https://github.com/NVIDIA/spark-rapids/pull/11907)|Fix generated doc for xxhash64 for Spark 400|
|[#11905](https://github.com/NVIDIA/spark-rapids/pull/11905)|Fix the build error for Spark 400|
|[#11904](https://github.com/NVIDIA/spark-rapids/pull/11904)|Eagerly initialize RapidsShuffleManager for SPARK-45762|
|[#11865](https://github.com/NVIDIA/spark-rapids/pull/11865)|Async write support for ORC|
|[#11816](https://github.com/NVIDIA/spark-rapids/pull/11816)|address some comments for 11792|
|[#11789](https://github.com/NVIDIA/spark-rapids/pull/11789)|Improve the retry support for nondeterministic expressions|
|[#11898](https://github.com/NVIDIA/spark-rapids/pull/11898)|Add missing json reader options for JsonScanRetrySuite|
|[#11859](https://github.com/NVIDIA/spark-rapids/pull/11859)|Xxhash64 supports nested types|
|[#11890](https://github.com/NVIDIA/spark-rapids/pull/11890)|Update operatorsScore,supportedExprs for TruncDate, TruncTimestamp|
|[#11886](https://github.com/NVIDIA/spark-rapids/pull/11886)|Support group-limit optimization for `ROW_NUMBER`|
|[#11887](https://github.com/NVIDIA/spark-rapids/pull/11887)|Make sure that the chunked packer bounce buffer is realease after the synchronize|
|[#11894](https://github.com/NVIDIA/spark-rapids/pull/11894)|Fix bug: add timeout for cache deps steps [skip ci]|
|[#11810](https://github.com/NVIDIA/spark-rapids/pull/11810)|Use faster multi-contains in `rlike` regex rewrite|
|[#11882](https://github.com/NVIDIA/spark-rapids/pull/11882)|Add metrics GpuPartitioning.CopyToHostTime|
|[#11864](https://github.com/NVIDIA/spark-rapids/pull/11864)|Add support for dumping write data to try and reproduce error cases|
|[#11781](https://github.com/NVIDIA/spark-rapids/pull/11781)|Fix non-nullable under nullable struct write|
|[#11877](https://github.com/NVIDIA/spark-rapids/pull/11877)|Fix auto merge conflict 11873 [skip ci]|
|[#11833](https://github.com/NVIDIA/spark-rapids/pull/11833)|Support `trunc` and `date_trunc` SQL function|
|[#11660](https://github.com/NVIDIA/spark-rapids/pull/11660)|Add `HiveHash` support for nested types|
|[#11855](https://github.com/NVIDIA/spark-rapids/pull/11855)|Add integration test for parquet async writer|
|[#11747](https://github.com/NVIDIA/spark-rapids/pull/11747)|Spill framework refactor for better performance and extensibility|
|[#11870](https://github.com/NVIDIA/spark-rapids/pull/11870)|Workaround: Exclude cudf_log.txt in RAT check|
|[#11867](https://github.com/NVIDIA/spark-rapids/pull/11867)|Generate the CHANGELOG based on the PR's target branch [skip ci]|
|[#11821](https://github.com/NVIDIA/spark-rapids/pull/11821)|add a few more stage level metrics|
|[#11856](https://github.com/NVIDIA/spark-rapids/pull/11856)|Document Hive text write serialization format checks|
|[#11805](https://github.com/NVIDIA/spark-rapids/pull/11805)|Enable some integration tests for `from_json`|
|[#11840](https://github.com/NVIDIA/spark-rapids/pull/11840)|Support running Databricks CI_PART2 integration tests with JARs built by CI_PART1|
|[#11847](https://github.com/NVIDIA/spark-rapids/pull/11847)|Some small improvements|
|[#11811](https://github.com/NVIDIA/spark-rapids/pull/11811)|Fix bug: populate cache deps [skip ci]|
|[#11817](https://github.com/NVIDIA/spark-rapids/pull/11817)|Optimize Databricks Jenkins scripts [skip ci]|
|[#11829](https://github.com/NVIDIA/spark-rapids/pull/11829)|Some minor improvements identified during benchmark|
|[#11827](https://github.com/NVIDIA/spark-rapids/pull/11827)|Deal with Spark changes for column<->expression conversions|
|[#11826](https://github.com/NVIDIA/spark-rapids/pull/11826)|Balance the pre-merge CI job's time for the ci_1 and ci_2 tests|
|[#11784](https://github.com/NVIDIA/spark-rapids/pull/11784)|Add support for kudo write metrics|
|[#11783](https://github.com/NVIDIA/spark-rapids/pull/11783)|Fix the task count check in TrafficController|
|[#11813](https://github.com/NVIDIA/spark-rapids/pull/11813)|Support some escape chars when rewriting regexp_replace to stringReplace|
|[#11819](https://github.com/NVIDIA/spark-rapids/pull/11819)|Add the 'test_type' parameter for Databricks script|
|[#11786](https://github.com/NVIDIA/spark-rapids/pull/11786)|Enable license header check|
|[#11791](https://github.com/NVIDIA/spark-rapids/pull/11791)|Incorporate checksum of internal dependencies in the GH cache key [skip ci]|
|[#11788](https://github.com/NVIDIA/spark-rapids/pull/11788)|Support running Databricks CI_PART2 integration tests with JARs built by CI_PART1|
|[#11778](https://github.com/NVIDIA/spark-rapids/pull/11778)|Remove unnecessary toBeReturned field from serialized batch iterators|
|[#11785](https://github.com/NVIDIA/spark-rapids/pull/11785)|Update advanced configs introduced by private repo [skip ci]|
|[#11772](https://github.com/NVIDIA/spark-rapids/pull/11772)|Update rapids JNI and private dependency to 25.02.0-SNAPSHOT|
|[#11756](https://github.com/NVIDIA/spark-rapids/pull/11756)|remove excluded release shim and TODO|

