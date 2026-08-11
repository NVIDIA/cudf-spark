# Change log
Generated on 2022-01-28

## Release 0.5

### Features
|||
|:---|:---|
|[#938](https://github.com/NVIDIA/spark-rapids/issues/938)|[FEA] Have hashed shuffle match spark|
|[#1604](https://github.com/NVIDIA/spark-rapids/issues/1604)|[FEA] Support casting structs to strings |
|[#1920](https://github.com/NVIDIA/spark-rapids/issues/1920)|[FEA] Support murmur3 hashing of structs|
|[#2018](https://github.com/NVIDIA/spark-rapids/issues/2018)|[FEA] A way for user to find out the plugin version and cudf version in REPL|
|[#77](https://github.com/NVIDIA/spark-rapids/issues/77)|[FEA] Support ArrayContains|
|[#1721](https://github.com/NVIDIA/spark-rapids/issues/1721)|[FEA] build cudf jars with NVTX enabled|
|[#1782](https://github.com/NVIDIA/spark-rapids/issues/1782)|[FEA] Shim layers to support spark versions|
|[#1625](https://github.com/NVIDIA/spark-rapids/issues/1625)|[FEA] Support Decimal Casts to String and String to Decimal|
|[#166](https://github.com/NVIDIA/spark-rapids/issues/166)|[FEA] Support get_json_object|
|[#1698](https://github.com/NVIDIA/spark-rapids/issues/1698)|[FEA] Support casting structs to string|
|[#1912](https://github.com/NVIDIA/spark-rapids/issues/1912)|[FEA] Let `Scalar Pandas UDF ` support array of struct type.|
|[#1136](https://github.com/NVIDIA/spark-rapids/issues/1136)|[FEA] Audit: Script to list commits between different Spark versions/tags|
|[#1921](https://github.com/NVIDIA/spark-rapids/issues/1921)|[FEA] cudf version check should be lenient on later patch version|
|[#19](https://github.com/NVIDIA/spark-rapids/issues/19)|[FEA] Out of core sorts|

### Performance
|||
|:---|:---|
|[#2090](https://github.com/NVIDIA/spark-rapids/issues/2090)|[FEA] Make row count estimates available to the cost-based optimizer|
|[#1341](https://github.com/NVIDIA/spark-rapids/issues/1341)|Optimize unnecessary columnar->row->columnar transitions with AQE|
|[#1558](https://github.com/NVIDIA/spark-rapids/issues/1558)|[FEA] Initialize UCX early|
|[#1633](https://github.com/NVIDIA/spark-rapids/issues/1633)|[FEA] Implement a cost-based optimizer|
|[#1727](https://github.com/NVIDIA/spark-rapids/issues/1727)|[FEA] Put RangePartitioner data path on the GPU|

### Bugs Fixed
|||
|:---|:---|
|[#2279](https://github.com/NVIDIA/spark-rapids/issues/2279)|[BUG] Hash Partitioning can fail for very small batches|
|[#2314](https://github.com/NVIDIA/spark-rapids/issues/2314)|[BUG] v0.5.0 pre-release pytests join_test.py::test_hash_join_array FAILED on SPARK-EGX Yarn Cluster|
|[#2317](https://github.com/NVIDIA/spark-rapids/issues/2317)|[BUG] GpuColumnarToRowIterator can stop after receiving an empty batch|
|[#2244](https://github.com/NVIDIA/spark-rapids/issues/2244)|[BUG] Executors hanging when running NDS benchmarks|
|[#2278](https://github.com/NVIDIA/spark-rapids/issues/2278)|[BUG] FullOuter join can produce too many results|
|[#2220](https://github.com/NVIDIA/spark-rapids/issues/2220)|[BUG] csv_test.py::test_csv_fallback FAILED on the EMR Cluster|
|[#2225](https://github.com/NVIDIA/spark-rapids/issues/2225)|[BUG] GpuSort fails on tables containing arrays.|
|[#2232](https://github.com/NVIDIA/spark-rapids/issues/2232)|[BUG] hash_aggregate_test.py::test_hash_grpby_pivot FAILED on the Databricks Cluster|
|[#2231](https://github.com/NVIDIA/spark-rapids/issues/2231)|[BUG]string_test.py::test_re_replace FAILED on the Dataproc Cluster|
|[#2042](https://github.com/NVIDIA/spark-rapids/issues/2042)|[BUG] NDS q14a fails with "GpuColumnarToRow does not implement doExecuteBroadcast"|
|[#2203](https://github.com/NVIDIA/spark-rapids/issues/2203)|[BUG] Spark nightly cache tests fail with -- master flag|
|[#2230](https://github.com/NVIDIA/spark-rapids/issues/2230)|[BUG] qa_nightly_select_test.py::test_select FAILED on the Dataproc Cluster|
|[#1711](https://github.com/NVIDIA/spark-rapids/issues/1711)|[BUG] find a way to stop allocating from RMM on the shuffle-client thread|
|[#2109](https://github.com/NVIDIA/spark-rapids/issues/2109)|[BUG] Fix high priority violations detected by code analysis tools|
|[#2217](https://github.com/NVIDIA/spark-rapids/issues/2217)|[BUG] qa_nightly_select_test failure in test_select |
|[#2127](https://github.com/NVIDIA/spark-rapids/issues/2127)|[BUG] Parsing with two-digit year should fall back to CPU|
|[#2078](https://github.com/NVIDIA/spark-rapids/issues/2078)|[BUG] java.lang.ArithmeticException: divide by zero when spark.sql.ansi.enabled=true|
|[#2048](https://github.com/NVIDIA/spark-rapids/issues/2048)|[BUG] split function+ repartition result in "ai.rapids.cudf.CudaException: device-side assert triggered"|
|[#2036](https://github.com/NVIDIA/spark-rapids/issues/2036)|[BUG] Stackoverflow when writing wide parquet files.|
|[#1973](https://github.com/NVIDIA/spark-rapids/issues/1973)|[BUG] generate_expr_test FAILED on Dataproc Cluster|
|[#2079](https://github.com/NVIDIA/spark-rapids/issues/2079)|[BUG] koalas.sql fails with java.lang.ArrayIndexOutOfBoundsException|
|[#217](https://github.com/NVIDIA/spark-rapids/issues/217)|[BUG] CudaUtil should be removed|
|[#1550](https://github.com/NVIDIA/spark-rapids/issues/1550)|[BUG] The ORC output data of a query is not readable|
|[#2074](https://github.com/NVIDIA/spark-rapids/issues/2074)|[BUG] Intermittent NPE in RapidsBufferCatalog when running test suite|
|[#2027](https://github.com/NVIDIA/spark-rapids/issues/2027)|[BUG] udf_cudf_test.py integration tests fail |
|[#1899](https://github.com/NVIDIA/spark-rapids/issues/1899)|[BUG] Some queries fail when cost-based optimizations are enabled|
|[#1914](https://github.com/NVIDIA/spark-rapids/issues/1914)|[BUG] Add in float, double, timestamp, and date support to murmur3|
|[#2014](https://github.com/NVIDIA/spark-rapids/issues/2014)|[BUG] earlyStart option added in 0.5 can cause errors when starting UCX|
|[#1984](https://github.com/NVIDIA/spark-rapids/issues/1984)|[BUG] NDS q58 Decimal scale (59) cannot be greater than precision (38).|
|[#2001](https://github.com/NVIDIA/spark-rapids/issues/2001)|[BUG] RapidsShuffleManager didn't pass `dirs` to `getBlockData` from a wrapped `ShuffleBlockResolver`|
|[#1797](https://github.com/NVIDIA/spark-rapids/issues/1797)|[BUG] occasional crashes in CI|
|[#1861](https://github.com/NVIDIA/spark-rapids/issues/1861)|Encountered column data outside the range of input buffer|
|[#1905](https://github.com/NVIDIA/spark-rapids/issues/1905)|[BUG] Large concat task time in GpuShuffleCoalesce with pinned memory pool|
|[#1638](https://github.com/NVIDIA/spark-rapids/issues/1638)|[BUG] Tests `test_window_aggs_for_rows_collect_list` fails when there are null values in columns.|
|[#1864](https://github.com/NVIDIA/spark-rapids/issues/1864)|[BUG]HostColumnarToGPU inefficient when only doing count()|
|[#1862](https://github.com/NVIDIA/spark-rapids/issues/1862)|[BUG] spark 3.2.0-snapshot integration test failed due to conf change|
|[#1844](https://github.com/NVIDIA/spark-rapids/issues/1844)|[BUG] branch-0.5 nightly IT FAILED on the The mortgage ETL test "Could not read footer for file: file:/xxx/xxx.snappy.parquet"|
|[#1627](https://github.com/NVIDIA/spark-rapids/issues/1627)|[BUG] GDS exception when restoring spilled buffer|
|[#1802](https://github.com/NVIDIA/spark-rapids/issues/1802)|[BUG] Many decimal integration test failures for 0.5|

### PRs
|||
|:---|:---|
|[#2326](https://github.com/NVIDIA/spark-rapids/pull/2326)|Update changelog for 0.5.0 release|
|[#2316](https://github.com/NVIDIA/spark-rapids/pull/2316)|Update doc to note that single quoted json strings are not ok|
|[#2319](https://github.com/NVIDIA/spark-rapids/pull/2319)|Disable hash partitioning on arrays|
|[#2318](https://github.com/NVIDIA/spark-rapids/pull/2318)|Fix ColumnarToRowIterator handling of empty batches|
|[#2304](https://github.com/NVIDIA/spark-rapids/pull/2304)|Update CHANGELOG.md|
|[#2301](https://github.com/NVIDIA/spark-rapids/pull/2301)|Update doc to reflect nanosleep problem with 460.32.03|
|[#2298](https://github.com/NVIDIA/spark-rapids/pull/2298)|Update changelog for v0.5.0 release [skip ci]|
|[#2293](https://github.com/NVIDIA/spark-rapids/pull/2293)|update cudf version to 0.19.2|
|[#2289](https://github.com/NVIDIA/spark-rapids/pull/2289)|Update docs to warn against 450.80.02 driver with 10.x toolkit|
|[#2285](https://github.com/NVIDIA/spark-rapids/pull/2285)|Require single batch for full outer join streaming|
|[#2281](https://github.com/NVIDIA/spark-rapids/pull/2281)|Remove download section for unreleased 0.4.2|
|[#2264](https://github.com/NVIDIA/spark-rapids/pull/2264)|Add spark312 and spark320 versions of cache serializer|
|[#2254](https://github.com/NVIDIA/spark-rapids/pull/2254)|updated gcp docs with custom dataproc image instructions|
|[#2247](https://github.com/NVIDIA/spark-rapids/pull/2247)|Allow specifying a superclass for non-GPU execs|
|[#2235](https://github.com/NVIDIA/spark-rapids/pull/2235)|Fix distributed cache to read requested schema |
|[#2261](https://github.com/NVIDIA/spark-rapids/pull/2261)|Make CBO row count test more robust|
|[#2237](https://github.com/NVIDIA/spark-rapids/pull/2237)|update cudf version to 0.19.1|
|[#2240](https://github.com/NVIDIA/spark-rapids/pull/2240)|Get the correct 'PIPESTATUS' in bash [skip ci]|
|[#2242](https://github.com/NVIDIA/spark-rapids/pull/2242)|Add shuffle doc section on the periodicGC configuration|
|[#2251](https://github.com/NVIDIA/spark-rapids/pull/2251)|Fix issue when out of core sorting nested data types|
|[#2204](https://github.com/NVIDIA/spark-rapids/pull/2204)|Run nightly tests for ParquetCachedBatchSerializer|
|[#2245](https://github.com/NVIDIA/spark-rapids/pull/2245)|Fix pivot bug for decimalType|
|[#2093](https://github.com/NVIDIA/spark-rapids/pull/2093)|Initial implementation of row count estimates in cost-based optimizer|
|[#2188](https://github.com/NVIDIA/spark-rapids/pull/2188)|Support GPU broadcast exchange reuse to feed CPU BHJ when AQE is enabled|
|[#2227](https://github.com/NVIDIA/spark-rapids/pull/2227)|ParquetCachedBatchSerializer broadcast AllConfs instead of SQLConf to fix distributed mode|
|[#2223](https://github.com/NVIDIA/spark-rapids/pull/2223)|Adds subquery aggregate tests from SPARK-31620|
|[#2222](https://github.com/NVIDIA/spark-rapids/pull/2222)|Remove groupId already specified in parent pom|
|[#2209](https://github.com/NVIDIA/spark-rapids/pull/2209)|Fixed a few issues with out of core sort|
|[#2218](https://github.com/NVIDIA/spark-rapids/pull/2218)|Fix incorrect RegExpReplace children handling on Spark 3.1+|
|[#2207](https://github.com/NVIDIA/spark-rapids/pull/2207)|fix batch size default values in the tuning guide|
|[#2208](https://github.com/NVIDIA/spark-rapids/pull/2208)|Revert "add nightly cache tests (#2083)"|
|[#2206](https://github.com/NVIDIA/spark-rapids/pull/2206)|Fix shim301db build|
|[#2192](https://github.com/NVIDIA/spark-rapids/pull/2192)|Fix index-based access to the head elements|
|[#2210](https://github.com/NVIDIA/spark-rapids/pull/2210)|Avoid redundant collection conversions|
|[#2190](https://github.com/NVIDIA/spark-rapids/pull/2190)|JNI fixes for StringWordCount native UDF example|
|[#2086](https://github.com/NVIDIA/spark-rapids/pull/2086)|Updating documentation for data format support|
|[#2172](https://github.com/NVIDIA/spark-rapids/pull/2172)|Remove easy unused symbols|
|[#2089](https://github.com/NVIDIA/spark-rapids/pull/2089)|Update PandasUDF doc|
|[#2195](https://github.com/NVIDIA/spark-rapids/pull/2195)|fix cudf 0.19.0 download link [skip ci]|
|[#2175](https://github.com/NVIDIA/spark-rapids/pull/2175)|Branch 0.5 doc update|
|[#2168](https://github.com/NVIDIA/spark-rapids/pull/2168)|Simplify GpuExpressions w/ withResourceIfAllowed|
|[#2055](https://github.com/NVIDIA/spark-rapids/pull/2055)|Support PivotFirst|
|[#2183](https://github.com/NVIDIA/spark-rapids/pull/2183)|GpuParquetScan#readBufferToTable remove dead code|
|[#2129](https://github.com/NVIDIA/spark-rapids/pull/2129)|Fall back to CPU when parsing two-digit years|
|[#2083](https://github.com/NVIDIA/spark-rapids/pull/2083)|add nightly cache tests|
|[#2151](https://github.com/NVIDIA/spark-rapids/pull/2151)|add corresponding close call for HostMemoryOutputStream|
|[#2169](https://github.com/NVIDIA/spark-rapids/pull/2169)|Work around bug in Spark for integration test|
|[#2130](https://github.com/NVIDIA/spark-rapids/pull/2130)|Fix divide-by-zero in GpuAverage with ansi mode|
|[#2149](https://github.com/NVIDIA/spark-rapids/pull/2149)|Auto generate the supported types for the file formats|
|[#2072](https://github.com/NVIDIA/spark-rapids/pull/2072)|Disable CSV parsing by default and update tests to better show what is left|
|[#2157](https://github.com/NVIDIA/spark-rapids/pull/2157)|fix merge conflict for 0.4.2 [skip ci]|
|[#2144](https://github.com/NVIDIA/spark-rapids/pull/2144)|Allow array and struct types to pass thru when doing join|
|[#2145](https://github.com/NVIDIA/spark-rapids/pull/2145)|Avoid GPU shuffle for round-robin of unsortable types|
|[#2021](https://github.com/NVIDIA/spark-rapids/pull/2021)|Add in support for murmur3 hashing of structs|
|[#2128](https://github.com/NVIDIA/spark-rapids/pull/2128)|Add in Partition type check support|
|[#2116](https://github.com/NVIDIA/spark-rapids/pull/2116)|Add dynamic Spark configuration for Databricks|
|[#2132](https://github.com/NVIDIA/spark-rapids/pull/2132)|Log plugin and cudf versions on startup|
|[#2135](https://github.com/NVIDIA/spark-rapids/pull/2135)|Disable Spark 3.2 shim by default|
|[#2125](https://github.com/NVIDIA/spark-rapids/pull/2125)|enable auto-merge from 0.5 to 0.6 [skip ci]|
|[#2120](https://github.com/NVIDIA/spark-rapids/pull/2120)|Materialize Stream before serialization|
|[#2119](https://github.com/NVIDIA/spark-rapids/pull/2119)|Add more comprehensive documentation on supported date formats|
|[#1717](https://github.com/NVIDIA/spark-rapids/pull/1717)|Decimal32 support|
|[#2114](https://github.com/NVIDIA/spark-rapids/pull/2114)|Modified the Download page for 0.4.1 and updated doc to point to K8s guide|
|[#2106](https://github.com/NVIDIA/spark-rapids/pull/2106)|Fix some buffer leaks|
|[#2097](https://github.com/NVIDIA/spark-rapids/pull/2097)|fix the bound row project empty issue in row frame|
|[#2099](https://github.com/NVIDIA/spark-rapids/pull/2099)|Remove verbose log prints to make the build/test log clean|
|[#2105](https://github.com/NVIDIA/spark-rapids/pull/2105)|Cleanup prior Spark sessions in tests consistently|
|[#2104](https://github.com/NVIDIA/spark-rapids/pull/2104)| Clone apache spark source code to parse the git commit IDs|
|[#2095](https://github.com/NVIDIA/spark-rapids/pull/2095)|fix refcount when materializing device buffer from GDS|
|[#2100](https://github.com/NVIDIA/spark-rapids/pull/2100)|[BUG] add wget for fetching conda [skip ci]|
|[#2096](https://github.com/NVIDIA/spark-rapids/pull/2096)|Adjust images for integration tests|
|[#2094](https://github.com/NVIDIA/spark-rapids/pull/2094)|Changed name of parquet files for Mortgage ETL Integration test|
|[#2035](https://github.com/NVIDIA/spark-rapids/pull/2035)|Accelerate data transfer for map Pandas UDF plan|
|[#2050](https://github.com/NVIDIA/spark-rapids/pull/2050)|stream shuffle buffers from GDS to UCX|
|[#2084](https://github.com/NVIDIA/spark-rapids/pull/2084)|Enable ORC write by default|
|[#2088](https://github.com/NVIDIA/spark-rapids/pull/2088)|Upgrade ScalaTest plugin to respect JAVA_HOME|
|[#1932](https://github.com/NVIDIA/spark-rapids/pull/1932)|Create a getting started on K8s page|
|[#2080](https://github.com/NVIDIA/spark-rapids/pull/2080)|Improve error message after failed RMM shutdown|
|[#2064](https://github.com/NVIDIA/spark-rapids/pull/2064)|Optimize unnecessary columnar->row->columnar transitions with AQE|
|[#2025](https://github.com/NVIDIA/spark-rapids/pull/2025)|Update the doc for pandas udf on databricks|
|[#2059](https://github.com/NVIDIA/spark-rapids/pull/2059)|Add the flag 'TEST_TYPE' to avoid integration tests silently skipping some test cases|
|[#2075](https://github.com/NVIDIA/spark-rapids/pull/2075)|Remove debug println from CBO test|
|[#2046](https://github.com/NVIDIA/spark-rapids/pull/2046)|support casting Decimal to String|
|[#1812](https://github.com/NVIDIA/spark-rapids/pull/1812)|allow spilled buffers to be unspilled|
|[#2061](https://github.com/NVIDIA/spark-rapids/pull/2061)|Run the pandas udf using cudf on Databricks|
|[#1893](https://github.com/NVIDIA/spark-rapids/pull/1893)|Plug-in support for get_json_object|
|[#2044](https://github.com/NVIDIA/spark-rapids/pull/2044)|Use partition for GPU hash partitioning|
|[#1954](https://github.com/NVIDIA/spark-rapids/pull/1954)|Fix CBO bug where incompatible plans were produced with AQE on|
|[#2049](https://github.com/NVIDIA/spark-rapids/pull/2049)|Remove incompatable int overflow checking|
|[#2056](https://github.com/NVIDIA/spark-rapids/pull/2056)|Remove Spark 3.2 from premerge and nightly CI run|
|[#1814](https://github.com/NVIDIA/spark-rapids/pull/1814)|Struct to string casting functionality|
|[#2037](https://github.com/NVIDIA/spark-rapids/pull/2037)|Fix warnings from use of deprecated cudf methods|
|[#2033](https://github.com/NVIDIA/spark-rapids/pull/2033)|Bump up pre-merge OS from ubuntu 16 to ubuntu 18 [skip ci]|
|[#1883](https://github.com/NVIDIA/spark-rapids/pull/1883)|Enable sort for single-level nesting struct columns on GPU|
|[#2016](https://github.com/NVIDIA/spark-rapids/pull/2016)|Refactor logic for parallel testing|
|[#2022](https://github.com/NVIDIA/spark-rapids/pull/2022)|Update order by to not load native libraries when sorting|
|[#2017](https://github.com/NVIDIA/spark-rapids/pull/2017)|Add in murmur3 support for float, double, date and timestamp|
|[#1981](https://github.com/NVIDIA/spark-rapids/pull/1981)|Fix GpuSize|
|[#1999](https://github.com/NVIDIA/spark-rapids/pull/1999)|support casting string to decimal|
|[#2006](https://github.com/NVIDIA/spark-rapids/pull/2006)|Enable windowed `collect_list` by default|
|[#2000](https://github.com/NVIDIA/spark-rapids/pull/2000)|Use Spark's HybridRowQueue to avoid MemoryConsumer API shim|
|[#2015](https://github.com/NVIDIA/spark-rapids/pull/2015)|Fix bug where rkey buffer is getting advanced after the first handshake|
|[#2007](https://github.com/NVIDIA/spark-rapids/pull/2007)|Fix unknown column name error when filtering ORC file with no names|
|[#2005](https://github.com/NVIDIA/spark-rapids/pull/2005)|Update to new is_before_spark_311 function name|
|[#1944](https://github.com/NVIDIA/spark-rapids/pull/1944)|Support running scalar pandas UDF with array type.|
|[#1991](https://github.com/NVIDIA/spark-rapids/pull/1991)|Fixes creation of invalid DecimalType in GpuDivide.tagExprForGpu|
|[#1958](https://github.com/NVIDIA/spark-rapids/pull/1958)|Support legacy behavior of parameterless count |
|[#1919](https://github.com/NVIDIA/spark-rapids/pull/1919)|Add support for Structs for UnionExec|
|[#2002](https://github.com/NVIDIA/spark-rapids/pull/2002)|Pass dirs to getBlockData for a wrapped shuffle resolver|
|[#1983](https://github.com/NVIDIA/spark-rapids/pull/1983)|document building against different CUDA Toolkit versions|
|[#1994](https://github.com/NVIDIA/spark-rapids/pull/1994)|Merge 0.4 to 0.5 [skip ci]|
|[#1982](https://github.com/NVIDIA/spark-rapids/pull/1982)|Update ORC pushdown filter building to latest Spark logic|
|[#1978](https://github.com/NVIDIA/spark-rapids/pull/1978)|Add audit script to list commits from Spark|
|[#1976](https://github.com/NVIDIA/spark-rapids/pull/1976)|Temp fix for parquet write changes|
|[#1970](https://github.com/NVIDIA/spark-rapids/pull/1970)|add maven profiles for supported CUDA versions|
|[#1951](https://github.com/NVIDIA/spark-rapids/pull/1951)|Branch 0.5 doc remove numpartitions|
|[#1967](https://github.com/NVIDIA/spark-rapids/pull/1967)|Update FAQ for Dataset API and format supported versions|
|[#1972](https://github.com/NVIDIA/spark-rapids/pull/1972)|support GpuSize|
|[#1966](https://github.com/NVIDIA/spark-rapids/pull/1966)|add xml report for codecov|
|[#1955](https://github.com/NVIDIA/spark-rapids/pull/1955)|Fix typo in Arrow optimization config|
|[#1956](https://github.com/NVIDIA/spark-rapids/pull/1956)|Fix NPE in plugin shutdown|
|[#1930](https://github.com/NVIDIA/spark-rapids/pull/1930)|Relax cudf version check for patch-level versions|
|[#1787](https://github.com/NVIDIA/spark-rapids/pull/1787)|support distributed file path in cloud environment|
|[#1961](https://github.com/NVIDIA/spark-rapids/pull/1961)|change premege GPU_TYPE from secret to global env [skip ci]|
|[#1957](https://github.com/NVIDIA/spark-rapids/pull/1957)|Update Spark 3.1.2 shim for float upcast behavior|
|[#1889](https://github.com/NVIDIA/spark-rapids/pull/1889)|Decimal DIV changes |
|[#1947](https://github.com/NVIDIA/spark-rapids/pull/1947)|Move doc of Pandas UDF to additional-functionality|
|[#1938](https://github.com/NVIDIA/spark-rapids/pull/1938)|Add spark.executor.resource.gpu.amount=1 to YARN and K8s docs|
|[#1937](https://github.com/NVIDIA/spark-rapids/pull/1937)|Fix merge conflict with branch-0.4|
|[#1878](https://github.com/NVIDIA/spark-rapids/pull/1878)|spillable cache for GpuCartesianRDD|
|[#1843](https://github.com/NVIDIA/spark-rapids/pull/1843)|Refactor GpuGenerateExec and Explode|
|[#1933](https://github.com/NVIDIA/spark-rapids/pull/1933)|Split DB scripts to make them common for the build and IT pipeline|
|[#1935](https://github.com/NVIDIA/spark-rapids/pull/1935)|Update Alias SQL quoting and float-to-timestamp casting to match Spark 3.2|
|[#1926](https://github.com/NVIDIA/spark-rapids/pull/1926)|Consolidate RAT settings in parent pom|
|[#1918](https://github.com/NVIDIA/spark-rapids/pull/1918)|Minor code cleanup in dateTImeExpressions|
|[#1906](https://github.com/NVIDIA/spark-rapids/pull/1906)|Remove get call on timeZoneId|
|[#1908](https://github.com/NVIDIA/spark-rapids/pull/1908)|Remove the Scala version of Mortgage ETL tests from nightly test|
|[#1894](https://github.com/NVIDIA/spark-rapids/pull/1894)|Modified Download Page to re-order the items and change the format of download links|
|[#1909](https://github.com/NVIDIA/spark-rapids/pull/1909)|Avoid pinned memory for shuffle host buffers|
|[#1891](https://github.com/NVIDIA/spark-rapids/pull/1891)|Connect UCX endpoints early during app startup|
|[#1877](https://github.com/NVIDIA/spark-rapids/pull/1877)|remove docker build in pre-merge [skip ci]|
|[#1830](https://github.com/NVIDIA/spark-rapids/pull/1830)|Enable the tests for collect over window.|
|[#1882](https://github.com/NVIDIA/spark-rapids/pull/1882)|GpuArrowColumnarBatchBuilder retains the references of ArrowBuf until HostToGpuCoalesceIterator put them into device|
|[#1868](https://github.com/NVIDIA/spark-rapids/pull/1868)|Increase row limit when doing count() for HostColumnarToGpu |
|[#1855](https://github.com/NVIDIA/spark-rapids/pull/1855)|Expose row count statistics in GpuShuffleExchangeExec|
|[#1875](https://github.com/NVIDIA/spark-rapids/pull/1875)|Fix merge conflict with branch-0.4|
|[#1841](https://github.com/NVIDIA/spark-rapids/pull/1841)|Add in support for DateAddInterval|
|[#1869](https://github.com/NVIDIA/spark-rapids/pull/1869)|Fix tests for Spark 3.2.0 shim|
|[#1858](https://github.com/NVIDIA/spark-rapids/pull/1858)|fix shuffle manager doc on ucx library path|
|[#1836](https://github.com/NVIDIA/spark-rapids/pull/1836)|Add shim for Spark 3.1.2|
|[#1852](https://github.com/NVIDIA/spark-rapids/pull/1852)|Fix Part Suite Tests|
|[#1616](https://github.com/NVIDIA/spark-rapids/pull/1616)|Cost-based optimizer|
|[#1834](https://github.com/NVIDIA/spark-rapids/pull/1834)|Add shim for Spark 3.0.3|
|[#1839](https://github.com/NVIDIA/spark-rapids/pull/1839)|Refactor join code to reduce duplicated code|
|[#1848](https://github.com/NVIDIA/spark-rapids/pull/1848)|Fix merge conflict with branch-0.4|
|[#1796](https://github.com/NVIDIA/spark-rapids/pull/1796)|Have most of range partitioning run on the GPU|
|[#1845](https://github.com/NVIDIA/spark-rapids/pull/1845)|Fix fails on the mortgage ETL test|
|[#1829](https://github.com/NVIDIA/spark-rapids/pull/1829)|Cleanup unused Jenkins files and scripts|
|[#1704](https://github.com/NVIDIA/spark-rapids/pull/1704)|Create a shim for Spark 3.2.0 development|
|[#1838](https://github.com/NVIDIA/spark-rapids/pull/1838)|Make databricks build.sh more convenient for dev|
|[#1835](https://github.com/NVIDIA/spark-rapids/pull/1835)|Fix merge conflict with branch-0.4|
|[#1808](https://github.com/NVIDIA/spark-rapids/pull/1808)|Update mortgage tests to support reading multiple dataset formats|
|[#1822](https://github.com/NVIDIA/spark-rapids/pull/1822)|Fix conflict 0.4 to 0.5|
|[#1807](https://github.com/NVIDIA/spark-rapids/pull/1807)|Fix merge conflict between branch-0.4 and branch-0.5|
|[#1788](https://github.com/NVIDIA/spark-rapids/pull/1788)|Spill metrics everywhere|
|[#1719](https://github.com/NVIDIA/spark-rapids/pull/1719)|Add in out of core sort|
|[#1728](https://github.com/NVIDIA/spark-rapids/pull/1728)|Skip RAPIDS accelerated Java UDF tests if UDF fails to load|
|[#1689](https://github.com/NVIDIA/spark-rapids/pull/1689)|Update docs for plugin 0.5.0-SNAPSHOT and cudf 0.19-SNAPSHOT|
|[#1682](https://github.com/NVIDIA/spark-rapids/pull/1682)|init CI/CD dependencies branch-0.5|

## Release 0.4.1

### Bugs Fixed
|||
|:---|:---|
|[#1985](https://github.com/NVIDIA/spark-rapids/issues/1985)|[BUG] broadcast exchange can fail on 0.4|

### PRs
|||
|:---|:---|
|[#1995](https://github.com/NVIDIA/spark-rapids/pull/1995)|update changelog 0.4.1 [skip ci]|
|[#1990](https://github.com/NVIDIA/spark-rapids/pull/1990)|Prepare for v0.4.1 release|
|[#1988](https://github.com/NVIDIA/spark-rapids/pull/1988)|broadcast exchange can fail when job group set|

## Release 0.4

### Features
|||
|:---|:---|
|[#1773](https://github.com/NVIDIA/spark-rapids/issues/1773)|[FEA] Spark 3.0.2 release support|
|[#80](https://github.com/NVIDIA/spark-rapids/issues/80)|[FEA] Support the struct SQL function|
|[#76](https://github.com/NVIDIA/spark-rapids/issues/76)|[FEA] Support CreateArray|
|[#1635](https://github.com/NVIDIA/spark-rapids/issues/1635)|[FEA] RAPIDS accelerated Java UDF|
|[#1333](https://github.com/NVIDIA/spark-rapids/issues/1333)|[FEA] Support window operations on Decimal|
|[#1419](https://github.com/NVIDIA/spark-rapids/issues/1419)|[FEA] Support GPU accelerated UDF alternative for higher order function "aggregate" over window|
|[#1580](https://github.com/NVIDIA/spark-rapids/issues/1580)|[FEA] Support Decimal for ParquetCachedBatchSerializer|
|[#1600](https://github.com/NVIDIA/spark-rapids/issues/1600)|[FEA] Support ScalarSubquery|
|[#1072](https://github.com/NVIDIA/spark-rapids/issues/1072)|[FEA] Support for a custom DataSource V2 which supplies Arrow data|
|[#906](https://github.com/NVIDIA/spark-rapids/issues/906)|[FEA] Clarify query explanation to directly state what will run on GPU|
|[#1335](https://github.com/NVIDIA/spark-rapids/issues/1335)|[FEA] Support CollectLimitExec for decimal|
|[#1485](https://github.com/NVIDIA/spark-rapids/issues/1485)|[FEA] Decimal Support for Parquet Write|
|[#1329](https://github.com/NVIDIA/spark-rapids/issues/1329)|[FEA] Decimal support for multiply int div, add, subtract and null safe equals|
|[#1351](https://github.com/NVIDIA/spark-rapids/issues/1351)|[FEA] Execute UDFs that provide a RAPIDS execution path|
|[#1330](https://github.com/NVIDIA/spark-rapids/issues/1330)|[FEA] Support Decimal Casts|
|[#1353](https://github.com/NVIDIA/spark-rapids/issues/1353)|[FEA] Example of RAPIDS UDF using custom GPU code|
|[#1487](https://github.com/NVIDIA/spark-rapids/issues/1487)|[FEA] Change spark 3.1.0 to 3.1.1|
|[#1334](https://github.com/NVIDIA/spark-rapids/issues/1334)|[FEA] Add support for count aggregate on decimal|
|[#1325](https://github.com/NVIDIA/spark-rapids/issues/1325)|[FEA] Add in join support for decimal|
|[#1326](https://github.com/NVIDIA/spark-rapids/issues/1326)|[FEA] Add in Broadcast support for decimal values|
|[#37](https://github.com/NVIDIA/spark-rapids/issues/37)|[FEA] round and bround SQL functions|
|[#78](https://github.com/NVIDIA/spark-rapids/issues/78)|[FEA] Support CreateNamedStruct function|
|[#1331](https://github.com/NVIDIA/spark-rapids/issues/1331)|[FEA] UnionExec and ExpandExec support for decimal|
|[#1332](https://github.com/NVIDIA/spark-rapids/issues/1332)|[FEA] Support CaseWhen, Coalesce and IfElse for decimal|
|[#937](https://github.com/NVIDIA/spark-rapids/issues/937)|[FEA] have murmur3 hash function that matches exactly with spark|
|[#1324](https://github.com/NVIDIA/spark-rapids/issues/1324)|[FEA] Support Parquet Read of Decimal FIXED_LENGTH_BYTE_ARRAY|
|[#1428](https://github.com/NVIDIA/spark-rapids/issues/1428)|[FEA] Add support for unary decimal operations abs, floor, ceil, unary - and unary +|
|[#1375](https://github.com/NVIDIA/spark-rapids/issues/1375)|[FEA] Add log statement for what the concurrentGpuTasks tasks is set to on executor startup|
|[#1352](https://github.com/NVIDIA/spark-rapids/issues/1352)|[FEA] Example of RAPIDS UDF using cudf Java APIs|
|[#1328](https://github.com/NVIDIA/spark-rapids/issues/1328)|[FEA] Support sorting and shuffle of decimal|
|[#1316](https://github.com/NVIDIA/spark-rapids/issues/1316)|[FEA] Support simple DECIMAL aggregates|

### Performance
|||
|:---|:---|
|[#1435](https://github.com/NVIDIA/spark-rapids/issues/1435)|[FEA]Improve the file reading by using local file caching|
|[#1738](https://github.com/NVIDIA/spark-rapids/issues/1738)|[FEA] Reduce regex usage in CAST string to date/timestamp|
|[#987](https://github.com/NVIDIA/spark-rapids/issues/987)|[FEA] Optimize CAST from string to temporal types by using cuDF is_timestamp function|
|[#1594](https://github.com/NVIDIA/spark-rapids/issues/1594)|[FEA] RAPIDS accelerated ScalaUDF|
|[#103](https://github.com/NVIDIA/spark-rapids/issues/103)|[FEA] GPU version of TakeOrderedAndProject|
|[#1024](https://github.com/NVIDIA/spark-rapids/issues/1024)|Cleanup RAPIDS transport calls to `receive`|
|[#1366](https://github.com/NVIDIA/spark-rapids/issues/1366)|Seeing performance differences of multi-threaded/coalesce/perfile Parquet reader type for a single file|
|[#1200](https://github.com/NVIDIA/spark-rapids/issues/1200)|[FEA] Accelerate the scan speed for coalescing parquet reader when reading files from multiple partitioned folders|

### Bugs Fixed
|||
|:---|:---|
|[#1885](https://github.com/NVIDIA/spark-rapids/issues/1885)|[BUG] natural join on string key results in a data frame with spurious NULLs|
|[#1785](https://github.com/NVIDIA/spark-rapids/issues/1785)|[BUG] Rapids pytest integration tests FAILED on Yarn cluster with unrecognized arguments: `--std_input_path=src/test/resources/`|
|[#999](https://github.com/NVIDIA/spark-rapids/issues/999)|[BUG] test_multi_types_window_aggs_for_rows_lead_lag fails against Spark 3.1.0|
|[#1818](https://github.com/NVIDIA/spark-rapids/issues/1818)|[BUG] unmoored doc comment warnings in GpuCast|
|[#1817](https://github.com/NVIDIA/spark-rapids/issues/1817)|[BUG] Developer build with local modifications fails during verify phase|
|[#1644](https://github.com/NVIDIA/spark-rapids/issues/1644)|[BUG] test_window_aggregate_udf_array_from_python fails on databricks|
|[#1771](https://github.com/NVIDIA/spark-rapids/issues/1771)|[BUG] Databricks AWS CI/CD failing to create cluster|
|[#1157](https://github.com/NVIDIA/spark-rapids/issues/1157)|[BUG] Fix regression supporting to_date on GPU with Spark 3.1.0|
|[#716](https://github.com/NVIDIA/spark-rapids/issues/716)|[BUG] Cast String to TimeStamp issues|
|[#1117](https://github.com/NVIDIA/spark-rapids/issues/1117)|[BUG] CAST string to date returns wrong values for dates with out-of-range values|
|[#1670](https://github.com/NVIDIA/spark-rapids/issues/1670)|[BUG] Some TPC-DS queries fail with AQE when decimal types enabled|
|[#1730](https://github.com/NVIDIA/spark-rapids/issues/1730)|[BUG] Range Partitioning can crash when processing is in the order-by|
|[#1726](https://github.com/NVIDIA/spark-rapids/issues/1726)|[BUG] java url decode test failing on databricks, emr, and dataproc|
|[#1651](https://github.com/NVIDIA/spark-rapids/issues/1651)|[BUG] GDS exception when writing shuffle file|
|[#1702](https://github.com/NVIDIA/spark-rapids/issues/1702)|[BUG] check all tests marked xfail for Spark 3.1.1|
|[#575](https://github.com/NVIDIA/spark-rapids/issues/575)|[BUG] Spark 3.1 FAILED join_test.py::test_broadcast_join_mixed[FullOuter][IGNORE_ORDER] failed|
|[#577](https://github.com/NVIDIA/spark-rapids/issues/577)|[BUG] Spark 3.1 log arithmetic functions fail|
|[#1541](https://github.com/NVIDIA/spark-rapids/issues/1541)|[BUG] Tests fail in integration in distributed mode after allowing nested types through in sort and shuffle|
|[#1626](https://github.com/NVIDIA/spark-rapids/issues/1626)|[BUG] TPC-DS-like query 77 at scale=3TB fails with maxResultSize exceeded error|
|[#1576](https://github.com/NVIDIA/spark-rapids/issues/1576)|[BUG] loading SPARK-32639 example parquet file triggers a JVM crash |
|[#1643](https://github.com/NVIDIA/spark-rapids/issues/1643)|[BUG] TPC-DS-Like q10, q35, and q69 - slow or hanging at leftSemiJoin|
|[#1650](https://github.com/NVIDIA/spark-rapids/issues/1650)|[BUG] BenchmarkRunner does not include query name in JSON summary filename when running multiple queries|
|[#1654](https://github.com/NVIDIA/spark-rapids/issues/1654)|[BUG] TPC-DS-like query 59 at scale=3TB with AQE fails with join mismatch|
|[#1274](https://github.com/NVIDIA/spark-rapids/issues/1274)|[BUG] OutOfMemoryError - Maximum pool size exceeded while running 24 day criteo ETL Transform stage|
|[#1497](https://github.com/NVIDIA/spark-rapids/issues/1497)|[BUG] Spark-rapids v0.3.0 pytest integration tests with UCX on FAILED on Yarn cluster|
|[#1534](https://github.com/NVIDIA/spark-rapids/issues/1534)|[BUG] Spark 3.1.1 test failure in writing due to removal of InMemoryFileIndex.shouldFilterOut|
|[#1155](https://github.com/NVIDIA/spark-rapids/issues/1155)|[BUG] on shutdown don't print `Socket closed` exception when shutting down UCX.scala|
|[#1510](https://github.com/NVIDIA/spark-rapids/issues/1510)|[BUG] IllegalArgumentException during shuffle|
|[#1513](https://github.com/NVIDIA/spark-rapids/issues/1513)|[BUG] executor not fully initialized may get calls from Spark, in the process setting the `catalog` incorrectly|
|[#1466](https://github.com/NVIDIA/spark-rapids/issues/1466)|[BUG] Databricks build must run before the rapids nightly|
|[#1456](https://github.com/NVIDIA/spark-rapids/issues/1456)|[BUG] Databricks 0.4 parquet integration tests fail|
|[#1400](https://github.com/NVIDIA/spark-rapids/issues/1400)|[BUG] Regressions in spark-shell usage of benchmark utilities|
|[#1119](https://github.com/NVIDIA/spark-rapids/issues/1119)|[BUG] inner join fails with Column size cannot be negative|
|[#1079](https://github.com/NVIDIA/spark-rapids/issues/1079)|[BUG]The Scala UDF function cannot invoke the UDF compiler when it's passed to "explode"|
|[#1298](https://github.com/NVIDIA/spark-rapids/issues/1298)|TPCxBB query16 failed at UnsupportedOperationException: org.apache.parquet.column.values.dictionary.PlainValuesDictionary$PlainIntegerDictionary|
|[#1271](https://github.com/NVIDIA/spark-rapids/issues/1271)|[BUG] CastOpSuite and AnsiCastOpSuite failing with ArithmeticException on Spark 3.1|
|[#84](https://github.com/NVIDIA/spark-rapids/issues/84)|[BUG] sort does not match spark for -0.0 and 0.0|
|[#578](https://github.com/NVIDIA/spark-rapids/issues/578)|[BUG] Spark 3.1 qa_nightly_select_test.py Full join test failures|
|[#586](https://github.com/NVIDIA/spark-rapids/issues/586)|[BUG] Spark3.1 tpch failures|
|[#837](https://github.com/NVIDIA/spark-rapids/issues/837)|[BUG] Distinct count of floating point values differs with regular spark|
|[#953](https://github.com/NVIDIA/spark-rapids/issues/953)|[BUG] 3.1.0 pos_explode tests are failing|
|[#127](https://github.com/NVIDIA/spark-rapids/issues/127)|[BUG] String CSV parsing does not respect nullValues|
|[#1203](https://github.com/NVIDIA/spark-rapids/issues/1203)|[BUG] tpcds query 51 fails with join error on Spark 3.1.0|
|[#750](https://github.com/NVIDIA/spark-rapids/issues/750)|[BUG] udf_cudf_test::test_with_column fails with IPC error |
|[#1348](https://github.com/NVIDIA/spark-rapids/issues/1348)|[BUG] Host columnar decimal conversions are failing|
|[#1270](https://github.com/NVIDIA/spark-rapids/issues/1270)|[BUG] Benchmark runner fails to produce report if benchmark fails due to an invalid query plan|
|[#1179](https://github.com/NVIDIA/spark-rapids/issues/1179)|[BUG] SerializeConcatHostBuffersDeserializeBatch may have thread issues|
|[#1115](https://github.com/NVIDIA/spark-rapids/issues/1115)|[BUG] Unchecked type warning in SparkQueryCompareTestSuite|

### PRs
|||
|:---|:---|
|[#1963](https://github.com/NVIDIA/spark-rapids/pull/1963)|Update changelog 0.4 [skip ci]|
|[#1960](https://github.com/NVIDIA/spark-rapids/pull/1960)|Replace sonatype staging link with maven central link|
|[#1945](https://github.com/NVIDIA/spark-rapids/pull/1945)|Update changelog 0.4 [skip ci]|
|[#1910](https://github.com/NVIDIA/spark-rapids/pull/1910)|Make hash partitioning match CPU|
|[#1927](https://github.com/NVIDIA/spark-rapids/pull/1927)|Change cuDF dependency to 0.18.1|
|[#1934](https://github.com/NVIDIA/spark-rapids/pull/1934)|Update documentation to use cudf version 0.18.1|
|[#1871](https://github.com/NVIDIA/spark-rapids/pull/1871)|Disable coalesce batch spilling to avoid cudf contiguous_split bug|
|[#1849](https://github.com/NVIDIA/spark-rapids/pull/1849)|Update changelog for 0.4|
|[#1744](https://github.com/NVIDIA/spark-rapids/pull/1744)|Fix NullPointerException on null partition insert|
|[#1842](https://github.com/NVIDIA/spark-rapids/pull/1842)|Update to note support for 3.0.2|
|[#1832](https://github.com/NVIDIA/spark-rapids/pull/1832)|Spark 3.1.1 shim no longer a snapshot shim|
|[#1831](https://github.com/NVIDIA/spark-rapids/pull/1831)|Spark 3.0.2 shim no longer a snapshot shim|
|[#1826](https://github.com/NVIDIA/spark-rapids/pull/1826)|Remove benchmarks|
|[#1828](https://github.com/NVIDIA/spark-rapids/pull/1828)|Update cudf dependency to 0.18|
|[#1813](https://github.com/NVIDIA/spark-rapids/pull/1813)|Fix LEAD/LAG failures in Spark 3.1.1|
|[#1819](https://github.com/NVIDIA/spark-rapids/pull/1819)|Fix scaladoc warning in GpuCast|
|[#1820](https://github.com/NVIDIA/spark-rapids/pull/1820)|[BUG] make modified check pre-merge only|
|[#1780](https://github.com/NVIDIA/spark-rapids/pull/1780)|Remove SNAPSHOT from test and integration_test READMEs|
|[#1809](https://github.com/NVIDIA/spark-rapids/pull/1809)|check if modified files after update_config/supported|
|[#1804](https://github.com/NVIDIA/spark-rapids/pull/1804)|Update UCX documentation for RX_QUEUE_LEN and Docker|
|[#1810](https://github.com/NVIDIA/spark-rapids/pull/1810)|Pandas UDF: Sort the data before computing the sum.|
|[#1751](https://github.com/NVIDIA/spark-rapids/pull/1751)|Exclude foldable expressions from GPU if constant folding is disabled|
|[#1798](https://github.com/NVIDIA/spark-rapids/pull/1798)|Add documentation about explain not on GPU when AQE is on|
|[#1766](https://github.com/NVIDIA/spark-rapids/pull/1766)|Branch 0.4 release docs|
|[#1794](https://github.com/NVIDIA/spark-rapids/pull/1794)|Build python output schema from udf expressions|
|[#1783](https://github.com/NVIDIA/spark-rapids/pull/1783)|Fix the collect_list over window tests failures on db|
|[#1781](https://github.com/NVIDIA/spark-rapids/pull/1781)|Better float/double cases for casting tests|
|[#1790](https://github.com/NVIDIA/spark-rapids/pull/1790)|Record row counts in benchmark runs that call collect|
|[#1779](https://github.com/NVIDIA/spark-rapids/pull/1779)|Add support of DateType and TimestampType for GetTimestamp expression|
|[#1768](https://github.com/NVIDIA/spark-rapids/pull/1768)|Updating getting started Databricks docs|
|[#1742](https://github.com/NVIDIA/spark-rapids/pull/1742)|Fix regression supporting to_date with Spark-3.1|
|[#1775](https://github.com/NVIDIA/spark-rapids/pull/1775)|Fix ambiguous ordering for some tests|
|[#1760](https://github.com/NVIDIA/spark-rapids/pull/1760)|Update GpuDataSourceScanExec and GpuBroadcastExchangeExec to fix audit issues|
|[#1750](https://github.com/NVIDIA/spark-rapids/pull/1750)|Detect task failures in benchmarks|
|[#1767](https://github.com/NVIDIA/spark-rapids/pull/1767)|Consistent Spark version for test and production|
|[#1741](https://github.com/NVIDIA/spark-rapids/pull/1741)|Reduce regex use in CAST|
|[#1756](https://github.com/NVIDIA/spark-rapids/pull/1756)|Skip RAPIDS accelerated Java UDF tests if UDF fails to load|
|[#1716](https://github.com/NVIDIA/spark-rapids/pull/1716)|Update RapidsShuffleManager documentation for branch 0.4|
|[#1740](https://github.com/NVIDIA/spark-rapids/pull/1740)|Disable ORC writes until bug can be fixed|
|[#1747](https://github.com/NVIDIA/spark-rapids/pull/1747)|Fix resource leaks in unit tests|
|[#1725](https://github.com/NVIDIA/spark-rapids/pull/1725)|Branch 0.4 FAQ reorg|
|[#1718](https://github.com/NVIDIA/spark-rapids/pull/1718)|CAST string to temporal type now calls isTimestamp|
|[#1734](https://github.com/NVIDIA/spark-rapids/pull/1734)|Disable range partitioning if computation is needed|
|[#1723](https://github.com/NVIDIA/spark-rapids/pull/1723)|Removed StructTypes support for ParquetCachedBatchSerializer as cudf doesn't support it yet|
|[#1714](https://github.com/NVIDIA/spark-rapids/pull/1714)|Add support for RAPIDS accelerated Java UDFs|
|[#1713](https://github.com/NVIDIA/spark-rapids/pull/1713)|Call GpuDeviceManager.shutdown when the executor plugin is shutting down|
|[#1596](https://github.com/NVIDIA/spark-rapids/pull/1596)|Added in Decimal support to ParquetCachedBatchSerializer|
|[#1706](https://github.com/NVIDIA/spark-rapids/pull/1706)|cleanup unused is_before_spark_310|
|[#1685](https://github.com/NVIDIA/spark-rapids/pull/1685)|Fix CustomShuffleReader replacement when decimal types enabled|
|[#1699](https://github.com/NVIDIA/spark-rapids/pull/1699)|Add docs about Spark 3.1 in standalone modes not needing extra class path|
|[#1701](https://github.com/NVIDIA/spark-rapids/pull/1701)|remove xfail for orc test_input_meta for spark 3.1.0|
|[#1703](https://github.com/NVIDIA/spark-rapids/pull/1703)|Remove xfail for spark 3.1.0 test_broadcast_join_mixed FullOuter|
|[#1676](https://github.com/NVIDIA/spark-rapids/pull/1676)|BenchmarkRunner option to generate query plan diagrams in DOT format|
|[#1695](https://github.com/NVIDIA/spark-rapids/pull/1695)|support alternate jar paths|
|[#1694](https://github.com/NVIDIA/spark-rapids/pull/1694)|increase mem and limit parallelism for pre-merge|
|[#1691](https://github.com/NVIDIA/spark-rapids/pull/1691)|add validate_execs_in_gpu_plan to pytest.ini|
|[#1692](https://github.com/NVIDIA/spark-rapids/pull/1692)|Add the integration test resources to the test tarball|
|[#1677](https://github.com/NVIDIA/spark-rapids/pull/1677)|When PTDS is enabled, print warning if the allocator is not ARENA|
|[#1683](https://github.com/NVIDIA/spark-rapids/pull/1683)|update changelog to verify autotmerge 0.5 setup [skip ci]|
|[#1673](https://github.com/NVIDIA/spark-rapids/pull/1673)|support auto-merge for branch 0.5 [skip ci]|
|[#1681](https://github.com/NVIDIA/spark-rapids/pull/1681)|Xfail the collect_list tests for databricks|
|[#1678](https://github.com/NVIDIA/spark-rapids/pull/1678)|Fix array/struct checks in Sort and HashAggregate and sorting tests in distributed mode|
|[#1671](https://github.com/NVIDIA/spark-rapids/pull/1671)|Allow metrics to be configurable by level|
|[#1675](https://github.com/NVIDIA/spark-rapids/pull/1675)|add run_pyspark_from_build.sh to the pytest distribution tarball|
|[#1548](https://github.com/NVIDIA/spark-rapids/pull/1548)|Support executing collect_list on GPU with windowing.|
|[#1593](https://github.com/NVIDIA/spark-rapids/pull/1593)|Avoid unnecessary Table instances after contiguous split|
|[#1592](https://github.com/NVIDIA/spark-rapids/pull/1592)|Add in support for Decimal divide|
|[#1668](https://github.com/NVIDIA/spark-rapids/pull/1668)|Implement way for python integration tests to validate Exec is in GPU plan|
|[#1669](https://github.com/NVIDIA/spark-rapids/pull/1669)|Add FAQ entries for executor-per-GPU questions|
|[#1661](https://github.com/NVIDIA/spark-rapids/pull/1661)|Enable Parquet test for file containing map struct key|
|[#1664](https://github.com/NVIDIA/spark-rapids/pull/1664)|Filter nulls for left semi and left anti join to work around cudf|
|[#1665](https://github.com/NVIDIA/spark-rapids/pull/1665)|Add better automated tests for Arrow columnar copy in HostColumnarToGpu|
|[#1614](https://github.com/NVIDIA/spark-rapids/pull/1614)|add alluxio getting start document|
|[#1639](https://github.com/NVIDIA/spark-rapids/pull/1639)|support GpuScalarSubquery|
|[#1656](https://github.com/NVIDIA/spark-rapids/pull/1656)|Move UDF to Catalyst Expressions to its own document|
|[#1663](https://github.com/NVIDIA/spark-rapids/pull/1663)|BenchmarkRunner - Include query name in JSON summary filename|
|[#1655](https://github.com/NVIDIA/spark-rapids/pull/1655)|Fix extraneous shuffles added by AQE|
|[#1652](https://github.com/NVIDIA/spark-rapids/pull/1652)|Fix typo in arrow optimized config name - spark.rapids.arrowCopyOptimizationEnabled|
|[#1645](https://github.com/NVIDIA/spark-rapids/pull/1645)|Run Databricks IT with python-xdist parallel, includes test fixes and xfail|
|[#1649](https://github.com/NVIDIA/spark-rapids/pull/1649)|Move building from source docs to contributing guide|
|[#1637](https://github.com/NVIDIA/spark-rapids/pull/1637)|Fail DivModLike on zero divisor in ANSI mode|
|[#1646](https://github.com/NVIDIA/spark-rapids/pull/1646)|Update links in rapids-udfs.md after moving to subfolder|
|[#1641](https://github.com/NVIDIA/spark-rapids/pull/1641)|Xfail struct and array order by tests on Dataproc|
|[#1565](https://github.com/NVIDIA/spark-rapids/pull/1565)|Add GPU accelerated array_contains operator|
|[#1617](https://github.com/NVIDIA/spark-rapids/pull/1617)|Enable nightly test checks for Apache Spark|
|[#1636](https://github.com/NVIDIA/spark-rapids/pull/1636)|RAPIDS accelerated Spark Scala UDF support|
|[#1634](https://github.com/NVIDIA/spark-rapids/pull/1634)|Fix databricks build since Arrow code added|
|[#1599](https://github.com/NVIDIA/spark-rapids/pull/1599)|Add division by zero tests for Spark 3.1 behavior|
|[#1619](https://github.com/NVIDIA/spark-rapids/pull/1619)|Update GpuFileSourceScanExec to be in sync with DataSourceScanExec|
|[#1631](https://github.com/NVIDIA/spark-rapids/pull/1631)|Explicitly add maven-jar-plugin version to improve incremental build time.|
|[#1624](https://github.com/NVIDIA/spark-rapids/pull/1624)|Update explain format to show what will and will not run on the GPU|
|[#1622](https://github.com/NVIDIA/spark-rapids/pull/1622)|Support faster copy for a custom DataSource V2 which supplies Arrow data|
|[#1621](https://github.com/NVIDIA/spark-rapids/pull/1621)|Additional functionality docs|
|[#1618](https://github.com/NVIDIA/spark-rapids/pull/1618)|update blossom-ci for security updates [skip ci]|
|[#1562](https://github.com/NVIDIA/spark-rapids/pull/1562)|add alluxio support|
|[#1597](https://github.com/NVIDIA/spark-rapids/pull/1597)|Documentation for Parquet serializer|
|[#1611](https://github.com/NVIDIA/spark-rapids/pull/1611)|Add in flag for integration tests to not skip required tests|
|[#1609](https://github.com/NVIDIA/spark-rapids/pull/1609)|Disable float round/bround by default|
|[#1615](https://github.com/NVIDIA/spark-rapids/pull/1615)|Add in window support for average|
|[#1610](https://github.com/NVIDIA/spark-rapids/pull/1610)|Limit length of spark app name in BenchmarkRunner|
|[#1579](https://github.com/NVIDIA/spark-rapids/pull/1579)|Support TakeOrderedAndProject|
|[#1581](https://github.com/NVIDIA/spark-rapids/pull/1581)|Support Decimal type for CollectLimitExec|
|[#1591](https://github.com/NVIDIA/spark-rapids/pull/1591)|Add support for running multiple queries in BenchmarkRunner|
|[#1595](https://github.com/NVIDIA/spark-rapids/pull/1595)|Fix Github documentation issue template|
|[#1577](https://github.com/NVIDIA/spark-rapids/pull/1577)|rename directory from spark310 to spark311|
|[#1578](https://github.com/NVIDIA/spark-rapids/pull/1578)|Test to track RAPIDS-side issues re SPARK-32639|
|[#1583](https://github.com/NVIDIA/spark-rapids/pull/1583)|fix request-action issue [skip ci]|
|[#1555](https://github.com/NVIDIA/spark-rapids/pull/1555)|Enable ANSI mode for CAST string to timestamp|
|[#1531](https://github.com/NVIDIA/spark-rapids/pull/1531)|Decimal Support for writing Parquet|
|[#1545](https://github.com/NVIDIA/spark-rapids/pull/1545)|Support comparing ORC data|
|[#1570](https://github.com/NVIDIA/spark-rapids/pull/1570)|Branch 0.4 doc cleanup|
|[#1569](https://github.com/NVIDIA/spark-rapids/pull/1569)|Add shim method shouldIgnorePath|
|[#1564](https://github.com/NVIDIA/spark-rapids/pull/1564)|Add in support for Decimal Multiply and DIV|
|[#1561](https://github.com/NVIDIA/spark-rapids/pull/1561)|Decimal support for add and subtract|
|[#1560](https://github.com/NVIDIA/spark-rapids/pull/1560)|support sum in window aggregation for decimal|
|[#1546](https://github.com/NVIDIA/spark-rapids/pull/1546)|Cleanup shutdown logging for UCX shuffle|
|[#1551](https://github.com/NVIDIA/spark-rapids/pull/1551)|RAPIDS-accelerated Hive UDFs support all types|
|[#1543](https://github.com/NVIDIA/spark-rapids/pull/1543)|Shuffle/transport enabled by default|
|[#1552](https://github.com/NVIDIA/spark-rapids/pull/1552)|Disable blackduck signature check|
|[#1540](https://github.com/NVIDIA/spark-rapids/pull/1540)|Handle ShuffleManager api calls when plugin is not fully initialized|
|[#1547](https://github.com/NVIDIA/spark-rapids/pull/1547)|Cleanup shuffle transport receive calls|
|[#1512](https://github.com/NVIDIA/spark-rapids/pull/1512)|Support window operations on Decimal|
|[#1532](https://github.com/NVIDIA/spark-rapids/pull/1532)|Support casting from decimal to decimal|
|[#1542](https://github.com/NVIDIA/spark-rapids/pull/1542)|Change the number of partitions to zero when a range is empty|
|[#1506](https://github.com/NVIDIA/spark-rapids/pull/1506)|Add --use-decimals flag to TPC-DS ConvertFiles|
|[#1511](https://github.com/NVIDIA/spark-rapids/pull/1511)|Remove unused Jenkinsfiles [skip ci]|
|[#1505](https://github.com/NVIDIA/spark-rapids/pull/1505)|Add least, greatest and eqNullSafe support for DecimalType|
|[#1484](https://github.com/NVIDIA/spark-rapids/pull/1484)|add doc for nsight systems bundled with cuda toolkit|
|[#1478](https://github.com/NVIDIA/spark-rapids/pull/1478)|Documentation for RAPIDS-accelerated Hive UDFs|
|[#1477](https://github.com/NVIDIA/spark-rapids/pull/1477)|Allow structs and arrays to pass through for Shuffle and Sort |
|[#1489](https://github.com/NVIDIA/spark-rapids/pull/1489)|Adds in some support for the array sql function|
|[#1438](https://github.com/NVIDIA/spark-rapids/pull/1438)|Cast from numeric types to decimal type|
|[#1493](https://github.com/NVIDIA/spark-rapids/pull/1493)|Moved ParquetRecordMaterializer to the shim package to follow convention|
|[#1495](https://github.com/NVIDIA/spark-rapids/pull/1495)|Fix merge conflict, merge branch 0.3 to branch 0.4 [skip ci]|
|[#1472](https://github.com/NVIDIA/spark-rapids/pull/1472)|Add an example RAPIDS-accelerated Hive UDF using native code|
|[#1488](https://github.com/NVIDIA/spark-rapids/pull/1488)|Rename Spark 3.1.0 shim to Spark 3.1.1 to match community|
|[#1474](https://github.com/NVIDIA/spark-rapids/pull/1474)|Fix link|
|[#1476](https://github.com/NVIDIA/spark-rapids/pull/1476)|DecimalType support for Aggregate Count|
|[#1475](https://github.com/NVIDIA/spark-rapids/pull/1475)| Join support for DecimalType|
|[#1244](https://github.com/NVIDIA/spark-rapids/pull/1244)|Support round and bround SQL functions |
|[#1458](https://github.com/NVIDIA/spark-rapids/pull/1458)|Add in support for struct and named_struct|
|[#1465](https://github.com/NVIDIA/spark-rapids/pull/1465)|DecimalType support for UnionExec and ExpandExec|
|[#1450](https://github.com/NVIDIA/spark-rapids/pull/1450)|Add dynamic configs for the spark-rapids IT pipelines|
|[#1207](https://github.com/NVIDIA/spark-rapids/pull/1207)|Spark SQL hash function using murmur3|
|[#1457](https://github.com/NVIDIA/spark-rapids/pull/1457)|Support reading decimal columns from parquet files on Databricks|
|[#1455](https://github.com/NVIDIA/spark-rapids/pull/1455)|Upgrade Scala Maven Plugin to 4.3.0|
|[#1453](https://github.com/NVIDIA/spark-rapids/pull/1453)|DecimalType support for IfElse and Coalesce|
|[#1452](https://github.com/NVIDIA/spark-rapids/pull/1452)|Support DecimalType for CaseWhen|
|[#1444](https://github.com/NVIDIA/spark-rapids/pull/1444)|Improve UX when running benchmarks from Spark shell|
|[#1294](https://github.com/NVIDIA/spark-rapids/pull/1294)|Support reading decimal columns from parquet files|
|[#1153](https://github.com/NVIDIA/spark-rapids/pull/1153)|Scala UDF will compile children expressions in Project|
|[#1416](https://github.com/NVIDIA/spark-rapids/pull/1416)|Optimize mvn dependency download scripts|
|[#1430](https://github.com/NVIDIA/spark-rapids/pull/1430)|Add project for testing code that requires Spark 3.1.0 or later|
|[#1425](https://github.com/NVIDIA/spark-rapids/pull/1425)|Add in Decimal support for abs, floor, ceil, unary - and unary +|
|[#1427](https://github.com/NVIDIA/spark-rapids/pull/1427)|Revert "Make the multi-threaded parquet reader the default"|
|[#1420](https://github.com/NVIDIA/spark-rapids/pull/1420)|Add udf jar to nightly integration tests|
|[#1422](https://github.com/NVIDIA/spark-rapids/pull/1422)|Log the number of concurrent gpu tasks allowed on Executor startup|
|[#1401](https://github.com/NVIDIA/spark-rapids/pull/1401)|Accelerate the coalescing parquet reader when reading files from multiple partitioned folders|
|[#1413](https://github.com/NVIDIA/spark-rapids/pull/1413)|Add config for cast float to integral types|
|[#1313](https://github.com/NVIDIA/spark-rapids/pull/1313)|Support spilling to disk directly via cuFile/GDS|
|[#1411](https://github.com/NVIDIA/spark-rapids/pull/1411)|Add udf-examples jar to databricks build|
|[#1412](https://github.com/NVIDIA/spark-rapids/pull/1412)|Fix a lot of tests marked with xfail for Spark 3.1.0 that no longer fail|
|[#1414](https://github.com/NVIDIA/spark-rapids/pull/1414)|Build merged code of HEAD and BASE branch for pre-merge [skip ci]|
|[#1409](https://github.com/NVIDIA/spark-rapids/pull/1409)|Add option to use decimals in tpc-ds csv to parquet conversion|
|[#1410](https://github.com/NVIDIA/spark-rapids/pull/1410)|Add Decimal support for In, InSet, AtLeastNNonNulls, GetArrayItem, GetStructField, and GenerateExec|
|[#1408](https://github.com/NVIDIA/spark-rapids/pull/1408)|Support RAPIDS-accelerated HiveGenericUDF|
|[#1407](https://github.com/NVIDIA/spark-rapids/pull/1407)|Update docs and tests for null CSV support|
|[#1393](https://github.com/NVIDIA/spark-rapids/pull/1393)|Support RAPIDS-accelerated HiveSimpleUDF|
|[#1392](https://github.com/NVIDIA/spark-rapids/pull/1392)|Turn on hash partitioning for decimal support|
|[#1402](https://github.com/NVIDIA/spark-rapids/pull/1402)|Better GPU Cast type checks|
|[#1404](https://github.com/NVIDIA/spark-rapids/pull/1404)|Fix branch 0.4 merge conflict|
|[#1323](https://github.com/NVIDIA/spark-rapids/pull/1323)|More advanced type checking and documentation|
|[#1391](https://github.com/NVIDIA/spark-rapids/pull/1391)|Remove extra null join filtering because cudf is fast for this now.|
|[#1395](https://github.com/NVIDIA/spark-rapids/pull/1395)|Fix branch-0.3 -> branch-0.4 automerge|
|[#1382](https://github.com/NVIDIA/spark-rapids/pull/1382)|Handle "MM[/-]dd" and "dd[/-]MM" datetime formats in UnixTimeExprMeta|
|[#1390](https://github.com/NVIDIA/spark-rapids/pull/1390)|Accelerated columnar to row/row to columnar for decimal|
|[#1380](https://github.com/NVIDIA/spark-rapids/pull/1380)|Adds in basic support for decimal sort, sum, and some shuffle|
|[#1367](https://github.com/NVIDIA/spark-rapids/pull/1367)|Reuse gpu expression conversion rules when checking sort order|
|[#1349](https://github.com/NVIDIA/spark-rapids/pull/1349)|Add canonicalization tests|
|[#1368](https://github.com/NVIDIA/spark-rapids/pull/1368)|Move to cudf 0.18-SNAPSHOT|
|[#1361](https://github.com/NVIDIA/spark-rapids/pull/1361)|Use the correct precision when reading spark columnar data.|
|[#1273](https://github.com/NVIDIA/spark-rapids/pull/1273)|Update docs and scripts to 0.4.0-SNAPSHOT|
|[#1321](https://github.com/NVIDIA/spark-rapids/pull/1321)|Refactor to stop inheriting from HashJoin|
|[#1311](https://github.com/NVIDIA/spark-rapids/pull/1311)|ParquetCachedBatchSerializer code cleanup|
|[#1303](https://github.com/NVIDIA/spark-rapids/pull/1303)|Add explicit outputOrdering for BHJ and SHJ in spark310 shim|
|[#1299](https://github.com/NVIDIA/spark-rapids/pull/1299)|Benchmark runner improved error handling|

## Release 0.3

### Features
|||
|:---|:---|
|[#1002](https://github.com/NVIDIA/spark-rapids/issues/1002)|[FEA] RapidsHostColumnVectorCore should verify cudf data with respect to the expected spark type |
|[#444](https://github.com/NVIDIA/spark-rapids/issues/444)|[FEA] Plugable Cache|
|[#1158](https://github.com/NVIDIA/spark-rapids/issues/1158)|[FEA] Better documentation on type support|
|[#57](https://github.com/NVIDIA/spark-rapids/issues/57)|[FEA] Support INT96 for parquet reads and writes|
|[#1003](https://github.com/NVIDIA/spark-rapids/issues/1003)|[FEA] Reduce overlap between RapidsHostColumnVector and RapidsHostColumnVectorCore|
|[#913](https://github.com/NVIDIA/spark-rapids/issues/913)|[FEA] In Pluggable Cache Support CalendarInterval while creating CachedBatches|
|[#1092](https://github.com/NVIDIA/spark-rapids/issues/1092)|[FEA] In Pluggable Cache handle nested types having CalendarIntervalType and NullType|
|[#670](https://github.com/NVIDIA/spark-rapids/issues/670)|[FEA] Support NullType|
|[#50](https://github.com/NVIDIA/spark-rapids/issues/50)|[FEA] support `spark.sql.legacy.timeParserPolicy`|
|[#1144](https://github.com/NVIDIA/spark-rapids/issues/1144)|[FEA] Remove Databricks 3.0.0 shim layer|
|[#1096](https://github.com/NVIDIA/spark-rapids/issues/1096)|[FEA] Implement parquet CreateDataSourceTableAsSelectCommand|
|[#688](https://github.com/NVIDIA/spark-rapids/issues/688)|[FEA] udf compiler should be auto-appended to `spark.sql.extensions`|
|[#502](https://github.com/NVIDIA/spark-rapids/issues/502)|[FEA] Support Databricks 7.3 LTS Runtime|
|[#764](https://github.com/NVIDIA/spark-rapids/issues/764)|[FEA] Sanity checks for cudf jar mismatch|
|[#1018](https://github.com/NVIDIA/spark-rapids/issues/1018)|[FEA] Log details related to GPU memory fragmentation on GPU OOM|
|[#619](https://github.com/NVIDIA/spark-rapids/issues/619)|[FEA] log whether libcudf and libcudfjni were built for PTDS|
|[#905](https://github.com/NVIDIA/spark-rapids/issues/905)|[FEA] create AWS EMR 3.0.1 shim|
|[#838](https://github.com/NVIDIA/spark-rapids/issues/838)|[FEA] Support window count for a column|
|[#864](https://github.com/NVIDIA/spark-rapids/issues/864)|[FEA] config option to enable RMM arena memory resource|
|[#430](https://github.com/NVIDIA/spark-rapids/issues/430)|[FEA] Audit: Parquet Writer support for TIMESTAMP_MILLIS|
|[#818](https://github.com/NVIDIA/spark-rapids/issues/818)|[FEA] Create shim layer for AWS EMR |
|[#608](https://github.com/NVIDIA/spark-rapids/issues/608)|[FEA] Parquet small file optimization improve handle merge schema|

### Performance
|||
|:---|:---|
|[#446](https://github.com/NVIDIA/spark-rapids/issues/446)|[FEA] Test jucx in 1.9.x branch|
|[#1038](https://github.com/NVIDIA/spark-rapids/issues/1038)|[FEA] Accelerate the data transfer for plan `WindowInPandasExec`|
|[#533](https://github.com/NVIDIA/spark-rapids/issues/533)|[FEA] Improve PTDS performance|
|[#849](https://github.com/NVIDIA/spark-rapids/issues/849)|[FEA] Have GpuColumnarBatchSerializer return GpuColumnVectorFromBuffer instances|
|[#784](https://github.com/NVIDIA/spark-rapids/issues/784)|[FEA] Allow Host Spilling to be more dynamic|
|[#627](https://github.com/NVIDIA/spark-rapids/issues/627)|[FEA] Further parquet reading small file improvements|
|[#5](https://github.com/NVIDIA/spark-rapids/issues/5)|[FEA] Support Adaptive Execution|

### Bugs Fixed
|||
|:---|:---|
|[#1423](https://github.com/NVIDIA/spark-rapids/issues/1423)|[BUG] Mortgage ETL sample failed with spark.sql.adaptive enabled on AWS EMR 6.2 |
|[#1369](https://github.com/NVIDIA/spark-rapids/issues/1369)|[BUG] TPC-DS Query Failing on EMR 6.2 with AQE|
|[#1344](https://github.com/NVIDIA/spark-rapids/issues/1344)|[BUG] Spark-rapids Pytests failed on On Databricks cluster spark standalone mode|
|[#1279](https://github.com/NVIDIA/spark-rapids/issues/1279)|[BUG] TPC-DS query 2 failing with NPE|
|[#1280](https://github.com/NVIDIA/spark-rapids/issues/1280)|[BUG] TPC-DS query 93 failing with UnsupportedOperationException|
|[#1308](https://github.com/NVIDIA/spark-rapids/issues/1308)|[BUG] TPC-DS query 14a runs much slower on 0.3|
|[#1284](https://github.com/NVIDIA/spark-rapids/issues/1284)|[BUG] TPC-DS query 77 at scale=1TB fails with maxResultSize exceeded error|
|[#1061](https://github.com/NVIDIA/spark-rapids/issues/1061)|[BUG] orc_test.py is failing|
|[#1197](https://github.com/NVIDIA/spark-rapids/issues/1197)|[BUG] java.lang.NullPointerException when exporting delta table|
|[#685](https://github.com/NVIDIA/spark-rapids/issues/685)|[BUG] In ParqueCachedBatchSerializer, serializing parquet buffers might blow up in certain cases|
|[#1269](https://github.com/NVIDIA/spark-rapids/issues/1269)|[BUG] GpuSubstring is not expected to be a part of a SortOrder|
|[#1246](https://github.com/NVIDIA/spark-rapids/issues/1246)|[BUG] Many TPC-DS benchmarks fail when writing to Parquet|
|[#961](https://github.com/NVIDIA/spark-rapids/issues/961)|[BUG] ORC predicate pushdown should work with case-insensitive analysis|
|[#962](https://github.com/NVIDIA/spark-rapids/issues/962)|[BUG] Loading columns from an ORC file without column names returns no data|
|[#1245](https://github.com/NVIDIA/spark-rapids/issues/1245)|[BUG] Code adding buffers to the spillable store should synchronize|
|[#570](https://github.com/NVIDIA/spark-rapids/issues/570)|[BUG] Continue debugging OOM after ensuring device store is empty|
|[#972](https://github.com/NVIDIA/spark-rapids/issues/972)|[BUG] total time metric is redundant with scan time|
|[#1039](https://github.com/NVIDIA/spark-rapids/issues/1039)|[BUG] UNBOUNDED window ranges on null timestamp columns produces incorrect results.|
|[#1195](https://github.com/NVIDIA/spark-rapids/issues/1195)|[BUG] AcceleratedColumnarToRowIterator queue empty|
|[#1177](https://github.com/NVIDIA/spark-rapids/issues/1177)|[BUG] leaks possible in the rapids shuffle if batches are received after the task completes|
|[#1216](https://github.com/NVIDIA/spark-rapids/issues/1216)|[BUG] Failure to recognize ORC file format when loaded via Hive|
|[#898](https://github.com/NVIDIA/spark-rapids/issues/898)|[BUG] count reductions are failing on databricks because lack for Complete support|
|[#1184](https://github.com/NVIDIA/spark-rapids/issues/1184)|[BUG] test_window_aggregate_udf_array_from_python fails on databricks 3.0.1|
|[#1151](https://github.com/NVIDIA/spark-rapids/issues/1151)|[BUG]Add databricks 3.0.1 shim layer for GpuWindowInPandasExec.|
|[#1199](https://github.com/NVIDIA/spark-rapids/issues/1199)|[BUG] No data size in Input column in Stages page from Spark UI when using Parquet as file source|
|[#1031](https://github.com/NVIDIA/spark-rapids/issues/1031)|[BUG] dependency info properties file contains error messages|
|[#1149](https://github.com/NVIDIA/spark-rapids/issues/1149)|[BUG] Scaladoc warnings in GpuDataSource|
|[#1185](https://github.com/NVIDIA/spark-rapids/issues/1185)|[BUG] test_hash_multiple_mode_query failing|
|[#724](https://github.com/NVIDIA/spark-rapids/issues/724)|[BUG] PySpark test_broadcast_nested_loop_join_special_case intermittent failure|
|[#1164](https://github.com/NVIDIA/spark-rapids/issues/1164)|[BUG] ansi_cast tests are failing in 3.1.0|
|[#1110](https://github.com/NVIDIA/spark-rapids/issues/1110)|[BUG] Special date "now" has wrong value on GPU|
|[#1139](https://github.com/NVIDIA/spark-rapids/issues/1139)|[BUG] Host columnar to GPU can be very slow|
|[#1094](https://github.com/NVIDIA/spark-rapids/issues/1094)|[BUG] unix_timestamp on GPU returns invalid data for special dates|
|[#1098](https://github.com/NVIDIA/spark-rapids/issues/1098)|[BUG] unix_timestamp on GPU returns invalid data for bad input|
|[#1082](https://github.com/NVIDIA/spark-rapids/issues/1082)|[BUG] string to timestamp conversion fails with split|
|[#1140](https://github.com/NVIDIA/spark-rapids/issues/1140)|[BUG] ConcurrentModificationException error after scala test suite completes|
|[#1073](https://github.com/NVIDIA/spark-rapids/issues/1073)|[BUG] java.lang.RuntimeException: BinaryExpressions must override either eval or nullSafeEval|
|[#975](https://github.com/NVIDIA/spark-rapids/issues/975)|[BUG] BroadcastExchangeExec fails to fall back to CPU on driver node on GCP Dataproc|
|[#773](https://github.com/NVIDIA/spark-rapids/issues/773)|[BUG] Investigate high task deserialization|
|[#1035](https://github.com/NVIDIA/spark-rapids/issues/1035)|[BUG] TPC-DS query 90 with AQE enabled fails with doExecuteBroadcast exception|
|[#825](https://github.com/NVIDIA/spark-rapids/issues/825)|[BUG] test_window_aggs_for_ranges intermittently fails|
|[#1008](https://github.com/NVIDIA/spark-rapids/issues/1008)|[BUG] limit function is producing inconsistent result when type is Byte, Long, Boolean and Timestamp|
|[#996](https://github.com/NVIDIA/spark-rapids/issues/996)|[BUG] TPC-DS benchmark via spark-submit does not provide option to disable appending .dat to path|
|[#1006](https://github.com/NVIDIA/spark-rapids/issues/1006)|[BUG] Spark3.1.0 changed BasicWriteTaskStats breaks BasicColumnarWriteTaskStatsTracker|
|[#985](https://github.com/NVIDIA/spark-rapids/issues/985)|[BUG] missing metric `dataSize`|
|[#881](https://github.com/NVIDIA/spark-rapids/issues/881)|[BUG] cannot disable Sort by itself|
|[#812](https://github.com/NVIDIA/spark-rapids/issues/812)|[BUG] Test failures for 0.2 when run with multiple executors|
|[#925](https://github.com/NVIDIA/spark-rapids/issues/925)|[BUG]Range window-functions with non-timestamp order-by expressions not falling back to CPU|
|[#852](https://github.com/NVIDIA/spark-rapids/issues/852)|[BUG] BenchUtils.compareResults cannot compare partitioned files when ignoreOrdering=false|
|[#868](https://github.com/NVIDIA/spark-rapids/issues/868)|[BUG] Rounding error when casting timestamp to string for timestamps before 1970|
|[#880](https://github.com/NVIDIA/spark-rapids/issues/880)|[BUG] doing a window operation with an orderby for a single constant crashes|
|[#776](https://github.com/NVIDIA/spark-rapids/issues/776)|[BUG] Integration test fails on spark 3.1.0-SNAPSHOT|
|[#874](https://github.com/NVIDIA/spark-rapids/issues/874)|[BUG] `RapidsConf.scala` has some un-consistency for `spark.rapids.sql.format.parquet.multiThreadedRead`|
|[#860](https://github.com/NVIDIA/spark-rapids/issues/860)|[BUG] we need to mark columns from received shuffle buffers as `GpuColumnVectorFromBuffer`|
|[#122](https://github.com/NVIDIA/spark-rapids/issues/122)|[BUG] CSV Timestamp parseing is broken for TS < 1902 and TS > 2038|
|[#810](https://github.com/NVIDIA/spark-rapids/issues/810)|[BUG] UDF Integration tests fail if pandas is not installed|
|[#746](https://github.com/NVIDIA/spark-rapids/issues/746)|[BUG] cudf_udf_test.py is flakey|
|[#811](https://github.com/NVIDIA/spark-rapids/issues/811)|[BUG] 0.3 nightly is timing out |
|[#574](https://github.com/NVIDIA/spark-rapids/issues/574)|[BUG] Fix GpuTimeSub for Spark 3.1.0|

### PRs
|||
|:---|:---|
|[#1496](https://github.com/NVIDIA/spark-rapids/pull/1496)|Update changelog for v0.3.0 release [skip ci]|
|[#1473](https://github.com/NVIDIA/spark-rapids/pull/1473)|Update documentation for 0.3 release|
|[#1371](https://github.com/NVIDIA/spark-rapids/pull/1371)|Start Guide for RAPIDS on AWS EMR 6.2|
|[#1446](https://github.com/NVIDIA/spark-rapids/pull/1446)|Update changelog for 0.3.0 release [skip ci]|
|[#1439](https://github.com/NVIDIA/spark-rapids/pull/1439)|when AQE enabled we fail to fix up exchanges properly and EMR|
|[#1433](https://github.com/NVIDIA/spark-rapids/pull/1433)|fix pandas 1.2 compatible issue|
|[#1424](https://github.com/NVIDIA/spark-rapids/pull/1424)|Make the multi-threaded parquet reader the default since coalescing doesn't handle partitioned files well|
|[#1389](https://github.com/NVIDIA/spark-rapids/pull/1389)|Update project version to 0.3.0|
|[#1387](https://github.com/NVIDIA/spark-rapids/pull/1387)|Update cudf version to 0.17|
|[#1370](https://github.com/NVIDIA/spark-rapids/pull/1370)|[REVIEW] init changelog 0.3 [skip ci]|
|[#1376](https://github.com/NVIDIA/spark-rapids/pull/1376)|MetaUtils.getBatchFromMeta should return batches with GpuColumnVectorFromBuffer|
|[#1358](https://github.com/NVIDIA/spark-rapids/pull/1358)|auto-merge: instant merge after creation [skip ci]|
|[#1359](https://github.com/NVIDIA/spark-rapids/pull/1359)|Use SortOrder from shims.|
|[#1343](https://github.com/NVIDIA/spark-rapids/pull/1343)|Do not run UDFs when the partition is empty.|
|[#1342](https://github.com/NVIDIA/spark-rapids/pull/1342)|Fix and edit docs for standalone mode|
|[#1350](https://github.com/NVIDIA/spark-rapids/pull/1350)|fix GpuRangePartitioning canonicalization|
|[#1281](https://github.com/NVIDIA/spark-rapids/pull/1281)|Documentation added for testing|
|[#1336](https://github.com/NVIDIA/spark-rapids/pull/1336)|Fix missing post-shuffle coalesce with AQE|
|[#1318](https://github.com/NVIDIA/spark-rapids/pull/1318)|Fix copying GpuFileSourceScanExec node|
|[#1337](https://github.com/NVIDIA/spark-rapids/pull/1337)|Use UTC instead of GMT|
|[#1307](https://github.com/NVIDIA/spark-rapids/pull/1307)|Fallback to cpu when reading Delta log files for stats|
|[#1310](https://github.com/NVIDIA/spark-rapids/pull/1310)|Fix canonicalization of GpuFileSourceScanExec, GpuShuffleCoalesceExec|
|[#1302](https://github.com/NVIDIA/spark-rapids/pull/1302)|Add GpuSubstring handling to SortOrder canonicalization|
|[#1265](https://github.com/NVIDIA/spark-rapids/pull/1265)|Chunking input before writing a ParquetCachedBatch|
|[#1278](https://github.com/NVIDIA/spark-rapids/pull/1278)|Add a config to disable decimal types by default|
|[#1272](https://github.com/NVIDIA/spark-rapids/pull/1272)|Add Alias to shims|
|[#1268](https://github.com/NVIDIA/spark-rapids/pull/1268)|Adds in support docs for 0.3 release|
|[#1235](https://github.com/NVIDIA/spark-rapids/pull/1235)|Trigger reading and handling control data.|
|[#1266](https://github.com/NVIDIA/spark-rapids/pull/1266)|Updating Databricks getting started for 0.3 release|
|[#1291](https://github.com/NVIDIA/spark-rapids/pull/1291)|Increase pre-merge resource requests [skip ci]|
|[#1275](https://github.com/NVIDIA/spark-rapids/pull/1275)|Temporarily disable more CAST tests for Spark 3.1.0|
|[#1264](https://github.com/NVIDIA/spark-rapids/pull/1264)|Fix race condition in batch creation|
|[#1260](https://github.com/NVIDIA/spark-rapids/pull/1260)|Update UCX license info in NOTIFY-binary for 1.9 and RAPIDS plugin copyright dates|
|[#1247](https://github.com/NVIDIA/spark-rapids/pull/1247)|Ensure column names are valid when writing benchmark query results to file|
|[#1240](https://github.com/NVIDIA/spark-rapids/pull/1240)|Fix loading from ORC file with no column names|
|[#1242](https://github.com/NVIDIA/spark-rapids/pull/1242)|Remove compatibility documentation about unsupported INT96|
|[#1192](https://github.com/NVIDIA/spark-rapids/pull/1192)|[REVIEW]  Support GpuFilter and GpuCoalesceBatches for decimal data|
|[#1170](https://github.com/NVIDIA/spark-rapids/pull/1170)|Add nested type support to MetaUtils|
|[#1194](https://github.com/NVIDIA/spark-rapids/pull/1194)|Drop redundant total time metric from scan|
|[#1248](https://github.com/NVIDIA/spark-rapids/pull/1248)|At BatchedTableCompressor.finish synchronize to allow for "right-size…|
|[#1169](https://github.com/NVIDIA/spark-rapids/pull/1169)|Use CUDF's "UNBOUNDED" window boundaries for time-range queries.|
|[#1204](https://github.com/NVIDIA/spark-rapids/pull/1204)|Avoid empty batches on columnar to row conversion|
|[#1133](https://github.com/NVIDIA/spark-rapids/pull/1133)|Refactor batch coalesce to be based solely on batch data size|
|[#1237](https://github.com/NVIDIA/spark-rapids/pull/1237)|In transport, limit pending transfer requests to fit within a bounce|
|[#1232](https://github.com/NVIDIA/spark-rapids/pull/1232)|Move SortOrder creation to shims|
|[#1068](https://github.com/NVIDIA/spark-rapids/pull/1068)|Write int96 to parquet|
|[#1193](https://github.com/NVIDIA/spark-rapids/pull/1193)|Verify shuffle of decimal columns|
|[#1180](https://github.com/NVIDIA/spark-rapids/pull/1180)|Remove batches if they are received after the iterator detects that t…|
|[#1173](https://github.com/NVIDIA/spark-rapids/pull/1173)|Support relational operators for decimal type|
|[#1220](https://github.com/NVIDIA/spark-rapids/pull/1220)|Support replacing ORC format when Hive is configured|
|[#1219](https://github.com/NVIDIA/spark-rapids/pull/1219)|Upgrade to jucx 1.9.0|
|[#1081](https://github.com/NVIDIA/spark-rapids/pull/1081)|Add option to upload benchmark summary JSON file|
|[#1217](https://github.com/NVIDIA/spark-rapids/pull/1217)|Aggregate reductions in Complete mode should use updateExpressions|
|[#1218](https://github.com/NVIDIA/spark-rapids/pull/1218)|Remove obsolete HiveStringType usage|
|[#1214](https://github.com/NVIDIA/spark-rapids/pull/1214)|changelog update 2020-11-30. Trigger automerge check [skip ci]|
|[#1210](https://github.com/NVIDIA/spark-rapids/pull/1210)|Support auto-merge for branch-0.4 [skip ci]|
|[#1202](https://github.com/NVIDIA/spark-rapids/pull/1202)|Fix a bug with the support for java.lang.StringBuilder.append.|
|[#1213](https://github.com/NVIDIA/spark-rapids/pull/1213)|Skip casting StringType to TimestampType for Spark 310|
|[#1201](https://github.com/NVIDIA/spark-rapids/pull/1201)|Replace only window expressions on databricks.|
|[#1208](https://github.com/NVIDIA/spark-rapids/pull/1208)|[BUG] Fix GHSL2020-239 [skip ci]|
|[#1205](https://github.com/NVIDIA/spark-rapids/pull/1205)|Fix missing input bytes read metric for Parquet|
|[#1206](https://github.com/NVIDIA/spark-rapids/pull/1206)|Update Spark 3.1 shim for ShuffleOrigin shuffle parameter|
|[#1196](https://github.com/NVIDIA/spark-rapids/pull/1196)|Rename ShuffleCoalesceExec to GpuShuffleCoalesceExec|
|[#1191](https://github.com/NVIDIA/spark-rapids/pull/1191)|Skip window array tests for databricks.|
|[#1183](https://github.com/NVIDIA/spark-rapids/pull/1183)|Support for CalendarIntervalType and NullType|
|[#1150](https://github.com/NVIDIA/spark-rapids/pull/1150)|udf spec|
|[#1188](https://github.com/NVIDIA/spark-rapids/pull/1188)|Add in tests for parquet nested pruning support|
|[#1189](https://github.com/NVIDIA/spark-rapids/pull/1189)|Enable NullType for First and Last in 3.0.1+|
|[#1181](https://github.com/NVIDIA/spark-rapids/pull/1181)|Fix resource leaks in unit tests|
|[#1186](https://github.com/NVIDIA/spark-rapids/pull/1186)|Fix compilation and scaladoc warnings|
|[#1187](https://github.com/NVIDIA/spark-rapids/pull/1187)|Updated documentation for distinct count compatibility|
|[#1182](https://github.com/NVIDIA/spark-rapids/pull/1182)|Close buffer catalog on device manager shutdown|
|[#1137](https://github.com/NVIDIA/spark-rapids/pull/1137)|Let GpuWindowInPandas declare ArrayType supported.|
|[#1176](https://github.com/NVIDIA/spark-rapids/pull/1176)|Add in support for null type|
|[#1174](https://github.com/NVIDIA/spark-rapids/pull/1174)|Fix race condition in SerializeConcatHostBuffersDeserializeBatch|
|[#1175](https://github.com/NVIDIA/spark-rapids/pull/1175)|Fix leaks seen in shuffle tests|
|[#1138](https://github.com/NVIDIA/spark-rapids/pull/1138)|[REVIEW] Support decimal type for GpuProjectExec|
|[#1162](https://github.com/NVIDIA/spark-rapids/pull/1162)|Set job descriptions in benchmark runner|
|[#1172](https://github.com/NVIDIA/spark-rapids/pull/1172)|Revert "Fix race condition (#1165)"|
|[#1060](https://github.com/NVIDIA/spark-rapids/pull/1060)|Show partition metrics for custom shuffler reader|
|[#1152](https://github.com/NVIDIA/spark-rapids/pull/1152)|Add spark301db shim layer for WindowInPandas.|
|[#1167](https://github.com/NVIDIA/spark-rapids/pull/1167)|Nulls out the dataframe if --gc-between-runs is set|
|[#1165](https://github.com/NVIDIA/spark-rapids/pull/1165)|Fix race condition in SerializeConcatHostBuffersDeserializeBatch|
|[#1163](https://github.com/NVIDIA/spark-rapids/pull/1163)|Add in support for GetStructField|
|[#1166](https://github.com/NVIDIA/spark-rapids/pull/1166)|Fix the cast tests for 3.1.0+|
|[#1159](https://github.com/NVIDIA/spark-rapids/pull/1159)|fix bug where 'now' had same value as 'today' for timestamps|
|[#1161](https://github.com/NVIDIA/spark-rapids/pull/1161)|Fix nightly build pipeline failure.|
|[#1160](https://github.com/NVIDIA/spark-rapids/pull/1160)|Fix some performance problems with columnar to columnar conversion|
|[#1105](https://github.com/NVIDIA/spark-rapids/pull/1105)|[REVIEW] Change ColumnViewAccess usage to work with ColumnView|
|[#1148](https://github.com/NVIDIA/spark-rapids/pull/1148)|Add in tests for Maps and extend map support where possible|
|[#1154](https://github.com/NVIDIA/spark-rapids/pull/1154)|Mark test as xfail until we can get a fix in|
|[#1113](https://github.com/NVIDIA/spark-rapids/pull/1113)|Support unix_timestamp on GPU for subset of formats|
|[#1156](https://github.com/NVIDIA/spark-rapids/pull/1156)|Fix warning introduced in iterator suite|
|[#1095](https://github.com/NVIDIA/spark-rapids/pull/1095)|Dependency info|
|[#1145](https://github.com/NVIDIA/spark-rapids/pull/1145)|Remove support for databricks 7.0 runtime - shim spark300db|
|[#1147](https://github.com/NVIDIA/spark-rapids/pull/1147)|Change the assert to require for handling TIMESTAMP_MILLIS in isDateTimeRebaseNeeded |
|[#1132](https://github.com/NVIDIA/spark-rapids/pull/1132)|Add in basic support to read structs from parquet|
|[#1121](https://github.com/NVIDIA/spark-rapids/pull/1121)|Shuffle/better error handling|
|[#1134](https://github.com/NVIDIA/spark-rapids/pull/1134)|Support saveAsTable for writing orc and parquet|
|[#1124](https://github.com/NVIDIA/spark-rapids/pull/1124)|Add shim layers for GpuWindowInPandasExec.|
|[#1131](https://github.com/NVIDIA/spark-rapids/pull/1131)|Add in some basic support for Structs|
|[#1127](https://github.com/NVIDIA/spark-rapids/pull/1127)|Add in basic support for reading lists from parquet|
|[#1129](https://github.com/NVIDIA/spark-rapids/pull/1129)|Fix resource leaks with new shuffle optimization|
|[#1116](https://github.com/NVIDIA/spark-rapids/pull/1116)|Optimize normal shuffle by coalescing smaller batches on host|
|[#1102](https://github.com/NVIDIA/spark-rapids/pull/1102)|Auto-register UDF extention when main plugin is set|
|[#1108](https://github.com/NVIDIA/spark-rapids/pull/1108)|Remove integration test pipelines on NGCC|
|[#1123](https://github.com/NVIDIA/spark-rapids/pull/1123)|Mark Pandas udf over window tests as xfail on databricks until they can be fixed|
|[#1120](https://github.com/NVIDIA/spark-rapids/pull/1120)|Add in support for filtering ArrayType|
|[#1080](https://github.com/NVIDIA/spark-rapids/pull/1080)|Support for CalendarIntervalType and NullType for ParquetCachedSerializer|
|[#994](https://github.com/NVIDIA/spark-rapids/pull/994)|Packs bounce buffers for highly partitioned shuffles|
|[#1112](https://github.com/NVIDIA/spark-rapids/pull/1112)|Remove bad config from pytest setup|
|[#1107](https://github.com/NVIDIA/spark-rapids/pull/1107)|closeOnExcept -> withResources in MetaUtils|
|[#1104](https://github.com/NVIDIA/spark-rapids/pull/1104)|Support lists to/from the GPU|
|[#1106](https://github.com/NVIDIA/spark-rapids/pull/1106)|Improve mechanism for expected exceptions in tests|
|[#1069](https://github.com/NVIDIA/spark-rapids/pull/1069)|Accelerate the data transfer between JVM and Python for the plan 'GpuWindowInPandasExec'|
|[#1099](https://github.com/NVIDIA/spark-rapids/pull/1099)|Update how we deal with type checking|
|[#1077](https://github.com/NVIDIA/spark-rapids/pull/1077)|Improve AQE transitions for shuffle and coalesce batches|
|[#1097](https://github.com/NVIDIA/spark-rapids/pull/1097)|Cleanup some instances of excess closure serialization|
|[#1090](https://github.com/NVIDIA/spark-rapids/pull/1090)|Fix the integration build|
|[#1086](https://github.com/NVIDIA/spark-rapids/pull/1086)|Speed up test performance using pytest-xdist|
|[#1084](https://github.com/NVIDIA/spark-rapids/pull/1084)|Avoid issues where more scalars that expected show up in an expression|
|[#1076](https://github.com/NVIDIA/spark-rapids/pull/1076)|[FEA] Support Databricks 7.3 LTS Runtime|
|[#1083](https://github.com/NVIDIA/spark-rapids/pull/1083)|Revert "Get cudf/spark dependency from the correct .m2 dir"|
|[#1062](https://github.com/NVIDIA/spark-rapids/pull/1062)|Get cudf/spark dependency from the correct .m2 dir|
|[#1078](https://github.com/NVIDIA/spark-rapids/pull/1078)|Another round of fixes for mapping of DataType to DType|
|[#1066](https://github.com/NVIDIA/spark-rapids/pull/1066)|More fixes for conversion to ColumnarBatch|
|[#1029](https://github.com/NVIDIA/spark-rapids/pull/1029)|BenchmarkRunner should produce JSON summary file even when queries fail|
|[#1055](https://github.com/NVIDIA/spark-rapids/pull/1055)|Fix build warnings|
|[#1064](https://github.com/NVIDIA/spark-rapids/pull/1064)|Use array instead of List for from(Table, DataType)|
|[#1057](https://github.com/NVIDIA/spark-rapids/pull/1057)|Fix empty table broadcast requiring a GPU on driver node|
|[#1047](https://github.com/NVIDIA/spark-rapids/pull/1047)|Sanity checks for cudf jar mismatch|
|[#1044](https://github.com/NVIDIA/spark-rapids/pull/1044)|Accelerated row to columnar and columnar to row transitions|
|[#1056](https://github.com/NVIDIA/spark-rapids/pull/1056)|Add query number to Spark app name when running benchmarks|
|[#1054](https://github.com/NVIDIA/spark-rapids/pull/1054)|Log total RMM allocated on GPU OOM|
|[#1053](https://github.com/NVIDIA/spark-rapids/pull/1053)|Remove isGpuBroadcastNestedLoopJoin from shims|
|[#1052](https://github.com/NVIDIA/spark-rapids/pull/1052)|Allow for GPUCoalesceBatch to deal with Map|
|[#1051](https://github.com/NVIDIA/spark-rapids/pull/1051)|Add simple retry for URM dependencies [skip ci]|
|[#1046](https://github.com/NVIDIA/spark-rapids/pull/1046)|Fix broken links|
|[#1017](https://github.com/NVIDIA/spark-rapids/pull/1017)|Log whether PTDS is enabled|
|[#1040](https://github.com/NVIDIA/spark-rapids/pull/1040)|Update to cudf 0.17-SNAPSHOT and fix tests|
|[#1042](https://github.com/NVIDIA/spark-rapids/pull/1042)|Fix inconsistencies in AQE support for broadcast joins|
|[#1037](https://github.com/NVIDIA/spark-rapids/pull/1037)|Add in support for the SQL functions Least and Greatest|
|[#1036](https://github.com/NVIDIA/spark-rapids/pull/1036)|Increase number of retries when waiting for databricks cluster|
|[#1034](https://github.com/NVIDIA/spark-rapids/pull/1034)|[BUG] To honor spark.rapids.memory.gpu.pool=NONE|
|[#854](https://github.com/NVIDIA/spark-rapids/pull/854)|Arbitrary function call in UDF|
|[#1028](https://github.com/NVIDIA/spark-rapids/pull/1028)|Update to cudf-0.16|
|[#1023](https://github.com/NVIDIA/spark-rapids/pull/1023)|Add --gc-between-run flag for TPC* benchmarks.|
|[#1001](https://github.com/NVIDIA/spark-rapids/pull/1001)|ColumnarBatch to CachedBatch and back|
|[#990](https://github.com/NVIDIA/spark-rapids/pull/990)|Parquet coalesce file reader for local filesystems|
|[#1014](https://github.com/NVIDIA/spark-rapids/pull/1014)|Add --append-dat flag for TPC-DS benchmark|
|[#991](https://github.com/NVIDIA/spark-rapids/pull/991)|Updated GCP Dataproc Mortgage-ETL-GPU.ipynb|
|[#886](https://github.com/NVIDIA/spark-rapids/pull/886)|Spark BinaryType and cast to BinaryType|
|[#1016](https://github.com/NVIDIA/spark-rapids/pull/1016)|Change Hash Aggregate to allow pass-through on MapType|
|[#984](https://github.com/NVIDIA/spark-rapids/pull/984)|Add support for MapType in selected operators |
|[#1012](https://github.com/NVIDIA/spark-rapids/pull/1012)|Update for new position parameter in Spark 3.1.0 RegExpReplace|
|[#995](https://github.com/NVIDIA/spark-rapids/pull/995)|Add shim for EMR 3.0.1 and EMR 3.0.1-SNAPSHOT|
|[#998](https://github.com/NVIDIA/spark-rapids/pull/998)|Update benchmark automation script|
|[#1000](https://github.com/NVIDIA/spark-rapids/pull/1000)|Always use RAPIDS shuffle when running TPCH and Mortgage tests|
|[#981](https://github.com/NVIDIA/spark-rapids/pull/981)|Change databricks build to dynamically create a cluster|
|[#986](https://github.com/NVIDIA/spark-rapids/pull/986)|Fix missing dataSize metric when using RAPIDS shuffle|
|[#914](https://github.com/NVIDIA/spark-rapids/pull/914)|Write InternalRow to CachedBatch|
|[#934](https://github.com/NVIDIA/spark-rapids/pull/934)|Iterator to make it easier to work with a window of blocks in the RAPIDS shuffle|
|[#992](https://github.com/NVIDIA/spark-rapids/pull/992)|Skip post-clean if aborted before the image build stage in pre-merge [skip ci]|
|[#988](https://github.com/NVIDIA/spark-rapids/pull/988)|Change in Spark caused the 3.1.0 CI to fail|
|[#983](https://github.com/NVIDIA/spark-rapids/pull/983)|clean jenkins file for premerge on NGCC|
|[#964](https://github.com/NVIDIA/spark-rapids/pull/964)|Refactor TPC benchmarks to reduce duplicate code|
|[#978](https://github.com/NVIDIA/spark-rapids/pull/978)|Enable scalastyle checks for udf-compiler module|
|[#949](https://github.com/NVIDIA/spark-rapids/pull/949)|Fix GpuWindowExec to work with a CPU SortExec|
|[#973](https://github.com/NVIDIA/spark-rapids/pull/973)|Stop reporting totalTime metric for GpuShuffleExchangeExec|
|[#968](https://github.com/NVIDIA/spark-rapids/pull/968)|XFail pos_explode tests until final fix can be put in|
|[#970](https://github.com/NVIDIA/spark-rapids/pull/970)|Add legacy config to clear active Spark 3.1.0 session in tests|
|[#918](https://github.com/NVIDIA/spark-rapids/pull/918)|Benchmark runner script|
|[#915](https://github.com/NVIDIA/spark-rapids/pull/915)|Add option to control number of partitions when converting from CSV to Parquet|
|[#944](https://github.com/NVIDIA/spark-rapids/pull/944)|Fix some issues with non-determinism|
|[#935](https://github.com/NVIDIA/spark-rapids/pull/935)|Add in support/tests for a window count on a column|
|[#940](https://github.com/NVIDIA/spark-rapids/pull/940)|Fix closeOnExcept suppressed exception handling|
|[#942](https://github.com/NVIDIA/spark-rapids/pull/942)|fix github action env setup [skip ci]|
|[#933](https://github.com/NVIDIA/spark-rapids/pull/933)|Update first/last tests to avoid non-determinisim and ordering differences|
|[#931](https://github.com/NVIDIA/spark-rapids/pull/931)|Fix checking for nullable columns in window range query|
|[#924](https://github.com/NVIDIA/spark-rapids/pull/924)|Benchmark guide update for command-line interface / spark-submit|
|[#926](https://github.com/NVIDIA/spark-rapids/pull/926)|Move pandas_udf functions into the tests functions|
|[#929](https://github.com/NVIDIA/spark-rapids/pull/929)|Pick a default tableId to use that is non 0 so that flatbuffers allow…|
|[#928](https://github.com/NVIDIA/spark-rapids/pull/928)|Fix RapidsBufferStore NPE when no spillable buffers are available|
|[#820](https://github.com/NVIDIA/spark-rapids/pull/820)|Benchmarking guide|
|[#859](https://github.com/NVIDIA/spark-rapids/pull/859)|Compare partitioned files in order|
|[#916](https://github.com/NVIDIA/spark-rapids/pull/916)|create new sparkContext explicitly in CPU notebook|
|[#917](https://github.com/NVIDIA/spark-rapids/pull/917)|create new SparkContext in GPU notebook explicitly.|
|[#919](https://github.com/NVIDIA/spark-rapids/pull/919)|Add label benchmark to performance subsection in changelog|
|[#850](https://github.com/NVIDIA/spark-rapids/pull/850)| Add in basic support for lead/lag|
|[#843](https://github.com/NVIDIA/spark-rapids/pull/843)|[REVIEW] Cache plugin to handle reading CachedBatch to an InternalRow|
|[#904](https://github.com/NVIDIA/spark-rapids/pull/904)|Add command-line argument for benchmark result filename|
|[#909](https://github.com/NVIDIA/spark-rapids/pull/909)|GCP preview version image name update|
|[#903](https://github.com/NVIDIA/spark-rapids/pull/903)|update getting-started-gcp.md with new component list|
|[#900](https://github.com/NVIDIA/spark-rapids/pull/900)|Turn off CollectLimitExec replacement by default|
|[#907](https://github.com/NVIDIA/spark-rapids/pull/907)|remove configs from databricks that shouldn't be used by default|
|[#893](https://github.com/NVIDIA/spark-rapids/pull/893)|Fix rounding error when casting timestamp to string for timestamps before 1970|
|[#899](https://github.com/NVIDIA/spark-rapids/pull/899)|Mark reduction corner case tests as xfail on databricks until they can be fixed|
|[#894](https://github.com/NVIDIA/spark-rapids/pull/894)|Replace whole-buffer slicing with direct refcounting|
|[#891](https://github.com/NVIDIA/spark-rapids/pull/891)|Add config to dump heap on GPU OOM|
|[#890](https://github.com/NVIDIA/spark-rapids/pull/890)|Clean up CoalesceBatch to use withResource|
|[#892](https://github.com/NVIDIA/spark-rapids/pull/892)|Only manifest the current batch in cached block shuffle read iterator|
|[#871](https://github.com/NVIDIA/spark-rapids/pull/871)|Add support for using the arena allocator|
|[#889](https://github.com/NVIDIA/spark-rapids/pull/889)|Fix crash on scalar only orderby|
|[#879](https://github.com/NVIDIA/spark-rapids/pull/879)|Update SpillableColumnarBatch to remove buffer from catalog on close|
|[#888](https://github.com/NVIDIA/spark-rapids/pull/888)|Shrink detect scope to compile only [skip ci]|
|[#885](https://github.com/NVIDIA/spark-rapids/pull/885)|[BUG] fix IT dockerfile arguments [skip ci]|
|[#883](https://github.com/NVIDIA/spark-rapids/pull/883)|[BUG] fix IT dockerfile args ordering [skip ci]|
|[#875](https://github.com/NVIDIA/spark-rapids/pull/875)|fix the non-consistency for `spark.rapids.sql.format.parquet.multiThreadedRead` in RapidsConf.scala|
|[#862](https://github.com/NVIDIA/spark-rapids/pull/862)|Migrate nightly&integration pipelines to blossom [skip ci]|
|[#872](https://github.com/NVIDIA/spark-rapids/pull/872)|Ensure that receive-side batches use GpuColumnVectorFromBuffer to avoid|
|[#833](https://github.com/NVIDIA/spark-rapids/pull/833)|Add nvcomp LZ4 codec support|
|[#870](https://github.com/NVIDIA/spark-rapids/pull/870)|Cleaned up tests and documentation for csv timestamp parsing|
|[#823](https://github.com/NVIDIA/spark-rapids/pull/823)|Add command-line interface for TPC-* for use with spark-submit|
|[#856](https://github.com/NVIDIA/spark-rapids/pull/856)|Move GpuWindowInPandasExec in shims layers|
|[#756](https://github.com/NVIDIA/spark-rapids/pull/756)|Add stream-time metric|
|[#832](https://github.com/NVIDIA/spark-rapids/pull/832)|Skip pandas tests if pandas cannot be found|
|[#841](https://github.com/NVIDIA/spark-rapids/pull/841)|Fix a hanging issue when processing empty data.|
|[#840](https://github.com/NVIDIA/spark-rapids/pull/840)|[REVIEW] Fixed failing cache tests|
|[#848](https://github.com/NVIDIA/spark-rapids/pull/848)|Update task memory and disk spill metrics when buffer store spills|
|[#851](https://github.com/NVIDIA/spark-rapids/pull/851)|Use contiguous table when deserializing columnar batch|
|[#857](https://github.com/NVIDIA/spark-rapids/pull/857)|fix pvc scheduling issue|
|[#853](https://github.com/NVIDIA/spark-rapids/pull/853)|Remove nodeAffinity from premerge pipeline|
|[#796](https://github.com/NVIDIA/spark-rapids/pull/796)|Record spark plan SQL metrics to JSON when running benchmarks|
|[#781](https://github.com/NVIDIA/spark-rapids/pull/781)|Add AQE unit tests|
|[#824](https://github.com/NVIDIA/spark-rapids/pull/824)|Skip cudf_udf test by default|
|[#839](https://github.com/NVIDIA/spark-rapids/pull/839)|First/Last reduction and cleanup of agg APIs|
|[#827](https://github.com/NVIDIA/spark-rapids/pull/827)|Add Spark 3.0 EMR Shim layer |
|[#816](https://github.com/NVIDIA/spark-rapids/pull/816)|[BUG] fix nightly is timing out|
|[#782](https://github.com/NVIDIA/spark-rapids/pull/782)|Benchmark utility to perform diff of output from benchmark runs, allowing for precision differences|
|[#813](https://github.com/NVIDIA/spark-rapids/pull/813)|Revert "Enable tests in udf_cudf_test.py"|
|[#788](https://github.com/NVIDIA/spark-rapids/pull/788)|[FEA] Persist workspace data on PVC for premerge|
|[#805](https://github.com/NVIDIA/spark-rapids/pull/805)|[FEA] nightly build trigger both IT on spark 300 and 301|
|[#797](https://github.com/NVIDIA/spark-rapids/pull/797)|Allow host spill store to fit a buffer larger than configured max size|
|[#807](https://github.com/NVIDIA/spark-rapids/pull/807)|Deploy integration-tests javadoc and sources|
|[#777](https://github.com/NVIDIA/spark-rapids/pull/777)|Enable tests in udf_cudf_test.py|
|[#790](https://github.com/NVIDIA/spark-rapids/pull/790)|CI: Update cudf python to 0.16 nightly|
|[#772](https://github.com/NVIDIA/spark-rapids/pull/772)|Add support for empty array construction.|
|[#783](https://github.com/NVIDIA/spark-rapids/pull/783)|Improved GpuArrowEvalPythonExec|
|[#771](https://github.com/NVIDIA/spark-rapids/pull/771)|Various improvements to benchmarks|
|[#763](https://github.com/NVIDIA/spark-rapids/pull/763)|[REVIEW] Allow CoalesceBatch to spill data that is not in active use|
|[#727](https://github.com/NVIDIA/spark-rapids/pull/727)|Update cudf dependency to 0.16-SNAPSHOT|
|[#726](https://github.com/NVIDIA/spark-rapids/pull/726)|parquet writer support for TIMESTAMP_MILLIS|
|[#674](https://github.com/NVIDIA/spark-rapids/pull/674)|Unit test for GPU exchange re-use with AQE|
|[#723](https://github.com/NVIDIA/spark-rapids/pull/723)|Update code coverage to find source files in new places|
|[#766](https://github.com/NVIDIA/spark-rapids/pull/766)|Update the integration Dockerfile to reduce the image size|
|[#762](https://github.com/NVIDIA/spark-rapids/pull/762)|Fixing conflicts in branch-0.3|
|[#738](https://github.com/NVIDIA/spark-rapids/pull/738)|[auto-merge] branch-0.2 to branch-0.3 - resolve conflict|
|[#722](https://github.com/NVIDIA/spark-rapids/pull/722)|Initial code changes to support spilling outside of shuffle|
|[#693](https://github.com/NVIDIA/spark-rapids/pull/693)|Update jenkins files for 0.3|
|[#692](https://github.com/NVIDIA/spark-rapids/pull/692)|Merge shims dependency to spark-3.0.1 into branch-0.3|
|[#690](https://github.com/NVIDIA/spark-rapids/pull/690)|Update the version to 0.3.0-SNAPSHOT|

## Release 0.2

### Features
|||
|:---|:---|
|[#696](https://github.com/NVIDIA/spark-rapids/issues/696)|[FEA] run integration tests against SPARK-3.0.1|
|[#455](https://github.com/NVIDIA/spark-rapids/issues/455)|[FEA] Support UCX shuffle with optimized AQE|
|[#510](https://github.com/NVIDIA/spark-rapids/issues/510)|[FEA] Investigate libcudf features needed to support struct schema pruning during loads|
|[#541](https://github.com/NVIDIA/spark-rapids/issues/541)|[FEA] Scala UDF:Support for null Value operands|
|[#542](https://github.com/NVIDIA/spark-rapids/issues/542)|[FEA] Scala UDF: Support for Date and Time |
|[#499](https://github.com/NVIDIA/spark-rapids/issues/499)|[FEA] disable any kind of warnings about ExecutedCommandExec not being on the GPU|
|[#540](https://github.com/NVIDIA/spark-rapids/issues/540)|[FEA] Scala UDF: Support for String replaceFirst()|
|[#340](https://github.com/NVIDIA/spark-rapids/issues/340)|[FEA] widen the rendered Jekyll pages|
|[#602](https://github.com/NVIDIA/spark-rapids/issues/602)|[FEA] don't release with any -SNAPSHOT dependencies|
|[#579](https://github.com/NVIDIA/spark-rapids/issues/579)|[FEA] Auto-merge between branches|
|[#515](https://github.com/NVIDIA/spark-rapids/issues/515)|[FEA] Write tests for AQE skewed join optimization|
|[#452](https://github.com/NVIDIA/spark-rapids/issues/452)|[FEA] Update HashSortOptimizerSuite to work with AQE|
|[#454](https://github.com/NVIDIA/spark-rapids/issues/454)|[FEA] Update GpuCoalesceBatchesSuite to work with AQE enabled|
|[#354](https://github.com/NVIDIA/spark-rapids/issues/354)|[FEA]Spark 3.1 FileSourceScanExec adds parameter optionalNumCoalescedBuckets|
|[#566](https://github.com/NVIDIA/spark-rapids/issues/566)|[FEA] Add support for StringSplit with an array index.|
|[#524](https://github.com/NVIDIA/spark-rapids/issues/524)|[FEA] Add GPU specific metrics to GpuFileSourceScanExec|
|[#494](https://github.com/NVIDIA/spark-rapids/issues/494)|[FEA] Add some AQE-specific tests to the PySpark test suite|
|[#146](https://github.com/NVIDIA/spark-rapids/issues/146)|[FEA] Python tests should support running with Adaptive Query Execution enabled|
|[#465](https://github.com/NVIDIA/spark-rapids/issues/465)|[FEA] Audit: Update script to audit multiple versions of Spark |
|[#488](https://github.com/NVIDIA/spark-rapids/issues/488)|[FEA] Ability to limit total GPU memory used|
|[#70](https://github.com/NVIDIA/spark-rapids/issues/70)|[FEA] Support StringSplit|
|[#403](https://github.com/NVIDIA/spark-rapids/issues/403)|[FEA] Add in support for GetArrayItem|
|[#493](https://github.com/NVIDIA/spark-rapids/issues/493)|[FEA] Implement shuffle optimization when AQE is enabled|
|[#500](https://github.com/NVIDIA/spark-rapids/issues/500)|[FEA] Add maven profiles for testing with AQE on or off|
|[#471](https://github.com/NVIDIA/spark-rapids/issues/471)|[FEA] create a formal process for updating the github-pages branch|
|[#233](https://github.com/NVIDIA/spark-rapids/issues/233)|[FEA] Audit DataWritingCommandExec |
|[#240](https://github.com/NVIDIA/spark-rapids/issues/240)|[FEA] Audit Api validation script follow on - Optimize StringToTypeTag |
|[#388](https://github.com/NVIDIA/spark-rapids/issues/388)|[FEA] Audit WindowExec|
|[#425](https://github.com/NVIDIA/spark-rapids/issues/425)|[FEA] Add tests for configs in BatchScan Readers|
|[#453](https://github.com/NVIDIA/spark-rapids/issues/453)|[FEA] Update HashAggregatesSuite to work with AQE|
|[#184](https://github.com/NVIDIA/spark-rapids/issues/184)|[FEA] Enable NoScalaDoc scalastyle rule|
|[#438](https://github.com/NVIDIA/spark-rapids/issues/438)|[FEA] Enable StringLPad|
|[#232](https://github.com/NVIDIA/spark-rapids/issues/232)|[FEA] Audit SortExec |
|[#236](https://github.com/NVIDIA/spark-rapids/issues/236)|[FEA] Audit ShuffleExchangeExec |
|[#355](https://github.com/NVIDIA/spark-rapids/issues/355)|[FEA] Support Multiple Spark versions in the same jar|
|[#385](https://github.com/NVIDIA/spark-rapids/issues/385)|[FEA] Support RangeExec on the GPU|
|[#317](https://github.com/NVIDIA/spark-rapids/issues/317)|[FEA] Write test wrapper to run SQL queries via pyspark|
|[#235](https://github.com/NVIDIA/spark-rapids/issues/235)|[FEA] Audit BroadcastExchangeExec|
|[#234](https://github.com/NVIDIA/spark-rapids/issues/234)|[FEA] Audit BatchScanExec|
|[#238](https://github.com/NVIDIA/spark-rapids/issues/238)|[FEA] Audit ShuffledHashJoinExec |
|[#237](https://github.com/NVIDIA/spark-rapids/issues/237)|[FEA] Audit BroadcastHashJoinExec |
|[#316](https://github.com/NVIDIA/spark-rapids/issues/316)|[FEA] Add some basic Dataframe tests for CoalesceExec|
|[#145](https://github.com/NVIDIA/spark-rapids/issues/145)|[FEA] Scala tests should support running with Adaptive Query Execution enabled|
|[#231](https://github.com/NVIDIA/spark-rapids/issues/231)|[FEA] Audit ProjectExec |
|[#229](https://github.com/NVIDIA/spark-rapids/issues/229)|[FEA] Audit FileSourceScanExec |

### Performance
|||
|:---|:---|
|[#326](https://github.com/NVIDIA/spark-rapids/issues/326)|[DISCUSS] Shuffle read-side error handling|
|[#601](https://github.com/NVIDIA/spark-rapids/issues/601)|[FEA] Optimize unnecessary sorts when replacing SortAggregate|
|[#333](https://github.com/NVIDIA/spark-rapids/issues/333)|[FEA] Better handling of reading lots of small Parquet files|
|[#511](https://github.com/NVIDIA/spark-rapids/issues/511)|[FEA] Connect shuffle table compression to shuffle exec metrics|
|[#15](https://github.com/NVIDIA/spark-rapids/issues/15)|[FEA] Multiple threads sharing the same GPU|
|[#272](https://github.com/NVIDIA/spark-rapids/issues/272)|[DOC] Getting started guide for UCX shuffle|

### Bugs Fixed
|||
|:---|:---|
|[#780](https://github.com/NVIDIA/spark-rapids/issues/780)|[BUG] Inner Join dropping data with bucketed Table input|
|[#569](https://github.com/NVIDIA/spark-rapids/issues/569)|[BUG] left_semi_join operation is abnormal and serious time-consuming|
|[#744](https://github.com/NVIDIA/spark-rapids/issues/744)|[BUG] TPC-DS query 6 now produces incorrect results.|
|[#718](https://github.com/NVIDIA/spark-rapids/issues/718)|[BUG] GpuBroadcastHashJoinExec ArrayIndexOutOfBoundsException|
|[#698](https://github.com/NVIDIA/spark-rapids/issues/698)|[BUG] batch coalesce can fail to appear between columnar shuffle and subsequent columnar operation|
|[#658](https://github.com/NVIDIA/spark-rapids/issues/658)|[BUG] GpuCoalesceBatches collectTime metric can be underreported|
|[#59](https://github.com/NVIDIA/spark-rapids/issues/59)|[BUG] enable tests for string literals in a select|
|[#486](https://github.com/NVIDIA/spark-rapids/issues/486)|[BUG] GpuWindowExec does not implement requiredChildOrdering|
|[#631](https://github.com/NVIDIA/spark-rapids/issues/631)|[BUG] Rows are dropped when AQE is enabled in some cases|
|[#671](https://github.com/NVIDIA/spark-rapids/issues/671)|[BUG] Databricks hash_aggregate_test fails trying to canonicalize a WrappedAggFunction|
|[#218](https://github.com/NVIDIA/spark-rapids/issues/218)|[BUG] Window function COUNT(x) includes null-values, when it shouldn't|
|[#153](https://github.com/NVIDIA/spark-rapids/issues/153)|[BUG] Incorrect output from partial-only hash aggregates with multiple distincts and non-distinct functions|
|[#656](https://github.com/NVIDIA/spark-rapids/issues/656)|[BUG] integration tests produce hive metadata files|
|[#607](https://github.com/NVIDIA/spark-rapids/issues/607)|[BUG] Fix misleading "cannot run on GPU" warnings when AQE is enabled|
|[#630](https://github.com/NVIDIA/spark-rapids/issues/630)|[BUG] GpuCustomShuffleReader metrics always show zero rows/batches output|
|[#643](https://github.com/NVIDIA/spark-rapids/issues/643)|[BUG] race condition while registering a buffer and spilling at the same time|
|[#606](https://github.com/NVIDIA/spark-rapids/issues/606)|[BUG] Multiple scans for same data source with TPC-DS query59 with delta format|
|[#626](https://github.com/NVIDIA/spark-rapids/issues/626)|[BUG] parquet_test showing leaked memory buffer|
|[#155](https://github.com/NVIDIA/spark-rapids/issues/155)|[BUG] Incorrect output from averages with filters in partial only mode|
|[#277](https://github.com/NVIDIA/spark-rapids/issues/277)|[BUG] HashAggregateSuite failure when AQE is enabled|
|[#276](https://github.com/NVIDIA/spark-rapids/issues/276)|[BUG] GpuCoalesceBatchSuite failure when AQE is enabled|
|[#598](https://github.com/NVIDIA/spark-rapids/issues/598)|[BUG] Non-deterministic output from MapOutputTracker.getStatistics() with AQE on GPU|
|[#192](https://github.com/NVIDIA/spark-rapids/issues/192)|[BUG] test_read_merge_schema fails on Databricks|
|[#341](https://github.com/NVIDIA/spark-rapids/issues/341)|[BUG] Document compression formats for readers/writers|
|[#587](https://github.com/NVIDIA/spark-rapids/issues/587)|[BUG] Spark3.1 changed FileScan which means or GpuScans need to be added to shim layer|
|[#362](https://github.com/NVIDIA/spark-rapids/issues/362)|[BUG] Implement getReaderForRange in the RapidsShuffleManager|
|[#528](https://github.com/NVIDIA/spark-rapids/issues/528)|[BUG] HashAggregateSuite "Avg Distinct with filter" no longer valid when testing against Spark 3.1.0|
|[#416](https://github.com/NVIDIA/spark-rapids/issues/416)|[BUG] Fix Spark 3.1.0 integration tests|
|[#556](https://github.com/NVIDIA/spark-rapids/issues/556)|[BUG] NPE when removing shuffle|
|[#553](https://github.com/NVIDIA/spark-rapids/issues/553)|[BUG] GpuColumnVector build warnings from raw type access|
|[#492](https://github.com/NVIDIA/spark-rapids/issues/492)|[BUG] Re-enable AQE integration tests|
|[#275](https://github.com/NVIDIA/spark-rapids/issues/275)|[BUG] TpchLike query 2 fails when AQE is enabled|
|[#508](https://github.com/NVIDIA/spark-rapids/issues/508)|[BUG] GpuUnion publishes metrics on the UI that are all 0|
|[#269](https://github.com/NVIDIA/spark-rapids/issues/269)|Needed to add `--conf spark.driver.extraClassPath=` |
|[#473](https://github.com/NVIDIA/spark-rapids/issues/473)|[BUG] PartMerge:countDistinct:sum fails sporadically|
|[#531](https://github.com/NVIDIA/spark-rapids/issues/531)|[BUG] Temporary RMM workaround needs to be removed|
|[#532](https://github.com/NVIDIA/spark-rapids/issues/532)|[BUG] NPE when enabling shuffle manager|
|[#525](https://github.com/NVIDIA/spark-rapids/issues/525)|[BUG] GpuFilterExec reports incorrect nullability of output in some cases|
|[#483](https://github.com/NVIDIA/spark-rapids/issues/483)|[BUG] Multiple scans for the same parquet data source|
|[#382](https://github.com/NVIDIA/spark-rapids/issues/382)|[BUG] Spark3.1 StringFallbackSuite regexp_replace null cpu fall back test fails.|
|[#489](https://github.com/NVIDIA/spark-rapids/issues/489)|[FEA] Fix Spark 3.1 GpuHashJoin since it now requires CodegenSupport|
|[#441](https://github.com/NVIDIA/spark-rapids/issues/441)|[BUG] test_broadcast_nested_loop_join_special_case fails on databricks|
|[#347](https://github.com/NVIDIA/spark-rapids/issues/347)|[BUG] Failed to read Parquet file generated by GPU-enabled Spark.|
|[#433](https://github.com/NVIDIA/spark-rapids/issues/433)|`InSet` operator produces an error for Strings|
|[#144](https://github.com/NVIDIA/spark-rapids/issues/144)|[BUG] spark.sql.legacy.parquet.datetimeRebaseModeInWrite is ignored|
|[#323](https://github.com/NVIDIA/spark-rapids/issues/323)|[BUG] GpuBroadcastNestedLoopJoinExec can fail if there are no columns|
|[#356](https://github.com/NVIDIA/spark-rapids/issues/356)|[BUG] Integration cache test for BroadcastNestedLoopJoin failure|
|[#280](https://github.com/NVIDIA/spark-rapids/issues/280)|[BUG] Full Outer Join does not work on nullable keys|
|[#149](https://github.com/NVIDIA/spark-rapids/issues/149)|[BUG] Spark driver fails to load native libs when running on node without CUDA|

### PRs
|||
|:---|:---|
|[#826](https://github.com/NVIDIA/spark-rapids/pull/826)|Fix link to cudf-0.15-cuda11.jar|
|[#815](https://github.com/NVIDIA/spark-rapids/pull/815)|Update documentation for Scala UDFs in 0.2 since you need two things|
|[#802](https://github.com/NVIDIA/spark-rapids/pull/802)|Update 0.2 CHANGELOG|
|[#793](https://github.com/NVIDIA/spark-rapids/pull/793)|Update Jenkins scripts for release|
|[#798](https://github.com/NVIDIA/spark-rapids/pull/798)|Fix shims provider override config not being seen by executors|
|[#785](https://github.com/NVIDIA/spark-rapids/pull/785)|Make shuffle run on CPU if we do a join where we read from bucketed table|
|[#765](https://github.com/NVIDIA/spark-rapids/pull/765)|Add config to override shims provider class|
|[#759](https://github.com/NVIDIA/spark-rapids/pull/759)|Add CHANGELOG for release 0.2|
|[#758](https://github.com/NVIDIA/spark-rapids/pull/758)|Skip the udf test fails periodically.|
|[#752](https://github.com/NVIDIA/spark-rapids/pull/752)|Fix snapshot plugin jar version in docs|
|[#751](https://github.com/NVIDIA/spark-rapids/pull/751)|Correct the channel for cudf installation|
|[#754](https://github.com/NVIDIA/spark-rapids/pull/754)|Filter nulls from joins where possible to improve performance|
|[#732](https://github.com/NVIDIA/spark-rapids/pull/732)|Add a timeout for RapidsShuffleIterator to prevent jobs to hang infin…|
|[#637](https://github.com/NVIDIA/spark-rapids/pull/637)|Documentation changes for 0.2 release |
|[#747](https://github.com/NVIDIA/spark-rapids/pull/747)|Disable udf tests that fail periodically|
|[#745](https://github.com/NVIDIA/spark-rapids/pull/745)|Revert Null Join Filter|
|[#741](https://github.com/NVIDIA/spark-rapids/pull/741)|Fix issue with parquet partitioned reads|
|[#733](https://github.com/NVIDIA/spark-rapids/pull/733)|Remove GPU Types from github|
|[#720](https://github.com/NVIDIA/spark-rapids/pull/720)|Stop removing GpuCoalesceBatches from non-AQE queries when AQE is enabled|
|[#729](https://github.com/NVIDIA/spark-rapids/pull/729)|Fix collect time metric in CoalesceBatches|
|[#640](https://github.com/NVIDIA/spark-rapids/pull/640)|Support running Pandas UDFs on GPUs in Python processes.|
|[#721](https://github.com/NVIDIA/spark-rapids/pull/721)|Add some more checks to databricks build scripts|
|[#714](https://github.com/NVIDIA/spark-rapids/pull/714)|Move spark 3.0.1-shims out of snapshot-shims|
|[#711](https://github.com/NVIDIA/spark-rapids/pull/711)|fix blossom checkout repo|
|[#709](https://github.com/NVIDIA/spark-rapids/pull/709)|[BUG] fix unexpected indentation issue in blossom yml|
|[#642](https://github.com/NVIDIA/spark-rapids/pull/642)|Init workflow for blossom-ci|
|[#705](https://github.com/NVIDIA/spark-rapids/pull/705)|Enable configuration check for cast string to timestamp|
|[#702](https://github.com/NVIDIA/spark-rapids/pull/702)|Update slack channel for Jenkins builds|
|[#701](https://github.com/NVIDIA/spark-rapids/pull/701)|fix checkout-ref for automerge|
|[#695](https://github.com/NVIDIA/spark-rapids/pull/695)|Fix spark-3.0.1 shim to be released|
|[#668](https://github.com/NVIDIA/spark-rapids/pull/668)|refactor automerge to support merge for protected branch|
|[#687](https://github.com/NVIDIA/spark-rapids/pull/687)|Include the UDF compiler in the dist jar|
|[#689](https://github.com/NVIDIA/spark-rapids/pull/689)|Change shims dependency to spark-3.0.1|
|[#677](https://github.com/NVIDIA/spark-rapids/pull/677)|Use multi-threaded parquet read with small files|
|[#638](https://github.com/NVIDIA/spark-rapids/pull/638)|Add Parquet-based cache serializer|
|[#613](https://github.com/NVIDIA/spark-rapids/pull/613)|Enable UCX + AQE|
|[#684](https://github.com/NVIDIA/spark-rapids/pull/684)|Enable test for literal string values in a select|
|[#686](https://github.com/NVIDIA/spark-rapids/pull/686)|Remove sorts when replacing sort aggregate if possible|
|[#675](https://github.com/NVIDIA/spark-rapids/pull/675)|Added TimeAdd|
|[#645](https://github.com/NVIDIA/spark-rapids/pull/645)|[window] Add GpuWindowExec requiredChildOrdering|
|[#676](https://github.com/NVIDIA/spark-rapids/pull/676)|fixUpJoinConsistency rule now works when AQE is enabled|
|[#683](https://github.com/NVIDIA/spark-rapids/pull/683)|Fix issues with cannonicalization of WrappedAggFunction|
|[#682](https://github.com/NVIDIA/spark-rapids/pull/682)|Fix path to start-slave.sh script in docs|
|[#673](https://github.com/NVIDIA/spark-rapids/pull/673)|Increase build timeouts on nightly and premerge builds|
|[#648](https://github.com/NVIDIA/spark-rapids/pull/648)|add signoff-check use github actions|
|[#593](https://github.com/NVIDIA/spark-rapids/pull/593)|Add support for isNaN and datetime related instructions in UDF compiler|
|[#666](https://github.com/NVIDIA/spark-rapids/pull/666)|[window] Disable GPU for COUNT(exp) queries|
|[#655](https://github.com/NVIDIA/spark-rapids/pull/655)|Implement AQE unit test for InsertAdaptiveSparkPlan|
|[#614](https://github.com/NVIDIA/spark-rapids/pull/614)|Fix for aggregation with multiple distinct and non distinct functions|
|[#657](https://github.com/NVIDIA/spark-rapids/pull/657)|Fix verify build after integration tests are run|
|[#660](https://github.com/NVIDIA/spark-rapids/pull/660)|Add in neverReplaceExec and several rules for it|
|[#639](https://github.com/NVIDIA/spark-rapids/pull/639)|BooleanType test shouldn't xfail|
|[#652](https://github.com/NVIDIA/spark-rapids/pull/652)|Mark UVM config as internal until supported|
|[#653](https://github.com/NVIDIA/spark-rapids/pull/653)|Move to the cudf-0.15 release|
|[#647](https://github.com/NVIDIA/spark-rapids/pull/647)|Improve warnings about AQE nodes not supported on GPU|
|[#646](https://github.com/NVIDIA/spark-rapids/pull/646)|Stop reporting zero metrics for GpuCustomShuffleReader|
|[#644](https://github.com/NVIDIA/spark-rapids/pull/644)|Small fix for race in catalog where a buffer could get spilled while …|
|[#623](https://github.com/NVIDIA/spark-rapids/pull/623)|Fix issues with canonicalization|
|[#599](https://github.com/NVIDIA/spark-rapids/pull/599)|[FEA] changelog generator|
|[#563](https://github.com/NVIDIA/spark-rapids/pull/563)|cudf and spark version info in artifacts|
|[#633](https://github.com/NVIDIA/spark-rapids/pull/633)|Fix leak if RebaseHelper throws during Parquet read|
|[#632](https://github.com/NVIDIA/spark-rapids/pull/632)|Copy function isSearchableType from Spark because signature changed in 3.0.1|
|[#583](https://github.com/NVIDIA/spark-rapids/pull/583)|Add udf compiler unit tests|
|[#617](https://github.com/NVIDIA/spark-rapids/pull/617)|Documentation updates for branch 0.2|
|[#616](https://github.com/NVIDIA/spark-rapids/pull/616)|Add config to reserve GPU memory|
|[#612](https://github.com/NVIDIA/spark-rapids/pull/612)|[REVIEW] Fix incorrect output from averages with filters in partial only mode|
|[#609](https://github.com/NVIDIA/spark-rapids/pull/609)|fix minor issues with instructions for building ucx|
|[#611](https://github.com/NVIDIA/spark-rapids/pull/611)|Added in profile to enable shims for SNAPSHOT releases|
|[#595](https://github.com/NVIDIA/spark-rapids/pull/595)|Parquet small file reading optimization|
|[#582](https://github.com/NVIDIA/spark-rapids/pull/582)|fix #579 Auto-merge between branches|
|[#536](https://github.com/NVIDIA/spark-rapids/pull/536)|Add test for skewed join optimization when AQE is enabled|
|[#603](https://github.com/NVIDIA/spark-rapids/pull/603)|Fix data size metric always 0 when using RAPIDS shuffle|
|[#600](https://github.com/NVIDIA/spark-rapids/pull/600)|Fix calculation of string data for compressed batches|
|[#597](https://github.com/NVIDIA/spark-rapids/pull/597)|Remove the xfail for parquet test_read_merge_schema on Databricks|
|[#591](https://github.com/NVIDIA/spark-rapids/pull/591)|Add ucx license in NOTICE-binary|
|[#596](https://github.com/NVIDIA/spark-rapids/pull/596)|Add Spark 3.0.2 to Shim layer|
|[#594](https://github.com/NVIDIA/spark-rapids/pull/594)|Filter nulls from joins where possible to improve performance.|
|[#590](https://github.com/NVIDIA/spark-rapids/pull/590)|Move GpuParquetScan/GpuOrcScan into Shim|
|[#588](https://github.com/NVIDIA/spark-rapids/pull/588)|xfail the tpch spark 3.1.0 tests that fail|
|[#572](https://github.com/NVIDIA/spark-rapids/pull/572)|Update buffer store to return compressed batches directly, add compression NVTX ranges|
|[#558](https://github.com/NVIDIA/spark-rapids/pull/558)|Fix unit tests when AQE is enabled|
|[#580](https://github.com/NVIDIA/spark-rapids/pull/580)|xfail the Spark 3.1.0 integration tests that fail |
|[#565](https://github.com/NVIDIA/spark-rapids/pull/565)|Minor improvements to TPC-DS benchmarking code|
|[#567](https://github.com/NVIDIA/spark-rapids/pull/567)|Explicitly disable AQE in one test|
|[#571](https://github.com/NVIDIA/spark-rapids/pull/571)|Fix Databricks shim layer for GpuFileSourceScanExec and GpuBroadcastExchangeExec|
|[#564](https://github.com/NVIDIA/spark-rapids/pull/564)|Add GPU decode time metric to scans|
|[#562](https://github.com/NVIDIA/spark-rapids/pull/562)|getCatalog can be called from the driver, and can return null|
|[#555](https://github.com/NVIDIA/spark-rapids/pull/555)|Fix build warnings for ColumnViewAccess|
|[#560](https://github.com/NVIDIA/spark-rapids/pull/560)|Fix databricks build for AQE support|
|[#557](https://github.com/NVIDIA/spark-rapids/pull/557)|Fix tests failing on Spark 3.1|
|[#547](https://github.com/NVIDIA/spark-rapids/pull/547)|Add GPU metrics to GpuFileSourceScanExec|
|[#462](https://github.com/NVIDIA/spark-rapids/pull/462)|Implement optimized AQE support so that exchanges run on GPU where possible|
|[#550](https://github.com/NVIDIA/spark-rapids/pull/550)|Document Parquet and ORC compression support|
|[#539](https://github.com/NVIDIA/spark-rapids/pull/539)|Update script to audit multiple Spark versions|
|[#543](https://github.com/NVIDIA/spark-rapids/pull/543)|Add metrics to GpuUnion operator|
|[#549](https://github.com/NVIDIA/spark-rapids/pull/549)|Move spark shim properties to top level pom|
|[#497](https://github.com/NVIDIA/spark-rapids/pull/497)|Add UDF compiler implementations|
|[#487](https://github.com/NVIDIA/spark-rapids/pull/487)|Add framework for batch compression of shuffle partitions|
|[#544](https://github.com/NVIDIA/spark-rapids/pull/544)|Add in driverExtraClassPath for standalone mode docs|
|[#546](https://github.com/NVIDIA/spark-rapids/pull/546)|Fix Spark 3.1.0 shim build error in GpuHashJoin|
|[#537](https://github.com/NVIDIA/spark-rapids/pull/537)|Use fresh SparkSession when capturing to avoid late capture of previous query|
|[#538](https://github.com/NVIDIA/spark-rapids/pull/538)|Revert "Temporary workaround for RMM initial pool size bug (#530)"|
|[#517](https://github.com/NVIDIA/spark-rapids/pull/517)|Add config to limit maximum RMM pool size|
|[#527](https://github.com/NVIDIA/spark-rapids/pull/527)|Add support for split and getArrayIndex|
|[#534](https://github.com/NVIDIA/spark-rapids/pull/534)|Fixes bugs around GpuShuffleEnv initialization|
|[#529](https://github.com/NVIDIA/spark-rapids/pull/529)|[BUG] Degenerate table metas were not getting copied to the heap|
|[#530](https://github.com/NVIDIA/spark-rapids/pull/530)|Temporary workaround for RMM initial pool size bug|
|[#526](https://github.com/NVIDIA/spark-rapids/pull/526)|Fix bug with nullability reporting in GpuFilterExec|
|[#521](https://github.com/NVIDIA/spark-rapids/pull/521)|Fix typo with databricks shim classname SparkShimServiceProvider|
|[#522](https://github.com/NVIDIA/spark-rapids/pull/522)|Use SQLConf instead of SparkConf when looking up SQL configs|
|[#518](https://github.com/NVIDIA/spark-rapids/pull/518)|Fix init order issue in GpuShuffleEnv when RAPIDS shuffle configured|
|[#514](https://github.com/NVIDIA/spark-rapids/pull/514)|Added clarification of RegExpReplace, DateDiff, made descriptive text consistent|
|[#506](https://github.com/NVIDIA/spark-rapids/pull/506)|Add in basic support for running tpcds like queries|
|[#504](https://github.com/NVIDIA/spark-rapids/pull/504)|Add ability to ignore tests depending on spark shim version|
|[#503](https://github.com/NVIDIA/spark-rapids/pull/503)|Remove unused async buffer spill support|
|[#501](https://github.com/NVIDIA/spark-rapids/pull/501)|disable codegen in 3.1 shim for hash join|
|[#466](https://github.com/NVIDIA/spark-rapids/pull/466)|Optimize and fix Api validation script|
|[#481](https://github.com/NVIDIA/spark-rapids/pull/481)|Codeowners|
|[#439](https://github.com/NVIDIA/spark-rapids/pull/439)|Check a PR has been committed using git signoff|
|[#319](https://github.com/NVIDIA/spark-rapids/pull/319)|Update partitioning logic in ShuffledBatchRDD|
|[#491](https://github.com/NVIDIA/spark-rapids/pull/491)|Temporarily ignore AQE integration tests|
|[#490](https://github.com/NVIDIA/spark-rapids/pull/490)|Fix Spark 3.1.0 build for HashJoin changes|
|[#482](https://github.com/NVIDIA/spark-rapids/pull/482)|Prevent bad practice in python tests|
|[#485](https://github.com/NVIDIA/spark-rapids/pull/485)|Show plan in assertion message if test fails|
|[#480](https://github.com/NVIDIA/spark-rapids/pull/480)|Fix link from README to getting-started.md|
|[#448](https://github.com/NVIDIA/spark-rapids/pull/448)|Preliminary support for keeping broadcast exchanges on GPU when AQE is enabled|
|[#478](https://github.com/NVIDIA/spark-rapids/pull/478)|Fall back to CPU for binary as string in parquet|
|[#477](https://github.com/NVIDIA/spark-rapids/pull/477)|Fix special case joins in broadcast nested loop join|
|[#469](https://github.com/NVIDIA/spark-rapids/pull/469)|Update HashAggregateSuite to work with AQE|
|[#475](https://github.com/NVIDIA/spark-rapids/pull/475)|Udf compiler pom followup|
|[#434](https://github.com/NVIDIA/spark-rapids/pull/434)|Add UDF compiler skeleton|
|[#474](https://github.com/NVIDIA/spark-rapids/pull/474)|Re-enable noscaladoc check|
|[#461](https://github.com/NVIDIA/spark-rapids/pull/461)|Fix comments style to pass scala style check|
|[#468](https://github.com/NVIDIA/spark-rapids/pull/468)|fix broken link|
|[#456](https://github.com/NVIDIA/spark-rapids/pull/456)|Add closeOnExcept to clean up code that closes resources only on exceptions|
|[#464](https://github.com/NVIDIA/spark-rapids/pull/464)|Turn off noscaladoc rule until codebase is fixed|
|[#449](https://github.com/NVIDIA/spark-rapids/pull/449)|Enforce NoScalaDoc rule in scalastyle checks|
|[#450](https://github.com/NVIDIA/spark-rapids/pull/450)|Enable scalastyle for shuffle plugin|
|[#451](https://github.com/NVIDIA/spark-rapids/pull/451)|Databricks remove unneeded files and fix build to not fail on rm when file missing|
|[#442](https://github.com/NVIDIA/spark-rapids/pull/442)|Shim layer support for Spark 3.0.0 Databricks|
|[#447](https://github.com/NVIDIA/spark-rapids/pull/447)|Add scalastyle plugin to shim module|
|[#426](https://github.com/NVIDIA/spark-rapids/pull/426)|Update BufferMeta to support multiple codec buffers per table|
|[#440](https://github.com/NVIDIA/spark-rapids/pull/440)|Run mortgage test both with AQE on and off|
|[#445](https://github.com/NVIDIA/spark-rapids/pull/445)|Added in StringRPad and StringLPad|
|[#422](https://github.com/NVIDIA/spark-rapids/pull/422)|Documentation updates|
|[#437](https://github.com/NVIDIA/spark-rapids/pull/437)|Fix bug with InSet and Strings|
|[#435](https://github.com/NVIDIA/spark-rapids/pull/435)|Add in checks for Parquet LEGACY date/time rebase|
|[#432](https://github.com/NVIDIA/spark-rapids/pull/432)|Fix batch use-after-close in partitioning, shuffle env init|
|[#423](https://github.com/NVIDIA/spark-rapids/pull/423)|Fix duplicates includes in assembly jar|
|[#418](https://github.com/NVIDIA/spark-rapids/pull/418)|CI Add unit tests running for Spark 3.0.1|
|[#421](https://github.com/NVIDIA/spark-rapids/pull/421)|Make it easier to run TPCxBB benchmarks from spark shell|
|[#413](https://github.com/NVIDIA/spark-rapids/pull/413)|Fix download link|
|[#414](https://github.com/NVIDIA/spark-rapids/pull/414)|Shim Layer to support multiple Spark versions |
|[#406](https://github.com/NVIDIA/spark-rapids/pull/406)|Update cast handling to deal with new libcudf casting limitations|
|[#405](https://github.com/NVIDIA/spark-rapids/pull/405)|Change slave->worker|
|[#395](https://github.com/NVIDIA/spark-rapids/pull/395)|Databricks doc updates|
|[#401](https://github.com/NVIDIA/spark-rapids/pull/401)|Extended the FAQ|
|[#398](https://github.com/NVIDIA/spark-rapids/pull/398)|Add tests for GpuPartition|
|[#352](https://github.com/NVIDIA/spark-rapids/pull/352)|Change spark tgz package name|
|[#397](https://github.com/NVIDIA/spark-rapids/pull/397)|Fix small bug in ShuffleBufferCatalog.hasActiveShuffle|
|[#286](https://github.com/NVIDIA/spark-rapids/pull/286)|[REVIEW] Updated join tests for cache|
|[#393](https://github.com/NVIDIA/spark-rapids/pull/393)|Contributor license agreement|
|[#389](https://github.com/NVIDIA/spark-rapids/pull/389)|Added in support for RangeExec|
|[#390](https://github.com/NVIDIA/spark-rapids/pull/390)|Ucx getting started|
|[#391](https://github.com/NVIDIA/spark-rapids/pull/391)|Hide slack channel in Jenkins scripts|
|[#387](https://github.com/NVIDIA/spark-rapids/pull/387)|Remove the term whitelist|
|[#365](https://github.com/NVIDIA/spark-rapids/pull/365)|[REVIEW] Timesub tests|
|[#383](https://github.com/NVIDIA/spark-rapids/pull/383)|Test utility to compare SQL query results between CPU and GPU|
|[#380](https://github.com/NVIDIA/spark-rapids/pull/380)|Fix databricks notebook link|
|[#378](https://github.com/NVIDIA/spark-rapids/pull/378)|Added in FAQ and fixed spelling|
|[#377](https://github.com/NVIDIA/spark-rapids/pull/377)|Update heading in configs.md|
|[#373](https://github.com/NVIDIA/spark-rapids/pull/373)|Modifying branch name to conform with rapidsai branch name change|
|[#376](https://github.com/NVIDIA/spark-rapids/pull/376)|Add our session extension correctly if there are other extensions configured|
|[#374](https://github.com/NVIDIA/spark-rapids/pull/374)|Fix rat issue for notebooks|
|[#364](https://github.com/NVIDIA/spark-rapids/pull/364)|Update Databricks patch for changes to GpuSortMergeJoin|
|[#371](https://github.com/NVIDIA/spark-rapids/pull/371)|fix typo and use regional bucket per GCP's update|
|[#359](https://github.com/NVIDIA/spark-rapids/pull/359)|Karthik changes|
|[#353](https://github.com/NVIDIA/spark-rapids/pull/353)|Fix broadcast nested loop join for the no column case|
|[#313](https://github.com/NVIDIA/spark-rapids/pull/313)|Additional tests for broadcast hash join|
|[#342](https://github.com/NVIDIA/spark-rapids/pull/342)|Implement build-side rules for shuffle hash join|
|[#349](https://github.com/NVIDIA/spark-rapids/pull/349)|Updated join code to treat null equality properly|
|[#335](https://github.com/NVIDIA/spark-rapids/pull/335)|Integration tests on spark 3.0.1-SNAPSHOT & 3.1.0-SNAPSHOT|
|[#346](https://github.com/NVIDIA/spark-rapids/pull/346)|Update the Title Header for Fine Tuning|
|[#344](https://github.com/NVIDIA/spark-rapids/pull/344)|Fix small typo in readme|
|[#331](https://github.com/NVIDIA/spark-rapids/pull/331)|Adds iterator and client unit tests, and prepares for more fetch failure handling|
|[#337](https://github.com/NVIDIA/spark-rapids/pull/337)|Fix Scala compile phase to allow Java classes referencing Scala classes|
|[#332](https://github.com/NVIDIA/spark-rapids/pull/332)|Match GPU overwritten functions with SQL functions from FunctionRegistry|
|[#339](https://github.com/NVIDIA/spark-rapids/pull/339)|Fix databricks build|
|[#338](https://github.com/NVIDIA/spark-rapids/pull/338)|Move GpuPartitioning to a separate file|
|[#310](https://github.com/NVIDIA/spark-rapids/pull/310)|Update release Jenkinsfile for Databricks|
|[#330](https://github.com/NVIDIA/spark-rapids/pull/330)|Hide private info in Jenkins scripts|
|[#324](https://github.com/NVIDIA/spark-rapids/pull/324)|Add in basic support for GpuCartesianProductExec|
|[#328](https://github.com/NVIDIA/spark-rapids/pull/328)|Enable slack notification for Databricks build|
|[#321](https://github.com/NVIDIA/spark-rapids/pull/321)|update databricks patch for GpuBroadcastNestedLoopJoinExec|
|[#322](https://github.com/NVIDIA/spark-rapids/pull/322)|Add oss.sonatype.org to download the cudf jar|
|[#320](https://github.com/NVIDIA/spark-rapids/pull/320)|Don't mount passwd/group to the container|
|[#258](https://github.com/NVIDIA/spark-rapids/pull/258)|Enable running TPCH tests with AQE enabled|
|[#318](https://github.com/NVIDIA/spark-rapids/pull/318)|Build docker image with Dockerfile|
|[#309](https://github.com/NVIDIA/spark-rapids/pull/309)|Update databricks patch to latest changes|
|[#312](https://github.com/NVIDIA/spark-rapids/pull/312)|Trigger branch-0.2 integration test|
|[#307](https://github.com/NVIDIA/spark-rapids/pull/307)|[Jenkins] Update the release script and Jenkinsfile|
|[#304](https://github.com/NVIDIA/spark-rapids/pull/304)|[DOC][Minor] Fix typo in spark config name.|
|[#303](https://github.com/NVIDIA/spark-rapids/pull/303)|Update compatibility doc for -0.0 issues|
|[#301](https://github.com/NVIDIA/spark-rapids/pull/301)|Add info about branches in README.md|
|[#296](https://github.com/NVIDIA/spark-rapids/pull/296)|Added in basic support for broadcast nested loop join|
|[#297](https://github.com/NVIDIA/spark-rapids/pull/297)|Databricks CI improvements and support runtime env parameter to xfail certain tests|
|[#292](https://github.com/NVIDIA/spark-rapids/pull/292)|Move artifacts version in version-def.sh|
|[#254](https://github.com/NVIDIA/spark-rapids/pull/254)|Cleanup QA tests|
|[#289](https://github.com/NVIDIA/spark-rapids/pull/289)|Clean up GpuCollectLimitMeta and add in metrics|
|[#287](https://github.com/NVIDIA/spark-rapids/pull/287)|Add in support for right join and fix issues build right|
|[#273](https://github.com/NVIDIA/spark-rapids/pull/273)|Added releases to the README.md|
|[#285](https://github.com/NVIDIA/spark-rapids/pull/285)|modify run_pyspark_from_build.sh to be bash 3 friendly|
|[#281](https://github.com/NVIDIA/spark-rapids/pull/281)|Add in support for Full Outer Join on non-null keys|
|[#274](https://github.com/NVIDIA/spark-rapids/pull/274)|Add RapidsDiskStore tests|
|[#259](https://github.com/NVIDIA/spark-rapids/pull/259)|Add RapidsHostMemoryStore tests|
|[#282](https://github.com/NVIDIA/spark-rapids/pull/282)|Update Databricks patch for 0.2 branch|
|[#261](https://github.com/NVIDIA/spark-rapids/pull/261)|Add conditional xfail test for DISTINCT aggregates with NaN|
|[#263](https://github.com/NVIDIA/spark-rapids/pull/263)|More time ops|
|[#256](https://github.com/NVIDIA/spark-rapids/pull/256)|Remove special cases for contains, startsWith, and endWith|
|[#253](https://github.com/NVIDIA/spark-rapids/pull/253)|Remove GpuAttributeReference and GpuSortOrder|
|[#271](https://github.com/NVIDIA/spark-rapids/pull/271)|Update the versions for 0.2.0 properly for the databricks build|
|[#162](https://github.com/NVIDIA/spark-rapids/pull/162)|Integration tests for corner cases in window functions.|
|[#264](https://github.com/NVIDIA/spark-rapids/pull/264)|Add a local mvn repo for nightly pipeline|
|[#262](https://github.com/NVIDIA/spark-rapids/pull/262)|Refer to branch-0.2|
|[#255](https://github.com/NVIDIA/spark-rapids/pull/255)|Revert change to make dependencies of shaded jar optional|
|[#257](https://github.com/NVIDIA/spark-rapids/pull/257)|Fix link to RAPIDS cudf in index.md|
|[#252](https://github.com/NVIDIA/spark-rapids/pull/252)|Update to 0.2.0-SNAPSHOT and cudf-0.15-SNAPSHOT|

## Release 0.1

### Features
|||
|:---|:---|
|[#74](https://github.com/NVIDIA/spark-rapids/issues/74)|[FEA] Support ToUnixTimestamp|
|[#21](https://github.com/NVIDIA/spark-rapids/issues/21)|[FEA] NormalizeNansAndZeros|
|[#105](https://github.com/NVIDIA/spark-rapids/issues/105)|[FEA] integration tests for equi-joins|

### Bugs Fixed
|||
|:---|:---|
|[#116](https://github.com/NVIDIA/spark-rapids/issues/116)|[BUG] calling replace with a NULL throws an exception|
|[#168](https://github.com/NVIDIA/spark-rapids/issues/168)|[BUG] GpuUnitTests Date tests leak column vectors|
|[#209](https://github.com/NVIDIA/spark-rapids/issues/209)|[BUG] Developers section in pom need to be updated|
|[#204](https://github.com/NVIDIA/spark-rapids/issues/204)|[BUG] Code coverage docs are out of date|
|[#154](https://github.com/NVIDIA/spark-rapids/issues/154)|[BUG] Incorrect output from partial-only averages with nulls|
|[#61](https://github.com/NVIDIA/spark-rapids/issues/61)|[BUG] Cannot disable Parquet, ORC, CSV reading when using FileSourceScanExec|

### PRs
|||
|:---|:---|
|[#249](https://github.com/NVIDIA/spark-rapids/pull/249)|Compatability -> Compatibility|
|[#247](https://github.com/NVIDIA/spark-rapids/pull/247)|Add index.md for default doc page, fix table formatting for configs|
|[#241](https://github.com/NVIDIA/spark-rapids/pull/241)|Let default branch to master per the release rule|
|[#177](https://github.com/NVIDIA/spark-rapids/pull/177)|Fixed leaks in unit test and use ColumnarBatch for testing|
|[#243](https://github.com/NVIDIA/spark-rapids/pull/243)|Jenkins file for Databricks release|
|[#225](https://github.com/NVIDIA/spark-rapids/pull/225)|Make internal project dependencies optional for shaded artifact|
|[#242](https://github.com/NVIDIA/spark-rapids/pull/242)|Add site pages|
|[#221](https://github.com/NVIDIA/spark-rapids/pull/221)|Databricks Build Support|
|[#215](https://github.com/NVIDIA/spark-rapids/pull/215)|Remove CudfColumnVector|
|[#213](https://github.com/NVIDIA/spark-rapids/pull/213)|Add RapidsDeviceMemoryStore tests|
|[#214](https://github.com/NVIDIA/spark-rapids/pull/214)|[REVIEW] Test failure to pass Attribute as GpuAttribute|
|[#211](https://github.com/NVIDIA/spark-rapids/pull/211)|Add project leads to pom developer list|
|[#210](https://github.com/NVIDIA/spark-rapids/pull/210)|Updated coverage docs|
|[#195](https://github.com/NVIDIA/spark-rapids/pull/195)|Support public release for plugin jar|
|[#208](https://github.com/NVIDIA/spark-rapids/pull/208)|Remove unneeded comment from pom.xml|
|[#191](https://github.com/NVIDIA/spark-rapids/pull/191)|WindowExec handle different spark distributions|
|[#181](https://github.com/NVIDIA/spark-rapids/pull/181)|Remove INCOMPAT for NormalizeNanAndZero, KnownFloatingPointNormalized|
|[#196](https://github.com/NVIDIA/spark-rapids/pull/196)|Update Spark dependency to the released 3.0.0 artifacts|
|[#206](https://github.com/NVIDIA/spark-rapids/pull/206)|Change groupID to 'com.nvidia' in IT scripts|
|[#202](https://github.com/NVIDIA/spark-rapids/pull/202)|Fixed issue for contains when searching for an empty string|
|[#201](https://github.com/NVIDIA/spark-rapids/pull/201)|Fix name of scan|
|[#200](https://github.com/NVIDIA/spark-rapids/pull/200)|Fix issue with GpuAttributeReference not overrideing references|
|[#197](https://github.com/NVIDIA/spark-rapids/pull/197)|Fix metrics for writes|
|[#186](https://github.com/NVIDIA/spark-rapids/pull/186)|Fixed issue with nullability on concat|
|[#193](https://github.com/NVIDIA/spark-rapids/pull/193)|Add RapidsBufferCatalog tests|
|[#188](https://github.com/NVIDIA/spark-rapids/pull/188)|rebrand to com.nvidia instead of ai.rapids|
|[#189](https://github.com/NVIDIA/spark-rapids/pull/189)|Handle AggregateExpression having resultIds parameter instead of a single resultId|
|[#190](https://github.com/NVIDIA/spark-rapids/pull/190)|FileSourceScanExec can have logicalRelation parameter on some distributions|
|[#185](https://github.com/NVIDIA/spark-rapids/pull/185)|Update type of parameter of GpuExpandExec to make it consistent|
|[#172](https://github.com/NVIDIA/spark-rapids/pull/172)|Merge qa test to integration test|
|[#180](https://github.com/NVIDIA/spark-rapids/pull/180)|Add MetaUtils unit tests|
|[#171](https://github.com/NVIDIA/spark-rapids/pull/171)|Cleanup scaladoc warnings about missing links|
|[#176](https://github.com/NVIDIA/spark-rapids/pull/176)|Updated join tests to cover more data.|
|[#169](https://github.com/NVIDIA/spark-rapids/pull/169)|Remove dependency on shaded Spark artifact|
|[#174](https://github.com/NVIDIA/spark-rapids/pull/174)|Added in fallback tests|
|[#165](https://github.com/NVIDIA/spark-rapids/pull/165)|Move input metadata tests to pyspark|
|[#173](https://github.com/NVIDIA/spark-rapids/pull/173)|Fix setting local mode for tests|
|[#160](https://github.com/NVIDIA/spark-rapids/pull/160)|Integration tests for normalizing NaN/zeroes.|
|[#163](https://github.com/NVIDIA/spark-rapids/pull/163)|Ignore the order locally for repartition tests|
|[#157](https://github.com/NVIDIA/spark-rapids/pull/157)|Add partial and final only hash aggregate tests and fix nulls corner case for Average|
|[#159](https://github.com/NVIDIA/spark-rapids/pull/159)|Add integration tests for joins|
|[#158](https://github.com/NVIDIA/spark-rapids/pull/158)|Orc merge schema fallback and FileScan format configs|
|[#164](https://github.com/NVIDIA/spark-rapids/pull/164)|Fix compiler warnings|
|[#152](https://github.com/NVIDIA/spark-rapids/pull/152)|Moved cudf to 0.14 for CI|
|[#151](https://github.com/NVIDIA/spark-rapids/pull/151)|Switch CICD pipelines to Github|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
