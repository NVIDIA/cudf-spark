UDF Compiler
============

How to run tests
----------------

From `rapids-plugin-4-spark` root directory, use this command to run the `OpcodeSuite`:

```
mvn test -DwildcardSuites=com.nvidia.spark.OpcodeSuite
```

How to run
----------

The UDF compiler is included in the rapids-4-spark jar that is produced by the `dist` maven project.  Set up your cluster to run the RAPIDS Accelerator for Apache Spark
and this udf plugin will be automatically injected to spark extensions when `com.nvidia.spark.SQLPlugin` is set.

The plugin is still disabled by default and you will need to set `spark.rapids.sql.udfCompiler.enabled` to `true` to enable it. 
