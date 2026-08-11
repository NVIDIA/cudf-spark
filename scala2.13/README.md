# Scala 2.13 Notes

Currently the RAPIDS Accelerator for Apache Spark can be built with Scala 2.13 only with 
Apache Spark versions 3.3.0 and higher. It does **not** support Cloudera or Databricks 
distributions of Apache Spark. You can only use the Scala 2.13 plugin JAR with an Apache Spark 
distribution compiled with Scala 2.13.

## Notes on source organization

For some parts of the code, we need to have specific versions of Scala code for both Scala 2.12 and 
2.13. You will see these organized using the following directory pattern:

```
spark-rapids/<module>/src/main/scala-<scala_version>/<path to package>/Source.scala
```

This is necessary in a handful of cases where there are distinct differences in the language 
requirements needed. First, try to ensure the same code compiles with both Scala 2.12 and 2.13. If 
you must diverge the code for supporting the different languages, please ensure that you refactor
the code as much as possible to avoid any code duplication that might occur.

## Building with Scala 2.13

You can use Maven to build the plugin. Like with Scala 2.12, we recommend building up to the `verify`
phase.

```shell script
mvn verify -f scala2.13/
```

After a successful build, the RAPIDS Accelerator jar will be in the `scala2.13/dist/target/` directory.
This will build the plugin for a single version of Spark.  By default, this is Apache Spark
3.3.0. To build against other versions of Spark you use the `-Dbuildver=XXX` command line option
to Maven. For instance to build Spark 3.3.0 you would use:

```shell script
mvn -Dbuildver=330 verify
```

See [CONTRIBUTING](../CONTRIBUTING.md#building-from-source) for more information on how to use Maven 
to build the plugin. All of the options used for Scala 2.12 apply to Scala 2.13

You can also use the `buildall` script in the parent directory to build against all supported versions 
of Apache Spark.

```shell script
./build/buildall --profile=noSnapshotsScala213
```

Or if you want to build against multiple specific versions of Spark, you can specify the
`--scala213` argument:

```shell script
./build/buildall --profile=330,340 --scala213
```

## Updating build files for Scala 2.13

If you make changes in the parent `pom.xml` or any other of the module `pom.xml` files, you must
run the following command to sync the changes between the Scala 2.12 and 2.13 pom files:

```shell script
./build/make-scala-version-build-files.sh 2.13
```

That way any new dependencies or other changes will be picked up in the Scala 2.13 build.

## IDE Integration with IntelliJ

You should be able to open the `scala2.13` directory directly in IntelliJ as a separate project. You can build and 
debug as normal, although there are slight differences in how to navigate the source. In particular, when you select 
a particular build profile, you will only be able to navigate the source used by modules that are included for that
spark version.
