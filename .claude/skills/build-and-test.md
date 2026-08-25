# Build and Test

Build, test, and validate spark-rapids modules.

## Build Commands

```bash
# Full build (skip tests)
mvn clean verify -DskipTests

# Build specific module
mvn clean verify -DskipTests -pl sql-plugin

# Unit tests (default shim)
mvn test -pl tests

# Unit tests for specific Spark version
mvn test -pl tests -Dbuildver=341

# Integration tests (requires running Spark cluster with GPU)
cd integration_tests
./run_pyspark_from_build.sh

# Run a specific integration test
TEST=src/main/python/string_test.py::test_like ./run_pyspark_from_build.sh

# Delta Lake integration tests (requires delta-lake module JAR)
cd integration_tests
./run_pyspark_from_build.sh --delta_lake

# Iceberg integration tests (requires iceberg module JAR)
cd integration_tests
./run_pyspark_from_build.sh --iceberg

# Pre-merge build versions (check pom.xml for current list)
mvn verify -Dbuildver=330
mvn verify -Dbuildver=341

# Scalastyle check (whole project)
mvn scalastyle:check

# Scalastyle check (single module)
mvn scalastyle:check -pl sql-plugin

# Build all supported shims (slow)
mvn clean verify -DskipTests -Dbuildall
```

## Performance Validation

- Use targeted micro-benchmarks for expression/operator changes (NOT NDS)
- Use real-world benchmarks (NDS/TPC-DS) for framework-level changes
- Distinguish planning-time vs runtime impact
- Run multiple times to confirm results are outside noise range
- Verify test settings are reasonable: GPU type, cluster size, data size, data distribution, test query
