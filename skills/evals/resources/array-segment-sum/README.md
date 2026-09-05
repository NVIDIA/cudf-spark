# Array Segment Sum Resources

`common/` contains the shared fixture files used as prerequisites across UDF skill evals:

- the CPU UDF, used by all skills
- a known-good unit test (output of `udf-gen-test`, used by conversion/benchmark/optimization skills)
- a known-good cuDF RapidsUDF (output of `udf-convert-to-cudf`, used by benchmark/optimization skills)

`judge/` contains intentionally flawed test fixtures for the judge-conversion eval. These are used to check whether the judge skill catches weak validation, such as incomplete edge-case coverage or tests that do not prove real GPU execution.
