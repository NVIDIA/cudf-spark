# AST JIT follow-up work

## Low priority: structured ANSI error identity

Status: deferred while project AST JIT remains experimental and disabled by default.

cuDF currently reports the JIT failure category, such as `OVERFLOW` or
`DIVISION_BY_ZERO`, but not the specific failing row-IR node. When a fused
expression contains multiple fallible operations, cudf-spark must infer the
Spark operator and query context from the first compatible error site. The
error category is preserved, but the reported operator or query context can
differ from Spark CPU.

Follow-up work:

- Add a structured cuDF/JNI error code instead of matching exception text.
- Return a stable failing-node or site identifier from row-IR execution.
- Carry that identifier through the Java bindings and map it to the exact
  `AstJitErrorSite` in cudf-spark.
- Remove the fallback first-compatible-site inference and the corresponding
  compatibility limitation.

Relevant code:

- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/basicPhysicalOperators.scala`
  (`GpuProjectAstExec.translateAstJitError`)
- `docs/compatibility.md` (`Project AST JIT ANSI error reporting`)

Acceptance criteria:

- Nested and sibling fallible operations report the same Spark error class,
  operator, and query context as CPU execution.
- Error translation does not depend on the full cuDF exception message.
