# Java interoperability for unified readers

The new readers use Java contracts in `com.nvidia.spark.rapids.reader`.
Keep Scala-specific calls in Java adapters in `sql-plugin`, so the contracts
remain usable by format and table modules without changing original readers.

| Existing helper | Java boundary |
| --- | --- |
| `Arm.withResource` and `Arm.closeOnExcept` | `ReaderResources` handles ownership and preserves the operation's exception if cleanup fails. |
| `RmmRapidsRetryIterator.withRetryNoSplit` | `ReaderResources.withRetryNoSplit` accepts an idempotent Java supplier. |
| `TrampolineUtil.setTaskContext/unsetTaskContext` | `ReaderTaskContext` installs the captured Spark context and restores a worker's prior context. |
| `UnboundedAsyncRunner` and `MemoryBoundedAsyncRunner` | Accessible base classes for the separate Java task-scope adapter. Resource-bounded executors cannot accept ordinary callables. |
| `MakeParquetTableProducer` and Iceberg post-processors | Accessible JVM helpers to evaluate in their format-specific implementation issues; not generic reader dependencies. |
| Reader instance methods such as Iceberg `filterParquetBlocks` | Behavioral references only. Add new Java planning instead of inheriting or extracting old reader methods. |

Java uses the existing module language target. Java adapters must compile against
both supported Scala binary versions and corresponding Spark profiles. New unit
tests follow the Java/JUnit conventions under `tests/src/test/java`.

Resource-owning callbacks transfer ownership only on successful return unless
their contract explicitly consumes inputs on entry. Closeable output iterators own
retained decode resources; returned batches belong to their consumer.
