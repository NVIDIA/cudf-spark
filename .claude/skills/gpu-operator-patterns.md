# GPU Operator Patterns

Common patterns for implementing GPU operators in spark-rapids.

## GPU Operator Registration

New GPU operators are registered in `GpuOverrides.scala`:
```scala
GpuOverrides.expr[MyExpression](
  "Description of what this does on GPU",
  ExprChecks.unaryProject(
    TypeSig.commonCudf,  // output types
    TypeSig.all,         // Spark output types
    TypeSig.commonCudf,  // input types
    TypeSig.all),        // Spark input types
  (expr, conf, parent, rule) => new UnaryExprMeta[MyExpression](expr, conf, parent, rule) {
    override def convertToGpu(child: Expression): GpuExpression =
      GpuMyExpression(child)
  })
```

## CPU Fallback

When a GPU operator cannot handle certain inputs:
```scala
override def tagExprForGpu(): Unit = {
  if (someUnsupportedCondition) {
    willNotWorkOnGpu("reason for fallback")
  }
}
```

## Spill Management

```scala
// Wrap batch for spill support
val spillable = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_BATCHING_PRIORITY)

// Use within retry
withRetryNoSplit(spillable) { attempt =>
  withResource(attempt.getColumnarBatch()) { batch =>
    // GPU work here
  }
}
```
