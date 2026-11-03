# GPU Operator Patterns

Common patterns for implementing GPU operators in spark-rapids.

## GPU Operator Registration

Add a shared expression rule to the closest domain registry, such as
`GpuMathAndDateExpressionOverrides` or `GpuCollectionExpressionOverrides`.
Define its metadata as a named case class in the corresponding
`Gpu*ExpressionRuleMetas.scala` file, or beside the GPU operator when they are
tightly coupled:

```scala
case class MyExpressionRuleMeta(
    expression: MyExpression,
    override val conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends UnaryExprMeta[MyExpression](expression, conf, parent, rule) {

  override def convertToGpu(child: Expression): GpuExpression =
    GpuMyExpression(child)
}
```

Pass the named case class constructor to the registry rule:

```scala
expr[MyExpression](
  "Description of what this does on GPU",
  ExprChecks.unaryProject(
    TypeSig.commonCudfTypes,  // output types
    TypeSig.all,              // Spark output types
    TypeSig.commonCudfTypes,  // input types
    TypeSig.all),             // Spark input types
  MyExpressionRuleMeta)
```

Do not add shared rules directly to `GpuOverrides` or use a constructor lambda.
Register Spark-version-specific expressions through the appropriate shim. See
[`docs/dev/README.md`](../../docs/dev/README.md#registering-a-gpu-expression)
for the domain registry list and validation guidance.

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
