/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.rapids.aggregate

import ai.rapids.cudf
import ai.rapids.cudf.GroupByAggregation
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.{ShimExpression, ShimUnaryExpression}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, ExprId}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/**
 * Trait that all aggregate functions implement.
 *
 * Aggregates start with some input from the child plan or from another aggregate
 * (or from itself if the aggregate is merging several batches).
 *
 * In general terms an aggregate function can be in one of two modes of operation:
 * update or merge. Either the function is aggregating raw input, or it is merging
 * previously aggregated data. Normally, Spark breaks up the processing of the aggregate
 * in two exec nodes (a partial aggregate and a final), and the are separated by a
 * shuffle boundary. That is not true for all aggregates, especially when looking at
 * other flavors of Spark. What doesn't change is the core function of updating or
 * merging. Note that an aggregate can merge right after an update is
 * performed, as we have cases where input batches are update-aggregated and then
 * a bigger batch is built by merging together those pre-aggregated inputs.
 *
 * Aggregates have an interface to Spark and that is defined by `aggBufferAttributes`.
 * This collection of attributes must match the Spark equivalent of the aggregate,
 * so that if half of the aggregate (update or merge) executes on the CPU, we can
 * be compatible. The GpuAggregateFunction adds special steps to ensure that it can
 * produce (and consume) batches in the shape of `aggBufferAttributes`.
 *
 * The general transitions that are implemented in the aggregate function are as
 * follows:
 *
 * 1) `inputProjection` -> `updateAggregates`: `inputProjection` creates a sequence of
 *    values that are operated on by the `updateAggregates`. The length of `inputProjection`
 *    must be the same as `updateAggregates`, and `updateAggregates` (cuDF aggregates) should
 *    be able to work with the product of the `inputProjection` (i.e. types are compatible)
 *
 * 2) `updateAggregates` -> `postUpdate`: after the cuDF update aggregate, a post process step
 *    can (optionally) be performed. The `postUpdate` takes the output of `updateAggregate`
 *    that must match the order of columns and types as specified in `aggBufferAttributes`.
 *
 * 3) `postUpdate` -> `preMerge`: preMerge prepares batches before going into the `mergeAggregate`.
 *    The `preMerge` step binds to `aggBufferAttributes`, so it can be used to transform Spark
 *    compatible batch to a batch that the cuDF merge aggregate expects. Its input has the
 *    same shape as that produced by `postUpdate`.
 *
 * 4) `mergeAggregates`->`postMerge`: postMerge optionally transforms the output of the cuDF merge
 *    aggregate in two situations:
 *      1 - The step is used to match the `aggBufferAttributes` references for partial
 *           aggregates where each partially aggregated batch is getting merged with
 *           `AggHelper(merge=true)`
 *      2 - In a final aggregate where the merged batches are transformed to what
 *          `evaluateExpression` expects. For simple aggregates like sum or count,
 *          `evaluateExpression` is just `aggBufferAttributes`, but for more complex
 *          aggregates, it is an expression (see GpuAverage and GpuM2 subclasses) that
 *          relies on the merge step producing a columns in the shape of `aggBufferAttributes`.
 */
trait GpuAggregateFunction extends GpuExpression
    with ShimExpression
    with GpuUnevaluable {

  def filteredInputProjection(filter: Expression): Seq[Expression] =
    inputProjection.map { ip =>
      GpuIf(filter, ip, GpuLiteral(null, ip.dataType))
    }

  /**
   * These are values that spark calls initial because it uses
   * them to initialize the aggregation buffer, and returns them in case
   * of an empty aggregate when there are no expressions.
   *
   * In our case they are only used in a very specific case:
   * the empty input reduction case. In this case we don't have input
   * to reduce, but we do have reduction functions, so each reduction function's
   * `initialValues` is invoked to populate a single row of output.
   **/
  val initialValues: Seq[Expression]

  /**
   * Using the child reference, define the shape of input batches sent to
   * the update expressions
   * @note this can be thought of as "pre" update: as update consumes its
   *       output in order
   */
  val inputProjection: Seq[Expression]

  /**
   * update: first half of the aggregation
   * The sequence of `CudfAggregate` must match the shape of `inputProjections`,
   * and care must be taken to ensure that each cuDF aggregate is able to work
   * with the corresponding inputProjection (i.e. inputProjection[i] is the input
   * to updateAggregates[i]).
   */
  val updateAggregates: Seq[CudfAggregate]

  /**
   * This is the last step in the update phase. It can optionally modify the result of the
   * cuDF update aggregates, or be a pass-through.
   * postUpdateAttr: matches the order (and types) of `updateAggregates`
   * postUpdate: binds to `postUpdateAttr` and defines an expression that results
   *             in what Spark expects from the update.
   *             By default this is `postUpdateAttr`, as it should match the
   *             shape of the Spark agg buffer leaving cuDF, but in the
   *             M2 and Count cases we overwrite it, because the cuDF shape isn't
   *             what Spark expects.
   */
  final lazy val postUpdateAttr: Seq[AttributeReference] = updateAggregates.map(_.attr)
  lazy val postUpdate: Seq[Expression] = postUpdateAttr

  /**
   * This step is the first step into the merge phase. It can optionally modify the result of
   * the postUpdate before it goes into the cuDF merge aggregation.
   * preMerge: modify a partial batch to match the input required by a merge aggregate
   *
   * This always binds to `aggBufferAttributes` as that is the inbound schema
   * for this aggregate from Spark. If it is set to `aggBufferAttributes` by default
   * so the bind behaves like a pass through in most cases.
   */
  lazy val preMerge: Seq[Expression] = aggBufferAttributes

  /**
   * merge: second half of the aggregation. Also used to merge multiple batches in the
   * update or merge stages. These cuDF aggregates consume the output of `preMerge`.
   * The sequence of `CudfAggregate` must match the shape of `aggBufferAttributes`,
   * and care must be taken to ensure that each cuDF aggregate is able to work
   * with the corresponding input (i.e. aggBufferAttributes[i] is the input
   * to mergeAggregates[i]). If a transformation is required, `preMerge` can be used
   * to mutate the batches before they arrive at `mergeAggregates`.
   */
  val mergeAggregates: Seq[CudfAggregate]

  /**
   * This is the last aggregation step, which optionally changes the result of the
   * `mergeAggregate`.
   * postMergeAttr: matches the order (and types) of `mergeAggregates`
   * postMerge: binds to `postMergeAttr` and defines an expression that results
   *            in what Spark expects from the merge. We set this to `postMergeAttr`
   *            by default, for the pass through case (like in `postUpdate`). GpuM2
   *            is the exception, where `postMerge` mutates the result of the
   *            `mergeAggregates` to output what Spark expects.
   */
  final lazy val postMergeAttr: Seq[AttributeReference] = mergeAggregates.map(_.attr)
  lazy val postMerge: Seq[Expression] = postMergeAttr

  /**
   * This takes the output of `postMerge` computes the final result of the aggregation.
   * @note `evaluateExpression` is bound to `aggBufferAttributes`, so the references used in
   *       `evaluateExpression` must also be used in `aggBufferAttributes`.
   */
  val evaluateExpression: Expression

  /**
   * This is the contract with the outside world. It describes what the output of postUpdate should
   * look like, and what the input to preMerge looks like. It also describes what the output of
   * postMerge must look like.
   */
  def aggBufferAttributes: Seq[AttributeReference]

  def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$prettyName($distinct${children.map(_.sql).mkString(", ")})"
  }

  /** String representation used in explain plans. */
  def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    prettyName + flatArguments.mkString(start, ", ", ")")
  }

  /** An aggregate function is not foldable. */
  final override def foldable: Boolean = false
}

case class WrappedAggFunction(aggregateFunction: GpuAggregateFunction, filter: Expression)
    extends GpuAggregateFunction {
  override val inputProjection: Seq[Expression] = aggregateFunction.filteredInputProjection(filter)

  /** Attributes of fields in aggBufferSchema. */
  override def aggBufferAttributes: Seq[AttributeReference] =
    aggregateFunction.aggBufferAttributes
  override def nullable: Boolean = aggregateFunction.nullable

  override def dataType: DataType = aggregateFunction.dataType

  override def children: Seq[Expression] = Seq(aggregateFunction, filter)

  override val initialValues: Seq[Expression] = aggregateFunction.initialValues
  override lazy val updateAggregates: Seq[CudfAggregate] = aggregateFunction.updateAggregates
  override lazy val mergeAggregates: Seq[CudfAggregate] = aggregateFunction.mergeAggregates
  override val evaluateExpression: Expression = aggregateFunction.evaluateExpression

  override lazy val postUpdate: Seq[Expression] = aggregateFunction.postUpdate

  override lazy val preMerge: Seq[Expression] = aggregateFunction.preMerge
  override lazy val postMerge: Seq[Expression] = aggregateFunction.postMerge
}

case class GpuAggregateExpression(origAggregateFunction: GpuAggregateFunction,
                                  mode: AggregateMode,
                                  isDistinct: Boolean,
                                  filter: Option[Expression],
                                  resultId: ExprId)
  extends GpuExpression
      with ShimExpression
      with GpuUnevaluable {

  val aggregateFunction: GpuAggregateFunction = if (filter.isDefined) {
    WrappedAggFunction(origAggregateFunction, filter.get)
  } else {
    origAggregateFunction
  }

  //
  // Overrides form AggregateExpression
  //

  // We compute the same thing regardless of our final result.
  override lazy val canonicalized: Expression = {
    val normalizedAggFunc = mode match {
      // For Partial, PartialMerge, or Final mode, the input to the `aggregateFunction` is
      // aggregate buffers, and the actual children of `aggregateFunction` is not used,
      // here we normalize the expr id.
      case Partial | PartialMerge | Final => aggregateFunction.transform {
        case a: AttributeReference => a.withExprId(ExprId(0))
      }
      case Complete => aggregateFunction
    }

    GpuAggregateExpression(
      normalizedAggFunc.canonicalized.asInstanceOf[GpuAggregateFunction],
      mode,
      isDistinct,
      filter.map(_.canonicalized),
      ExprId(0))
  }

  override def nullable: Boolean = aggregateFunction.nullable
  override def dataType: DataType = aggregateFunction.dataType
  override def children: Seq[Expression] = aggregateFunction +: filter.toSeq

  @transient
  override lazy val references: AttributeSet = {
    mode match {
      case Partial | Complete => aggregateFunction.references
      case PartialMerge | Final => AttributeSet(aggregateFunction.aggBufferAttributes)
    }
  }

  override def toString: String = {
    val prefix = mode match {
      case Partial => "partial_"
      case PartialMerge => "merge_"
      case Final | Complete => ""
    }
    prefix + origAggregateFunction.toAggString(isDistinct)
  }

  override def sql: String = aggregateFunction.sql(isDistinct)
}

trait CudfAggregate extends Serializable {
  // we use this to get the ordinal of the bound reference, s.t. we can ask cudf to perform
  // the aggregate on that column
  val reductionAggregate: cudf.ColumnVector => cudf.Scalar
  val groupByAggregate: GroupByAggregation
  def dataType: DataType
  val name: String
  override def toString: String = name

  // the purpose of this attribute is for catalyst expressions that need to
  // refer to the output of a cuDF aggregate (`CudfAggregate`) in `postUpdate` or `postMerge`.
  final lazy val attr: AttributeReference = AttributeReference(name, dataType)()
}

trait CpuToGpuAggregateBufferConverter {
  def createExpression(child: Expression): CpuToGpuBufferTransition
}

trait GpuToCpuAggregateBufferConverter {
  def createExpression(child: Expression): GpuToCpuBufferTransition
}

trait CpuToGpuBufferTransition extends ShimUnaryExpression with CodegenFallback

trait GpuToCpuBufferTransition extends ShimUnaryExpression with CodegenFallback {
  override def dataType: DataType = BinaryType
}
