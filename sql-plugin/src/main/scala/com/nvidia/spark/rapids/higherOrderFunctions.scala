/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import scala.collection.mutable

import ai.rapids.cudf
import ai.rapids.cudf.{DType, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.{AutoCloseableProducingArray,
  ReallyAGpuExpression}
import com.nvidia.spark.rapids.jni.GpuMapZipWithUtils
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.expressions.{Add, And, ArrayAggregate, Attribute, AttributeReference, AttributeSeq, CaseWhen, Cast, Expression, ExprId, Greatest, If, LambdaFunction, Least, Literal, Multiply, NamedExpression, NamedLambdaVariable, Or}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DataType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, Metadata, NumericType, ShortType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * A named lambda variable. In Spark on the CPU this includes an AtomicReference to the value that
 * is updated each time a lambda function is called. On the GPU we have to bind this and turn it
 * into a GpuBoundReference for a modified input batch. In the future this should also work with AST
 * when cudf supports that type of operation.
 */
case class GpuNamedLambdaVariable(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    exprId: ExprId = NamedExpression.newExprId)
    extends GpuLeafExpression
        with NamedExpression
        with GpuUnevaluable {

  override def qualifier: Seq[String] = Seq.empty

  override def newInstance(): NamedExpression =
    copy(exprId = NamedExpression.newExprId)

  override def toAttribute: Attribute = {
    AttributeReference(name, dataType, nullable, Metadata.empty)(exprId, Seq.empty)
  }

  override def toString: String = s"lambda $name#${exprId.id}$typeSuffix"

  override def simpleString(maxFields: Int): String = {
    s"lambda $name#${exprId.id}: ${dataType.simpleString(maxFields)}"
  }
}

/**
 * A lambda function and its arguments on the GPU. This is mostly just a wrapper around the
 * function expression, but it holds references to the arguments passed into it.
 */
case class GpuLambdaFunction(
    function: Expression,
    arguments: Seq[NamedExpression],
    hidden: Boolean = false)
    extends GpuExpression with ShimExpression {

  override def children: Seq[Expression] = function +: arguments
  override def dataType: DataType = function.dataType
  override def nullable: Boolean = function.nullable

  override def disableTieredProjectCombine: Boolean = true

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
    function.asInstanceOf[GpuExpression].columnarEval(batch)
}

/**
 * A higher order function takes one or more (lambda) functions and applies these to some objects.
 * The function produces a number of variables which can be consumed by some lambda function.
 */
trait GpuHigherOrderFunction extends GpuExpression with ShimExpression {

  override def nullable: Boolean = arguments.exists(_.nullable)

  override def children: Seq[Expression] = arguments ++ functions

  /**
   * Arguments of the higher ordered function.
   */
  def arguments: Seq[Expression]

  /**
   * Functions applied by the higher order function.
   */
  def functions: Seq[Expression]
}

/**
 * Trait for functions having as input one argument and one function.
 */
trait GpuSimpleHigherOrderFunction extends GpuHigherOrderFunction with GpuBind {

  def argument: Expression

  override def arguments: Seq[Expression] = argument :: Nil

  def function: Expression

  protected val lambdaFunction: GpuLambdaFunction = function.asInstanceOf[GpuLambdaFunction]

  override def functions: Seq[Expression] = function :: Nil

  /**
   * Do the core work of binding this and its lambda function.
   * @param input the input attributes
   * @return the bound child GpuLambdaFunction, the bound argument, and project expressions for
   *         everything except the lambda function's arguments, because how you get those is
   *         often dependent on the type of processing you are doing.
   */
  protected def bindLambdaFunc(input: AttributeSeq): (GpuLambdaFunction, GpuExpression,
      Seq[GpuExpression]) = {
    // Bind the argument parameter, but it can also be a lambda variable...
    val boundArg = GpuBindReferences.bindRefInternal[Expression, GpuExpression](argument, input, {
      case lr: GpuNamedLambdaVariable if input.indexOf(lr.exprId) >= 0 =>
        val ordinal = input.indexOf(lr.exprId)
        GpuBoundReference(ordinal, lr.dataType, input(ordinal).nullable)(lr.exprId, lr.name)
    })

    // `function` is a lambda function. In CPU Spark a lambda function's parameters are wrapping
    // AtomicReference values and the parent expression sets the values before they are processed.
    // That does not work for us. When processing a lambda function we pass in a modified
    // columnar batch, which includes the arguments to that lambda function. To make this work
    // we have to bind the GpuNamedLambdaVariable to a GpuBoundReference and also handle the
    // binding of AttributeReference to GpuBoundReference based on the attributes in the new batch
    // that will be passed to the lambda function. This get especially tricky when dealing with
    // nested lambda functions. So to make that work we first have to find all of the
    // GpuNamedLambdaVariable instances that are provided by lambda expressions below us in the
    // expression tree

    val namedVariablesProvidedByChildren = mutable.HashSet[ExprId]()
    // We purposely include the arguments to the lambda function just below us because
    // we will add them in as a special case later on.
    lambdaFunction.foreach {
      case childLambda: GpuLambdaFunction =>
        namedVariablesProvidedByChildren ++= childLambda.arguments.map(_.exprId)
      case _ => // ignored
    }
    // With this information we can now find all of the AttributeReference and
    // GpuNamedLambdaVariable instances below us so we know what columns in `input` we have
    // to pass on. This is a performance and memory optimization because we are going to explode
    // the columns that are used below us, which can end up using a lot of memory
    val usedReferences = new mutable.HashMap[ExprId, Attribute]()
    function.foreach {
      case att: AttributeReference => usedReferences(att.exprId) = att
      case namedLambda: GpuNamedLambdaVariable =>
        if (!namedVariablesProvidedByChildren.contains(namedLambda.exprId)) {
          usedReferences(namedLambda.exprId) = namedLambda.toAttribute
        } // else it is provided by something else so ignore it
      case _ => // ignored
    }
    val references = usedReferences.toSeq.sortBy(_._1.id)

    // The format of the columnar batch passed to `lambdaFunction` will be
    // `references ++ lambdaFunction.arguments` We are going to take the references
    // and turn them into bound references from `input` so the bound version of this operator
    // knows how to create the `references` part of the batch that is passed down.

    val boundIntermediate = references.map {
      case (_, att) => GpuBindReferences.bindGpuReferenceNoMetrics(att, input)
    }

    // Now get the full set of attributes that we will pass to `lambdaFunction` so any nested
    // higher order functions know how to bind their arguments, and also so we can build a
    // mapping to know how to replace expressions

    val argsAndReferences = references ++ lambdaFunction.arguments.map { expr =>
      (expr.exprId, expr)
    }

    val argsAndRefsAtters = argsAndReferences.map {
      case (_, named: NamedExpression) => named.toAttribute
    }

    val replacementMap = argsAndReferences.zipWithIndex.map {
      case ((exprId, expr), ordinal) =>
        (exprId, GpuBoundReference(ordinal, expr.dataType, expr.nullable)(exprId, expr.name))
    }.toMap

    // Now we actually bind all of the attribute references and GpuNamedLambdaVariables
    // with the appropriate replacements.

    val childFunction = GpuBindReferences.transformNoRecursionOnReplacement(lambdaFunction) {
      case bind: GpuBind =>
        bind.bind(argsAndRefsAtters)
      case a: AttributeReference =>
        replacementMap(a.exprId)
      case lr: GpuNamedLambdaVariable if replacementMap.contains(lr.exprId) =>
        replacementMap(lr.exprId)
    }
    val boundFunc =
      GpuLambdaFunction(childFunction, lambdaFunction.arguments, lambdaFunction.hidden)

    (boundFunc, boundArg, boundIntermediate)
  }
}


/**
 * Common explode + lambda projection plumbing for higher-order functions over arrays.
 * Subclasses choose how to consume the lambda's per-element result by either extending
 * GpuArrayElementWiseTransform (one row in -> one row out via transformListColumnView) or
 * implementing columnarEval themselves (e.g. segmented reductions like GpuArrayAggregate).
 */
trait GpuArrayTransformBase extends GpuSimpleHigherOrderFunction {
  def isBound: Boolean
  def boundIntermediate: Seq[GpuExpression]

  protected lazy val inputToLambda: Seq[DataType] = {
    assert(isBound)
    boundIntermediate.map(_.dataType) ++ lambdaFunction.arguments.map(_.dataType)
  }

  private[rapids] def lambdaArgumentCount: Int = lambdaFunction.arguments.length

  private[rapids] def lambdaArgumentTypes: Seq[DataType] =
    lambdaFunction.arguments.map(_.dataType)

  protected def makeElementProjectBatch(
      inputBatch: ColumnarBatch,
      argColumn: GpuColumnVector): ColumnarBatch = {
    assert(isBound, "Trying to execute an un-bound transform expression")
    GpuArrayTransformBase.makeExplodedElementBatch(
      inputBatch, argColumn, boundIntermediate, inputToLambda, lambdaArgumentCount)
  }

}

private[rapids] object GpuArrayTransformBase {
  private[rapids] def makeExplodedElementBatch(
      inputBatch: ColumnarBatch,
      argColumn: GpuColumnVector,
      intermediate: Seq[GpuExpression],
      elementTypes: Seq[DataType],
      lambdaArgumentCount: Int): ColumnarBatch = {
    assert(argColumn.getBase.getType.equals(DType.LIST))

    def projectAndExplode(explodeOp: Table => Table): Table = {
      val projectedBatch = withResource(GpuProjectExec.project(inputBatch, intermediate)) {
        intermediateBatch =>
          GpuColumnVector.appendColumns(intermediateBatch, argColumn)
      }
      val projectedTable = withResource(projectedBatch) { projectedBatch =>
        GpuColumnVector.from(projectedBatch)
      }
      withResource(projectedTable) { projectedTable =>
        explodeOp(projectedTable)
      }
    }

    if (lambdaArgumentCount >= 2) {
      val explodedTable = projectAndExplode { projectedTable =>
        projectedTable.explodePosition(intermediate.length)
      }
      val reorderedTable = withResource(explodedTable) { explodedTable =>
        reorderExplodePositionOutput(explodedTable)
      }
      withResource(reorderedTable) { reorderedTable =>
        GpuColumnVector.from(reorderedTable, elementTypes.toArray)
      }
    } else {
      val explodedTable = projectAndExplode { projectedTable =>
        projectedTable.explode(intermediate.length)
      }
      withResource(explodedTable) { explodedTable =>
        GpuColumnVector.from(explodedTable, elementTypes.toArray)
      }
    }
  }

  // cuDF returns [..., position, element], but Spark lambda inputs use [..., element, index].
  private def reorderExplodePositionOutput(explodedTable: Table): Table = {
    val cols = new Array[cudf.ColumnVector](explodedTable.getNumberOfColumns)
    val numOtherColumns = explodedTable.getNumberOfColumns - 2
    (0 until numOtherColumns).foreach { index =>
      cols(index) = explodedTable.getColumn(index)
    }
    cols(numOtherColumns) = explodedTable.getColumn(numOtherColumns + 1)
    cols(numOtherColumns + 1) = explodedTable.getColumn(numOtherColumns)

    new cudf.Table(cols: _*)
  }
}

/**
 * Specialization for HOFs that produce one output row per input row by post-processing the
 * lambda's elementwise result. Subclasses implement transformListColumnView and inherit the
 * standard columnarEval that drives the explode -> lambda eval -> rewrap chain.
 */
trait GpuArrayElementWiseTransform extends GpuArrayTransformBase {
  /*
   * Post-process the column view of the array after applying the function parameter.
   * @param lambdaTransformedCV the results of the lambda expression running
   * @param arg the original input array from the expression.
   */
  protected def transformListColumnView(
    lambdaTransformedCV: cudf.ColumnView,
    arg: cudf.ColumnView): GpuColumnVector

  private[rapids] def transformElementResults(
      lambdaTransformed: GpuColumnVector,
      arg: GpuColumnVector): GpuColumnVector = {
    // Lambda evaluation is flat, so restore the original list shape before post-processing.
    withResource(GpuListUtils.replaceListDataColumnAsView(
        arg.getBase, lambdaTransformed.getBase)) { cv =>
      transformListColumnView(cv, arg.getBase)
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(argument.columnarEval(batch)) { arg =>
      val dataCol = withResource(makeElementProjectBatch(batch, arg)) { cb =>
        function.columnarEval(cb)
      }
      withResource(dataCol) { _ =>
        transformElementResults(dataCol, arg)
      }
    }
  }
}

/**
 * Shares explode and intermediate projection across compatible top-level array HOFs in a Project.
 * Lambda evaluation and result reconstruction remain independent for each HOF.
 */
private[rapids] object GpuArrayHofFusion {
  private case class HofInProject(outputIndex: Int, hof: GpuArrayTransformBase)

  private case class PlannedHof(
      outputIndex: Int,
      hof: GpuArrayTransformBase,
      lambdaColumnIndexes: Array[Int],
      useSharedBatch: Boolean)

  private case class HofGroup(
      members: Seq[HofInProject],
      sharedIntermediate: Seq[GpuExpression]) {
    val startIndex: Int = members.head.outputIndex
    val outputIndexes: Seq[Int] = members.map(_.outputIndex)
    val first: GpuArrayTransformBase = members.head.hof
    val elementTypes: Seq[DataType] =
      sharedIntermediate.map(_.dataType) ++ first.lambdaArgumentTypes
    val plannedHofs: Seq[PlannedHof] = members.map { member =>
      val intermediateIndexes = member.hof.boundIntermediate.map { expr =>
        val index = sharedIntermediate.indexWhere(_.semanticEquals(expr))
        assert(index >= 0, s"Missing shared HOF intermediate: $expr")
        index
      }
      val lambdaArgStart = sharedIntermediate.length
      val lambdaIndexes = lambdaArgStart until lambdaArgStart + first.lambdaArgumentCount
      val columnIndexes = (intermediateIndexes ++ lambdaIndexes).toArray
      val useSharedBatch = columnIndexes.length == elementTypes.length &&
        columnIndexes.indices.forall(index => columnIndexes(index) == index)
      PlannedHof(member.outputIndex, member.hof, columnIndexes, useSharedBatch)
    }
  }

  private[rapids] def project(
      batch: ColumnarBatch,
      boundExprs: Seq[Expression]): Option[ColumnarBatch] = {
    val fusedGroups = findFusedGroups(boundExprs)
    if (fusedGroups.isEmpty) {
      None
    } else {
      val groupsByStartIndex = fusedGroups.map(group => group.startIndex -> group).toMap
      Some(projectWithFusedGroups(batch, boundExprs, groupsByStartIndex))
    }
  }

  private[rapids] def findFusedGroupIndexes(
      boundExprs: Seq[Expression]): Seq[Seq[Int]] =
    findFusedGroups(boundExprs).map(_.outputIndexes)

  private def findFusedGroups(
      boundExprs: Seq[Expression]): Seq[HofGroup] = {
    val fusedGroups = mutable.ArrayBuffer[HofGroup]()
    val groups = mutable.ArrayBuffer[mutable.ArrayBuffer[HofInProject]]()

    def flushGroups(): Unit = {
      fusedGroups ++= groups.flatMap(groupByIntermediateCoverage)
      groups.clear()
    }

    boundExprs.zipWithIndex.foreach {
      case (expr, index) =>
        extractHof(expr).filter(canFuse) match {
          case Some(hof) =>
            groups.find(g => canShareExplode(g.head.hof, hof)) match {
              case Some(group) => group += HofInProject(index, hof)
              case None => groups += mutable.ArrayBuffer(HofInProject(index, hof))
            }
          case None if !canReorderExpression(expr) =>
            flushGroups()
          case None =>
        }
    }
    flushGroups()
    fusedGroups.sortBy(_.startIndex).toSeq
  }

  // Keep the shared explode no wider than an explode already required by one group member.
  private def groupByIntermediateCoverage(
      candidates: mutable.ArrayBuffer[HofInProject]): Seq[HofGroup] = {
    val assigned = mutable.HashSet[Int]()
    candidates.sortBy(member => -member.hof.boundIntermediate.length).flatMap { anchor =>
      if (assigned.contains(anchor.outputIndex)) {
        None
      } else {
        val members = candidates.filter { member =>
          !assigned.contains(member.outputIndex) &&
            isIntermediateSubset(member.hof.boundIntermediate, anchor.hof.boundIntermediate)
        }
        assigned ++= members.map(_.outputIndex)
        if (members.length > 1) {
          Some(HofGroup(members.toSeq, anchor.hof.boundIntermediate))
        } else {
          None
        }
      }
    }.toSeq
  }

  private def isIntermediateSubset(
      subset: Seq[GpuExpression],
      superset: Seq[GpuExpression]): Boolean =
    subset.forall(expr => superset.exists(_.semanticEquals(expr)))

  private def extractHof(
      expr: Expression): Option[GpuArrayTransformBase] = expr match {
    case GpuAlias(transform: GpuArrayTransformBase, _) => Some(transform)
    case transform: GpuArrayTransformBase => Some(transform)
    case _ => None
  }

  private def canFuse(transform: GpuArrayTransformBase): Boolean = {
    isSupportedTransform(transform) &&
      transform.isBound &&
      transform.deterministic &&
      !transform.hasSideEffects &&
      transform.argument.deterministic &&
      transform.boundIntermediate.forall(_.deterministic) &&
      (transform.lambdaArgumentCount == 1 || transform.lambdaArgumentCount == 2)
  }

  private def isSupportedTransform(transform: GpuArrayTransformBase): Boolean = transform match {
    case _: GpuArrayElementWiseTransform | _: GpuArrayAggregate => true
    case _ => false
  }

  private def canShareExplode(
      left: GpuArrayTransformBase,
      right: GpuArrayTransformBase): Boolean = {
    left.lambdaArgumentTypes == right.lambdaArgumentTypes &&
      left.argument.semanticEquals(right.argument)
  }

  private def canReorderExpression(expr: Expression): Boolean =
    expr.deterministic && (expr match {
      case gpuExpr: GpuExpression => !gpuExpr.hasSideEffects
      case _ => false
    })

  private def projectWithFusedGroups(
      batch: ColumnarBatch,
      boundExprs: Seq[Expression],
      groupsByStartIndex: Map[Int, HofGroup]): ColumnarBatch = {
    val outputColumns = new Array[ColumnVector](boundExprs.length)
    closeOnExcept(outputColumns) { _ =>
      boundExprs.indices.foreach { index =>
        if (outputColumns(index) == null) {
          groupsByStartIndex.get(index) match {
            case Some(group) =>
              evaluateFusedGroup(batch, group, outputColumns)
            case None =>
              outputColumns(index) = boundExprs(index).columnarEval(batch)
          }
        }
      }
      new ColumnarBatch(outputColumns, batch.numRows())
    }
  }

  private def evaluateFusedGroup(
      batch: ColumnarBatch,
      group: HofGroup,
      outputColumns: Array[ColumnVector]): Unit = {
    withResource(group.first.argument.columnarEval(batch)) { arg =>
      withResource(GpuArrayTransformBase.makeExplodedElementBatch(
          batch, arg, group.sharedIntermediate, group.elementTypes,
          group.first.lambdaArgumentCount)) { sharedBatch =>
        group.plannedHofs.foreach { planned =>
          val dataCol = if (planned.useSharedBatch) {
            planned.hof.function.columnarEval(sharedBatch)
          } else {
            withResource(makeHofLambdaBatch(sharedBatch, planned)) { lambdaBatch =>
              planned.hof.function.columnarEval(lambdaBatch)
            }
          }
          outputColumns(planned.outputIndex) =
            consumeElementResults(batch, planned.hof, dataCol, arg)
        }
      }
    }
  }

  private def consumeElementResults(
      batch: ColumnarBatch,
      transform: GpuArrayTransformBase,
      dataCol: GpuColumnVector,
      arg: GpuColumnVector): GpuColumnVector = transform match {
    case elementWise: GpuArrayElementWiseTransform =>
      withResource(dataCol) { dataCol =>
        elementWise.transformElementResults(dataCol, arg)
      }
    case aggregate: GpuArrayAggregate =>
      aggregate.aggregateAndCloseElementResults(batch, dataCol, arg)
    case other =>
      withResource(dataCol) { _ =>
        throw new IllegalStateException(
          s"Unsupported array HOF fusion expression: ${other.getClass.getName}")
      }
  }

  private def makeHofLambdaBatch(
      sharedBatch: ColumnarBatch,
      planned: PlannedHof): ColumnarBatch = {
    val columns = planned.lambdaColumnIndexes.safeMap[ColumnVector] { index =>
      sharedBatch.column(index).asInstanceOf[GpuColumnVector].incRefCount()
    }
    closeOnExcept(columns) { columns =>
      new ColumnarBatch(columns, sharedBatch.numRows())
    }
  }
}

case class GpuArrayTransform(
  argument: Expression,
  function: Expression,
  isBound: Boolean = false,
  boundIntermediate: Seq[GpuExpression] = Seq.empty) extends GpuArrayElementWiseTransform {

  override def dataType: ArrayType = ArrayType(function.dataType, function.nullable)

  override def prettyName: String = "transform"

  override def bind(input: AttributeSeq): GpuExpression = {
    val (boundFunc, boundArg, boundIntermediate) = bindLambdaFunc(input)

    GpuArrayTransform(boundArg, boundFunc, isBound = true, boundIntermediate)
  }

  override protected def transformListColumnView(
    lambdaTransformedCV: cudf.ColumnView, arg: cudf.ColumnView): GpuColumnVector = {
    GpuColumnVector.from(lambdaTransformedCV.copyToColumnVector(), dataType)
  }
}

case class GpuArrayExists(
    argument: Expression,
    function: Expression,
    followThreeValuedLogic: Boolean,
    isBound: Boolean = false,
    boundIntermediate: Seq[GpuExpression] = Seq.empty) extends GpuArrayElementWiseTransform {

  override def dataType: DataType = BooleanType

  override def prettyName: String = "exists"

  override def nullable: Boolean = super.nullable || function.nullable

  override def bind(input: AttributeSeq): GpuExpression = {
    val (boundFunc, boundArg, boundIntermediate) = bindLambdaFunc(input)
    GpuArrayExists(boundArg, boundFunc,  followThreeValuedLogic,isBound = true, boundIntermediate)
  }

  private def imputeFalseForEmptyArrays(
    transformedCV: cudf.ColumnView,
    result: cudf.ColumnView
  ): GpuColumnVector = {

    val isEmptyList = withResource(cudf.Scalar.fromInt(0)) { zeroScalar =>
      withResource(transformedCV.countElements()) {
        _.equalTo(zeroScalar)
      }
    }

    withResource(isEmptyList) { _ =>
      withResource(cudf.Scalar.fromBool(false)) { falseScalar =>
        GpuColumnVector.from(isEmptyList.ifElse(falseScalar, result), dataType)
      }
    }
  }

  private def existsReduce(columnView: cudf.ColumnView, nullPolicy: cudf.NullPolicy) = {
    columnView.listReduce(
      cudf.SegmentedReductionAggregation.any(),
      nullPolicy,
      DType.BOOL8)
  }

  /*
   * The difference between legacyExists and EXCLUDE nulls reduction
   * is that the list without valid values (all nulls) should produce false
   * which is equivalent to replacing nulls with false after lambda prior
   * to aggregation
   */
  private def legacyExists(cv: cudf.ColumnView): cudf.ColumnView = {
    withResource(cudf.Scalar.fromBool(false)) { falseScalar =>
      withResource(cv.getChildColumnView(0)) { childView =>
        withResource(childView.replaceNulls(falseScalar)) { noNullsChildView =>
          withResource(cv.replaceListChild(noNullsChildView)) { reduceInput =>
            existsReduce(reduceInput, cudf.NullPolicy.EXCLUDE)
          }
        }
      }
    }
  }

  /*
   * 3VL is true if EXCLUDE nulls reduce is true
   * 3VL is false if INCLUDE nulls reduce is false
   * 3VL is null if
   *    EXCLUDE null reduce is false and
   *    INCLUDE nulls reduce is null
   */
  private def threeValueExists(cv: cudf.ColumnView): cudf.ColumnView = {
    withResource(existsReduce(cv, cudf.NullPolicy.EXCLUDE)) { existsNullsExcludedCV =>
      withResource(existsReduce(cv, cudf.NullPolicy.INCLUDE)) { existsNullsIncludedCV =>
        existsNullsExcludedCV.ifElse(existsNullsExcludedCV, existsNullsIncludedCV)
      }
    }
  }

  private def exists(cv: cudf.ColumnView) = {
    if (followThreeValuedLogic) {
      threeValueExists(cv)
    } else {
      legacyExists(cv)
    }
  }

  override protected def transformListColumnView(
    lambdaTransformedCV: cudf.ColumnView,
    arg: cudf.ColumnView
  ): GpuColumnVector = {
    withResource(exists(lambdaTransformedCV)) { existsCV =>
      // exists is false for empty arrays
      // post process empty arrays until cudf allows specifying
      // the initial value for a list reduction (i.e. similar to Scala fold)
      // https://github.com/rapidsai/cudf/issues/10455
      imputeFalseForEmptyArrays(lambdaTransformedCV, existsCV)
    }
  }

}

case class GpuArrayFilter(
    argument: Expression,
    function: Expression,
    isBound: Boolean = false,
    boundIntermediate: Seq[GpuExpression] = Seq.empty) extends GpuArrayElementWiseTransform {

  override def dataType: DataType = argument.dataType

  override def nodeName: String = "filter"

  override def bind(input: AttributeSeq): GpuExpression = {
    val (boundFunc, boundArg, boundIntermediate) = bindLambdaFunc(input)
    GpuArrayFilter(boundArg, boundFunc,isBound = true, boundIntermediate)
  }

  override protected def transformListColumnView(lambdaTransformedCV: cudf.ColumnView,
                                                 arg: cudf.ColumnView): GpuColumnVector = {
    closeOnExcept(arg.applyBooleanMask(lambdaTransformedCV)) { ret =>
      GpuColumnVector.from(ret, dataType)
    }
  }
}

trait GpuMapSimpleHigherOrderFunction extends GpuSimpleHigherOrderFunction with GpuBind {

  protected def isBound: Boolean
  protected def boundIntermediate: Seq[GpuExpression]

  protected lazy val inputToLambda: Seq[DataType] = {
    assert(isBound)
    boundIntermediate.map(_.dataType) ++ lambdaFunction.arguments.map(_.dataType)
  }

  protected def makeElementProjectBatch(
      inputBatch: ColumnarBatch,
      listColumn: cudf.ColumnVector): ColumnarBatch = {
    assert(listColumn.getType.equals(DType.LIST))
    assert(isBound, "Trying to execute an un-bound transform value expression")

    // Need to do an explode followed by pulling out the key/value columns
    val boundProject = boundIntermediate :+ argument
    val explodedTable = withResource(GpuProjectExec.project(inputBatch, boundProject)) {
      projectedBatch =>
        withResource(GpuColumnVector.from(projectedBatch)) { projectedTable =>
          projectedTable.explode(boundIntermediate.length)
        }
    }
    val moddedTable = withResource(explodedTable) { explodedTable =>
      // The last column is a struct column with key/values pairs in it. We need to pull them
      // out into stand alone columns
      val cols = new Array[cudf.ColumnVector](explodedTable.getNumberOfColumns + 1)
      val numOtherColumns = explodedTable.getNumberOfColumns - 1
      (0 until numOtherColumns).foreach { index =>
        cols(index) = explodedTable.getColumn(index)
      }
      val keyValuePairColumn = explodedTable.getColumn(numOtherColumns)
      val keyCol = withResource(
        keyValuePairColumn.getChildColumnView(GpuMapUtils.KEY_INDEX)) { keyView =>
        keyView.copyToColumnVector()
      }
      withResource(keyCol) { keyCol =>
        val valCol = withResource(
          keyValuePairColumn.getChildColumnView(GpuMapUtils.VALUE_INDEX)) { valueView =>
          valueView.copyToColumnVector()
        }

        withResource(valCol) { valCol =>
          cols(numOtherColumns) = keyCol
          cols(numOtherColumns + 1) = valCol
          new cudf.Table(cols: _*)
        }
      }
    }
    withResource(moddedTable) { moddedTable =>
      GpuColumnVector.from(moddedTable, inputToLambda.toArray)
    }
  }
}

case class GpuTransformKeys(
    argument: Expression,
    function: Expression,
    isBound: Boolean = false,
    boundIntermediate: Seq[GpuExpression] = Seq.empty)
    extends GpuMapSimpleHigherOrderFunction {

  @transient lazy val MapType(keyType, valueType, valueContainsNull) = argument.dataType

  override def dataType: DataType = MapType(function.dataType, valueType, valueContainsNull)

  override def prettyName: String = "transform_keys"

  // Spark 4.1+ returns an enum value instead of String, so use toString first
  private def exceptionOnDupKeys =
    SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY).toString.toUpperCase == "EXCEPTION"

  override lazy val hasSideEffects: Boolean =
    function.nullable || exceptionOnDupKeys || super.hasSideEffects

  override def bind(input: AttributeSeq): GpuExpression = {
    val (boundFunc, boundArg, boundIntermediate) = bindLambdaFunc(input)

    GpuTransformKeys(boundArg, boundFunc, isBound = true, boundIntermediate)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(argument.columnarEval(batch)) { arg =>
      val newKeysCol = withResource(makeElementProjectBatch(batch, arg.getBase)) { cb =>
        function.columnarEval(cb)
      }
      withResource(newKeysCol) { newKeysCol =>
        withResource(GpuMapUtils.replaceExplodedKeyAsView(arg.getBase, newKeysCol.getBase)) {
          updatedMapView => {
            GpuMapUtils.assertNoNullKeys(updatedMapView)
            withResource(updatedMapView.dropListDuplicatesWithKeysValues()) { deduped =>
              if (exceptionOnDupKeys) {
                // Compare child data row count before and after removing duplicates to determine
                // if there were duplicates.
                withResource(deduped.getChildColumnView(0)) { a =>
                  withResource(updatedMapView.getChildColumnView(0)) { b =>
                    if (a.getRowCount != b.getRowCount) {
                      throw GpuMapUtils.duplicateMapKeyFoundError
                    }
                  }
                }
              }
              GpuColumnVector.from(deduped.incRefCount(), dataType)
            }
          }
        }
      }
    }
  }
}

case class GpuTransformValues(
    argument: Expression,
    function: Expression,
    isBound: Boolean = false,
    boundIntermediate: Seq[GpuExpression] = Seq.empty)
    extends GpuMapSimpleHigherOrderFunction {

  @transient lazy val MapType(keyType, valueType, valueContainsNull) = argument.dataType

  override def dataType: DataType = MapType(keyType, function.dataType, function.nullable)

  override def prettyName: String = "transform_values"

  override def bind(input: AttributeSeq): GpuExpression = {
    val (boundFunc, boundArg, boundIntermediate) = bindLambdaFunc(input)

    GpuTransformValues(boundArg, boundFunc, isBound = true, boundIntermediate)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(argument.columnarEval(batch)) { arg =>
      val newValueCol = withResource(makeElementProjectBatch(batch, arg.getBase)) { cb =>
        function.columnarEval(cb)
      }
      withResource(newValueCol) { newValueCol =>
        withResource(GpuMapUtils.replaceExplodedValueAsView(arg.getBase, newValueCol.getBase)) {
          updatedMapView =>
            GpuColumnVector.from(updatedMapView.copyToColumnVector(), dataType)
        }
      }
    }
  }
}

trait GpuTwoArgumentHigherOrderFunction extends GpuHigherOrderFunction with GpuBind {

  def arguments: Seq[Expression]

  def function: Expression

  protected val lambdaFunction: GpuLambdaFunction = function.asInstanceOf[GpuLambdaFunction]

  override def functions: Seq[Expression] = function :: Nil

  /**
   * Do the core work of binding this and its lambda function.
   * @param input the input attributes
   * @return the bound child GpuLambdaFunction, the bound arguments, and project expressions for
   *         everything except the lambda function's arguments, because how you get those is
   *         often dependent on the type of processing you are doing.
   */
  protected def bindLambdaFunc(input: AttributeSeq): (GpuLambdaFunction, Seq[GpuExpression],
      Seq[GpuExpression]) = {
    // Bind the argument parameters, but they can also be lambda variables...
    val boundArgs = arguments.map { argument =>
      GpuBindReferences.bindRefInternal[Expression, GpuExpression](argument, input, {
        case lr: GpuNamedLambdaVariable if input.indexOf(lr.exprId) >= 0 =>
          val ordinal = input.indexOf(lr.exprId)
          GpuBoundReference(ordinal, lr.dataType, input(ordinal).nullable)(lr.exprId, lr.name)
      })
    }

    // `function` is a lambda function. In CPU Spark a lambda function's parameters are wrapping
    // AtomicReference values and the parent expression sets the values before they are processed.
    // That does not work for us. When processing a lambda function we pass in a modified
    // columnar batch, which includes the arguments to that lambda function. To make this work
    // we have to bind the GpuNamedLambdaVariable to a GpuBoundReference and also handle the
    // binding of AttributeReference to GpuBoundReference based on the attributes in the new batch
    // that will be passed to the lambda function. This get especially tricky when dealing with
    // nested lambda functions. So to make that work we first have to find all of the
    // GpuNamedLambdaVariable instances that are provided by lambda expressions below us in the
    // expression tree

    val namedVariablesProvidedByChildren = mutable.HashSet[ExprId]()
    // We purposely include the arguments to the lambda function just below us because
    // we will add them in as a special case later on.
    lambdaFunction.foreach {
      case childLambda: GpuLambdaFunction =>
        namedVariablesProvidedByChildren ++= childLambda.arguments.map(_.exprId)
      case _ => // ignored
    }
    // With this information we can now find all of the AttributeReference and
    // GpuNamedLambdaVariable instances below us so we know what columns in `input` we have
    // to pass on. This is a performance and memory optimization because we are going to explode
    // the columns that are used below us, which can end up using a lot of memory
    val usedReferences = new mutable.HashMap[ExprId, Attribute]()
    function.foreach {
      case att: AttributeReference => usedReferences(att.exprId) = att
      case namedLambda: GpuNamedLambdaVariable =>
        if (!namedVariablesProvidedByChildren.contains(namedLambda.exprId)) {
          usedReferences(namedLambda.exprId) = namedLambda.toAttribute
        } // else it is provided by something else so ignore it
      case _ => // ignored
    }
    val references = usedReferences.toSeq.sortBy(_._1.id)

    // The format of the columnar batch passed to `lambdaFunction` will be
    // `references ++ lambdaFunction.arguments` We are going to take the references
    // and turn them into bound references from `input` so the bound version of this operator
    // knows how to create the `references` part of the batch that is passed down.

    val boundIntermediate = references.map {
      case (_, att) => GpuBindReferences.bindGpuReferenceNoMetrics(att, input)
    }

    // Now get the full set of attributes that we will pass to `lambdaFunction` so any nested
    // higher order functions know how to bind their arguments, and also so we can build a
    // mapping to know how to replace expressions

    val argsAndReferences = references ++ lambdaFunction.arguments.map { expr =>
      (expr.exprId, expr)
    }

    val argsAndRefsAtters = argsAndReferences.map {
      case (_, named: NamedExpression) => named.toAttribute
    }

    val replacementMap = argsAndReferences.zipWithIndex.map {
      case ((exprId, expr), ordinal) =>
        (exprId, GpuBoundReference(ordinal, expr.dataType, expr.nullable)(exprId, expr.name))
    }.toMap

    // Now we actually bind all of the attribute references and GpuNamedLambdaVariables
    // with the appropriate replacements.

    val childFunction = GpuBindReferences.transformNoRecursionOnReplacement(lambdaFunction) {
      case bind: GpuBind =>
        bind.bind(argsAndRefsAtters)
      case a: AttributeReference =>
        replacementMap(a.exprId)
      case lr: GpuNamedLambdaVariable if replacementMap.contains(lr.exprId) =>
        replacementMap(lr.exprId)
    }
    val boundFunc =
      GpuLambdaFunction(childFunction, lambdaFunction.arguments, lambdaFunction.hidden)

    (boundFunc, boundArgs, boundIntermediate)
  }
}


/**
 * Expression that performs mapZip operation on two map columns.
 * This expression takes two map columns and zips them together using the GpuMapZipWithUtils.
 */
case class GpuMapZipExpression(
    leftMap: Expression,
    rightMap: Expression)
    extends GpuExpression with ShimExpression {

  override def children: Seq[Expression] = Seq(leftMap, rightMap)

  @transient lazy val MapType(leftKeyType, leftValueType, leftValueContainsNull) = leftMap.dataType
  @transient lazy val MapType(rightKeyType, rightValueType, rightValueContainsNull) 
  = rightMap.dataType

  @transient lazy val keyType =
    TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(leftKeyType, rightKeyType).get

  override def dataType: DataType = {
    MapType(keyType, StructType(Seq(
      StructField("left", leftValueType),
      StructField("right", rightValueType)
    )), leftValueContainsNull || rightValueContainsNull)
  }

  override def nullable: Boolean = leftMap.nullable || rightMap.nullable

  override def prettyName: String = "map_zip"

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(leftMap.columnarEval(batch)) { leftCol =>
      withResource(rightMap.columnarEval(batch)) { rightCol =>
        val result = GpuMapZipWithUtils.mapZip(leftCol.getBase, rightCol.getBase)
        GpuColumnVector.from(result, dataType)
      }
    }
  }
}

trait GpuMapTwoArgumentHigherOrderFunction extends GpuTwoArgumentHigherOrderFunction {

  protected def isBound: Boolean
  protected def boundIntermediate: Seq[GpuExpression]

  protected lazy val inputToLambda: Seq[DataType] = {
    assert(isBound)
    boundIntermediate.map(_.dataType) ++ lambdaFunction.arguments.map(_.dataType)
  }

  protected def makeElementProjectBatch(
      inputBatch: ColumnarBatch): (ColumnarBatch, cudf.ColumnVector) = {
    assert(isBound, "Trying to execute an un-bound transform value expression")

    val mapZipExpr = GpuMapZipExpression(arguments(0), arguments(1))

    val boundProject = boundIntermediate :+ mapZipExpr
    val explodedTable = withResource(GpuProjectExec.project(inputBatch, boundProject)) {
      projectedBatch =>
        withResource(GpuColumnVector.from(projectedBatch)) { projectedTable =>
          projectedTable.explode(boundIntermediate.length)
        }
    }
    val moddedTable = withResource(explodedTable) { explodedTable =>
      // The last column is a struct column with key/values pairs in it. We need to pull them
      // out into stand alone columns
      val cols = new Array[cudf.ColumnVector](explodedTable.getNumberOfColumns + 2)
      val numOtherColumns = explodedTable.getNumberOfColumns - 1
      (0 until numOtherColumns).foreach { index =>
        cols(index) = explodedTable.getColumn(index)
      }
      val keyValuePairColumn = explodedTable.getColumn(numOtherColumns)
      val keyCol = withResource(
        keyValuePairColumn.getChildColumnView(GpuMapUtils.KEY_INDEX)) { keyView =>
        keyView.copyToColumnVector()
      }
      withResource(keyCol) { keyCol =>
        val val1Col = withResource(
          keyValuePairColumn.getChildColumnView(GpuMapUtils.VALUE_INDEX).getChildColumnView(0)) 
          { valueView =>
          valueView.copyToColumnVector()
        }
        val val2Col = withResource(
          keyValuePairColumn.getChildColumnView(GpuMapUtils.VALUE_INDEX).getChildColumnView(1)) 
         { valueView =>
          valueView.copyToColumnVector()
        }
        withResource(val1Col) { val1Col =>
          withResource(val2Col) { val2Col =>
            cols(numOtherColumns) = keyCol
            cols(numOtherColumns + 1) = val1Col
            cols(numOtherColumns + 2) = val2Col
            new cudf.Table(cols: _*)
          }
        }
      }
    }
    
    // Get the original map structure for reconstruction
    val zippedMap = withResource(GpuProjectExec.project(inputBatch, Seq(mapZipExpr))) {
      projectedBatch =>
        withResource(GpuColumnVector.from(projectedBatch)) { projectedTable =>
          projectedTable.getColumn(0).copyToColumnVector()
        }
    }
    
    val lambdaBatch = withResource(moddedTable) { moddedTable =>
      GpuColumnVector.from(moddedTable, inputToLambda.toArray)
    }
    
    (lambdaBatch, zippedMap)
  }
}

case class GpuMapZipWith(
    argument1: Expression,
    argument2: Expression,
    function: Expression,
    isBound: Boolean = false,
    boundIntermediate: Seq[GpuExpression] = Seq.empty)
    extends GpuMapTwoArgumentHigherOrderFunction {

  @transient lazy val MapType(keyType1, valueType1, valueContainsNull1) = argument1.dataType
  @transient lazy val MapType(keyType2, valueType2, valueContainsNull2) = argument2.dataType

  @transient lazy val keyType =
    TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(keyType1, keyType2).get

  override def dataType: DataType = MapType(keyType, function.dataType, 
    valueContainsNull1 || valueContainsNull2)

  override def prettyName: String = "map_zip_with"

  override def bind(input: AttributeSeq): GpuExpression = {
    val (boundFunc, boundArgs, boundIntermediate) = bindLambdaFunc(input)

    GpuMapZipWith(boundArgs(0), boundArgs(1), boundFunc, isBound = true, boundIntermediate)
  }

  override def arguments: Seq[Expression] = Seq(argument1, argument2)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val (lambdaBatch, zippedMap) = makeElementProjectBatch(batch)
    withResource(lambdaBatch) { lambdaBatch =>
      withResource(zippedMap) { zippedMap =>
        val newValueCol = function.columnarEval(lambdaBatch)
        withResource(newValueCol) { newValueCol =>
          withResource(GpuMapUtils.replaceExplodedValueAsView(zippedMap,
            newValueCol.getBase)) {
            updatedMapView =>
              GpuColumnVector.from(updatedMapView.copyToColumnVector(), dataType)
          }
        }
      }
    }
  }
}


case class GpuMapFilter(argument: Expression,
    function: Expression,
    isBound: Boolean = false,
    boundIntermediate: Seq[GpuExpression] = Seq.empty)
    extends GpuMapSimpleHigherOrderFunction {

  override def dataType: DataType = argument.dataType

  override def prettyName: String = "map_filter"

  override def bind(input: AttributeSeq): GpuExpression = {
    val (boundFunc, boundArg, boundIntermediate) = bindLambdaFunc(input)

    GpuMapFilter(boundArg, boundFunc, isBound = true, boundIntermediate)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(argument.columnarEval(batch)) { mapArg =>
      // `mapArg` is list of struct(key, value)
      val plainBoolCol = withResource(makeElementProjectBatch(batch, mapArg.getBase)) { cb =>
        function.columnarEval(cb)
      }

      withResource(plainBoolCol) { plainBoolCol =>
        assert(plainBoolCol.dataType() == BooleanType, "map_filter should have a predicate filter")
        withResource(mapArg.getBase.getListOffsetsView) { argOffsetsCv =>
          // convert the one dimension plain bool column to list of bool column
          withResource(plainBoolCol.getBase.makeListFromOffsets(mapArg.getRowCount, argOffsetsCv)) {
            listOfBoolCv =>
              // extract entries for each map in the `mapArg` column
              // according to the `listOfBoolCv` column
              // `mapArg` is a map column containing no duplicate keys and null keys,
              // so no need to `assertNoNullKeys` and `assertNoDuplicateKeys` after the extraction
              val retCv = mapArg.getBase.applyBooleanMask(listOfBoolCv)
              GpuColumnVector.from(retCv, dataType)
          }
        }
      }
    }
  }
}


sealed trait AggOp {
  def name: String
  def cudfAgg: cudf.SegmentedReductionAggregation
  def nullPolicy: cudf.NullPolicy
  /** Identity scalar typed to match `t` so ifElse / binaryOp don't hit width mismatch. */
  def identityScalar(t: DataType): cudf.Scalar
  /**
   * Catalyst-side identity, used by the decomposer to plug into `If`/`CaseWhen` branches
   * that are bare `acc` (treated as `op(acc, identity)` so the branch can be lifted out).
   * Must satisfy `op(acc, identityLiteral(t)) == acc` for any acc of type `t`.
   */
  def identityLiteral(t: DataType): Literal
  /**
   * `reduced OP zero`, typed to outDType, with Spark-matching null propagation. `zero` is
   * a `BinaryOperable` so callers can pass either a `cudf.Scalar` (when the Spark-side
   * `zero` is a Literal — saves one full-row column allocation per batch) or a `ColumnView`
   * (when `zero` references an outer column).
   */
  def combineWithZero(
      reduced: cudf.ColumnVector,
      zero: cudf.BinaryOperable,
      outDType: DType): cudf.ColumnVector
  /** Return (left, right) if the body is this op's Catalyst shape. */
  def matchBinary(body: Expression): Option[(Expression, Expression)]
  def supportsType(sparkType: DataType): Boolean
}

case object SumOp extends AggOp {
  val name = "SUM"
  def cudfAgg: cudf.SegmentedReductionAggregation = cudf.SegmentedReductionAggregation.sum()
  // INCLUDE: Spark iteratively computes `acc + x` and null poisons the accumulator, so
  // one null element anywhere in the list yields null.
  val nullPolicy: cudf.NullPolicy = cudf.NullPolicy.INCLUDE
  def identityScalar(t: DataType): cudf.Scalar = t match {
    case ByteType => cudf.Scalar.fromByte(0.toByte)
    case ShortType => cudf.Scalar.fromShort(0.toShort)
    case IntegerType => cudf.Scalar.fromInt(0)
    case LongType => cudf.Scalar.fromLong(0L)
    case FloatType => cudf.Scalar.fromFloat(0.0f)
    case DoubleType => cudf.Scalar.fromDouble(0.0)
    case d: DecimalType => GpuScalar.from(0, d)
    case other => throw new IllegalStateException(s"SUM identity not defined for $other")
  }
  // Each arm builds the value at exactly the right Scala type so Spark's Literal
  // type-compatibility check (validateLiteralValue) doesn't reject e.g. Int into LongType.
  def identityLiteral(t: DataType): Literal = t match {
    case ByteType => Literal(0.toByte, ByteType)
    case ShortType => Literal(0.toShort, ShortType)
    case IntegerType => Literal(0, IntegerType)
    case LongType => Literal(0L, LongType)
    case FloatType => Literal(0.0f, FloatType)
    case DoubleType => Literal(0.0, DoubleType)
    case d: DecimalType => Literal(Decimal(0L, d.precision, d.scale), d)
    case other => throw new IllegalStateException(s"SUM identity literal not defined for $other")
  }
  def combineWithZero(r: cudf.ColumnVector, z: cudf.BinaryOperable, out: DType) = r.add(z, out)
  def matchBinary(e: Expression): Option[(Expression, Expression)] = e match {
    case a: Add => Some((a.left, a.right))
    case _ => None
  }
  // Float/Double are gated behind `spark.rapids.sql.variableFloatAgg.enabled` (same conf
  // as scalar GpuSum/GpuAverage) — cuDF's parallel tree-reduction sums in a different
  // order than Spark's sequential left-fold, so the low-bit answer can differ even though
  // both are valid IEEE 754 results. The check happens in GpuArrayAggregateMeta.
  //
  // Decimal is unsupported: Spark non-ANSI decimal addition returns null on overflow via
  // CheckOverflow(nullOnOverflow = true), while cuDF's segmented sum has no overflow-to-null
  // variant.
  def supportsType(t: DataType): Boolean = t match {
    case _: NumericType => !t.isInstanceOf[DecimalType]
    case _ => false
  }
}

case object ProductOp extends AggOp {
  val name = "PRODUCT"
  def cudfAgg: cudf.SegmentedReductionAggregation =
    cudf.SegmentedReductionAggregation.product()
  val nullPolicy: cudf.NullPolicy = cudf.NullPolicy.INCLUDE
  def identityScalar(t: DataType): cudf.Scalar = t match {
    case ByteType => cudf.Scalar.fromByte(1.toByte)
    case ShortType => cudf.Scalar.fromShort(1.toShort)
    case IntegerType => cudf.Scalar.fromInt(1)
    case LongType => cudf.Scalar.fromLong(1L)
    case FloatType => cudf.Scalar.fromFloat(1.0f)
    case DoubleType => cudf.Scalar.fromDouble(1.0)
    case other => throw new IllegalStateException(s"PRODUCT identity not defined for $other")
  }
  def identityLiteral(t: DataType): Literal = t match {
    case ByteType => Literal(1.toByte, ByteType)
    case ShortType => Literal(1.toShort, ShortType)
    case IntegerType => Literal(1, IntegerType)
    case LongType => Literal(1L, LongType)
    case FloatType => Literal(1.0f, FloatType)
    case DoubleType => Literal(1.0, DoubleType)
    case other => throw new IllegalStateException(
      s"PRODUCT identity literal not defined for $other")
  }
  def combineWithZero(r: cudf.ColumnVector, z: cudf.BinaryOperable, out: DType) = r.mul(z, out)
  def matchBinary(e: Expression): Option[(Expression, Expression)] = e match {
    case m: Multiply => Some((m.left, m.right))
    case _ => None
  }
  // Float/Double gated by variableFloatAgg.enabled (see SumOp). Decimal would also need
  // DecimalUtils.multiplyDecimals for overflow handling — out of scope, so PRODUCT
  // excludes Decimal entirely.
  def supportsType(t: DataType): Boolean = t match {
    case _: NumericType => !t.isInstanceOf[DecimalType]
    case _ => false
  }
}

/**
 * MaxOp / MinOp share EXCLUDE null policy: Spark's Greatest / Least skip null operands.
 * combineWithZero uses cuDF's NULL_MAX / NULL_MIN (the same primitive GpuGreatest/GpuLeast
 * use), which returns the non-null operand when one side is null — exactly Spark's
 * behavior on integral types.
 *
 * Float / Double are unsupported: cuDF's segmented `max` / `min` follow IEEE 754, where
 * `fmax(NaN, x) = x` (NaN is absorbed). Spark's `Greatest` / `Least` use `Double.compare`,
 * which treats NaN as larger than every other value and propagates it. Until we add an
 * explicit NaN-propagation step, restrict to integral types.
 */
sealed trait ExtremumOp extends AggOp {
  val nullPolicy: cudf.NullPolicy = cudf.NullPolicy.EXCLUDE
  def binaryOp: cudf.BinaryOp
  def combineWithZero(r: cudf.ColumnVector, z: cudf.BinaryOperable, out: DType)
      : cudf.ColumnVector = r.binaryOp(binaryOp, z, out)
  def supportsType(t: DataType): Boolean = t match {
    case ByteType | ShortType | IntegerType | LongType => true
    case _ => false
  }
}

case object MaxOp extends ExtremumOp {
  val name = "MAX"
  def cudfAgg: cudf.SegmentedReductionAggregation = cudf.SegmentedReductionAggregation.max()
  val binaryOp: cudf.BinaryOp = cudf.BinaryOp.NULL_MAX
  def identityScalar(t: DataType): cudf.Scalar = t match {
    case ByteType => cudf.Scalar.fromByte(Byte.MinValue)
    case ShortType => cudf.Scalar.fromShort(Short.MinValue)
    case IntegerType => cudf.Scalar.fromInt(Int.MinValue)
    case LongType => cudf.Scalar.fromLong(Long.MinValue)
    case other => throw new IllegalStateException(s"MAX identity not defined for $other")
  }
  def identityLiteral(t: DataType): Literal = t match {
    case ByteType => Literal(Byte.MinValue, ByteType)
    case ShortType => Literal(Short.MinValue, ShortType)
    case IntegerType => Literal(Int.MinValue, IntegerType)
    case LongType => Literal(Long.MinValue, LongType)
    case other => throw new IllegalStateException(s"MAX identity literal not defined for $other")
  }
  def matchBinary(e: Expression): Option[(Expression, Expression)] = e match {
    case g: Greatest if g.children.size == 2 => Some((g.children.head, g.children(1)))
    case _ => None
  }
}

case object MinOp extends ExtremumOp {
  val name = "MIN"
  def cudfAgg: cudf.SegmentedReductionAggregation = cudf.SegmentedReductionAggregation.min()
  val binaryOp: cudf.BinaryOp = cudf.BinaryOp.NULL_MIN
  def identityScalar(t: DataType): cudf.Scalar = t match {
    case ByteType => cudf.Scalar.fromByte(Byte.MaxValue)
    case ShortType => cudf.Scalar.fromShort(Short.MaxValue)
    case IntegerType => cudf.Scalar.fromInt(Int.MaxValue)
    case LongType => cudf.Scalar.fromLong(Long.MaxValue)
    case other => throw new IllegalStateException(s"MIN identity not defined for $other")
  }
  def identityLiteral(t: DataType): Literal = t match {
    case ByteType => Literal(Byte.MaxValue, ByteType)
    case ShortType => Literal(Short.MaxValue, ShortType)
    case IntegerType => Literal(Int.MaxValue, IntegerType)
    case LongType => Literal(Long.MaxValue, LongType)
    case other => throw new IllegalStateException(s"MIN identity literal not defined for $other")
  }
  def matchBinary(e: Expression): Option[(Expression, Expression)] = e match {
    case l: Least if l.children.size == 2 => Some((l.children.head, l.children(1)))
    case _ => None
  }
}

case object AllOp extends AggOp {
  val name = "ALL"
  def cudfAgg: cudf.SegmentedReductionAggregation = cudf.SegmentedReductionAggregation.all()
  // Nullable elements and nullable g(x) fall back because cuDF segmented ALL does not
  // implement Spark row-wise 3VL for every mixed-null input.
  val nullPolicy: cudf.NullPolicy = cudf.NullPolicy.INCLUDE
  def identityScalar(t: DataType): cudf.Scalar = cudf.Scalar.fromBool(true)
  def identityLiteral(t: DataType): Literal = Literal(true, BooleanType)
  def combineWithZero(r: cudf.ColumnVector, z: cudf.BinaryOperable, out: DType) =
    r.binaryOp(cudf.BinaryOp.NULL_LOGICAL_AND, z, out)
  def matchBinary(e: Expression): Option[(Expression, Expression)] = e match {
    case a: And => Some((a.left, a.right))
    case _ => None
  }
  def supportsType(t: DataType): Boolean = t == BooleanType
}

case object AnyOp extends AggOp {
  val name = "ANY"
  def cudfAgg: cudf.SegmentedReductionAggregation = cudf.SegmentedReductionAggregation.any()
  // Nullable elements and nullable g(x) fall back because cuDF segmented ANY does not
  // implement Spark row-wise 3VL for every mixed-null input.
  val nullPolicy: cudf.NullPolicy = cudf.NullPolicy.INCLUDE
  def identityScalar(t: DataType): cudf.Scalar = cudf.Scalar.fromBool(false)
  def identityLiteral(t: DataType): Literal = Literal(false, BooleanType)
  def combineWithZero(r: cudf.ColumnVector, z: cudf.BinaryOperable, out: DType) =
    r.binaryOp(cudf.BinaryOp.NULL_LOGICAL_OR, z, out)
  def matchBinary(e: Expression): Option[(Expression, Expression)] = e match {
    case o: Or => Some((o.left, o.right))
    case _ => None
  }
  def supportsType(t: DataType): Boolean = t == BooleanType
}


/**
 * Result of successfully matching a Spark ArrayAggregate's merge lambda against a
 * registered AggOp.
 *
 * @param op             the chosen aggregation operator
 * @param g              the lifted Catalyst sub-expression for `g(x)`. For a plain
 *                       `op(acc, g(x))` body this is the body's non-acc child; for an
 *                       `If` / `CaseWhen` body it is rebuilt with bare-acc branches
 *                       replaced by `op.identityLiteral` so it never references acc
 * @param accVarExprId   the accumulator NamedLambdaVariable's exprId
 * @param elemVar        the element NamedLambdaVariable (used to build the g lambda)
 */
case class ArrayAggregateDecomposition(
    op: AggOp,
    g: Expression,
    accVarExprId: ExprId,
    elemVar: NamedLambdaVariable)

private case class ExtractedG(g: Expression, hasBareAccBranch: Boolean)


/**
 * Decomposes a Spark ArrayAggregate's merge lambda of shape `(acc, x) -> op(acc, g(x))`
 * where `op` is one of the registered AggOps and the finish lambda is identity.
 *
 * decompose owns every reason ArrayAggregate cannot run on the GPU — shape, type, and
 * nullability — so the meta layer is just a single Either match.
 */
object ArrayAggregateDecomposer {

  /** All ops the decomposer will try, in order. */
  val allOps: Seq[AggOp] = Seq(SumOp, ProductOp, MaxOp, MinOp, AllOp, AnyOp)

  def decompose(
      merge: Expression,
      finish: Expression,
      argType: DataType,
      zeroType: DataType,
      zeroNullable: Boolean = false): Either[String, ArrayAggregateDecomposition] = {
    val mergeLambda = merge match {
      case lf: LambdaFunction => lf
      case _ => return Left("merge expression is not a LambdaFunction")
    }
    val (accVar, elemVar) = mergeLambda.arguments match {
      case Seq(a: NamedLambdaVariable, e: NamedLambdaVariable) => (a, e)
      case _ => return Left("merge lambda must take exactly 2 NamedLambdaVariable arguments")
    }
    if (!isFinishIdentity(finish)) {
      return Left("finish lambda is not an identity (only `acc -> acc` is supported)")
    }

    val body = mergeLambda.function
    val accId = accVar.exprId
    val matched = allOps.view.flatMap { op =>
      extractG(body, accId, op, zeroType).map { extracted => (op, extracted) }
    }.headOption

    val (op, extracted) = matched.getOrElse {
      return Left("merge body does not match (acc, x) -> op(acc, g(x)) for any registered " +
        "op (" + allOps.map(_.name).mkString(", ") + "); If / CaseWhen branches must be " +
        "op-of-acc or bare acc")
    }
    val g = extracted.g

    if (!op.supportsType(zeroType)) {
      return Left(s"${op.name} is not supported on GPU for type $zeroType")
    }
    // g's output type must equal the accumulator/zero type so the segmented reduce output
    // matches the Spark-expected result type directly.
    if (!DataType.equalsStructurally(g.dataType, zeroType, ignoreNullability = true)) {
      return Left(s"g(x) output type (${g.dataType}) does not match accumulator/zero type " +
        s"($zeroType)")
    }
    // cuDF's segmented ALL/ANY with INCLUDE nulls doesn't match Spark's AND/OR 3VL
    // (specifically: `false AND null = false` short-circuit, or `true OR null = true`, are
    // both missed by cuDF which returns null whenever any null is present). Fall back to
    // CPU when either the input array can contain nulls or the lifted g(x) can produce nulls.
    if (op == AllOp || op == AnyOp) {
      argType match {
        case ArrayType(_, true) =>
          return Left(s"${op.name} is not supported on GPU for arrays that may contain " +
            "nulls; cuDF's INCLUDE-nulls semantics don't match Spark's AND/OR 3VL")
        case _ =>
      }
      if (g.nullable) {
        return Left(s"${op.name} is not supported on GPU when lifted g(x) can be nullable; " +
          "cuDF's INCLUDE-nulls semantics don't match Spark's AND/OR 3VL")
      }
    }
    if ((op == MaxOp || op == MinOp) && zeroNullable && extracted.hasBareAccBranch) {
      return Left(s"${op.name} is not supported on GPU when the initial zero can be " +
        "null and a branch can leave the accumulator unchanged; the rewrite would turn " +
        "that no-contribution branch into an identity value")
    }

    Right(ArrayAggregateDecomposition(op, g, accId, elemVar))
  }

  /**
   * Try to extract g from the merge body, given a candidate op. Accepts direct
   * `op(acc, g)` / `op(g, acc)` and recurses into `If` / `CaseWhen` whose conditions
   * don't reference `acc`. A bare-acc branch is treated as `op(acc, identityLiteral)`,
   * so `If(cond, acc + 1, acc)` lifts to `acc + If(cond, 1, 0)`.
   */
  private def extractG(
      body: Expression,
      accId: ExprId,
      op: AggOp,
      accType: DataType): Option[ExtractedG] = {
    matchOpOfAcc(body, accId, op).orElse(extractFromBranching(body, accId, op, accType))
  }

  /** body matches op directly: returns `g` if body is `op(acc, g)` / `op(g, acc)`. */
  private def matchOpOfAcc(
      e: Expression,
      accId: ExprId,
      op: AggOp): Option[ExtractedG] = {
    op.matchBinary(unwrapDecimalPatternWrappers(e)).flatMap { case (l, r) =>
      if (isAccRef(l, accId) && !containsAccRef(r, accId)) Some(ExtractedG(r, false))
      else if (isAccRef(r, accId) && !containsAccRef(l, accId)) Some(ExtractedG(l, false))
      else None
    }
  }

  /**
   * Decompose a single If/CaseWhen branch. Either it's an op-of-acc form (returns the
   * non-acc side), or it's a bare acc-ref (returns the op's identity literal).
   *
   * Recursively delegates to `extractG` so nested If is handled.
   */
  private def extractBranch(
      branch: Expression,
      accId: ExprId,
      op: AggOp,
      accType: DataType): Option[ExtractedG] = {
    if (isAccRef(branch, accId)) {
      Some(ExtractedG(op.identityLiteral(accType), true))
    } else {
      extractG(branch, accId, op, accType)
    }
  }

  private def extractFromBranching(
      body: Expression,
      accId: ExprId,
      op: AggOp,
      accType: DataType): Option[ExtractedG] = body match {
    case If(cond, t, f) if !containsAccRef(cond, accId) =>
      for {
        tG <- extractBranch(t, accId, op, accType)
        fG <- extractBranch(f, accId, op, accType)
      } yield ExtractedG(If(cond, tG.g, fG.g), tG.hasBareAccBranch || fG.hasBareAccBranch)

    case CaseWhen(branches, Some(elseValue))
        if branches.forall { case (c, _) => !containsAccRef(c, accId) } =>
      // Decompose every (cond, val) branch + the else branch.
      val branchDecs = branches.map { case (c, v) => (c, extractBranch(v, accId, op, accType)) }
      val elseDec = extractBranch(elseValue, accId, op, accType)
      if (branchDecs.exists(_._2.isEmpty) || elseDec.isEmpty) {
        None
      } else {
        val gBranches = branchDecs.map { case (c, dec) => (c, dec.get.g) }
        val hasBareAccBranch = branchDecs.exists(_._2.exists(_.hasBareAccBranch)) ||
          elseDec.exists(_.hasBareAccBranch)
        Some(ExtractedG(CaseWhen(gBranches, Some(elseDec.get.g)), hasBareAccBranch))
      }

    case _ => None
  }

  private def isFinishIdentity(finish: Expression): Boolean = finish match {
    case LambdaFunction(body, Seq(accVar: NamedLambdaVariable), _) =>
      body match {
        case v: NamedLambdaVariable => v.exprId == accVar.exprId
        case _ => false
      }
    case _ => false
  }

  private def isAccRef(e: Expression, id: ExprId): Boolean = e match {
    case v: NamedLambdaVariable => v.exprId == id
    case c: Cast => isAccRef(c.child, id)
    case other if isDecimalPatternWrapper(other) && other.children.length == 1 =>
      isAccRef(other.children.head, id)
    case _ => false
  }

  private def unwrapDecimalPatternWrappers(e: Expression): Expression = {
    if (isDecimalPatternWrapper(e) && e.children.length == 1) {
      unwrapDecimalPatternWrappers(e.children.head)
    } else {
      e
    }
  }

  // Spark 3.3 and earlier wrap decimal arithmetic as CheckOverflow(PromotePrecision(op(...))).
  // Use class names so this still compiles on Spark versions where PromotePrecision is gone.
  private def isDecimalPatternWrapper(e: Expression): Boolean = {
    val name = e.getClass.getName
    name.endsWith(".CheckOverflow") || name.endsWith(".PromotePrecision")
  }

  private def containsAccRef(e: Expression, id: ExprId): Boolean = e.exists {
    case v: NamedLambdaVariable if v.exprId == id => true
    case _ => false
  }
}


/**
 * GPU implementation of ArrayAggregate for lambdas decomposable via ArrayAggregateDecomposer.
 * See `columnarEval` for the per-batch pipeline.
 */
case class GpuArrayAggregate(
    argument: Expression,
    zero: Expression,
    function: Expression,
    op: AggOp,
    isBound: Boolean = false,
    boundIntermediate: Seq[GpuExpression] = Seq.empty) extends GpuArrayTransformBase {

  override def dataType: DataType = zero.dataType

  // Matches Spark's ArrayAggregate.nullable = argument.nullable || finish.nullable. The
  // finish lambda's accumulator variable is bound with nullable=true (Spark's
  // ArrayAggregate.bind uses `zero.dataType -> true` for the acc slot), so the CPU side
  // is effectively always true. Also covers the INCLUDE-policy case where a null element
  // in a non-null list poisons the reduce and yields a null output row.
  override def nullable: Boolean = true

  override def prettyName: String = "array_aggregate"

  override def children: Seq[Expression] = argument :: zero :: function :: Nil

  override def bind(input: AttributeSeq): GpuExpression = {
    val (boundFunc, boundArg, boundInter) = bindLambdaFunc(input)
    val boundZero = GpuBindReferences.bindGpuReferenceNoMetrics(zero, input)
    GpuArrayAggregate(boundArg, boundZero, boundFunc, op, isBound = true, boundInter)
  }

  /**
   * Mask of empty-and-not-null lists where an INCLUDE-policy reduce needs the op identity.
   * Null-poisoned reduces stay null and propagate through the combine step.
   */
  private def emptyIncludeListMask(listCol: cudf.ColumnView): cudf.ColumnVector = {
    val isEmpty = withResource(listCol.countElements()) { counts =>
      withResource(cudf.Scalar.fromInt(0)) { zero =>
        counts.equalTo(zero)
      }
    }
    // Exclude null-list rows from the mask so the final null-restoration step handles them.
    // Skip when the input list has no nulls — `m.and(all-true)` is a wasted kernel.
    if (listCol.getNullCount > 0) {
      withResource(isEmpty) { m =>
        withResource(listCol.isNotNull) { isNotNull => m.and(isNotNull) }
      }
    } else {
      isEmpty
    }
  }

  private[rapids] def aggregateAndCloseElementResults(
      batch: ColumnarBatch,
      transformedData: GpuColumnVector,
      arg: GpuColumnVector): GpuColumnVector = {
    val outDType = GpuColumnVector.getNonNestedRapidsType(dataType)
    // Step 1: g(x) over children + segmented reduce.
    val reduced: cudf.ColumnVector = withResource(transformedData) { transformedData =>
      withResource(GpuListUtils.replaceListDataColumnAsView(
          arg.getBase, transformedData.getBase)) { listOfGView =>
        listOfGView.listReduce(op.cudfAgg, op.nullPolicy, outDType)
      }
    }

    // Step 2: substitute op's identity where needed.
    val adjusted: cudf.ColumnVector = withResource(reduced) { reduced =>
      if (op.nullPolicy == cudf.NullPolicy.EXCLUDE) {
        // MAX/MIN should keep no-contribution rows as null until combineWithZero.
        // NULL_MAX/NULL_MIN then return zero when it is non-null, and preserve a null zero.
        reduced.incRefCount()
      } else {
        withResource(emptyIncludeListMask(arg.getBase)) { mask =>
          withResource(op.identityScalar(dataType)) { idScalar =>
            mask.ifElse(idScalar, reduced)
          }
        }
      }
    }

    // Step 3: combine with zero. When `zero` is a Literal (the common 4-arg
    // `aggregate(arr, 0, ...)` shape) skip the per-batch column broadcast and pass a
    // cudf.Scalar instead — `add/mul/and/or/binaryOp` all accept BinaryOperable.
    val combined: cudf.ColumnVector = withResource(adjusted) { adjusted =>
      zero match {
        case lit: GpuLiteral =>
          withResource(GpuScalar.from(lit.value, lit.dataType)) { zeroScalar =>
            op.combineWithZero(adjusted, zeroScalar, outDType)
          }
        case _ =>
          withResource(zero.asInstanceOf[GpuExpression].columnarEval(batch)) { zeroCv =>
            op.combineWithZero(adjusted, zeroCv.getBase, outDType)
          }
      }
    }

    // Step 4: restore null on rows where the input list itself was null. cuDF NULL_MAX /
    // NULL_MIN / LOGICAL_AND / LOGICAL_OR don't propagate null the way Spark's 3VL would,
    // so the combine step alone can't preserve it. Skip outright when the list has no nulls.
    if (arg.getBase.getNullCount > 0) {
      withResource(combined) { combined =>
        GpuColumnVector.from(NullUtilities.mergeNulls(combined, arg.getBase), dataType)
      }
    } else {
      GpuColumnVector.from(combined, dataType)
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(argument.asInstanceOf[GpuExpression].columnarEval(batch)) { arg =>
      val transformedData = withResource(makeElementProjectBatch(batch, arg)) { cb =>
        function.asInstanceOf[GpuExpression].columnarEval(cb)
      }
      aggregateAndCloseElementResults(batch, transformedData, arg)
    }
  }
}


/**
 * Expression-level meta for Spark's ArrayAggregate. Accepts lambdas that
 * ArrayAggregateDecomposer can decompose into one of the registered AggOps with an
 * identity finish; falls back to CPU otherwise.
 */
class GpuArrayAggregateMeta(
    expr: ArrayAggregate,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[ArrayAggregate](expr, conf, parent, rule) {

  private var decomposition: Option[ArrayAggregateDecomposition] = None
  private var gMeta: Option[BaseExprMeta[_]] = None

  override def tagExprForGpu(): Unit = {
    ArrayAggregateDecomposer.decompose(
      expr.merge, expr.finish, expr.argument.dataType, expr.zero.dataType,
      expr.zero.nullable) match {
      case Left(reason) => willNotWorkOnGpu(reason)
      case Right(d) =>
        if (d.op == SumOp || d.op == ProductOp) {
          // SUM/PRODUCT on Float/Double diverge between cuDF's parallel tree-reduction
          // and Spark's sequential left-fold. Same conf gate as GpuSum / GpuAverage —
          // willNotWorkOnGpu when variableFloatAgg.enabled=false.
          GpuOverrides.checkAndTagFloatAgg(expr.zero.dataType, this.conf, this)
          // ANSI: row-wise GpuAdd/GpuMultiply check overflow and raise, but cuDF's
          // segmented reduce we delegate to here has no overflow-checking variant —
          // it wraps. Fall back to CPU for the integer cases that ANSI requires to raise.
          if (SQLConf.get.ansiEnabled) {
            expr.zero.dataType match {
              case ByteType | ShortType | IntegerType | LongType =>
                willNotWorkOnGpu(s"${d.op.name} in ANSI mode is not yet supported on " +
                  "GPU; cuDF segmented reduce wraps on overflow rather than raising " +
                  "ArithmeticException")
              case _ =>
            }
          }
        }
        val taggedGMeta = GpuOverrides.wrapExpr(d.g, this.conf, Some(this))
        taggedGMeta.tagForGpu()
        if (!taggedGMeta.canExprTreeBeReplaced) {
          willNotWorkOnGpu(
            s"lifted g sub-expression cannot run on GPU: ${taggedGMeta.explain(all = false)}")
        }
        decomposition = Some(d)
        gMeta = Some(taggedGMeta)
    }
  }

  override def convertToGpuImpl(): GpuExpression = {
    val d = decomposition.getOrElse(
      throw new IllegalStateException("tagExprForGpu must succeed before convertToGpu"))

    val argGpu = childExprs.head.convertToGpu()
    val zeroGpu = childExprs(1).convertToGpu()
    // The lifted g may have a different shape from any sub-tree of the original merge body
    // (If/CaseWhen branches get rewritten and identity literals get inserted), so we can't
    // pick it out of childExprs(2)'s meta tree by index. tagExprForGpu wraps and tags it
    // separately so unsupported lifted expressions fall back instead of failing conversion.
    val taggedGMeta = gMeta.getOrElse {
      throw new IllegalStateException(
        "tagExprForGpu must tag g sub-expression before convertToGpu")
    }
    val gGpu = taggedGMeta.convertToGpu().asInstanceOf[GpuExpression]
    val elemVarGpu = GpuNamedLambdaVariable(
      d.elemVar.name, d.elemVar.dataType, d.elemVar.nullable, d.elemVar.exprId)
    val gLambda = GpuLambdaFunction(gGpu, Seq(elemVarGpu))

    GpuArrayAggregate(argGpu, zeroGpu, gLambda, d.op)
  }
}
