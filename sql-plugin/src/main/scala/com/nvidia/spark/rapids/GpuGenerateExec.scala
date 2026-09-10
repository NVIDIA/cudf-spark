/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, DType, OrderByArg, Scalar, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuOverrides.extractLit
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM
import com.nvidia.spark.rapids.shims.{ShimExpression, ShimUnaryExecNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, Generator, ReplicateRows, Stack}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{GenerateExec, SparkPlan}
import org.apache.spark.sql.rapids.GpuCreateArray
import org.apache.spark.sql.rapids.execution.GpuSubPartitionHashJoin.safeIteratorFromSeq
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuGenerateExecSparkPlanMeta(
    gen: GenerateExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends SparkPlanMeta[GenerateExec](gen, conf, p, r) {

  override val childExprs: Seq[BaseExprMeta[_]] = {
    (Seq(gen.generator) ++ gen.requiredChildOutput).map(
      GpuOverrides.wrapExpr(_, conf, Some(this)))
  }

  override def tagPlanForGpu(): Unit = {
    if (gen.outer && childExprs.head.isInstanceOf[GeneratorExprMeta[_]] &&
      !childExprs.head.asInstanceOf[GeneratorExprMeta[Generator]].supportOuter) {
      willNotWorkOnGpu(s"outer is not currently supported with ${gen.generator.nodeName}")
    }
  }

  private def getExpandExecForStack(stackMeta: GpuStackMeta): GpuExec = {
    val numRows = extractLit(stackMeta.childExprs.head.convertToCpu()) match {
      case Some(lit) => lit.value.asInstanceOf[Int]
      case _ => throw new IllegalStateException("First parameter of stack should be a literal Int")
    }
    val numFields = Math.ceil((stackMeta.childExprs.length - 1.0) / numRows).toInt
    val projections: Seq[Seq[Expression]] = for (row <- 0 until numRows) yield {
      val childExprsGpu = childExprs.tail.map(_.convertToGpu())
      val otherProj: Seq[Expression] = for (req <- childExprsGpu) yield {
        req
      }
      val stackProj: Seq[Expression] = for (col <- 0 until numFields) yield {
        val index = row * numFields + col
        val dataChildren = stackMeta.childExprs.tail
        if (index >= dataChildren.length) {
          val typeInfo = dataChildren(col).dataType
          GpuLiteral(null, typeInfo)
        } else {
          dataChildren(index).convertToGpu()
        }
      }
      otherProj ++ stackProj
    }
    val output: Seq[Attribute] = gen.requiredChildOutput ++ gen.generatorOutput.take(numFields)
    GpuExpandExec(projections, output, childPlans.head.convertIfNeeded())(
        preprojectEnabled = conf.isExpandPreprojectEnabled)
  }

  override def convertToGpu(): GpuExec = {
    // if child expression contains Stack, use GpuExpandExec instead
    childExprs.head match {
      case stackMeta: GpuStackMeta =>
        getExpandExecForStack(stackMeta)
      case genMeta =>
        GpuGenerateExec(
          genMeta.convertToGpu().asInstanceOf[GpuGenerator],
          gen.requiredChildOutput,
          gen.outer,
          gen.generatorOutput,
          childPlans.head.convertIfNeeded())
    }
  }
}

class GpuStackMeta(
    stack: Stack,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends BaseExprMeta[Stack](stack, conf, parent, rule) {

  override val childExprs: Seq[BaseExprMeta[_]] = stack.children
      .map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  
  override def convertToGpuImpl(): GpuExpression = {
    // There is no need to implement convertToGpu() here, because GpuGenerateExec will handle
    // stack logic in terms of GpuExpandExec, no convertToGpu() will be called during the process
    throw new UnsupportedOperationException(s"Should not be here: $this")
  }
}

abstract class GeneratorExprMeta[INPUT <: Generator](
    gen: INPUT,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends ExprMeta[INPUT](gen, conf, p, r) {
  /* whether supporting outer generate or not */
  val supportOuter: Boolean = false
}

/**
 * Base class for metadata around GeneratorExprMeta.
 */
abstract class ReplicateRowsExprMeta[INPUT <: ReplicateRows](
    gen: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GeneratorExprMeta[INPUT](gen, conf, parent, rule) {

  override final def convertToGpuImpl(): GpuExpression =
    convertToGpu(childExprs.map(_.convertToGpu()))

  def convertToGpu(childExprs: Seq[Expression]): GpuExpression
}

/**
 * GPU overrides of `Generator`, corporate with `GpuGenerateExec`.
 */
trait GpuGenerator extends GpuUnevaluable {

  override def dataType: DataType = ArrayType(elementSchema)

  override def foldable: Boolean = false

  override def nullable: Boolean = false

  /**
   * The output element schema.
   */
  def elementSchema: StructType

  /**
   * Apply generator to produce result ColumnarBatch from input batch.
   *
   * This is a specialized method for GPU runtime, which is called by GpuGenerateExec who owns
   * the generator. The reason of creating a new method rather than implementing columnarEval is
   * that generator is an integrated Table transformer instead of column transformer in terms of
   * cuDF.
   *
   * @param batches iterator of spillable batches
   * @param generatorOffset column offset of generator's input columns in `inputBatch`
   * @param outer           when true, each input row will be output at least once, even if the
   *                        output of the given `generator` is empty.
   * @return result ColumnarBatch
   */
  def generate(
    batches: Iterator[SpillableColumnarBatch],
    generatorOffset: Int,
    outer: Boolean): Iterator[ColumnarBatch]

  /**
   * Compute split indices for generator's input batches.
   *
   * This is a specialized method for GPU runtime, which is called by GpuGenerateExec to split up
   * input batches to reduce total memory cost during generating. It is necessary because most of
   * generators may produce multiple records for each input record, which make output batch size
   * much larger than input size.
   *
   * @param inputBatch      projected input data, which ensures appending columns are ahead of
   *                        generators' inputs. So, generators can distinguish them with an offset.
   * @param generatorOffset column offset of generator's input columns in `inputBatch`
   * @param outer           when true, each input row will be output at least once, even if the
   *                        output of the given `generator` is empty.
   * @param targetSizeBytes the target number of bytes for a GPU batch, one of `RapidsConf`
   * @param maxRows         the target number of rows for a GPU batch, exposed for testing purposes.
   * @return split indices of input batch
   */
  def inputSplitIndices(inputBatch: ColumnarBatch,
      generatorOffset: Int,
      outer: Boolean,
      targetSizeBytes: Long,
      maxRows: Int = Int.MaxValue): Array[Int]

  /**
   * Extract lazy expressions from generator if exists.
   *
   * This is a specialized method for GPU runtime, which is called by GpuGenerateExec to determine
   * whether current generation plan can be executed with optimized lazy array generation or not.
   *
   * @return fixed length lazy expressions for generation. Nil value means no lazy expressions to
   *         extract, which indicates fixed length lazy array generation is unavailable.
   */
  def fixedLenLazyExpressions: Seq[Expression] = Nil

  /**
   * Optimized lazy generation interface which is specialized for fixed length array input.
   *
   * For some generators (like explode), it is possible to improve performance through lazy
   * evaluation when input schema is fixed length array.
   *
   * @param inputIterator          input iterator from child plan
   * @param boundLazyProjectList   lazy expressions bounded with child outputs
   * @param boundOthersProjectList other required expressions bounded with child outputs
   * @param outputSchema           result schema of GpuGenerateExec
   * @param outer                  when true, each input row will be output at least once, even if
   *                               the output of the given `generator` is empty.
   * @param numOutputRows          Gpu spark metric of output rows
   * @param numOutputBatches       Gpu spark metric of output batches
   * @param opTime                 Gpu spark metric of time on GPU by GpuGenerateExec
   * @return result iterator
   */
  def fixedLenLazyArrayGenerate(inputIterator: Iterator[ColumnarBatch],
      boundLazyProjectList: Seq[Expression],
      boundOthersProjectList: Seq[Expression],
      outputSchema: Array[DataType],
      outer: Boolean,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      opTime: GpuMetric): Iterator[ColumnarBatch] = {
    throw new NotImplementedError("The method should be implemented by specific generators.")
  }
}

case class GpuReplicateRows(children: Seq[Expression]) extends GpuGenerator with ShimExpression {

  override def elementSchema: StructType =
    StructType(children.tail.zipWithIndex.map {
      case (e, index) => StructField(s"col$index", e.dataType)
    })

  override def generate(
      inputBatches: Iterator[SpillableColumnarBatch],
      generatorOffset: Int,
      outer: Boolean): Iterator[ColumnarBatch]  = {
    withRetry(inputBatches, splitSpillableInHalfByRows) { attempt =>
      withResource(attempt.getColumnarBatch()) { inputBatch =>
        val schema = GpuColumnVector.extractTypes(inputBatch)
        withResource(GpuColumnVector.from(inputBatch)) { table =>
          val replicateVector = table.getColumn(generatorOffset)
          withResource(table.repeat(replicateVector)) { replicatedTable =>
            GpuColumnVector.from(replicatedTable, schema)
          }
        }
      }
    }
  }

  override def inputSplitIndices(inputBatch: ColumnarBatch,
      generatorOffset: Int,
      outer: Boolean,
      targetSizeBytes: Long,
      maxRows: Int = Int.MaxValue): Array[Int] = {
    val vectors = GpuColumnVector.extractBases(inputBatch)
    val inputRows = inputBatch.numRows()
    if (inputRows == 0) return Array()

    // Calculate the number of rows that needs to be replicated. Here we find the mean of the
    // generator column. Multiplying the mean with size of projected columns would give us the
    // approximate memory required.
    val meanOutputRows = math.ceil(withResource(vectors(generatorOffset).mean()) {
      _.getDouble
    })
    val estimatedOutputRows = meanOutputRows * inputRows

    // input size of columns to be repeated
    val repeatColsInputSize = vectors.slice(0, generatorOffset).map(_.getDeviceMemorySize).sum
    // estimated total output size
    val estimatedOutputSizeBytes = repeatColsInputSize * estimatedOutputRows / inputRows

    // how may splits will we need to keep the output size under the target size
    val numSplitsForTargetSize = math.ceil(estimatedOutputSizeBytes / targetSizeBytes).toInt
    // how may splits will we need to keep the output rows under max value
    val numSplitsForTargetRow = math.ceil(estimatedOutputRows / maxRows).toInt
    // how may splits will we need to keep replicateRows working safely
    val numSplits = numSplitsForTargetSize max numSplitsForTargetRow

    if (numSplits == 0) {
      Array()
    } else {
      GpuBatchUtils.generateSplitIndices(inputRows, numSplits)
    }
  }
}

abstract class GpuExplodeBase extends GpuUnevaluableUnaryExpression with GpuGenerator {

  /** The position of an element within the collection should also be returned. */
  def position: Boolean

  // hive-compatible default alias for explode function ("col" for array, "key", "value" for map)
  override def elementSchema: StructType = child.dataType match {
    case ArrayType(et, containsNull) =>
      if (position) {
        new StructType()
          .add("pos", IntegerType, nullable = false)
          .add("col", et, containsNull)
      } else {
        new StructType()
          .add("col", et, containsNull)
      }
    case MapType(kt, vt, valueContainsNull) =>
      if (position) {
        new StructType()
          .add("pos", IntegerType, nullable = false)
          .add("key", kt, nullable = false)
          .add("value", vt, valueContainsNull)
      } else {
        new StructType()
          .add("key", kt, nullable = false)
          .add("value", vt, valueContainsNull)
      }
  }

  private def getPerRowRepetition(explodingColumn: ColumnVector, outer: Boolean): ColumnVector = {
    withResource(explodingColumn.countElements()) { arrayLengths =>
      if (outer) {
        // for outer, empty arrays and null arrays will produce a row
        withResource(GpuScalar.from(1, IntegerType)) { one =>
          withResource(arrayLengths.replaceNulls(one)) { noNulls =>
            withResource(noNulls.greaterOrEqualTo(one)) { isGeOne =>
              isGeOne.ifElse(noNulls, one)
            }
          }
        }
      } else {
        // ensure no nulls in this output
        withResource(GpuScalar.from(0, IntegerType)) { zero =>
          GpuNvl(arrayLengths, zero)
        }
      }
    }
  }

  private def getRowByteCount(column: Seq[ColumnVector]): ColumnVector = {
    NvtxRegistry.GENERATE_GET_ROW_BYTE_COUNT {
      val bits = withResource(new Table(column: _*)) { tbl =>
        tbl.rowBitCount()
      }
      withResource(bits) { _ =>
        withResource(Scalar.fromLong(8)) { toBytes =>
          bits.trueDiv(toBytes, DType.INT64)
        }
      }
    }
  }

  override def inputSplitIndices(inputBatch: ColumnarBatch,
      generatorOffset: Int, outer: Boolean,
      targetSizeBytes: Long,
      maxRows: Int = Int.MaxValue): Array[Int] = {
    val inputRows = inputBatch.numRows()

    // if the number of input rows is 1 or less, cannot split
    if (inputRows <= 1) return Array()

    val vectors = GpuColumnVector.extractBases(inputBatch)

    val explodingColumn = vectors(generatorOffset)

    // Get the output size in bytes of the column that we are going to explode
    // along with an estimate of how many output rows produced by the explode
    val (explodeColOutputSize, estimatedOutputRows) =
      withResource(explodingColumn.getChildColumnView(0)) { listValues =>
        val totalSize = listValues.getDeviceMemorySize
        // get the number of elements in the array child
        var totalCount = listValues.getRowCount
        // when we are calculating an explode_outer, we need to add to the row count
        // the cases where the parent element has a null array, as we are going to produce
        // these rows.
        if (outer) {
          totalCount += explodingColumn.getNullCount
        }
        (totalSize.toDouble, totalCount.toDouble)
      }

    // we know we are going to output at least this much
    var estimatedOutputSizeBytes = explodeColOutputSize

    val splitIndices = if (generatorOffset == 0) {
      // this explodes the array column, and the table has no other columns to go
      // along with it
      val numSplitsForTargetSize =
        math.min(inputRows,
          math.ceil(estimatedOutputSizeBytes / targetSizeBytes).toInt)
      if (numSplitsForTargetSize == 0) {
        // estimatedOutputSizeBytes is 0 when every exploding row is empty/null,
        // so no further splitting is needed. Matches the else-branch guard below.
        Array.empty[Int]
      } else {
        GpuBatchUtils.generateSplitIndices(inputRows, numSplitsForTargetSize).distinct
      }
    } else {
      NvtxRegistry.GENERATE_ESTIMATE_REPETITION {
        // get the # of repetitions per row of the input for this explode
        val perRowRepetition = getPerRowRepetition(explodingColumn, outer)
        val repeatingColumns = vectors.slice(0, generatorOffset)

        // get per row byte count of every column, except the exploding one
        // NOTE: in the future, we may want to factor in the exploding column size
        // into this math, if there is skew in the column to explode.
        val repeatingByteCount = withResource(perRowRepetition) { _ =>
          withResource(getRowByteCount(repeatingColumns)) { byteCountBeforeRepetition =>
            byteCountBeforeRepetition.mul(perRowRepetition)
          }
        }

        // compute prefix sum of byte sizes, this can be used to find input row
        // split points at which point the output batch is estimated to be roughly
        // prefixSum(row) bytes ( + exploding column size for `row`)
        val prefixSum = withResource(repeatingByteCount) {
          _.prefixSum
        }

        val splitIndices = withResource(prefixSum) { _ =>
          // the last element of `repeatedSizeEstimate` is the overall sum
          val repeatedSizeEstimate =
            withResource(prefixSum.subVector((prefixSum.getRowCount - 1).toInt)) { lastRow =>
              withResource(lastRow.copyToHost()) { hc =>
                hc.getLong(0)
              }
            }

          estimatedOutputSizeBytes += repeatedSizeEstimate

          // how may splits will we need to keep the output size under the target size
          val numSplitsForTargetSize =
            math.min(inputRows,
              math.ceil(estimatedOutputSizeBytes / targetSizeBytes).toInt)

          val idealSplits = if (numSplitsForTargetSize == 0) {
            Array.empty[Long]
          } else {
            // we need to apply the splits onto repeated size, because the prefixSum
            // is only done for repeated sizes
            val sizePerSplit =
              math.ceil(repeatedSizeEstimate.toDouble / numSplitsForTargetSize).toLong
            (1 until numSplitsForTargetSize).map { s =>
              s * sizePerSplit
            }.toArray
          }

          if (idealSplits.length == 0) {
            Array.empty[Int]
          } else {
            val lowerBound =
              withResource(new Table(prefixSum)) { prefixSumTable =>
                withResource(ColumnVector.fromLongs(idealSplits: _*)) { idealBounds =>
                  withResource(new Table(idealBounds)) { idealBoundsTable =>
                    prefixSumTable.lowerBound(idealBoundsTable, OrderByArg.asc(0))
                  }
                }
              }

            val largestSplitIndex = inputRows - 1
            val splits = withResource(lowerBound) { _ =>
              withResource(lowerBound.copyToHost) { hostBounds =>
                (0 until hostBounds.getRowCount.toInt).map { s =>
                  // add 1 to the bound because you get the row index of the last
                  // row at which was smaller or equal to the bound, for example:
                  // prefixSum=[8, 16, 24, 32]
                  // if you are looking for a bound of 16, you get index 1. That said, to
                  // split this, you want the index to be between 16 and 24, so that's index 2.
                  // Make sure that the maximum bound is inputRows - 1, otherwise we can trigger
                  // a cuDF exception.
                  math.min(hostBounds.getInt(s) + 1, largestSplitIndex)
                }
              }
            }

            // apply distinct in the case of extreme skew, where for example we have all nulls
            // except for 1 row that has all the data.
            splits.distinct.toArray
          }
        }
        splitIndices
      }
    }

    // how may splits will we need to keep the output rows under max value
    val numSplitsForTargetRow = math.ceil(estimatedOutputRows / maxRows).toInt

    // If the number of splits needed to keep the row limits for cuDF is higher than
    // the splits we found by size, we need to use the row-based splits.
    // Note, given skewed input, we could be left with batches split at bad places,
    // e.g. all of the non nulls are in a single split. So we may need to re-split
    // that row-based slice using the size approach.
    if (numSplitsForTargetRow > splitIndices.length) {
      GpuBatchUtils.generateSplitIndices(inputRows, numSplitsForTargetRow)
    } else {
      splitIndices
    }
  }

  // Infer result schema of GenerateExec from input schema
  protected def resultSchema(inputSchema: Array[DataType],
    genOffset: Int): Array[DataType] = {
    val outputSchema = ArrayBuffer[DataType]()
    inputSchema.zipWithIndex.foreach {
      // extract output type of explode from input ArrayData
      case (dataType, index) if index == genOffset =>
        if (position) {
          outputSchema += IntegerType
        }
        dataType match {
          case ArrayType(elementType, _) =>
            outputSchema += elementType
          case MapType(keyType, valueType, _) =>
            outputSchema += keyType
            outputSchema += valueType
        }
      // map types of other required columns
      case (dataType, _) =>
        outputSchema += dataType
    }
    outputSchema.toArray
  }

  /**
   * A function that will do the explode or position explode
   */
  private[this] def explodeFun(
      inputTable: Table,
      genOffset: Int,
      outer: Boolean,
      fixUpOffset: Long): Table = {
    if (position) {
      val toFixUp = if (outer) {
        inputTable.explodeOuterPosition(genOffset)
      } else {
        inputTable.explodePosition(genOffset)
      }
      // the position column is at genOffset and the exploded column at genOffset + 1
      withResource(toFixUp) { _ =>
        val posColToFix = toFixUp.getColumn(genOffset)
        val fixedUpPosCol = withResource(Scalar.fromLong(fixUpOffset)) { offset =>
          posColToFix.add(offset, posColToFix.getType)
        }
        withResource(fixedUpPosCol) { _ =>
          val newCols = (0 until toFixUp.getNumberOfColumns).map { i =>
            if (i == genOffset) {
              fixedUpPosCol
            } else {
              toFixUp.getColumn(i)
            }
          }.toArray
          new Table(newCols: _*)
        }
      }
    } else {
      if (outer) {
        inputTable.explodeOuter(genOffset)
      } else {
        inputTable.explode(genOffset)
      }
    }
  }

  def generateSplitSpillableInHalfByRows(
      genOffset: Int): BatchToGenerate => Seq[BatchToGenerate] = {
    (batchToGenerate: BatchToGenerate) => {
      withResource(batchToGenerate) { _ =>
        val spillable = batchToGenerate.spillable
        val toSplitRows = spillable.numRows()
        if (toSplitRows == 1) {
          // single row, we need to actually add row duplicates, then split
          val tbl = withResource(spillable.getColumnarBatch()) { src =>
            GpuColumnVector.from(src)
          }
          withResource(tbl) { _ =>
            if (tbl.getColumn(genOffset).getNullCount == 1) {
              // 1 row, and the element to explode is null, we cannot
              // handle this case, and should never reach it either.
              throw new GpuSplitAndRetryOOM(
                "GPU OutOfMemory: cannot split a batch of 1 rows when " +
                    "the list to explode is null!")
            }
            val explodingColList = tbl.getColumn(genOffset)
            val explodedTbl = {
              withResource(explodingColList.getChildColumnView(0)) {
                explodingColElements =>
                  if (explodingColElements.getRowCount < 2) {
                    throw new GpuSplitAndRetryOOM(
                      "GPU OutOfMemory: cannot split a batch of 1 rows when " +
                          "the list has a single element!")
                  }
                  withResource(explodingColElements.copyToColumnVector()) { col =>
                    new Table(col)
                  }
              }
            }

            // Because we are likely to be here solving a cuDF column length limit
            // we are just going to split in half blindly until we can fit it.
            val splits = withResource(explodedTbl) { _ =>
              explodedTbl.contiguousSplit(explodedTbl.getRowCount.toInt / 2)
            }

            val result = new Array[BatchToGenerate](splits.size)
            closeOnExcept(result) { _ =>
              closeOnExcept(splits) { _ =>
                // start our fixup offset to the one previously computed
                // in the last retry. We will need to start again at this offset,
                // and sub-lists we generate need to account for it if `pos_explode`.
                var offset = batchToGenerate.fixUpOffset
                (0 until splits.length).foreach { split =>
                  val explodedTblToConvertBack = splits(split)
                  val cols = new Array[ColumnVector](tbl.getNumberOfColumns)
                  withResource(explodedTblToConvertBack) { _ =>
                    val colToReconstitute = explodedTblToConvertBack.getTable.getColumn(0)
                    closeOnExcept(cols) { _ =>
                      (0 until tbl.getNumberOfColumns).foreach { col =>
                        cols(col) = if (col == genOffset) {
                          // turn this column back onto a list, by pairing it with an offset
                          // column that is the length of the list.
                          require(colToReconstitute.getRowCount < Int.MaxValue,
                            s"The number of elements in the list to explode " +
                                s"${colToReconstitute.getRowCount} is larger than " +
                                "cuDF column row limits.")
                          withResource(
                            ColumnVector.fromInts(0, colToReconstitute.getRowCount.toInt)) {
                            offsets =>
                              colToReconstitute.makeListFromOffsets(1, offsets)
                          }
                        } else {
                          tbl.getColumn(col).incRefCount()
                        }
                      }
                    }
                  }
                  val newOffset = splits(split).getRowCount
                  splits(split) = null
                  val finalTbl = withResource(cols) { _ =>
                    new Table(cols: _*)
                  }
                  withResource(finalTbl) { _ =>
                    result(split) = new BatchToGenerate(offset, SpillableColumnarBatch(
                      GpuColumnVector.from(finalTbl, spillable.dataTypes),
                      SpillPriorities.ACTIVE_BATCHING_PRIORITY))

                    // we update our fixup offset for future batches, as they will
                    // need to add the number of rows seen so far to the position column
                    // if this is a `pos_explode`
                    offset += newOffset
                  }
                }
              }
            }
            result
          }
        } else {
          // more than 1 rows, we just do the regular split-by-rows,
          // we need to keep track of the fixup offset
          RmmRapidsRetryIterator.splitSpillableInHalfByRows(spillable)
              .map(new BatchToGenerate(batchToGenerate.fixUpOffset, _))
        }
      }
    }
  }

  override def generate(
      inputSpillables: Iterator[SpillableColumnarBatch],
      generatorOffset: Int,
      outer: Boolean): Iterator[ColumnarBatch] = {
    val batchesToGenerate = inputSpillables.map(new BatchToGenerate(0, _))
    withRetry(batchesToGenerate, generateSplitSpillableInHalfByRows(generatorOffset)) { attempt =>
      val (exploded, schema) = withResource(attempt.spillable.getColumnarBatch()) { inputBatch =>
        require(inputBatch.numCols() - 1 == generatorOffset,
          s"Internal Error ${getClass.getSimpleName} supports one and only one input attribute.")
        val schema = resultSchema(GpuColumnVector.extractTypes(inputBatch), generatorOffset)
        withResource(GpuColumnVector.from(inputBatch)) { table =>
          (explodeFun(table, generatorOffset, outer, attempt.fixUpOffset), schema)
        }
      }
      withResource(exploded) { _ =>
        child.dataType match {
          case _: ArrayType =>
            GpuColumnVector.from(exploded, schema)
          case MapType(kt, vt, _) =>
            // We need to pull the key and value of of the struct column
            withResource(convertMapOutput(exploded, generatorOffset, kt, vt, outer)) { fixed =>
              GpuColumnVector.from(fixed, schema)
            }
          case other =>
            throw new IllegalArgumentException(
              s"$other is not supported as explode input right now")
        }
      }
    }
  }

  private[this] def convertMapOutput(exploded: Table,
      genOffset: Int,
      kt: DataType,
      vt: DataType,
      fixChildValidity: Boolean): Table = {
    val numPos = if (position) 1 else 0
    // scalastyle:off line.size.limit
    // The input will look like the following, and we just want to expand the key, value in the
    // struct into separate columns
    // INDEX [0, genOffset)| genOffset   | genOffset + numPos | [genOffset + numPos + 1, exploded.getNumberOfColumns)
    // SOME INPUT COLUMNS  | POS COLUMN? | STRUCT(KEY, VALUE) | MORE INPUT COLUMNS
    // scalastyle:on line.size.limit
    val structPos = genOffset + numPos
    withResource(ArrayBuffer.empty[ColumnVector]) { newColumns =>
      (0 until exploded.getNumberOfColumns).foreach { index =>
        if (index == structPos) {
          val kvStructCol = exploded.getColumn(index)
          if (fixChildValidity) {
            // TODO once explode outer is fixed remove the following workaround
            //  https://github.com/rapidsai/cudf/issues/9003
            withResource(kvStructCol.isNull) { isNull =>
              newColumns += withResource(kvStructCol.getChildColumnView(0)) { keyView =>
                withResource(GpuScalar.from(null, kt)) { nullKey =>
                  isNull.ifElse(nullKey, keyView)
                }
              }
              newColumns += withResource(kvStructCol.getChildColumnView(1)) { valueView =>
                withResource(GpuScalar.from(null, vt)) { nullValue =>
                  isNull.ifElse(nullValue, valueView)
                }
              }
            }
          } else {
            newColumns += withResource(kvStructCol.getChildColumnView(0)) { keyView =>
              keyView.copyToColumnVector()
            }
            newColumns += withResource(kvStructCol.getChildColumnView(1)) { valueView =>
              valueView.copyToColumnVector()
            }
          }
        } else {
          newColumns += exploded.getColumn(index).incRefCount()
        }
      }
      new Table(newColumns.toSeq: _*)
    }
  }

  override def fixedLenLazyExpressions: Seq[Expression] = child match {
    // GpuLiteral of ArrayData will be converted to GpuCreateArray with GpuLiterals
    case GpuCreateArray(expressions, _) => expressions
    case GpuAlias(GpuCreateArray(expressions, _), _) => expressions
    case _ => Nil
  }

  override def fixedLenLazyArrayGenerate(inputIterator: Iterator[ColumnarBatch],
      boundLazyProjectList: Seq[Expression],
      boundOthersProjectList: Seq[Expression],
      outputSchema: Array[DataType],
      outer: Boolean,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      opTime: GpuMetric): Iterator[ColumnarBatch] = {

    val numArrayColumns = boundLazyProjectList.length

    new Iterator[ColumnarBatch] {
      var spillableCurrentBatch: SpillableColumnarBatch = _
      var indexIntoData = 0

      private def closeCurrent(): Unit = if (spillableCurrentBatch != null) {
        spillableCurrentBatch.close()
        spillableCurrentBatch = null
      }

      onTaskCompletion(closeCurrent())

      def fetchNextBatch(): Unit = {
        indexIntoData = 0
        closeCurrent()
        if (inputIterator.hasNext) {
          spillableCurrentBatch = SpillableColumnarBatch(inputIterator.next(),
            SpillPriorities.ACTIVE_BATCHING_PRIORITY)
        }
      }

      override def hasNext: Boolean = {
        if (spillableCurrentBatch == null || indexIntoData >= numArrayColumns) {
          fetchNextBatch()
        }
        spillableCurrentBatch != null
      }

      override def next(): ColumnarBatch = {
        NvtxIdWithMetrics(NvtxRegistry.GPU_GENERATE_EXEC, opTime) {
          val boundExprs = if (position) {
            boundOthersProjectList ++ Seq(GpuLiteral.create(indexIntoData, IntegerType),
              boundLazyProjectList(indexIntoData))
          } else {
            boundOthersProjectList :+ boundLazyProjectList(indexIntoData)
          }
          // Do projection with bound expressions
          val projectCb =
            GpuProjectExec.projectWithRetrySingleBatch(spillableCurrentBatch, boundExprs)
          indexIntoData += 1
          numOutputBatches += 1
          numOutputRows += projectCb.numRows()
          projectCb
        }
      }
    }
  }
}

case class GpuExplode(child: Expression) extends GpuExplodeBase {
  override val position: Boolean = false
}

case class GpuPosExplode(child: Expression) extends GpuExplodeBase {
  override def position: Boolean = true
}

case class GpuGenerateExec(
    generator: GpuGenerator,
    requiredChildOutput: Seq[Attribute],
    outer: Boolean,
    generatorOutput: Seq[Attribute],
    child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    CPU_BRIDGE_PROCESSING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_PROCESSING_TIME),
    CPU_BRIDGE_WAIT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_WAIT_TIME)
  )

  override def output: Seq[Attribute] = requiredChildOutput ++ generatorOutput

  override def producedAttributes: AttributeSet = AttributeSet(generatorOutput)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override protected val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val cbTargetSize = new RapidsConf(conf).gpuTargetBatchSizeBytes

    generator.fixedLenLazyExpressions match {
      // If lazy expressions can be extracted from generator,
      // perform optimized lazy generation via `generator.fixedLenLazyArrayGenerate`
      case expressions if expressions.nonEmpty =>
        val boundLazyProjectList =
          GpuBindReferences.bindGpuReferences(expressions, child.output, allMetrics).toArray
        val boundOthersProjectList =
          GpuBindReferences.bindGpuReferences(requiredChildOutput, child.output, allMetrics).toArray
        val outputSchema = output.map(_.dataType).toArray

        child.executeColumnar().mapPartitions { iter =>
          generator.fixedLenLazyArrayGenerate(iter,
            boundLazyProjectList,
            boundOthersProjectList,
            outputSchema,
            outer,
            numOutputRows,
            numOutputBatches,
            opTime)
        }

      // Otherwise, perform common generation via `generator.generate`
      case _ =>
        val genProjectList: Seq[GpuExpression] =
          GpuBindReferences.bindGpuReferences(generator.children, child.output, allMetrics)
        val othersProjectList: Seq[GpuExpression] =
          GpuBindReferences.bindGpuReferences(requiredChildOutput, child.output, allMetrics)

        child.executeColumnar().flatMap { inputFromChild =>
          doGenerateAndClose(inputFromChild, genProjectList, othersProjectList, cbTargetSize,
            numOutputRows, numOutputBatches, opTime)
        }
    }
  }

  private def doGenerateAndClose(input: ColumnarBatch,
      genProjectList: Seq[GpuExpression],
      othersProjectList: Seq[GpuExpression],
      batchTargetSize: Long,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      opTime: GpuMetric): Iterator[ColumnarBatch] = {
    // Project input columns, setting other columns ahead of generator's input columns.
    // With the projected batches and an offset, generators can extract input columns or
    // other required columns separately.
    val projectedInput = NvtxIdWithMetrics(NvtxRegistry.GPU_GENERATE_PROJECT_SPLIT, opTime) {
      GpuProjectExec.projectAndCloseWithRetrySingleBatch(
        SpillableColumnarBatch(input, SpillPriorities.ACTIVE_ON_DECK_PRIORITY),
        othersProjectList ++ genProjectList)
    }
    val splits = GpuGenerateUtils.getSplitsWithRetryAndClose(projectedInput, generator,
      othersProjectList.length, outer, batchTargetSize, opTime)
    new GpuGenerateIterator(splits, generator, othersProjectList.length, outer,
      numOutputRows, numOutputBatches, opTime)
  }
}

object GpuGenerateUtils {
  /**
   * Split up the input batch to call generate on each split.
   * (Move it out of the GpuGenerateExec class for unit tests)
   */
  private[rapids] def getSplitsWithRetryAndClose(cb: ColumnarBatch, generator: GpuGenerator,
      generatorOffset: Int, outer: Boolean, targetSize: Long,
      opTime: GpuMetric): Iterator[Array[SpillableColumnarBatch]] = {
    val spillableBatch = SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
    withRetry(spillableBatch, splitSpillableInHalfByRows) { attempt =>
      NvtxIdWithMetrics(NvtxRegistry.GPU_GENERATE_PROJECT_SPLIT, opTime) {
        // compute split indices of input batch
        withResource(attempt.getColumnarBatch()) { attemptCB =>
          val splitIndices = generator.inputSplitIndices(attemptCB,
            generatorOffset, outer, targetSize)
          // split up input batch with indices
          makeSplits(attemptCB, splitIndices)
        }
      }
    }
  }

  // Split a ColumnarBatch according to a list of indices.
  // Returns a list of spillable batches corresponding to the splits.
  private def makeSplits(input: ColumnarBatch,
      splitIndices: Array[Int]): Array[SpillableColumnarBatch] = {
    val schema = GpuColumnVector.extractTypes(input)

    if (splitIndices.isEmpty) {
      // The caller will close input, so we increment here to offset.
      Array(SpillableColumnarBatch(GpuColumnVector.incRefCounts(input),
        SpillPriorities.ACTIVE_BATCHING_PRIORITY))
    } else {
      val splitInput = withResource(GpuColumnVector.from(input)) { table =>
        table.contiguousSplit(splitIndices: _*)
      }
      closeOnExcept(splitInput) { _ =>
        splitInput.zipWithIndex.safeMap { case (ct, i) =>
          splitInput(i) = null
          SpillableColumnarBatch(ct, schema,
            SpillPriorities.ACTIVE_BATCHING_PRIORITY)
        }
      }
    }
  }
}

class BatchToGenerate(val fixUpOffset: Long, val spillable: SpillableColumnarBatch)
  extends AutoCloseable {
  override def close(): Unit = {
    spillable.close()
  }

  override def toString: String =
    s"BatchToGenerate fixUpOffset:$fixUpOffset, spillable:$spillable"
}

class GpuGenerateIterator(
    inputs: Iterator[Array[SpillableColumnarBatch]],
    generator: GpuGenerator,
    generatorOffset: Int,
    outer: Boolean,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] {
  private var generateIter: Iterator[ColumnarBatch] = Iterator.empty

  override def hasNext: Boolean = generateIter.hasNext || {
    // generateIter is empty, try to get the next bundle of batches
    if (inputs.hasNext) {
      val scbs = inputs.next()
      generateIter = generator.generate(
        safeIteratorFromSeq(scbs.toSeq), generatorOffset, outer)
    }
    generateIter.hasNext
  }

  override def next(): ColumnarBatch = {
    if (!this.hasNext) {
      throw new NoSuchElementException("no more batches")
    }
    NvtxIdWithMetrics(NvtxRegistry.GPU_GENERATE_ITERATOR, opTime) {
      val cb = generateIter.next()
      numOutputBatches += 1
      numOutputRows += cb.numRows()
      cb
    }
  }
}
