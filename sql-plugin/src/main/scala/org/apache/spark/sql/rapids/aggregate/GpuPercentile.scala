/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import ai.rapids.cudf.{DType, GroupByAggregation, ReductionAggregation}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.RapidsPluginImplicits.ReallyAGpuExpression
import com.nvidia.spark.rapids.jni.Histogram
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression,  UnsafeArrayData, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

case class CudfHistogram(override val dataType: DataType) extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (input: cudf.ColumnVector) => input.reduce(ReductionAggregation.histogram(), DType.LIST)
  override lazy val groupByAggregate: GroupByAggregation = GroupByAggregation.histogram()
  override val name: String = "CudfHistogram"
}

case class CudfMergeHistogram(override val dataType: DataType)
  extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (input: cudf.ColumnVector) =>
      input.getType match {
        // This is called from updateAggregate in GpuPercentileWithFrequency.
        case DType.STRUCT => input.reduce(ReductionAggregation.mergeHistogram(), DType.LIST)

        // This is always called from mergeAggregate.
        case DType.LIST => withResource(input.getChildColumnView(0)) { histogram =>
          histogram.reduce(ReductionAggregation.mergeHistogram(), DType.LIST)
        }

        case _ => throw new IllegalStateException("Invalid input in CudfMergeHistogram.")
      }

  override lazy val groupByAggregate: GroupByAggregation = GroupByAggregation.mergeHistogram()
  override val name: String = "CudfMergeHistogram"
}

/**
 * Perform the final evaluation step to compute percentiles from histograms.
 */
case class GpuPercentileEvaluation(childExpr: Expression, percentage: Either[Double, Array[Double]],
                                   outputType: DataType, isReduction: Boolean)
  extends GpuExpression with ShimExpression {
  override def dataType: DataType = outputType
  override def prettyName: String = "percentile_evaluation"
  override def nullable: Boolean = true
  override def children: Seq[Expression] = Seq(childExpr)

  private lazy val percentageArray = percentage match {
    case Left(p) => Array(p)
    case Right(p) => p
  }
  private lazy val outputAsList = outputType match {
    case _: ArrayType => true
    case _ => false
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(childExpr.columnarEval(batch)) { histograms =>
      val percentiles = Histogram.percentileFromHistogram(histograms.getBase,
        percentageArray, outputAsList)
      GpuColumnVector.from(percentiles, outputType)
    }
  }
}

abstract class GpuPercentile(childExpr: Expression, percentageLit: GpuLiteral,
                             isReduction: Boolean)
  extends GpuAggregateFunction with Serializable {
  protected lazy val histogramBufferType: DataType =
    ArrayType(StructType(Seq(StructField("value", childExpr.dataType),
                             StructField("frequency", LongType))),
      containsNull = false)
  protected lazy val histogramBuffer: AttributeReference =
    AttributeReference("histogramBuff", histogramBufferType)()
  override def aggBufferAttributes: Seq[AttributeReference] = histogramBuffer :: Nil

  override lazy val initialValues: Seq[Expression] =
    Seq(GpuLiteral.create(new GenericArrayData(Array.empty[Any]), histogramBufferType))
  override lazy val mergeAggregates: Seq[CudfAggregate] =
    Seq(CudfMergeHistogram(histogramBufferType))
  override lazy val evaluateExpression: Expression =
    GpuPercentileEvaluation(histogramBuffer, percentages, outputType, isReduction)

  override def dataType: DataType = histogramBufferType
  override def prettyName: String = "percentile"
  override def nullable: Boolean = true
  override def children: Seq[Expression] = Seq(childExpr, percentageLit)

  private lazy val (returnPercentileArray, percentages): (Boolean, Either[Double, Array[Double]]) =
    percentageLit.value match {
      case null => (false, Right(Array()))
      case num: Double => (false, Left(num))
      case arrayData: ArrayData => (true, Right(arrayData.toDoubleArray()))
      case other => throw new IllegalStateException(s"Invalid percentage expression: $other")
    }
  private lazy val outputType: DataType =
    if (returnPercentileArray) ArrayType(DoubleType, containsNull = false) else DoubleType
}

/**
 * Compute percentiles from just the input values.
 */
case class GpuPercentileDefault(childExpr: Expression, percentage: GpuLiteral,
                                isReduction: Boolean)
  extends GpuPercentile(childExpr, percentage, isReduction) {
  override lazy val inputProjection: Seq[Expression] = Seq(childExpr)
  override lazy val updateAggregates: Seq[CudfAggregate] = Seq(CudfHistogram(histogramBufferType))
}

/**
 * Compute percentiles from the input values associated with frequencies.
 */
case class GpuPercentileWithFrequency(childExpr: Expression, percentage: GpuLiteral,
                                      frequencyExpr: Expression, isReduction: Boolean)
  extends GpuPercentile(childExpr, percentage, isReduction) {
  override lazy val inputProjection: Seq[Expression] = {
    val outputType: DataType = if(isReduction) {
      StructType(Seq(StructField("value", childExpr.dataType), StructField("frequency", LongType)))
    } else {
      histogramBufferType
    }
    Seq(GpuCreateHistogramIfValid(childExpr, frequencyExpr, isReduction, outputType))
  }
  override lazy val updateAggregates: Seq[CudfAggregate] =
    Seq(CudfMergeHistogram(histogramBufferType))
}

/**
 * Create a histogram buffer from the input values and frequencies.
 *
 * The frequencies are also checked to ensure that they are non-negative. If a negative frequency
 * exists, an exception will be thrown.
 */
case class GpuCreateHistogramIfValid(valuesExpr: Expression, frequenciesExpr: Expression,
                                     isReduction: Boolean, outputType: DataType)
  extends GpuExpression with ShimExpression {
  override def dataType: DataType = outputType
  override def prettyName: String = "create_histogram_if_valid"
  override def nullable: Boolean = false
  override def children: Seq[Expression] = Seq(valuesExpr, frequenciesExpr)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(valuesExpr.columnarEval(batch)) { values =>
      withResourceIfAllowed(frequenciesExpr.columnarEval(batch)) { frequencies =>
        // If a negative frequency exists, an exception will be thrown from here.
        val histograms = Histogram.createHistogramIfValid(values.getBase, frequencies.getBase,
          /*outputAsLists = */ !isReduction)
        GpuColumnVector.from(histograms, outputType)
      }
    }
  }
}

object GpuPercentile{
  def apply(childExpr: Expression, percentageLit: GpuLiteral, frequencyExpr: Expression,
            isReduction: Boolean): GpuPercentile = {
    frequencyExpr match {
      case GpuLiteral(freq, LongType) if freq == 1 =>
        GpuPercentileDefault(childExpr, percentageLit, isReduction)
      case _  =>
        GpuPercentileWithFrequency(childExpr, percentageLit, frequencyExpr, isReduction)
    }
  }
}

/**
 * Convert the incoming byte stream received from Spark CPU into internal histogram buffer format.
 */
case class CpuToGpuPercentileBufferConverter(elementType: DataType)
  extends CpuToGpuAggregateBufferConverter {
  override def createExpression(child: Expression): CpuToGpuBufferTransition =
    CpuToGpuPercentileBufferTransition(child, elementType)
}

case class CpuToGpuPercentileBufferTransition(override val child: Expression, elementType: DataType)
  extends CpuToGpuBufferTransition {
  override def dataType: DataType =
    ArrayType(StructType(Seq(StructField("value", elementType),
      StructField("frequency", LongType))),
      containsNull = false)

  // Deserialization from the input byte stream into the internal histogram buffer format.
  override protected def nullSafeEval(input: Any): ArrayData = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(bis)

    // Store a column of STRUCT<element, count>
    val histogram = ArrayBuffer[InternalRow]()
    val row = new UnsafeRow(2)

    try {
      var sizeOfNextRow = ins.readInt()
      while (sizeOfNextRow >= 0) {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        row.pointTo(bs, sizeOfNextRow)
        val element = row.get(0, elementType)
        val count = row.get(1, LongType).asInstanceOf[Long]
        histogram.append(InternalRow.apply(element, count))
        sizeOfNextRow = ins.readInt()
      }
      ArrayData.toArrayData(histogram)
    } finally {
      ins.close()
      bis.close()
    }
  }
}

/**
 * Convert the internal histogram buffer into a byte stream that can be deserialized by Spark CPU.
 */
case class GpuToCpuPercentileBufferConverter(elementType: DataType)
  extends GpuToCpuAggregateBufferConverter {
  override def createExpression(child: Expression): GpuToCpuBufferTransition =
    GpuToCpuPercentileBufferTransition(child, elementType)
}

case class GpuToCpuPercentileBufferTransition(override val child: Expression, elementType: DataType)
  extends GpuToCpuBufferTransition {
  // Serialization the internal histogram buffer into a byte array.
  override protected def nullSafeEval(input: Any): Array[Byte] = {
    val buffer = new Array[Byte](4 << 10) // 4K
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)

    val histogram = input.asInstanceOf[UnsafeArrayData]
    val projection = UnsafeProjection.create(Array[DataType](elementType, LongType))

    try {
      (0 until histogram.numElements()).foreach { i =>
        val row = histogram.getStruct(i, 2)
        val element = row.get(0, elementType)
        // The internal histogram buffer may contain null elements.
        // We need to skip them as the Spark CPU does not process nulls after
        // the updateAggregates step.
        if(element!= null) {
          val unsafeRow = projection.apply(row)
          out.writeInt(unsafeRow.getSizeInBytes)
          unsafeRow.writeToStream(out, buffer)
        }
      }
      // Need to write a negative integer to indicate the end of the stream.
      out.writeInt(-1)
      out.flush()
      bos.toByteArray
    } finally {
      out.close()
      bos.close()
    }
  }
}
