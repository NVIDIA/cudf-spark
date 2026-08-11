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

package org.apache.spark.sql.rapids

import java.util.Optional

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.{BinaryExprMeta, DataFromReplacementRule, GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuLiteral, GpuProjectExec, GpuScalar, GpuUnaryExpression, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.Hash
import com.nvidia.spark.rapids.shims.{HashUtils, NullIntolerantShim, ShimExpression}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Sha2}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuMd5(child: Expression)
  extends GpuUnaryExpression with ImplicitCastInputTypes with NullIntolerantShim {
  override def toString: String = s"md5($child)"
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
  override def dataType: DataType = StringType

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(HashUtils.normalizeInput(input.getBase)) { normalized =>
      withResource(ColumnVector.md5Hash(normalized)) { fullResult =>
        fullResult.mergeAndSetValidity(BinaryOp.BITWISE_AND, normalized)
      }
    }
  }
}

case class GpuSha1(child: Expression)
  extends GpuUnaryExpression with ImplicitCastInputTypes with NullIntolerantShim {
  override def toString: String = s"sha1($child)"
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
  override def dataType: DataType = StringType

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    val normalizedStringCV = withResource(HashUtils.normalizeInput(input.getBase))
    { normalized =>
      // at the moment sha1 on CuDF doesn't support lists, so have to translate
      // BinaryType into StringType first before we operate on it
      withResource(normalized.getChildColumnView(0)) { dataCol =>
        withResource(new ColumnView(DType.STRING, normalized.getRowCount,
          Optional.of[java.lang.Long](normalized.getNullCount),
          dataCol.getData, normalized.getValid, normalized.getOffsets)) { cv =>
          cv.copyToColumnVector()
        }
      }
    }
    withResource(normalizedStringCV) { normalizedStringCV => 
      withResource(ColumnVector.sha1Hash(normalizedStringCV)) { fullResult =>
        // necessary because cudf treats nulls as "" for hashing
        fullResult.mergeAndSetValidity(BinaryOp.BITWISE_AND, normalizedStringCV)
      }
    }
  }
}

/**
 * GPU implementation of the SHA2 hash function set.
 * Note:
 *   - Only bit lengths of 0, 224, 256, 384, and 512 are supported.
 *   - A bit length of 0 is treated as 256, following Spark behaviour.
 *   - For the GPU implementation, the bit length must be provided as a literal.
 *   - For unsupported (including negative) bit lengths, SHA2 returns null.
 *     This differs from the CPU implementation, where the bit length can differ
 *     per row.
 * @see https://spark.apache.org/docs/latest/api/sql/index.html#sha2
 * @param left The BINARY input column to be hashed.
 * @param right The INTEGER literal bitlength for hashing.
 */
case class GpuSha2(left: Expression, right: Expression)
  extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerantShim {
  private val childExpr: Expression = left
  private val bitLengthExpr: Expression = right
  override def toString: String = s"sha2($childExpr, $bitLength)"
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, IntegerType)
  override def dataType: DataType = StringType

  private val bitLength: Option[Int] = bitLengthExpr match {
    case lit: GpuLiteral if lit.value.isInstanceOf[Int] =>
      lit.value.asInstanceOf[Int] match {
        case 224 | 256 | 384 | 512 => Some(lit.value.asInstanceOf[Int])
        case 0 => Some(256) // Spark default
        case _ => None // SHA2 returns null for other bit lengths.
      }
    case _ => None
  }

  override def nullable: Boolean = childExpr.nullable || bitLength.isEmpty

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    if (bitLength.isEmpty) {
      // SHA2 returns null for "unsupported" bit lengths.
      withResource(Scalar.fromNull(DType.STRING)) { nullStringExemplar =>
        ColumnVector.fromScalar(nullStringExemplar, lhs.getRowCount.toInt)
      }
    } else {
      bitLength match {
        case Some(224) => withResource(GpuSha2.getStringViewOfBinaryColumn(lhs.getBase)) {
          Hash.sha224NullsPreserved
        }
        case Some(256) => withResource(GpuSha2.getStringViewOfBinaryColumn(lhs.getBase)) {
          Hash.sha256NullsPreserved
        }
        case Some(384) => withResource(GpuSha2.getStringViewOfBinaryColumn(lhs.getBase)) {
          Hash.sha384NullsPreserved
        }
        case Some(512) => withResource(GpuSha2.getStringViewOfBinaryColumn(lhs.getBase)) {
          Hash.sha512NullsPreserved
        }
        case unexpected =>
          throw new UnsupportedOperationException(s"Unsupported bit length for SHA2: $unexpected")
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) {
      doColumnar(_, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    throw new UnsupportedOperationException("Only literal bit lengths are supported for SHA2")
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    throw new UnsupportedOperationException("Only literal bit lengths are supported for SHA2")
  }
}

object GpuSha2 {

  private def getStringViewOfBinaryColumn(col: ColumnView): ColumnView = {
    withResource(col.getChildColumnView(0)) { dataCol =>
      new ColumnView(DType.STRING, col.getRowCount,
        Optional.of[java.lang.Long](col.getNullCount),
        dataCol.getData, col.getValid, col.getOffsets)
    }
  }

  class Meta(e: Sha2,
              conf: RapidsConf,
              p: Option[RapidsMeta[_, _, _]],
              r: DataFromReplacementRule)
    extends BinaryExprMeta[Sha2](e, conf, p, r) {
    override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = GpuSha2(lhs, rhs)
  }
}

abstract class GpuHashExpression extends GpuExpression with ShimExpression {
  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = false

  private def hasMapType(dt: DataType): Boolean = {
    dt.existsRecursively(_.isInstanceOf[MapType])
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length < 1) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName requires at least one argument")
    } else if (children.exists(child => hasMapType(child.dataType)) &&
      !SQLConf.get.getConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE)) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName cannot contain elements of MapType. In Spark, same maps " +
          "may have different hashcode, thus hash expressions are prohibited on MapType elements." +
          s" To restore previous behavior set ${SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE.key} " +
          "to true.")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }
}

object GpuMurmur3Hash {
  def compute(batch: ColumnarBatch,
      boundExpr: Seq[Expression],
      seed: Int = 42): ColumnVector = {
    withResource(GpuProjectExec.project(batch, boundExpr)) { args =>
      val bases = GpuColumnVector.extractBases(args)
      val normalized = bases.safeMap { cv =>
        HashUtils.normalizeInput(cv).asInstanceOf[ColumnView]
      }
      withResource(normalized) { _ =>
        Hash.murmurHash32(seed, normalized)
      }
    }
  }
}

case class GpuMurmur3Hash(children: Seq[Expression], seed: Int) extends GpuHashExpression {
  override def dataType: DataType = IntegerType

  override def prettyName: String = "hash"

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
    GpuColumnVector.from(GpuMurmur3Hash.compute(batch, children, seed), dataType)
}

case class GpuXxHash64(children: Seq[Expression], seed: Long) extends GpuHashExpression {
  override def dataType: DataType = LongType

  override def prettyName: String = "xxhash64"

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(children.safeMap(_.columnarEval(batch))) { childCols =>
      val cudfCols = childCols.map(_.getBase.asInstanceOf[ColumnView]).toArray
      GpuColumnVector.from(Hash.xxhash64(seed, cudfCols), dataType)
    }
  }
}

object XxHash64Utils {
  // Mirrors Hash.MAX_STACK_DEPTH from spark-rapids-jni (hash.hpp).
  // Duplicated here to avoid triggering JNI native library loading during
  // Driver-side plan conversion (see github.com/NVIDIA/spark-rapids/issues/14184).
  val MAX_STACK_DEPTH: Int = 8

  /**
   * Compute the max stack size that `inputType` will use,
   * refer to the function `check_nested_depth` in src/main/cpp/src/xxhash64.cu
   * in spark-rapids-jni repo.
   * Note:
   * - This should be sync with `check_nested_depth`
   * - Map in cuDF is list of struct
   *
   * @param inputType the input type
   * @return the max stack size that inputType will use for this input type.
   */
  def computeMaxStackSize(inputType: DataType): Int = {
    inputType match {
      case ArrayType(c: StructType, _) => 1 + computeMaxStackSize(c)
      case ArrayType(c, _) => computeMaxStackSize(c)
      case st: StructType =>
        1 + st.map(f => computeMaxStackSize(f.dataType)).max
      case mt: MapType =>
        2 + math.max(computeMaxStackSize(mt.keyType), computeMaxStackSize(mt.valueType))
      case _ => 1 // primitive types
    }
  }
}

case class GpuHiveHash(children: Seq[Expression]) extends GpuHashExpression {
  override def dataType: DataType = IntegerType

  override def prettyName: String = "hive-hash"

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(GpuProjectExec.project(batch, children)) { args =>
      val bases = GpuColumnVector.extractBases(args)
      val normalized = bases.safeMap { cv =>
        HashUtils.normalizeInput(cv).asInstanceOf[ColumnView]
      }
      GpuColumnVector.from(withResource(normalized)(Hash.hiveHash), dataType)
    }
  }
}
