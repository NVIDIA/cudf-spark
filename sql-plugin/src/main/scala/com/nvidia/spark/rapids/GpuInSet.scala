/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, DType, Scalar}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DoubleType, FloatType}

case class GpuInSet(
    child: Expression,
    list: Seq[Any]) extends GpuUnaryExpression with Predicate {
  require(list != null, "list should not be null")

  @transient private[this] lazy val hasNull: Boolean = list.contains(null)

  private val checkNaN: Boolean = child.dataType match {
    case DoubleType | FloatType => true
    case _ => false
  }
  private val legacyNullInEmptyBehavior: Boolean = GpuInSet.isLegacyNullInEmptyBehavior

  override def nullable: Boolean = child.nullable || hasNull

  override def doColumnar(haystack: GpuColumnVector): ColumnVector = {
    if (list.isEmpty && !legacyNullInEmptyBehavior) {
      // Follow the CPU behavior: (https://issues.apache.org/jira/browse/SPARK-44550)
      withResource(Scalar.fromBool(false)) { falseS =>
        ColumnVector.fromScalar(falseS, haystack.getRowCount.toInt)
      }
    } else {
      val ret = withResource(buildNeedles)(haystack.getBase.contains)
      if (hasNull) {
        // replace all the "false" rows (excluding rows for "NaN") with "null"s
        // to follow the CPU behavior.
        val toReplaceFalseFlags = closeOnExcept(ret) { _ =>
          withResource(Scalar.fromBool(false)) { falseS =>
            val falseFlags = ret.equalTo(falseS)
            if (checkNaN) {
              withResource(falseFlags) { _ =>
                withResource(haystack.getBase.isNotNan) { isNotNaNFlags =>
                  falseFlags.and(isNotNaNFlags)
                }
              }
            } else {
              falseFlags
            }
          }
        }

        withResource(ret) { _ =>
          withResource(toReplaceFalseFlags) { _ =>
            withResource(Scalar.fromNull(DType.BOOL8)) { nullS =>
              toReplaceFalseFlags.ifElse(nullS, ret)
            }
          }
        }
      } else {
        ret
      } // end of "if (hasNull)"
    }
  } // end of "doColumnar"

  private def buildNeedles: ColumnVector =
    GpuScalar.columnVectorFromLiterals(list, child.dataType)

  override def toString: String = {
    val listString = list
        .map(elem => Literal(elem, child.dataType).toString)
        // Sort elements for deterministic behaviours
        .sorted
        .mkString(", ")
    s"$child INSET $listString"
  }

  override def sql: String = {
    val valueSQL = child.sql
    val listSQL = list
        .map(elem => Literal(elem, child.dataType).sql)
        // Sort elements for deterministic behaviours
        .sorted
        .mkString(", ")
    s"($valueSQL IN ($listSQL))"
  }
}

object GpuInSet {
  private def isLegacyNullInEmptyBehavior: Boolean = {
    try {
      // Use reflection to determine if this conf is defined to avoid the
      // complicated shim things for both GpuInSet and GpuInSubqueryExec.
      SQLConf.getClass.getMethod("LEGACY_NULL_IN_EMPTY_LIST_BEHAVIOR")
      // Spark 3.5.0 or later, since this conf is defined, so get the value from it.
      SQLConf.get.getConfString("spark.sql.legacy.nullInEmptyListBehavior", "true").toBoolean
    } catch {
      case _: NoSuchMethodException =>
        // Spark before 3.5.0, no this conf, so always returns true
        true
    }
  }
}
