/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.types.{DataType, StringType}

object GpuV1WriteUtils {

  /** A function that converts the empty string to null for partition values. */
  case class GpuEmpty2Null(child: Expression) extends GpuUnaryExpression {
    override def nullable: Boolean = true

    override def doColumnar(input: GpuColumnVector): ColumnVector = {
      withResource(ColumnVector.fromStrings("")) { from =>
        withResource(ColumnVector.fromStrings(null)) { to =>
          input.getBase.findAndReplaceAll(from, to)
        }
      }
    }

    override def dataType: DataType = StringType
  }

  def convertGpuEmptyToNull(
      output: Seq[Attribute],
      partitionSet: AttributeSet): List[NamedExpression] = {
    var needConvert = false
    val projectList: List[NamedExpression] = output.map {
      case p if partitionSet.contains(p) && p.dataType == StringType && p.nullable =>
        needConvert = true
        GpuAlias(GpuEmpty2Null(p), p.name)()
      case other => other
    }.toList // Force list to avoid recursive Java serialization of lazy list Seq implementation

    if (needConvert) projectList else Nil
  }

  def hasGpuEmptyToNull(expressions: Seq[Expression]): Boolean = {
    expressions.exists(_.find(_.isInstanceOf[GpuEmpty2Null]).isDefined)
  }
}
