/*
 * Copyright (c) 2019-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.udf

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

case class GpuScalaUDFLogical(udf: ScalaUDF) extends ShimExpression with Logging {
  override def nullable: Boolean = udf.nullable

  override def eval(input: InternalRow): Any = {
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    null
  }

  override def dataType: DataType = udf.dataType

  override def children: Seq[Expression] = udf.children

  def compile(isTestEnabled: Boolean): Expression = {
    // call the compiler
    try {
      val expr = CatalystExpressionBuilder(udf.function).compile(udf.children)
      if (expr.isDefined) {
        expr.get
      } else {
        udf
      }
    } catch {
      case e: SparkException =>
        val udfName = udf.udfName.getOrElse("<unknown>")
        logDebug(s"UDF $udfName compilation failure: $e")
        if (isTestEnabled) {
          throw e
        }
        udf
      case NonFatal(e) =>
        val udfName = udf.udfName.getOrElse("<unknown>")
        logWarning(s"Unable to translate UDF $udfName: $e")
        udf
    }
  }
}
