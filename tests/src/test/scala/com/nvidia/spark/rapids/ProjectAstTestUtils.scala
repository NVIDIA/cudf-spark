/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.expressions.Expression

object ProjectAstTestUtils {
  def collectExpressions[T <: Expression : ClassTag](
      expressions: Seq[Expression]): Seq[T] = {
    val runtimeClass = implicitly[ClassTag[T]].runtimeClass
    expressions.flatMap(_.collect {
      case expression if runtimeClass.isInstance(expression) => expression.asInstanceOf[T]
    })
  }

  def tierReferences(expression: Expression): Seq[GpuBoundReference] = {
    expression.collect {
      case reference: GpuBoundReference if reference.name.startsWith("tiered_input_") => reference
    }
  }
}
