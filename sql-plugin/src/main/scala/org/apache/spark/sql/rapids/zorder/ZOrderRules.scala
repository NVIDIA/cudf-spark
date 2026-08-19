/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.zorder

import com.nvidia.spark.rapids.{ExprChecks, ExprMeta, ExprRule, GpuExpression, GpuMetric, GpuRangePartitioner, GpuSorter, RepeatingParamCheck, ShimReflectionUtils, TypeSig, UnaryExprMeta}
import com.nvidia.spark.rapids.GpuOverrides.{expr, pluginSupportedOrderableSig}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, SortOrder, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering

/**
 * These are rules to be able to replace zorder operators. For now they all come from deltalake,
 * but not all versions of deltalake support zordering so the simplest solution is to pick them
 * apart with reflection.  That way if they exist on the classpath we can replace them.
 */
object ZOrderRules {

  private[this] def readFieldWithReflection[T](instance: T, fieldName: String): Any = {
    val clazz = instance.getClass
    val field = clazz.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(instance)
  }

  def partExprRule(partExprClass: Class[_ <: Expression]): ExprRule[_ <: Expression] =
    expr[UnaryExpression](
      "Partitioning for zorder (normalize a column)",
      ExprChecks.unaryProject(
        TypeSig.INT,
        TypeSig.INT,
        // Same as RangePartitioningExec
        pluginSupportedOrderableSig,
        TypeSig.orderable),
      (a, conf, p, r) => new UnaryExprMeta[UnaryExpression](a, conf, p, r) {
        def getBoundsNOrder: (Array[InternalRow], SortOrder) = {
          // We have to pull pick apart the partitioner apart, but we have to use reflection
          // to do it, because what we want is private
          val parter = readFieldWithReflection(a, "partitioner")
          val rangeBounds = readFieldWithReflection(parter, "rangeBounds")
              .asInstanceOf[Array[InternalRow]]
          val ordering = readFieldWithReflection(parter, "ordering")
              .asInstanceOf[LazilyGeneratedOrdering]

          val singleOrder = ordering.ordering.head
          (rangeBounds, singleOrder)
        }

        override def tagExprForGpu(): Unit = {
          try {
            // Run this first to be sure that we can get what we need to convert
            getBoundsNOrder
          } catch {
            case _: NoSuchFieldError | _: SecurityException | _: IllegalAccessException =>
              willNotWorkOnGpu("The version of the partitioner does not have " +
                  "the expected fields in it.")
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = {
          val (rangeBounds, singleOrder) = getBoundsNOrder
          // We need to create a GpuSorter, but where we are at will not have a schema/etc
          // so we need to make one up. We know the type and that there will only ever be
          // one column, so
          val sortCol = AttributeReference("sort_col", child.dataType, child.nullable)()
          val ourSortOrder =
            SortOrder(sortCol, singleOrder.direction, singleOrder.nullOrdering, Seq.empty)
          val sorter = new GpuSorter(Seq(ourSortOrder), Array(sortCol),
            Map.empty[String, GpuMetric])
          val gpuPart = new GpuRangePartitioner(rangeBounds, sorter)
          GpuPartitionerExpr(child, gpuPart)
        }
      })

  def openSourceExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    try {
      val interleaveClazz =
        ShimReflectionUtils.loadClass("org.apache.spark.sql.delta.expressions.InterleaveBits")
            .asInstanceOf[Class[Expression]]
      val interleaveRule = expr[Expression](
        "Interleave bit as a part of deltalake zorder",
        ExprChecks.projectOnly(
          TypeSig.BINARY,
          TypeSig.BINARY,
          repeatingParamCheck =
            Some(RepeatingParamCheck("input",
              TypeSig.INT,
              TypeSig.INT))),
        (a, conf, p, r) => new ExprMeta[Expression](a, conf, p, r) {
          override def convertToGpuImpl(): GpuExpression =
            GpuInterleaveBits(childExprs.map(_.convertToGpu()))
        })

      val partExprClass =
        ShimReflectionUtils.loadClass("org.apache.spark.sql.delta.expressions.PartitionerExpr")
            .asInstanceOf[Class[Expression]]
      val partRule = partExprRule(partExprClass)

      Map(interleaveClazz -> interleaveRule,
        partExprClass -> partRule)
    } catch {
      case _: ClassNotFoundException =>
        Map.empty
    }
  }

  def databricksExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    try {
      val hilbertClazz =
        ShimReflectionUtils.loadClass("com.databricks.sql.expressions.HilbertLongIndex")
            .asInstanceOf[Class[Expression]]
      val hilbertRule = expr[Expression](
        "Hilbert long index as a part of Databrick's deltalake zorder",
        ExprChecks.projectOnly(
          TypeSig.LONG,
          TypeSig.LONG,
          repeatingParamCheck =
            Some(RepeatingParamCheck("input",
              TypeSig.INT,
              TypeSig.INT))),
        (a, conf, p, r) => new ExprMeta[Expression](a, conf, p, r) {
          override def convertToGpuImpl(): GpuExpression = {
            val numBitsField = hilbertClazz.getDeclaredField("numBits")
            numBitsField.setAccessible(true)
            val numBits = numBitsField.get(a).asInstanceOf[java.lang.Integer]
            GpuHilbertLongIndex(numBits, childExprs.map(_.convertToGpu()))
          }
        })

      val partExprClass =
        ShimReflectionUtils.loadClass("com.databricks.sql.expressions.PartitionerExpr")
            .asInstanceOf[Class[Expression]]
      val partRule = partExprRule(partExprClass)
      Map(hilbertClazz -> hilbertRule,
        partExprClass -> partRule)
    } catch {
      case _: ClassNotFoundException =>
        Map.empty
    }
  }

  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] =
    openSourceExprs ++ databricksExprs
}
