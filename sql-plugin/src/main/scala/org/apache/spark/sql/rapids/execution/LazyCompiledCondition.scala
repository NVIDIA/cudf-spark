/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
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
package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.ast.CompiledExpression
import com.nvidia.spark.rapids.{GpuBoundReference, GpuBuildLeft, GpuBuildRight, GpuBuildSide, GpuExpression}

/**
 * Provides lazy, cached AST compilation for join conditions.
 *
 * The key problem this class solves is that AST expressions are compiled with a specific
 * table ordering. When doing hash joins, we want to dynamically choose the build side,
 * but the AST compilation depends on which side is the build side.
 *
 * The cudf join APIs for BuildLeft variants (innerHashJoinBuildLeft, rightOuterHashJoinBuildLeft)
 * swap the table arguments, so they need an AST compiled with right table columns first.
 * The cudf join APIs for BuildRight variants need an AST with left table columns first.
 *
 * This class stores a single condition bound to `leftOutput ++ rightOutput` and lazily
 * transforms it for BuildLeft by rewriting the bound reference ordinals.
 *
 * @param leftRightBoundCondition condition bound to leftOutput ++ rightOutput
 * @param numLeftColumns the number of columns from the left table
 * @param numRightColumns the number of columns from the right table
 */
class LazyCompiledCondition(
    val leftRightBoundCondition: GpuExpression,
    val numLeftColumns: Int,
    val numRightColumns: Int) extends AutoCloseable {

  private val totalColumns = numLeftColumns + numRightColumns

  @volatile private var compiledBuildRight: CompiledExpression = _
  @volatile private var compiledBuildLeft: CompiledExpression = _
  @volatile private var transformedConditionBuildLeft: GpuExpression = _
  
  /**
   * Get the compiled AST for when the right side is the build side.
   *
   * In this configuration, the condition is bound to leftOutput ++ rightOutput, and
   * cudf join APIs receive (leftTable, rightTable). The AST maps:
   * - AST LEFT table references → leftTable (stream side)
   * - AST RIGHT table references → rightTable (build side)
   *
   * @return the compiled AST expression for build-right configuration
   */
  def getForBuildRight: CompiledExpression = {
    if (compiledBuildRight == null) {
      synchronized {
        if (compiledBuildRight == null) {
          // Condition is bound to leftOutput ++ rightOutput
          // numFirstTableColumns = numLeftColumns maps left columns to AST LEFT
          compiledBuildRight = leftRightBoundCondition.convertToAst(numLeftColumns).compile()
        }
      }
    }
    compiledBuildRight
  }

  /**
   * Get the compiled AST for when the left side is the build side.
   *
   * In this configuration, cudf join APIs receive (rightTable, leftTable) due to the swap
   * in BuildLeft variants. We need the AST to map:
   * - AST LEFT table references → rightTable (stream side)  
   * - AST RIGHT table references → leftTable (build side)
   *
   * We achieve this by transforming the bound condition from leftOutput ++ rightOutput
   * to rightOutput ++ leftOutput order by rewriting the bound reference ordinals.
   *
   * @return the compiled AST expression for build-left configuration
   */
  def getForBuildLeft: CompiledExpression = {
    if (compiledBuildLeft == null) {
      synchronized {
        if (compiledBuildLeft == null) {
          // Transform condition from left++right to right++left binding
          if (transformedConditionBuildLeft == null) {
            transformedConditionBuildLeft = transformForBuildLeft(leftRightBoundCondition)
          }
          // numFirstTableColumns = numRightColumns maps right columns to AST LEFT
          compiledBuildLeft = transformedConditionBuildLeft.convertToAst(numRightColumns).compile()
        }
      }
    }
    compiledBuildLeft
  }

  /**
   * Transform a condition bound to leftOutput ++ rightOutput into one bound to
   * rightOutput ++ leftOutput by rewriting the bound reference ordinals.
   *
   * The transformation is: newOrdinal = (ordinal + numRightColumns) % totalColumns
   * - Left column ordinal i (0 <= i < numLeftColumns) becomes numRightColumns + i
   * - Right column ordinal j (numLeftColumns <= j < total) becomes j - numLeftColumns
   */
  private def transformForBuildLeft(expr: GpuExpression): GpuExpression = {
    expr.mapChildren {
      case br: GpuBoundReference =>
        val newOrdinal = (br.ordinal + numRightColumns) % totalColumns
        GpuBoundReference(newOrdinal, br.dataType, br.nullable)(br.exprId, br.name)
      case other: GpuExpression =>
        transformForBuildLeft(other)
      case other =>
        // Non-GpuExpression children (shouldn't happen for bound conditions)
        other
    }.asInstanceOf[GpuExpression]
  }

  /**
   * Get the compiled AST for the specified build side.
   *
   * The build side parameter represents the semantic build side used for determining
   * AST table ordering (e.g., which side is the "build" side for the target join type).
   * This is typically the data movement build side or the cuDF API build side for sub-joins.
   *
   * @param buildSide which side is the semantic build side for AST compilation
   * @return the compiled AST expression for the specified configuration
   */
  def getForBuildSide(buildSide: GpuBuildSide): CompiledExpression = {
    buildSide match {
      case GpuBuildRight => getForBuildRight
      case GpuBuildLeft => getForBuildLeft
    }
  }

  /**
   * Get the number of columns in the first table for the specified build side.
   * This is useful when you need the numFirstTableColumns value for other purposes.
   *
   * @param buildSide which side is the build side
   * @return the number of columns in the first table for AST compilation
   */
  def getNumFirstTableColumns(buildSide: GpuBuildSide): Int = {
    buildSide match {
      case GpuBuildRight => numLeftColumns
      case GpuBuildLeft => numRightColumns
    }
  }

  /**
   * Check if the AST has been compiled for a specific build side.
   * Useful for testing and debugging.
   */
  def isCompiledForBuildRight: Boolean = compiledBuildRight != null
  def isCompiledForBuildLeft: Boolean = compiledBuildLeft != null

  override def close(): Unit = {
    synchronized {
      if (compiledBuildRight != null) {
        compiledBuildRight.close()
        compiledBuildRight = null
      }
      if (compiledBuildLeft != null) {
        compiledBuildLeft.close()
        compiledBuildLeft = null
      }
      transformedConditionBuildLeft = null
    }
  }
}

object LazyCompiledCondition {
  /**
   * Create a LazyCompiledCondition from a bound condition.
   *
   * @param leftRightBoundCondition condition bound to leftOutput ++ rightOutput
   * @param numLeftColumns number of columns from the left side
   * @param numRightColumns number of columns from the right side
   * @return a new LazyCompiledCondition
   */
  def apply(
      leftRightBoundCondition: GpuExpression,
      numLeftColumns: Int,
      numRightColumns: Int): LazyCompiledCondition = {
    new LazyCompiledCondition(leftRightBoundCondition, numLeftColumns, numRightColumns)
  }
}