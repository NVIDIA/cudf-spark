/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.shims.WindowGroupLimitFilterMatcher.filterIsAtLeastAsRestrictive
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{Add, And, AttributeReference, CaseWhen, Cast, Coalesce, EqualTo, ExprId, GreaterThan, GreaterThanOrEqual, If, In, LessThan, LessThanOrEqual, Literal, Not, Or}
import org.apache.spark.sql.types.{IntegerType, LongType}

/**
 * Unit tests for WindowGroupLimitFilterMatcher.
 *
 * These tests verify the filter matching logic used to determine if a filter
 * on a rank column is at least as restrictive as a WindowGroupLimit.
 *
 * IMPORTANT: This tests edge cases that Spark's optimizer would never generate,
 * but which could cause correctness issues if someone modifies the code incorrectly.
 *
 * The key invariant being tested:
 * - For the optimization to be safe, the filter must be AT LEAST AS RESTRICTIVE
 *   as the WindowGroupLimit (keeping same or fewer rows)
 *
 * Supported patterns (matching Spark's InferWindowGroupLimit.extractLimits):
 * - rank <= n: safe if n <= limit
 * - rank < n: safe if n-1 <= limit (since rank < n equals rank <= n-1)
 * - rank = n: safe if n <= limit (keeps only rank n)
 * - n >= rank: same as rank <= n
 * - n > rank: same as rank < n
 * - n = rank: same as rank = n
 * - AND conditions: extract all limits, use minimum
 *
 * Examples with limit = 4:
 *   - rank <= 3: keeps 1,2,3 -- MORE restrictive, safe
 *   - rank <= 4: keeps 1,2,3,4 -- same as limit, safe
 *   - rank <= 5: keeps 1,2,3,4,5 -- LESS restrictive, NOT safe
 *   - rank < 5: keeps 1,2,3,4 (same as rank <= 4), safe
 *   - rank < 6: keeps 1,2,3,4,5 -- LESS restrictive, NOT safe
 *   - rank = 4: keeps only 4 -- MORE restrictive, safe
 *   - rank = 5: keeps only 5 -- LESS restrictive, NOT safe
 *   - rank > 3 AND rank <= 5: uses min(5) = 5, NOT safe since 5 > 4
 *   - rank >= 1 AND rank <= 4: uses min(4) = 4, safe since 4 <= 4
 */
class WindowGroupLimitExecSuite extends AnyFunSuite {

  // Create a unique expression ID for testing
  private val testExprId = 42L

  // Create an AttributeReference with a specific expression ID for testing
  private def rankAttrWithId(id: Long): AttributeReference = {
    AttributeReference("rnk", IntegerType, nullable = false)(exprId = ExprId(id))
  }

  // ============================================================================
  // Tests for LessThanOrEqual (rank <= n)
  // ============================================================================

  test("rank <= n where n < limit: MORE restrictive, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThanOrEqual(rankAttr, Literal(3))
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank <= 3 with limit=4: filter is MORE restrictive (keeps fewer rows), should match")
  }

  test("rank <= n where n == limit: SAME restrictiveness, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThanOrEqual(rankAttr, Literal(4))
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank <= 4 with limit=4: filter is SAME as limit, should match")
  }

  test("rank <= n where n > limit: LESS restrictive, should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThanOrEqual(rankAttr, Literal(5))
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank <= 5 with limit=4: filter is LESS restrictive (keeps more rows), should NOT match")
  }

  // ============================================================================
  // Tests for LessThan (rank < n)
  // ============================================================================

  test("rank < n where n == limit + 1: equivalent to rank <= limit, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThan(rankAttr, Literal(5))
    val limit = 4
    
    // rank < 5 is equivalent to rank <= 4
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank < 5 with limit=4: equivalent to rank <= 4, should match")
  }

  test("rank < n where n < limit + 1: MORE restrictive, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThan(rankAttr, Literal(4))
    val limit = 4
    
    // rank < 4 is equivalent to rank <= 3, which is MORE restrictive than limit=4
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank < 4 with limit=4: equivalent to rank <= 3, MORE restrictive, should match")
  }

  test("rank < n where n > limit + 1: LESS restrictive, should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThan(rankAttr, Literal(6))
    val limit = 4
    
    // rank < 6 is equivalent to rank <= 5, which is LESS restrictive than limit=4
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank < 6 with limit=4: equivalent to rank <= 5, LESS restrictive, should NOT match")
  }

  test("rank < n where n == 1: keeps no rows for positive ranks, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThan(rankAttr, Literal(1))
    val limit = 4
    
    // rank < 1 keeps nothing (ranks are positive), definitely MORE restrictive
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank < 1 with limit=4: keeps no positive ranks, should match")
  }

  // ============================================================================
  // Tests for GreaterThanOrEqual (n >= rank, same as rank <= n)
  // ============================================================================

  test("n >= rank where n < limit: MORE restrictive, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = GreaterThanOrEqual(Literal(3), rankAttr)
    val limit = 4
    
    // 3 >= rank is equivalent to rank <= 3
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "3 >= rank with limit=4: equivalent to rank <= 3, MORE restrictive, should match")
  }

  test("n >= rank where n == limit: SAME restrictiveness, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = GreaterThanOrEqual(Literal(4), rankAttr)
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "4 >= rank with limit=4: equivalent to rank <= 4, should match")
  }

  test("n >= rank where n > limit: LESS restrictive, should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = GreaterThanOrEqual(Literal(5), rankAttr)
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "5 >= rank with limit=4: equivalent to rank <= 5, LESS restrictive, should NOT match")
  }

  // ============================================================================
  // Tests for GreaterThan (n > rank, same as rank < n)
  // ============================================================================

  test("n > rank where n == limit + 1: equivalent to rank < n = rank <= limit, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = GreaterThan(Literal(5), rankAttr)
    val limit = 4
    
    // 5 > rank is equivalent to rank < 5 = rank <= 4
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "5 > rank with limit=4: equivalent to rank <= 4, should match")
  }

  test("n > rank where n < limit + 1: MORE restrictive, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = GreaterThan(Literal(3), rankAttr)
    val limit = 4
    
    // 3 > rank is equivalent to rank < 3 = rank <= 2
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "3 > rank with limit=4: equivalent to rank <= 2, MORE restrictive, should match")
  }

  test("n > rank where n > limit + 1: LESS restrictive, should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = GreaterThan(Literal(6), rankAttr)
    val limit = 4
    
    // 6 > rank is equivalent to rank < 6 = rank <= 5
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "6 > rank with limit=4: equivalent to rank <= 5, LESS restrictive, should NOT match")
  }

  // ============================================================================
  // Tests for wrong expression ID (filter on different column)
  // ============================================================================

  test("filter on different column should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val differentExprId = 999L
    val condition = LessThanOrEqual(rankAttr, Literal(4))
    
    assert(!filterIsAtLeastAsRestrictive(condition, differentExprId, 4),
      "Filter on different column than the rank column should NOT match")
  }

  // ============================================================================
  // Tests for edge cases with Long literals
  // ============================================================================

  test("rank <= Long literal that equals limit: should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThanOrEqual(rankAttr, Literal(4L))
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank <= 4L with limit=4: should match (Long literal converted to Int)")
  }

  test("rank <= Long literal greater than limit: should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThanOrEqual(rankAttr, Literal(5L))
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank <= 5L with limit=4: should NOT match")
  }

  // ============================================================================
  // Tests for EqualTo (rank = n)
  // Following Spark's InferWindowGroupLimit which supports: rn = 5 and 5 = rn
  // ============================================================================

  test("rank = n where n < limit: MORE restrictive, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = EqualTo(rankAttr, Literal(3))
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank = 3 with limit=4: keeps only rank 3, MORE restrictive, should match")
  }

  test("rank = n where n == limit: SAME restrictiveness, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = EqualTo(rankAttr, Literal(4))
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank = 4 with limit=4: keeps only rank 4, same as limit, should match")
  }

  test("rank = n where n > limit: LESS restrictive, should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = EqualTo(rankAttr, Literal(5))
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank = 5 with limit=4: filter would keep rank 5, but limit only keeps 1-4, NOT safe")
  }

  test("n = rank where n < limit: MORE restrictive, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    // Reversed operand order: 3 = rank
    val condition = EqualTo(Literal(3), rankAttr)
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "3 = rank with limit=4: same as rank = 3, should match")
  }

  test("n = rank where n == limit: SAME restrictiveness, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = EqualTo(Literal(4), rankAttr)
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "4 = rank with limit=4: should match")
  }

  test("n = rank where n > limit: LESS restrictive, should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = EqualTo(Literal(5), rankAttr)
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "5 = rank with limit=4: should NOT match")
  }

  test("rank = 1 with limit = 1: should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = EqualTo(rankAttr, Literal(1))
    val limit = 1
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank = 1 with limit=1: should match")
  }

  // ============================================================================
  // Boundary tests
  // ============================================================================

  test("limit = 1 with rank <= 1: should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThanOrEqual(rankAttr, Literal(1))
    val limit = 1
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank <= 1 with limit=1: should match")
  }

  test("limit = 1 with rank <= 2: should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThanOrEqual(rankAttr, Literal(2))
    val limit = 1
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank <= 2 with limit=1: LESS restrictive, should NOT match")
  }

  test("limit = 1 with rank < 2: should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThan(rankAttr, Literal(2))
    val limit = 1
    
    // rank < 2 is equivalent to rank <= 1
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank < 2 with limit=1: equivalent to rank <= 1, should match")
  }

  test("limit = 1 with rank < 3: should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = LessThan(rankAttr, Literal(3))
    val limit = 1
    
    // rank < 3 is equivalent to rank <= 2, which is LESS restrictive than limit=1
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank < 3 with limit=1: equivalent to rank <= 2, LESS restrictive, should NOT match")
  }

  // ============================================================================
  // Complex condition tests - These MUST return false to be conservative
  // The implementation must only optimize simple, provable patterns.
  // ============================================================================

  test("OR condition: rank < 5 OR rank == 5 should NOT match (complex condition)") {
    val rankAttr = rankAttrWithId(testExprId)
    // rank < 5 OR rank == 5 is logically equivalent to rank <= 5, but we can't prove it
    val condition = Or(
      LessThan(rankAttr, Literal(5)),
      EqualTo(rankAttr, Literal(5))
    )
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "OR condition should NOT match - we cannot prove the combined condition is restrictive")
  }

  test("OR condition: rank < 5 OR other_col < 5 should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val otherAttr = rankAttrWithId(999L)  // Different column
    // Even though rank < 5 alone would be valid, the OR with another condition
    // means rows could pass even if rank >= 5
    val condition = Or(
      LessThan(rankAttr, Literal(5)),
      LessThan(otherAttr, Literal(5))
    )
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "OR with different column should NOT match - rows with rank >= 5 could pass")
  }

  // ============================================================================
  // Tests for AND conditions
  // Following Spark's InferWindowGroupLimit which uses splitConjunctivePredicates
  // and takes the minimum limit from all rank-limiting predicates
  // ============================================================================

  test("AND condition: rank < 5 AND other_condition: extracts limit from rank, should match") {
    val rankAttr = rankAttrWithId(testExprId)
    val otherAttr = rankAttrWithId(999L)
    // rank < 5 gives effective limit 4, the other condition is ignored
    val condition = And(
      LessThan(rankAttr, Literal(5)),
      LessThan(otherAttr, Literal(10))
    )
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank < 5 AND other: extracts limit 4 from rank < 5, should match")
  }

  test("AND condition: rank <= 3 AND rank <= 5: uses minimum (3), should match with limit=4") {
    val rankAttr = rankAttrWithId(testExprId)
    // Both predicates limit rank: min(3, 5) = 3, which is <= 4
    val condition = And(
      LessThanOrEqual(rankAttr, Literal(3)),
      LessThanOrEqual(rankAttr, Literal(5))
    )
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank <= 3 AND rank <= 5: uses min=3, should match with limit=4")
  }

  test("AND condition: rank <= 5 AND rank <= 6: uses minimum (5), should NOT match with limit=4") {
    val rankAttr = rankAttrWithId(testExprId)
    // min(5, 6) = 5, which is > 4
    val condition = And(
      LessThanOrEqual(rankAttr, Literal(5)),
      LessThanOrEqual(rankAttr, Literal(6))
    )
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank <= 5 AND rank <= 6: uses min=5, should NOT match with limit=4")
  }

  test("AND condition: rank > 2 AND rank <= 4: uses only rank <= 4 (limit 4), should match") {
    val rankAttr = rankAttrWithId(testExprId)
    // rank > 2 doesn't contribute a limit (it's a lower bound)
    // rank <= 4 contributes limit 4
    val condition = And(
      GreaterThan(rankAttr, Literal(2)),
      LessThanOrEqual(rankAttr, Literal(4))
    )
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank > 2 AND rank <= 4: only rank <= 4 contributes limit, should match")
  }

  test("AND condition: rank > 2 AND rank <= 5: uses limit 5, should NOT match with limit=4") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = And(
      GreaterThan(rankAttr, Literal(2)),
      LessThanOrEqual(rankAttr, Literal(5))
    )
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank > 2 AND rank <= 5: uses limit 5, should NOT match with limit=4")
  }

  test("AND condition: rank = 3 AND other_col > 0: uses limit 3, should match with limit=4") {
    val rankAttr = rankAttrWithId(testExprId)
    val otherAttr = rankAttrWithId(999L)
    val condition = And(
      EqualTo(rankAttr, Literal(3)),
      GreaterThan(otherAttr, Literal(0))
    )
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank = 3 AND other: extracts limit 3 from rank = 3, should match")
  }

  test("AND condition: rank = 5 AND other_col > 0: uses limit 5, should NOT match with limit=4") {
    val rankAttr = rankAttrWithId(testExprId)
    val otherAttr = rankAttrWithId(999L)
    val condition = And(
      EqualTo(rankAttr, Literal(5)),
      GreaterThan(otherAttr, Literal(0))
    )
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "rank = 5 AND other: extracts limit 5 from rank = 5, should NOT match")
  }

  test("Triple AND: rank <= 10 AND rank < 6 AND rank <= 4: uses minimum (4), should match") {
    val rankAttr = rankAttrWithId(testExprId)
    // limits: 10, 5 (from rank < 6), 4 => min = 4
    val condition = And(
      And(
        LessThanOrEqual(rankAttr, Literal(10)),
        LessThan(rankAttr, Literal(6))
      ),
      LessThanOrEqual(rankAttr, Literal(4))
    )
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "Triple AND with min=4, should match with limit=4")
  }

  test("Nested in If: IF(rank < 5, true, false) should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    // The rank comparison is nested inside an If expression
    val condition = If(
      LessThan(rankAttr, Literal(5)),
      Literal(true),
      Literal(false)
    )
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "Nested condition in If should NOT match - not a direct comparison")
  }

  test("NOT condition: NOT(rank > 4) should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    // NOT(rank > 4) is logically equivalent to rank <= 4, but we don't support NOT
    val condition = Not(
      GreaterThan(rankAttr, Literal(4))
    )
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "NOT condition should NOT match - not a direct comparison")
  }

  test("CASE WHEN with rank should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    // CASE WHEN rank < 5 THEN true ELSE false END
    val condition = CaseWhen(
      Seq((LessThan(rankAttr, Literal(5)), Literal(true))),
      Some(Literal(false))
    )
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "CASE WHEN condition should NOT match - not a direct comparison")
  }

  test("IN condition: rank IN (1, 2, 3, 4) should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    // Even though rank IN (1,2,3,4) is equivalent to rank <= 4, we don't support IN
    val condition = In(
      rankAttr,
      Seq(Literal(1), Literal(2), Literal(3), Literal(4))
    )
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "IN condition should NOT match - not a direct comparison")
  }

  test("BETWEEN condition equivalent: rank >= 1 AND rank <= 4 should match with limit=4") {
    val rankAttr = rankAttrWithId(testExprId)
    // rank >= 1 doesn't contribute a limit (lower bound)
    // rank <= 4 contributes limit 4
    val condition = And(
      GreaterThanOrEqual(rankAttr, Literal(1)),
      LessThanOrEqual(rankAttr, Literal(4))
    )
    val limit = 4
    
    assert(filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "BETWEEN 1 AND 4: uses limit 4 from rank <= 4, should match with limit=4")
  }

  test("BETWEEN condition: rank >= 1 AND rank <= 5 should NOT match with limit=4") {
    val rankAttr = rankAttrWithId(testExprId)
    val condition = And(
      GreaterThanOrEqual(rankAttr, Literal(1)),
      LessThanOrEqual(rankAttr, Literal(5))
    )
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "BETWEEN 1 AND 5: uses limit 5 from rank <= 5, should NOT match with limit=4")
  }

  test("Arithmetic on rank: (rank + 1) <= 5 should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    // Even though (rank + 1) <= 5 means rank <= 4, we don't support arithmetic
    val rankPlusOne = Add(rankAttr, Literal(1))
    val condition = LessThanOrEqual(rankPlusOne, Literal(5))
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "Arithmetic on rank should NOT match - rank must be directly compared")
  }

  test("Comparison with non-literal: rank <= other_col should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val otherAttr = rankAttrWithId(999L)
    // We can't prove anything when comparing to a non-literal value
    val condition = LessThanOrEqual(rankAttr, otherAttr)
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "Comparison with non-literal should NOT match - can't determine restrictiveness")
  }

  test("Coalesce around rank: COALESCE(rank, 0) <= 4 should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val coalesced = Coalesce(
      Seq(rankAttr, Literal(0))
    )
    val condition = LessThanOrEqual(coalesced, Literal(4))
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "Coalesce around rank should NOT match - rank must be directly compared")
  }

  test("Cast on rank: CAST(rank AS BIGINT) <= 4 should NOT match") {
    val rankAttr = rankAttrWithId(testExprId)
    val casted = Cast(
      rankAttr, 
      LongType
    )
    val condition = LessThanOrEqual(casted, Literal(4L))
    val limit = 4
    
    assert(!filterIsAtLeastAsRestrictive(condition, testExprId, limit),
      "Cast on rank should NOT match - rank must be directly compared")
  }

}
