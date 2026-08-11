/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.AssertUtils.assertInTests
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite to verify that AssertUtils correctly detects test environments
 * via the com.nvidia.spark.rapids.runningTests system property.
 */
class AssertUtilsVerificationSuite extends AnyFunSuite {

  test("assertInTests should execute in test environment") {
    // This should NOT throw an assertion error when running as a test
    var executed = false
    assertInTests({
      executed = true
      true
    }, "This assertion should execute in test mode")
    
    // Verify the assertion body was actually executed
    assert(executed, "assertInTests body should have been executed in test environment")
  }

  test("assertInTests should catch false conditions") {
    // This SHOULD throw an assertion error
    assertThrows[AssertionError] {
      assertInTests(false, "This should throw")
    }
  }

  test("assertInTests with message should include message in error") {
    val caught = intercept[AssertionError] {
      assertInTests(false, "custom error message")
    }
    assert(caught.getMessage.contains("custom error message"))
  }

  test("verify test environment is correctly detected") {
    // Verify the property that gates test assertions is set
    val runningTests = System.getProperty("com.nvidia.spark.rapids.runningTests")
    assert(runningTests == "true", 
      s"com.nvidia.spark.rapids.runningTests property should be 'true' but was: $runningTests")
  }

  test("verify iterator assertion pattern works") {
    // Simulate the problematic iterator pattern
    val iter = Iterator(1, 2, 3)
    
    // This should NOT consume the iterator in production,
    // but SHOULD verify it has elements in test mode
    assertInTests(iter.hasNext, "Iterator should have elements")
    
    // Iterator should still have all elements
    assert(iter.next() == 1)
    assert(iter.next() == 2)
    assert(iter.next() == 3)
  }

  test("verify expensive operation pattern works") {
    var expensiveOpCount = 0
    
    def expensiveOperation(): Boolean = {
      expensiveOpCount += 1
      true
    }
    
    // This should call the expensive operation in tests
    assertInTests(expensiveOperation())
    
    // Verify it was called
    assert(expensiveOpCount == 1, "Expensive operation should be called in test mode")
  }

  test("verify collection traversal pattern works") {
    val largeCollection = (1 to 1000).toSeq
    
    // This should traverse the collection, and fail at the last element
    assertThrows[AssertionError] {
      assertInTests(largeCollection.forall(i => i > 0 && i < 1000), 
        "All elements should be positive and less than 1000")
    }
    
    // This should traverse the collection, and not fail
    assertInTests(largeCollection.forall(i => i > 0), "All elements should be positive")
    
    // No exception thrown means it worked
  }
}

