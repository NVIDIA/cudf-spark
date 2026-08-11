/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.Arm._
import org.scalatest.funsuite.AnyFunSuite

class ArmSuite extends AnyFunSuite {
  class TestResource extends AutoCloseable {
    var closed = false

    override def close(): Unit = {
      closed = true
    }
  }

  class TestException extends RuntimeException

  class TestCloseException extends RuntimeException

  class TestThrowingResource extends AutoCloseable {
    override def close(): Unit = {
      throw new TestCloseException
    }
  }

  test("withResource for Option[AutoCloseable]") {
    val resource = Some(new TestResource)
    withResource(resource) { r =>
      assertResult(resource)(r)
      assertResult(false)(r.get.closed)
    }
    assert(resource.get.closed)

    val resource1 = None
    // test no exception
    withResource(resource1) { r =>
      assertResult(resource1)(r)
    }
    assert(resource1.isEmpty)
  }

  test("closeOnExcept single instance") {
    val resource = new TestResource
    closeOnExcept(resource) { r => assertResult(resource)(r) }
    assertResult(false)(resource.closed)
    try {
      closeOnExcept(resource) { _ => throw new TestException }
    } catch {
      case _: TestException =>
    }
    assert(resource.closed)
  }

  test("closeOnExcept sequence") {
    val resources = new Array[TestResource](3)
    resources(0) = new TestResource
    resources(2) = new TestResource
    closeOnExcept(resources) { r => assertResult(resources)(r) }
    assert(resources.forall(r => Option(r).forall(!_.closed)))
    try {
      closeOnExcept(resources) { _ => throw new TestException }
    } catch {
      case _: TestException =>
    }
    assert(resources.forall(r => Option(r).forall(_.closed)))
  }

  test("closeOnExcept arraybuffer") {
    val resources = new ArrayBuffer[TestResource]
    closeOnExcept(resources) { r =>
      r += new TestResource
      r += null
      r += new TestResource
    }
    assertResult(3)(resources.length)
    assert(resources.forall(r => Option(r).forall(!_.closed)))
    try {
      closeOnExcept(resources) { r =>
        r += new TestResource
        throw new TestException
      }
    } catch {
      case _: TestException =>
    }
    assertResult(4)(resources.length)
    assert(resources.forall(r => Option(r).forall(_.closed)))
  }

  test("closeOnExcept suppression single instance") {
    val resource = new TestThrowingResource
    try {
      closeOnExcept(resource) { _ => throw new TestException }
    } catch {
      case e: TestException =>
        assertResult(1)(e.getSuppressed.length)
        assert(e.getSuppressed.head.isInstanceOf[TestCloseException])
    }
  }

  test("closeOnExcept suppression sequence") {
    val resources = new Array[TestThrowingResource](3)
    resources(0) = new TestThrowingResource
    resources(2) = new TestThrowingResource
    try {
      closeOnExcept(resources) { _ => throw new TestException }
    } catch {
      case e: TestException =>
        assertResult(2)(e.getSuppressed.length)
        assert(e.getSuppressed.forall(_.isInstanceOf[TestCloseException]))
    }
  }

  test("closeOnExcept suppression arraybuffer") {
    val resources = new ArrayBuffer[TestThrowingResource]
    closeOnExcept(resources) { r =>
      r += new TestThrowingResource
      r += null
      r += new TestThrowingResource
    }
    assertResult(3)(resources.length)
    try {
      closeOnExcept(resources) { r =>
        r += new TestThrowingResource
        throw new TestException
      }
    } catch {
      case e: TestException =>
        assertResult(3)(e.getSuppressed.length)
        assert(e.getSuppressed.forall(_.isInstanceOf[TestCloseException]))
    }
  }
}
