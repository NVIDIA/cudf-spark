/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class ImplicitsTestSuite extends AnyFlatSpec with Matchers {
  private class RefCountTest (i: Int, throwOnClose: Boolean) extends AutoCloseable {
    var closeAttempted: Boolean = false
    var refCount: Int = 0
    override def close(): Unit = {
      closeAttempted = true
      refCount = refCount - 1
      if (refCount < 0) {
        throw new Exception(s"close called to many times for $i")
      }
      if (throwOnClose) {
        throw new Exception(s"cannot close $i")
      }
    }
    def incRefCount(): RefCountTest = {
      refCount = refCount + 1
      this
    }
    def leaked(): Boolean = {
      refCount > 0
    }
  }

  it should "handle exceptions within safeMap body" in {
    val resources = (0 until 10).map(new RefCountTest(_, false))

    assertThrows[Throwable] {
      resources.zipWithIndex.safeMap {
        case (res, i) =>
          if (i > 5) {
            throw new Exception("bad! " + i)
          }
          res.incRefCount()
      }
    }
    assert(resources.forall(!_.leaked))
  }

  it should "handle exceptions while closing safeMap" in {
    var threw = false
    val resources = (0 until 10).map(new RefCountTest(_, true))
    try {
      resources.zipWithIndex.safeMap { case (res, i) => {
        if (i > 5) {
          throw new Exception("bad!")
        }
        res.incRefCount()
      }}
    } catch {
      case t: Throwable => {
        threw = true
        assert(t.getSuppressed().length == 5)
      }
    }
    assert(threw)
    assert(resources.forall(!_.leaked))
  }

  it should "handle an error in a safeMap from a ColumnarBatch" in {
    val resources = new ArrayBuffer[RefCountTest]()
    val batch = new ColumnarBatch((0 until 10).map { ix =>
      val scalar = GpuScalar.from(ix, IntegerType)
      val col = try {
        GpuColumnVector.from(scalar, 5, IntegerType)
      } finally {
        scalar.close()
      }
      resources += new RefCountTest(ix, false)
      col
    }.toArray)

    var colIx = 0
    assertThrows[java.lang.Exception] {
      batch.safeMap(_ => {
        if (colIx > 5) {
          throw new Exception("this is going to close my cols")
        }
        val res = resources(colIx)
        colIx = colIx + 1
        res.incRefCount()
      })
    }
    batch.close()
    assert(resources.forall(!_.leaked))
  }

  it should "handle an error while closing in a safeMap from a ColumnarBatch" in {
    val resources = new ArrayBuffer[RefCountTest]()
    val batch = new ColumnarBatch((0 until 10).map { ix => {
      val scalar = GpuScalar.from(ix, IntegerType)
      val col = try {
        GpuColumnVector.from(scalar, 5, IntegerType)
      } finally {
        scalar.close()
      }
      resources += new RefCountTest(ix, true)
      col
    }}.toArray)

    var threw = false
    var colIx = 0
    try {
      batch.safeMap(_ => {
        if (colIx > 5) {
          throw new Exception("this is going to close my cols")
        }
        val res = resources(colIx)
        colIx = colIx + 1
        res.incRefCount()
      })
    } catch {
      case t: Throwable => {
        threw = true
        assert(t.getSuppressed().length == 5)
      }
    }
    batch.close()
    assert(threw)
    assert(resources.forall(!_.leaked))
  }

  it should "safeMap/safeClose handle the success case" in {
    val resources = (0 until 10).map(new RefCountTest(_, false))
    val extraReferences = resources.safeMap(_.incRefCount)
    extraReferences.safeClose()
    assert(resources.forall(!_.leaked))
  }

  it should "handle the successful case from a ColumnarBatch" in {
    val resources = new ArrayBuffer[RefCountTest]()
    val batch = new ColumnarBatch((0 until 10).map { ix => {
      val scalar = GpuScalar.from(ix, IntegerType)
      val col = try {
        GpuColumnVector.from(scalar, 5, IntegerType)
      } finally {
        scalar.close()
      }
      resources += new RefCountTest(ix, false)
      col
    }}.toArray)

    var colIx = 0
    val result = batch.safeMap(_ => {
      val res = resources(colIx)
      colIx = colIx + 1
      res.incRefCount()
    })
    assert(resources.forall(_.refCount == 1))
    batch.close()
    result.safeClose()
    assert(resources.forall(!_.leaked))
  }

  it should "handle safeMap on array" in {
    val resources = (0 until 10).map(new RefCountTest(_, false))

    assertThrows[Throwable] {
      resources.toArray.zipWithIndex.safeMap {
        case (res, i) =>
          if (i > 5) {
            throw new Exception("bad! " + i)
          }
          res.incRefCount()
      }
    }
    assert(resources.forall(!_.leaked))
  }

  it should "handle safeMap in the successful case" in {
    val resources = (0 until 10).map(new RefCountTest(_, false))

    val out = resources.toArray.zipWithIndex.safeMap {
      case (res, _) =>
        res.incRefCount()
    }

    assert(resources.forall(_.refCount == 1))
    out.safeClose()
    assert(resources.forall(!_.leaked))
  }

  it should "safeMap on a lazy sequence (Stream) with errors" in {
    // Not used in the plugin, but illustrates how this works for a lazy sequence
    // a) a new RefCountTest(i) gets produced,
    // b) the body of the safeMap executes then (interleaved with the first map)
    // c) if the body of the safeMap throws, it cleans at that point.
    // d) the safeMap stops executing in case of error, but the first map goes until the end of
    //    the stream
    val resources: Stream[RefCountTest] = (0 until 10).toStream.map(i => new RefCountTest(i, false))

    assertThrows[Throwable] {
      resources.zipWithIndex.safeMap {
        case (x, ix) => {
          if (ix > 5) {
            throw new Exception("bad! " + ix)
          }
          x.incRefCount()
        }
      }
    }
    assert(resources.forall(!_.leaked))
  }
}

