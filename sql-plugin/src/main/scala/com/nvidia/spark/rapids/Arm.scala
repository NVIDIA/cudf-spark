/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.ControlThrowable

import com.nvidia.spark.rapids.RapidsPluginImplicits._

/** Implementation of the automatic-resource-management pattern */
object Arm extends ArmScalaSpecificImpl {

  /** Executes the provided code block and then closes the resource */
  def withResource[T <: AutoCloseable, V](r: T)(block: T => V): V = {
    try {
      block(r)
    } finally {
      r.safeClose()
    }
  }

  /** Executes the provided code block and then closes the Option[resource] */
  def withResource[T <: AutoCloseable, V](r: Option[T])(block: Option[T] => V): V = {
    try {
      block(r)
    } finally {
      r.foreach(_.safeClose())
    }
  }

  /** Executes the provided code block and then closes the sequence of resources */
  def withResource[T <: AutoCloseable, V](r: Seq[T])(block: Seq[T] => V): V = {
    try {
      block(r)
    } finally {
      r.safeClose()
    }
  }

  /** Executes the provided code block and then closes the array of resources */
  def withResource[T <: AutoCloseable, V](r: Array[T])(block: Array[T] => V): V = {
    try {
      block(r)
    } finally {
      r.safeClose()
    }
  }

  /** Executes the provided code block and then closes the array buffer of resources */
  def withResource[T <: AutoCloseable, V](r: ArrayBuffer[T])(block: ArrayBuffer[T] => V): V = {
    try {
      block(r)
    } finally {
      r.safeClose()
    }
  }

  /** Executes the provided code block and then closes the queue of resources */
  def withResource[T <: AutoCloseable, V](r: mutable.Queue[T])(block: mutable.Queue[T] => V): V = {
    try {
      block(r)
    } finally {
      r.safeClose()
    }
  }

  /** Executes the provided code block and then closes the value if it is AutoCloseable */
  def withResourceIfAllowed[T, V](r: T)(block: T => V): V = {
    try {
      block(r)
    } finally {
      r match {
        case c: AutoCloseable => c.safeClose()
        case scala.util.Left(c: AutoCloseable) => c.safeClose()
        case scala.util.Right(c: AutoCloseable) => c.safeClose()
        case _ => //NOOP
      }
    }
  }

  /** Executes the provided code block, closing the resource only if an exception occurs */
  def closeOnExcept[T <: AutoCloseable, V](r: T)(block: T => V): V = {
    try {
      block(r)
    } catch {
      case t: ControlThrowable =>
        // Don't close for these cases..
        throw t
      case t: Throwable =>
        r.safeClose(t)
        throw t
    }
  }

  /** Executes the provided code block, closing the resources only if an exception occurs */
  def closeOnExcept[T <: AutoCloseable, V](r: Array[T])(block: Array[T] => V): V = {
    try {
      block(r)
    } catch {
      case t: ControlThrowable =>
        // Don't close for these cases..
        throw t
      case t: Throwable =>
        r.safeClose(t)
        throw t
    }
  }

  /** Executes the provided code block, closing the resources only if an exception occurs */
  def closeOnExcept[T <: AutoCloseable, V](r: ArrayBuffer[T])(block: ArrayBuffer[T] => V): V = {
    try {
      block(r)
    } catch {
      case t: ControlThrowable =>
        // Don't close for these cases..
        throw t
      case t: Throwable =>
        r.safeClose(t)
        throw t
    }
  }

  /** Executes the provided code block, closing the resources only if an exception occurs */
  def closeOnExcept[T <: AutoCloseable, V](r: ListBuffer[T])(block: ListBuffer[T] => V): V = {
    try {
      block(r)
    } catch {
      case t: ControlThrowable =>
        // Don't close for these cases..
        throw t
      case t: Throwable =>
        r.safeClose(t)
        throw t
    }
  }


  /** Executes the provided code block, closing the resources only if an exception occurs */
  def closeOnExcept[T <: AutoCloseable, V](r: mutable.Queue[T])(block: mutable.Queue[T] => V): V = {
    try {
      block(r)
    } catch {
      case t: ControlThrowable =>
        // Don't close for these cases..
        throw t
      case t: Throwable =>
        r.safeClose(t)
        throw t
    }
  }

  /** Executes the provided code block, closing the resources only if an exception occurs */
  def closeOnExcept[T <: AutoCloseable, V](r: Option[T])(block: Option[T] => V): V = {
    try {
      block(r)
    } catch {
      case t: ControlThrowable =>
        // Don't close for these cases..
        throw t
      case t: Throwable =>
        r.foreach(_.safeClose(t))
        throw t
    }
  }

  /** Executes the provided code block and then closes the resource */
  def withResource[T <: AutoCloseable, V](h: CloseableHolder[T])
      (block: CloseableHolder[T] => V): V = {
    try {
      block(h)
    } finally {
      h.close()
    }
  }
}

class CloseableHolder[T <: AutoCloseable](var t: T) {
  def setAndCloseOld(newT: T): Unit = {
    val oldT = t
    t = newT
    oldT.close()
  }

  def get: T = t

  def close(): Unit = t.close()
}
