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

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer
import scala.util.control.ControlThrowable

import com.nvidia.spark.rapids.RapidsPluginImplicits._

/** Implementation of the automatic-resource-management pattern */
trait ArmScalaSpecificImpl {

  /** Executes the provided code block and then closes the list buffer of resources */
  def withResource[T <: AutoCloseable, V](r: ListBuffer[T])(block: ListBuffer[T] => V): V = {
    try {
      block(r)
    } finally {
      r.safeClose()
    }
  }

  /** Executes the provided code block, closing the resources only if an exception occurs */
  def closeOnExcept[T <: AutoCloseable, V](r: immutable.Seq[T])
      (block: immutable.Seq[T] => V): V = {
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

  def closeOnExcept[T <: AutoCloseable, V](r: mutable.Seq[T])
      (block: mutable.Seq[T] => V): V = {
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
}
