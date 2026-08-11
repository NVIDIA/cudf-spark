/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
import scala.reflect.ClassTag

/**
 * Just a simple wrapper to make working with buffers of AutoClosable things play
 * nicely with withResource.
 */
class AutoClosableArrayBuffer[T <: AutoCloseable] extends AutoCloseable {
  val data = new ArrayBuffer[T]()

  def append(scb: T): Unit = data.append(scb)

  def last: T = data.last

  def removeLast(): T = data.remove(data.length - 1)

  def foreach[U](f: T => U): Unit = data.foreach(f)

  def map[U](f: T => U): Seq[U] = data.map(f).toSeq

  def toArray[B >: T : ClassTag]: Array[B] = data.toArray

  def size(): Int = data.size

  def clear(): Unit = data.clear()

  def forall(p: T => Boolean): Boolean = data.forall(p)

  def iterator: Iterator[T] = data.iterator

  override def toString: String = s"AutoCloseable(${super.toString})"

  override def close(): Unit = {
    data.foreach(_.close())
    data.clear()
  }
}
