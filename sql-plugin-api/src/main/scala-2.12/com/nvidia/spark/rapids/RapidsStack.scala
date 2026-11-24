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

import scala.collection.mutable.ArrayStack

class RapidsStack[T] {
  private val realImpl = new ArrayStack[T]()

  def push(elem1: T): Unit = {
    realImpl.push(elem1)
  }

  def pop(): T = {
    realImpl.pop()
  }

  def isEmpty: Boolean = {
    realImpl.isEmpty
  }

  def nonEmpty: Boolean = {
    realImpl.nonEmpty
  }

  def size: Int = {
    realImpl.size
  }

  def toSeq: Seq[T] = {
    realImpl.toSeq
  }

  def clear(): Unit = {
    realImpl.clear()
  }
}
