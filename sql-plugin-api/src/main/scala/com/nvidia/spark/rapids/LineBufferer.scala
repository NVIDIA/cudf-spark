/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

/**
 * We read text files in one line at a time from Spark. This provides an abstraction in how those
 * lines are buffered before being processed on the GPU.
 */
trait LineBufferer extends AutoCloseable {
  /**
   * Get the current size of the data that has been buffered.
   */
  def getLength: Long

  /**
   * Get the numnber of lines currently added to this that were not filtered out.
   */
  def getNumLines: Int

  def hasData: Boolean = getLength != 0

  /**
   * Add a new line of bytes to the data to process.
   */
  def add(line: Array[Byte], offset: Int, len: Int): Unit

  def isWhiteSpace(b: Byte): Boolean = {
    b == ' ' || b == '\t' || b == '\r' || b == '\n'
  }

  def isEmpty(line: Array[Byte], lineOffset: Int, lineLen: Int): Boolean = {
    var idx = 0
    while (idx < lineLen && isWhiteSpace(line(lineOffset + idx))) {
      idx += 1
    }
    idx >= lineLen
  }
}

/**
 * Factory to create a LineBufferer instance that can be used to buffer lines being read in.
 */
trait LineBuffererFactory[BUFF <: LineBufferer] {
  def createBufferer(estimatedSize: Long, lineSeparatorInRead: Array[Byte]): BUFF
}
