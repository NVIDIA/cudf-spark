/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package org.apache.spark.util.random.rapids

import org.apache.spark.util.random.XORShiftRandom

/** RAPIDS version of the Spark XORShiftRandom providing access to the internal seed. */
class RapidsXORShiftRandom(init: Long) extends XORShiftRandom(init) {

  private var curSeed = XORShiftRandom.hashSeed(init)

  // Only override "next", since it will be called by other nextXXX.
  override protected def next(bits: Int): Int = {
    var nextSeed = curSeed ^ (curSeed << 21)
    nextSeed ^= (nextSeed >>> 35)
    nextSeed ^= (nextSeed << 4)
    curSeed = nextSeed
    (nextSeed & ((1L << bits) - 1)).asInstanceOf[Int]
  }

  override def setSeed(s: Long): Unit = {
    curSeed = XORShiftRandom.hashSeed(s)
  }

  /* Set the hashed seed directly. (Not thread-safe) */
  def setHashedSeed(hashedSeed: Long): Unit = {
    curSeed = hashedSeed
  }

  /* Get the current internal seed. (Not thread-safe) */
  def currentSeed: Long = curSeed
}
