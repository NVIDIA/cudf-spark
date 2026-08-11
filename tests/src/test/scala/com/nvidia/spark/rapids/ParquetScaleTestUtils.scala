/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
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

import org.apache.spark.sql.tests.datagen.{DataGen, GeneratorFunction, LocationToSeedMapping, RowLocation}

case class NonNaNFloatGenFunc(mapping: LocationToSeedMapping = null) extends GeneratorFunction {
  override def apply(rowLoc: RowLocation): Any = {
    val v = java.lang.Float.intBitsToFloat(DataGen.getRandomFor(mapping(rowLoc)).nextInt())
    if (v.isNaN) {
      1.toFloat // just use 1.0
    } else {
      v
    }
  }

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    NonNaNFloatGenFunc(mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalStateException("value ranges are not supported for Float yet")
}

case class NonNaNDoubleGenFunc(mapping: LocationToSeedMapping = null) extends GeneratorFunction {
  override def apply(rowLoc: RowLocation): Any = {
    val v = java.lang.Double.longBitsToDouble(DataGen.nextLong(rowLoc, mapping))
    if (v.isNaN) {
      1.toDouble // just use 1.0
    } else {
      v
    }
  }

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    NonNaNDoubleGenFunc(mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalStateException("value ranges are not supported for Double yet")
}
