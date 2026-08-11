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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}

import org.apache.spark.serializer.KryoRegistrator

class GpuKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    val allClassesToRegister = Seq(
      "org.apache.spark.sql.rapids.execution.SerializeConcatHostBuffersDeserializeBatch",
      "org.apache.spark.sql.rapids.execution.SerializeBatchDeserializeHostBuffer")
    allClassesToRegister.foreach { classToRegister =>
      kryo.register(ShimReflectionUtils.loadClass(classToRegister), new KryoJavaSerializer())
    }
  }
}
