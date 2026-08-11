/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
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

package com.nvidia.spark.rapids.tests

import java.lang.reflect.Constructor

object DebugRange {
  var color: Option[_] = None
  var constructor: Option[Constructor[_]] = None
  var initialized: Boolean = false

  private def getNvtxRange(name: String): Option[AutoCloseable] = {
    this.synchronized {
      if (!initialized) {
        try {
          val colorCls = Class.forName("ai.rapids.cudf.NvtxColor")
          val cls = Class.forName("ai.rapids.cudf.NvtxRange")
          constructor = Some(cls.getConstructor(classOf[String], colorCls))
          color = Some(colorCls.getEnumConstants().head)
        } catch {
          case e: ClassNotFoundException =>
            System.err.println(s"\nCOULD NOT INITIALIZE NVTX RANGE $e")
        }
        initialized = true
      }
    }
    if (color.isDefined) {
      Some(constructor.get.newInstance(name, color.get.asInstanceOf[Object])
        .asInstanceOf[AutoCloseable])
    } else {
      System.err.println(s"\nCOULD NOT INITIALIZE NVTX RANGE $name")
      None
    }
  }
}

/**
 * Can be used to mark specific ranges in tests, that might show up in a debugger if a compatible
 * collector is found on the classpath (NVTX only for now)
 */
class DebugRange(val name: String) extends AutoCloseable {
  private val wrapped = DebugRange.getNvtxRange(name)

  override def close(): Unit = {
    wrapped.foreach(_.close())
  }
}
