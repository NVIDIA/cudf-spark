/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
/*
 * This is specifically for functions dealing with loading classes via reflection. This
 * class itself should not contain or import any shimmed/parallel world classes so that
 * it can also be called via reflection, like calling getMethod on ShimReflectionUtils.
 */
object ShimReflectionUtils {

  val log = org.slf4j.LoggerFactory.getLogger(getClass().getName().stripSuffix("$"))

  def loadClass(className: String): Class[_] = {
    val loader = ShimLoader.getShimClassLoader()
    log.debug(s"Loading $className using $loader with the parent loader ${loader.getParent}")
    loader.loadClass(className)
  }

  def newInstanceOf[T](className: String): T = {
    instantiateClass(ShimReflectionUtils.loadClass(className)).asInstanceOf[T]
  }

  // avoid cached constructors
  def instantiateClass[T](cls: Class[T]): T = {
    log.debug(s"Instantiate ${cls.getName} using classloader " + cls.getClassLoader)
    cls.getClassLoader match {
      case urcCl: java.net.URLClassLoader =>
        log.debug("urls " + urcCl.getURLs.mkString("\n"))
      case _ =>
    }
    val constructor = cls.getConstructor()
    constructor.newInstance()
  }
}
