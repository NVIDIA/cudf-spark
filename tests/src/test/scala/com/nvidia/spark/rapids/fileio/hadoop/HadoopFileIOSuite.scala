/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.fileio.hadoop

import com.nvidia.spark.rapids.jni.fileio.RapidsOutputFile
import org.scalatest.funsuite.AnyFunSuite

class HadoopFileIOSuite extends AnyFunSuite {
  test("PerfIO output accepts only exact s3 and s3a schemes") {
    Seq("s3", "s3a", "S3", "S3A").foreach { scheme =>
      assert(HadoopFileIO.isPerfIOS3OutputScheme(scheme))
    }
    Seq(null, "s3n", "s3mock", "hdfs").foreach { scheme =>
      assert(!HadoopFileIO.isPerfIOS3OutputScheme(scheme))
    }
  }

  test("newOutputFile preserves its covariant JVM method descriptor") {
    val returnTypes = classOf[HadoopFileIO].getDeclaredMethods
      .filter(method => method.getName == "newOutputFile" &&
        method.getParameterCount == 1 && method.getParameterTypes.head == classOf[String])
      .map(_.getReturnType)
      .toSet

    assert(returnTypes.contains(classOf[HadoopOutputFile]))
    assert(returnTypes.contains(classOf[RapidsOutputFile]))
    assert(classOf[HadoopOutputFile].isAssignableFrom(classOf[PerfIOOutputFile]))
  }
}
