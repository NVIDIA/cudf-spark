/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, FSDataOutputStream, Path}

object FileUtils {
  def createTempFile(
      conf: Configuration,
      pathPrefix: String,
      pathSuffix: String): (FSDataOutputStream, Path) = {
    val fs = new Path(pathPrefix).getFileSystem(conf)
    val rnd = new Random
    var out: FSDataOutputStream = null
    var path: Path = null
    var succeeded = false
    val suffix = if (pathSuffix != null) pathSuffix else ""
    while (!succeeded) {
      path = new Path(pathPrefix + rnd.nextInt(Integer.MAX_VALUE) + suffix)
      if (!fs.exists(path)) {
        scala.util.control.Exception.ignoring(classOf[FileAlreadyExistsException]) {
          out = fs.create(path, false)
          succeeded = true
        }
      }
    }
    (out, path)
  }
}
