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

package com.nvidia.spark.rapids

import java.io.FileNotFoundException

/**
 * Carries the owning file path across an asynchronous reader boundary.
 *
 * Spark 4.x needs the path to construct `FAILED_READ_FILE.FILE_NOT_EXIST`, but a
 * `Future.get()` otherwise exposes only the reader's `FileNotFoundException`.
 */
object GpuFileNotFoundException {
  private final class WithPath(
      val filePath: String,
      val originalException: FileNotFoundException)
    extends FileNotFoundException(originalException.getMessage) {
    initCause(originalException)
  }

  def apply(filePath: String, error: FileNotFoundException): FileNotFoundException = error match {
    case pathError: WithPath => pathError
    case _ => new WithPath(filePath, error)
  }

  def unapply(error: FileNotFoundException): Option[(String, FileNotFoundException)] = error match {
    case pathError: WithPath => Some((pathError.filePath, pathError.originalException))
    case _ => None
  }
}
