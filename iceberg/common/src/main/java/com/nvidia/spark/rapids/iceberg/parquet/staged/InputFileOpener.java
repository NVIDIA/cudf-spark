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

package com.nvidia.spark.rapids.iceberg.parquet.staged;

import java.io.IOException;

import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;

/**
 * Opens an independent input handle for one staged file source.
 *
 * <p>The staged reader can invoke this method from any of its I/O worker threads. Each call
 * must therefore return a handle that can be used independently of handles returned by other
 * calls. The caller owns the streams opened from the returned {@link RapidsInputFile}; this
 * interface does not itself transfer ownership of a shared file-system or catalog object.</p>
 */
@FunctionalInterface
public interface InputFileOpener {
  /**
   * Opens a file handle for the calling worker.
   *
   * @return an independently usable RAPIDS input file
   * @throws IOException if the source cannot be opened
   */
  RapidsInputFile open() throws IOException;
}
