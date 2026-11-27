/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.fileio.iceberg;

import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.jni.fileio.SeekableInputStream;
import org.apache.iceberg.io.InputFile;

import java.io.IOException;
import java.util.Objects;

/**
 * Implementation of {@link RapidsInputFile} using the Iceberg {@link InputFile}.
 * <br/>
 * This class wraps an Iceberg {@link InputFile} and provides methods to get the file length
 * and open a stream for reading.
 */
public class IcebergInputFile implements RapidsInputFile {
  private final InputFile delegate;
  private final long length;

  public IcebergInputFile(InputFile delegate) {
    this(delegate, -1L);
  }

  /**
   * Wrap an input file with its known length, or a negative value if the length is unknown.
   */
  public IcebergInputFile(InputFile delegate, long length) {
    Objects.requireNonNull(delegate, "delegate can't be null");
    this.delegate = delegate;
    this.length = length;
  }

  @Override
  public String path() {
    return delegate.location();
  }

  @Override
  public long getLength() throws IOException {
    return length >= 0 ? length : delegate.getLength();
  }

  @Override
  public SeekableInputStream open() throws IOException {
    return new IcebergInputStream(delegate.newStream());
  }

  /**
   * Returns the underlying Iceberg InputFile delegate.
   *
   * @return the Iceberg InputFile delegate
   */
  public InputFile getDelegate() {
    return delegate;
  }
}
