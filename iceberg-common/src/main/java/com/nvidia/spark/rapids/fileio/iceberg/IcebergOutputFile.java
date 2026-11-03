/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.jni.fileio.RapidsOutputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;
import java.util.Objects;

/**
 * Implementation of {@link RapidsOutputFile} using Iceberg {@link OutputFile}.
 */
public class IcebergOutputFile implements RapidsOutputFile {
  private final OutputFile delegate;

  public IcebergOutputFile(OutputFile delegate) {
    Objects.requireNonNull(delegate, "delegate can't be null");
    this.delegate = delegate;
  }

  @Override
  public IcebergOutputStream create(boolean overwrite) throws IOException {
    if (overwrite) {
      return new IcebergOutputStream(delegate.createOrOverwrite());
    }
    return new IcebergOutputStream(delegate.create());
  }

  @Override
  public String getPath() {
    return delegate.location();
  }
}
