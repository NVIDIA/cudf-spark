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

package com.nvidia.spark.rapids.tests.delta;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Test-only local file system that records which thread creates each deletion vector file. Next
 * to every {@code deletion_vector_*.bin} it creates an empty marker named
 * {@code _dv_writer_<thread name>_<file name>}. The files are written by executors, which may be
 * separate JVMs, so the marker is stored in the table directory, where a test can list it.
 */
public class DvWriterThreadRecordingFileSystem extends RawLocalFileSystem {
  public static final String DV_FILE_PREFIX = "deletion_vector_";
  public static final String MARKER_PREFIX = "_dv_writer_";

  private void recordWriter(Path file) throws IOException {
    if (file.getName().startsWith(DV_FILE_PREFIX)) {
      Path marker = new Path(file.getParent(),
          MARKER_PREFIX + Thread.currentThread().getName() + "_" + file.getName());
      super.create(marker, true, 4096, (short) 1, 4096L, null).close();
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    recordWriter(f);
    return super.create(f, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress)
      throws IOException {
    recordWriter(f);
    return super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }
}
