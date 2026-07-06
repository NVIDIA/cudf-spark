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

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.HostAlloc$;
import org.apache.spark.SparkEnv;
import scala.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Selects memory or executor-local disk for an exact-sized staged Parquet output.
 *
 * <p>The allocation is deliberately non-blocking: a successful
 * {@link com.nvidia.spark.rapids.HostAlloc} allocation chooses host memory, while failure chooses
 * disk instead of waiting for other task buffers to spill. Local files include task-attempt and
 * subtask identifiers for diagnostics but also use the JDK's random suffix for collision safety.
 * The returned output owns the allocation or local file.</p>
 */
public final class StagedParquetOutputFactory {
  private static final String SPARK_LOCAL_DIR = "spark.local.dir";
  private static final String SPARK_LOCAL_DIRS_ENV = "SPARK_LOCAL_DIRS";
  private static final AtomicInteger NEXT_LOCAL_DIRECTORY = new AtomicInteger();

  private StagedParquetOutputFactory() {
  }

  /**
   * Create storage for one planned synthetic Parquet file.
   *
   * @param exactSizeBytes exact planned capacity in bytes
   * @param taskAttemptId Spark task-attempt identifier used in a fallback filename
   * @param subtaskId staged-read subtask identifier used in a fallback filename
   * @return an owned writable output
   * @throws IOException if host allocation fails and no local temporary file can be created
   */
  public static StagedParquetOutput create(
      long exactSizeBytes,
      long taskAttemptId,
      long subtaskId) throws IOException {
    if (exactSizeBytes <= 0) {
      throw new IllegalArgumentException("exactSizeBytes must be positive: " + exactSizeBytes);
    }

    Option<HostMemoryBuffer> allocation =
        HostAlloc$.MODULE$.tryAlloc(exactSizeBytes, true);
    if (allocation.isDefined()) {
      return new MemoryStagedParquetOutput(allocation.get(), exactSizeBytes);
    }

    Path localFile = createLocalFile(taskAttemptId, subtaskId);
    return new FileStagedParquetOutput(localFile, exactSizeBytes);
  }

  private static Path createLocalFile(long taskAttemptId, long subtaskId) throws IOException {
    String prefix = "rapids-iceberg-staged-" + taskAttemptId + "-" + subtaskId + "-";
    IOException failure = null;
    List<Path> directories = candidateDirectories();
    if (directories.isEmpty()) {
      throw new IOException("no Spark or JVM local directory is configured");
    }
    int start = Math.floorMod(NEXT_LOCAL_DIRECTORY.getAndIncrement(), directories.size());
    for (int index = 0; index < directories.size(); index++) {
      Path directory = directories.get((start + index) % directories.size());
      try {
        Files.createDirectories(directory);
        return Files.createTempFile(directory, prefix, ".parquet");
      } catch (IOException | RuntimeException e) {
        IOException current = e instanceof IOException
            ? (IOException) e
            : new IOException("invalid Spark local directory " + directory, e);
        if (failure == null) {
          failure = current;
        } else {
          failure.addSuppressed(current);
        }
      }
    }
    throw new IOException("unable to create a local staged Parquet file", failure);
  }

  private static List<Path> candidateDirectories() {
    Set<String> candidates = new LinkedHashSet<>();
    String environmentDirectories = System.getenv(SPARK_LOCAL_DIRS_ENV);
    addDirectories(candidates, environmentDirectories);

    SparkEnv sparkEnv = SparkEnv.get();
    if (sparkEnv != null) {
      addDirectories(candidates,
          sparkEnv.conf().get(SPARK_LOCAL_DIR, System.getProperty("java.io.tmpdir")));
    }
    addDirectories(candidates, System.getProperty("java.io.tmpdir"));

    List<Path> paths = new ArrayList<>(candidates.size());
    for (String candidate : candidates) {
      paths.add(Paths.get(candidate));
    }
    return paths;
  }

  private static void addDirectories(Set<String> candidates, String directories) {
    if (directories == null) {
      return;
    }
    for (String candidate : directories.split(",")) {
      String trimmed = candidate.trim();
      if (!trimmed.isEmpty()) {
        candidates.add(trimmed);
      }
    }
  }
}
