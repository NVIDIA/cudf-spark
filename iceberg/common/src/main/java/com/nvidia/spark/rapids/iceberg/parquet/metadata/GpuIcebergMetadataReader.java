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

package com.nvidia.spark.rapids.iceberg.parquet.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import scala.collection.AbstractIterator;
import scala.collection.Iterator;

import com.nvidia.spark.rapids.ConcatAndConsumeAll;
import com.nvidia.spark.rapids.GpuSemaphore$;
import com.nvidia.spark.rapids.RmmRapidsRetryIterator;
import com.nvidia.spark.rapids.SpillPriorities;
import com.nvidia.spark.rapids.SpillableColumnarBatch;
import com.nvidia.spark.rapids.iceberg.ShimUtils;
import com.nvidia.spark.rapids.iceberg.parquet.GpuIcebergParquetReaderConf;
import com.nvidia.spark.rapids.utils.ResourceUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * Materialize each file's constants before concatenating its rows with other files.
 * Pending work contains only metadata, so retries can split even a single large file without
 * retaining GPU allocations or reading a Parquet footer.
 */
public final class GpuIcebergMetadataReader extends AbstractIterator<ColumnarBatch>
    implements AutoCloseable {
  private final MetadataReadContext context;
  private final MetadataBatchIterator groups;
  private final Iterator<ColumnarBatch> batches;
  private boolean closed;

  public GpuIcebergMetadataReader(List<FileMetadata> files, GpuIcebergParquetReaderConf conf) {
    context = new MetadataReadContext(conf);
    groups = new MetadataBatchIterator(files, context);
    batches = RmmRapidsRetryIterator.withRetry(groups, MetadataBatch::split, this::materialize);
  }

  /** Prove eligibility from Iceberg metadata, without constructing or opening input files. */
  public static Optional<List<FileMetadata>> plan(List<FileScanTask> tasks, Schema schema,
      Function<FileScanTask, Map<Integer, ?>> constants) {
    for (Types.NestedField field : schema.columns()) {
      int id = field.fieldId();
      if (id == MetadataColumns.ROW_POSITION.fieldId() ||
          id == MetadataColumns.IS_DELETED.fieldId() ||
          id == ShimUtils.rowIdFieldId() || id == ShimUtils.lastUpdatedSequenceNumberFieldId()) {
        return Optional.empty();
      }
    }
    for (FileScanTask task : tasks) {
      if (!canUseRecordCount(task)) {
        return Optional.empty();
      }
    }
    List<FileMetadata> files = new ArrayList<>(tasks.size());
    for (FileScanTask task : tasks) {
      Map<Integer, ?> values = constants.apply(task);
      for (Types.NestedField field : schema.columns()) {
        if (field.fieldId() != MetadataColumns.FILE_PATH.fieldId() &&
            !values.containsKey(field.fieldId())) {
          return Optional.empty();
        }
      }
      files.add(new FileMetadata(ShimUtils.locationOf(task.file()),
          task.file().recordCount(), values));
    }
    return Optional.of(files);
  }

  private static boolean canUseRecordCount(FileScanTask task) {
    DataFile file = task.file();
    // Offset-aware Iceberg tasks start at the first row group, after the Parquet header.
    // Such a task still represents every row if it extends through the end of the file.
    List<Long> offsets = file.splitOffsets();
    long firstOffset = offsets == null || offsets.isEmpty() ? 0 : offsets.get(0);
    boolean wholeFile = task.start() >= 0 && task.start() <= firstOffset &&
        task.length() == file.fileSizeInBytes() - task.start();
    return file.format() == FileFormat.PARQUET && file.recordCount() >= 0 && wholeFile &&
        task.deletes().isEmpty() && Expressions.alwaysTrue().equals(task.residual());
  }

  private ColumnarBatch materialize(MetadataBatch batch) {
    GpuSemaphore$.MODULE$.acquireIfNecessary(TaskContext.get());
    if (context.sparkTypes.length == 0) {
      return new ColumnarBatch(new ColumnVector[0], batch.rows);
    }
    if (batch.parts.length == 1) {
      return batch.parts[0].file.processor().process(
          new ColumnarBatch(new ColumnVector[0], batch.rows));
    }
    // Spill completed parts while constructing the remaining parts of the output batch.
    SpillableColumnarBatch[] parts = new SpillableColumnarBatch[batch.parts.length];
    Throwable failure = null;
    try {
      for (int i = 0; i < parts.length; i++) {
        MetadataFileRows part = batch.parts[i];
        ColumnarBatch materialized = part.file.processor().process(
            new ColumnarBatch(new ColumnVector[0], part.rows));
        parts[i] = ResourceUtil.closeOnExcept(materialized,
            cb -> SpillableColumnarBatch.apply(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY()));
      }
      ColumnarBatch[] materialized = new ColumnarBatch[parts.length];
      try {
        for (int i = 0; i < parts.length; i++) {
          materialized[i] = parts[i].getColumnarBatch();
        }
      } catch (Throwable t) {
        ResourceUtil.closeAll(t, materialized);
        throw t;
      }
      // Concatenation consumes all materialized inputs, including on failure.
      return ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(materialized, context.sparkTypes);
    } catch (Throwable t) {
      failure = t;
      throw t;
    } finally {
      ResourceUtil.closeAll(failure, parts);
    }
  }

  @Override
  public boolean hasNext() {
    return !closed && batches.hasNext();
  }

  @Override
  public ColumnarBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more Iceberg metadata batches");
    }
    return batches.next();
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      ResourceUtil.closeAll(null, groups, (AutoCloseable) batches);
    }
  }
}
