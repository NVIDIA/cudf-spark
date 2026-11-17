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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

import com.nvidia.spark.rapids.ConcatAndConsumeAll;
import com.nvidia.spark.rapids.DateTimeRebaseCorrected$;
import com.nvidia.spark.rapids.GpuSemaphore$;
import com.nvidia.spark.rapids.RmmRapidsRetryIterator;
import com.nvidia.spark.rapids.SpillPriorities;
import com.nvidia.spark.rapids.SpillableColumnarBatch;
import com.nvidia.spark.rapids.iceberg.ShimUtils;
import com.nvidia.spark.rapids.iceberg.parquet.GpuIcebergParquetReaderConf;
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetReaderPostProcessor;
import com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM;
import com.nvidia.spark.rapids.parquet.ParquetFileInfoWithBlockMeta;
import com.nvidia.spark.rapids.utils.ResourceUtil;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;

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
    if (context.getSparkTypes().length == 0) {
      return new ColumnarBatch(new ColumnVector[0], batch.getRows());
    }
    if (batch.getParts().length == 1) {
      return batch.getParts()[0].getFile().getProcessor().process(
          new ColumnarBatch(new ColumnVector[0], batch.getRows()));
    }
    // Spill completed parts while constructing the remaining parts of the output batch.
    SpillableColumnarBatch[] parts = new SpillableColumnarBatch[batch.getParts().length];
    Throwable failure = null;
    try {
      for (int i = 0; i < parts.length; i++) {
        MetadataFileRows part = batch.getParts()[i];
        ColumnarBatch materialized = part.getFile().getProcessor().process(
            new ColumnarBatch(new ColumnVector[0], part.getRows()));
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
      return ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(materialized, context.getSparkTypes());
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

  /** File-level metadata sufficient to produce rows without opening a data file. */
  public static final class FileMetadata {
    private final String path;
    private final long rowCount;
    private final Map<Integer, ?> constants;

    public FileMetadata(String path, long rowCount, Map<Integer, ?> constants) {
      this.path = path;
      this.rowCount = rowCount;
      this.constants = constants;
    }

    public String getPath() {
      return path;
    }

    public long getRowCount() {
      return rowCount;
    }

    public Map<Integer, ?> getConstants() {
      return constants;
    }
  }

  /** Schema and batch limits shared by all files in a reader. */
  private static final class MetadataReadContext {
    private final GpuIcebergParquetReaderConf conf;
    private final DataType[] sparkTypes;
    private final MessageType emptyParquetSchema;
    private final org.apache.parquet.schema.MessageType unshadedParquetSchema;
    private final int maxRows;
    private final long maxBytes;
    private final long columnLimit;

    MetadataReadContext(GpuIcebergParquetReaderConf conf) {
      this.conf = conf;
      StructField[] fields = SparkSchemaUtil.convert(conf.expectedSchema()).fields();
      sparkTypes = new DataType[fields.length];
      for (int i = 0; i < fields.length; i++) {
        sparkTypes[i] = fields[i].dataType();
      }
      emptyParquetSchema = new MessageType("metadata", Collections.emptyList());
      unshadedParquetSchema = ShimUtils.unshadeParquetSchema(emptyParquetSchema);
      maxRows = Math.max(1, conf.maxBatchSizeRows());
      // The total byte bound also keeps string/binary offsets below cuDF's row limit.
      maxBytes = Math.max(1L, Math.min(Integer.MAX_VALUE,
          Math.min(conf.maxBatchSizeBytes(), conf.targetBatchSizeBytes())));
      columnLimit = Math.min(conf.maxGpuColumnSizeBytes(), Integer.MAX_VALUE);
    }

    public GpuIcebergParquetReaderConf getConf() {
      return conf;
    }

    public DataType[] getSparkTypes() {
      return sparkTypes;
    }

    public MessageType getEmptyParquetSchema() {
      return emptyParquetSchema;
    }

    public org.apache.parquet.schema.MessageType getUnshadedParquetSchema() {
      return unshadedParquetSchema;
    }

    public int getMaxRows() {
      return maxRows;
    }

    public long getMaxBytes() {
      return maxBytes;
    }

    public long getColumnLimit() {
      return columnLimit;
    }
  }

  /** Projected constants, their sizes, and a lazily created materializer for one file. */
  private static final class MetadataFileState {
    private final FileMetadata file;
    private final long rowBytes;
    private final long[] leafSizes;
    private final MetadataReadContext context;
    private final Object[] values;
    private GpuParquetReaderPostProcessor processor;

    MetadataFileState(FileMetadata file, MetadataReadContext context) {
      this.file = file;
      this.context = context;
      List<Types.NestedField> fields = context.getConf().expectedSchema().columns();
      values = new Object[fields.size()];
      List<Long> leaves = new ArrayList<>();
      long bytes = 0;
      for (int i = 0; i < fields.size(); i++) {
        int id = fields.get(i).fieldId();
        values[i] = file.getConstants().containsKey(id) ? file.getConstants().get(id) :
            UTF8String.fromString(new Path(file.getPath()).toString());
        bytes += rowSize(values[i], context.getSparkTypes()[i], leaves);
      }
      rowBytes = bytes;
      leafSizes = new long[leaves.size()];
      for (int i = 0; i < leafSizes.length; i++) {
        leafSizes[i] = leaves.get(i);
      }
    }

    public long getRowBytes() {
      return rowBytes;
    }

    public long[] getLeafSizes() {
      return leafSizes;
    }

    public Object[] getValues() {
      return values;
    }

    boolean sameOutput(MetadataFileState other) {
      return Arrays.deepEquals(values, other.getValues());
    }

    public GpuParquetReaderPostProcessor getProcessor() {
      if (processor == null) {
        ParquetFileInfoWithBlockMeta info = new ParquetFileInfoWithBlockMeta(
            new Path(file.getPath()),
            JavaConverters.asScalaBufferConverter(Collections
                .<org.apache.parquet.hadoop.metadata.BlockMetaData>emptyList()).asScala().toList(),
            InternalRow.empty(), context.getUnshadedParquetSchema(), new StructType(),
            DateTimeRebaseCorrected$.MODULE$, DateTimeRebaseCorrected$.MODULE$, false,
            JavaConverters.asScalaBufferConverter(Collections.emptyList()).asScala().toList());
        processor = new GpuParquetReaderPostProcessor(info, file.getConstants(),
            context.getConf().expectedSchema(), context.getEmptyParquetSchema(),
            context.getConf().metrics());
      }
      return processor;
    }

    // A conservative batch-size estimate. Keep variable-width leaves separate so a short value
    // in one column cannot hide an oversized sibling, including inside a struct.
    private static long rowSize(Object value, DataType type, List<Long> leaves) {
      if (type.equals(DataTypes.StringType) || type.equals(DataTypes.BinaryType)) {
        long bytes = value == null ? 0 : type.equals(DataTypes.StringType) ?
            ((UTF8String) value).numBytes() : ((byte[]) value).length;
        leaves.add(bytes);
        return bytes + 5; // offsets and validity
      }
      if (type instanceof StructType) {
        StructField[] fields = ((StructType) type).fields();
        long bytes = 1;
        for (int i = 0; i < fields.length; i++) {
          DataType childType = fields[i].dataType();
          Object child = value == null ? null : ((InternalRow) value).get(i, childType);
          bytes += rowSize(child, childType, leaves);
        }
        return bytes;
      }
      return (long) type.defaultSize() + 1;
    }
  }

  /** A bounded run of rows with identical projected constants. */
  private static final class MetadataFileRows {
    private final MetadataFileState file;
    private final int rows;

    MetadataFileRows(MetadataFileState file, int rows) {
      this.file = file;
      this.rows = rows;
    }

    public MetadataFileState getFile() {
      return file;
    }

    public int getRows() {
      return rows;
    }
  }

  /** Pending work owns only metadata, so even a single file can be split on allocation failure. */
  private static final class MetadataBatch implements AutoCloseable {
    private final MetadataFileRows[] parts;
    private final int rows;

    MetadataBatch(MetadataFileRows[] parts) {
      this.parts = parts;
      int total = 0;
      for (MetadataFileRows part : parts) {
        total += part.getRows();
      }
      rows = total;
    }

    public MetadataFileRows[] getParts() {
      return parts;
    }

    public int getRows() {
      return rows;
    }

    static scala.collection.immutable.List<MetadataBatch> split(MetadataBatch batch) {
      if (batch.getRows() <= 1) {
        throw new GpuSplitAndRetryOOM("Cannot split one metadata row");
      }
      int leftRows = batch.getRows() / 2;
      List<MetadataFileRows> left = new ArrayList<>();
      List<MetadataFileRows> right = new ArrayList<>();
      for (MetadataFileRows part : batch.getParts()) {
        int take = Math.min(leftRows, part.getRows());
        if (take > 0) {
          left.add(new MetadataFileRows(part.getFile(), take));
        }
        if (take < part.getRows()) {
          right.add(new MetadataFileRows(part.getFile(), part.getRows() - take));
        }
        leftRows -= take;
      }
      return JavaConverters.asScalaBufferConverter(Arrays.asList(
          new MetadataBatch(left.toArray(new MetadataFileRows[0])),
          new MetadataBatch(right.toArray(new MetadataFileRows[0])))).asScala().toList();
    }

    @Override
    public void close() {
      // No GPU allocations are retained by a pending batch.
    }
  }

  /** Combine file metadata into batches bounded by row, byte, and variable-width column limits. */
  private static final class MetadataBatchIterator extends AbstractIterator<MetadataBatch>
      implements AutoCloseable {
    private final java.util.Iterator<FileMetadata> files;
    private final MetadataReadContext context;
    private MetadataFileState currentFile;
    private long remainingRows;
    private boolean closed;

    MetadataBatchIterator(List<FileMetadata> files, MetadataReadContext context) {
      this.files = files.iterator();
      this.context = context;
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        return false;
      }
      while (currentFile == null && files.hasNext()) {
        FileMetadata file = files.next();
        if (file.getRowCount() > 0) {
          currentFile = new MetadataFileState(file, context);
          remainingRows = file.getRowCount();
        }
      }
      return currentFile != null;
    }

    @Override
    public MetadataBatch next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more Iceberg metadata");
      }
      List<MetadataFileRows> parts = new ArrayList<>();
      int rows = 0;
      long bytes = 0;
      long[] leafBytes = new long[currentFile.getLeafSizes().length];
      while (rows < context.getMaxRows() && hasNext()) {
        MetadataFileState file = currentFile;
        long byteRows = context.getMaxRows();
        if (file.getRowBytes() > 0) {
          byteRows = Math.max(0L, (context.getMaxBytes() - bytes) / file.getRowBytes());
          // Like the data readers, allow one row larger than the soft batch byte limit.
          if (rows == 0) {
            byteRows = Math.max(1L, byteRows);
          }
        }
        long columnRows = context.getMaxRows();
        for (int i = 0; i < leafBytes.length; i++) {
          if (file.getLeafSizes()[i] > 0) {
            columnRows = Math.min(columnRows,
                (context.getColumnLimit() - leafBytes[i]) / file.getLeafSizes()[i]);
          }
        }
        int take = (int) Math.min(Math.min(remainingRows, context.getMaxRows() - rows),
            Math.min(byteRows, columnRows));
        if (take == 0) {
          if (rows == 0) {
            throw new IllegalArgumentException(
                "Iceberg metadata value exceeds the GPU column size limit");
          }
          break;
        }
        // Identical projected constants share one materialization, including across files.
        int last = parts.size() - 1;
        if (last >= 0 && parts.get(last).getFile().sameOutput(file)) {
          MetadataFileRows previous = parts.get(last);
          parts.set(last, new MetadataFileRows(previous.getFile(), previous.getRows() + take));
        } else {
          parts.add(new MetadataFileRows(file, take));
        }
        rows += take;
        bytes += file.getRowBytes() * take;
        for (int i = 0; i < leafBytes.length; i++) {
          leafBytes[i] += file.getLeafSizes()[i] * take;
        }
        remainingRows -= take;
        if (remainingRows == 0) {
          currentFile = null;
        }
      }
      return new MetadataBatch(parts.toArray(new MetadataFileRows[0]));
    }

    @Override
    public void close() {
      closed = true;
      currentFile = null;
    }
  }
}
