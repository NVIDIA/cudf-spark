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
import scala.collection.JavaConverters;

import com.nvidia.spark.rapids.ConcatAndConsumeAll;
import com.nvidia.spark.rapids.DateTimeRebaseCorrected$;
import com.nvidia.spark.rapids.GpuSemaphore$;
import com.nvidia.spark.rapids.SpillPriorities;
import com.nvidia.spark.rapids.SpillableColumnarBatch;
import com.nvidia.spark.rapids.iceberg.ShimUtils;
import com.nvidia.spark.rapids.iceberg.parquet.GpuIcebergParquetReaderConf;
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetReaderPostProcessor;
import com.nvidia.spark.rapids.iceberg.parquet.MultiFile;
import com.nvidia.spark.rapids.iceberg.parquet.MultiThread;
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
 * Reads eligible Iceberg metadata-only scans without opening Parquet data or footer files.
 *
 * <p>Iceberg already supplies exact record counts and constants for complete, unfiltered files
 * without deletes. When those values cover the projection, reading Parquet adds unnecessary I/O.
 *
 * <p>Each file produces a lazy sequence of batches sized from its constants and the configured
 * row, byte, and variable-width column limits before allocation. Optional combining follows the
 * reader mode and its file-metadata restrictions. Only planned batches that fit the output limits
 * can combine. Identical projected constants share one materialization; different constants are
 * materialized separately before concatenation, with completed inputs made spillable. Allocation
 * retries preserve the planned batch sizes;
 * OOM-driven splitting is not used to decide how many rows to read.
 */
public final class GpuIcebergMetadataReader extends AbstractIterator<ColumnarBatch>
    implements AutoCloseable {
  private final MetadataReadContext context;
  private final java.util.Iterator<FileMetadata> files;
  private MetadataFileReader currentFile;
  private boolean closed;

  public GpuIcebergMetadataReader(List<FileMetadata> files, GpuIcebergParquetReaderConf conf) {
    context = new MetadataReadContext(conf);
    this.files = files.iterator();
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

  private ColumnarBatch concatenate(SpillableColumnarBatch[] parts) {
    return ResourceUtil.withRetryNoSplit(() -> {
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
    });
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }
    while (currentFile == null || !currentFile.hasNext()) {
      currentFile = null;
      if (!files.hasNext()) {
        return false;
      }
      FileMetadata file = files.next();
      if (file.getRowCount() > 0) {
        currentFile = new MetadataFileReader(file, context);
      }
    }
    return true;
  }

  @Override
  public ColumnarBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more Iceberg metadata batches");
    }
    if (context.getCombineThresholdBytes() == 0 ||
        currentFile.getNextBatchRows() == context.getMaxRows() ||
        currentFile.getRowBytes() * currentFile.getNextBatchRows() >=
            context.getCombineThresholdBytes()) {
      return currentFile.next();
    }

    List<MetadataFileBatch> parts = new ArrayList<>();
    int rows = 0;
    long bytes = 0;
    long[] leafBytes = new long[currentFile.getLeafSizes().length];
    while (hasNext()) {
      int nextRows = currentFile.getNextBatchRows();
      long nextBytes = currentFile.getRowBytes() * nextRows;
      long[] leafSizes = currentFile.getLeafSizes();
      if (!parts.isEmpty()) {
        boolean fits = nextRows <= context.getMaxRows() - rows &&
            nextBytes <= context.getMaxBytes() - bytes;
        for (int i = 0; fits && i < leafBytes.length; i++) {
          fits = leafSizes[i] * nextRows <= context.getColumnLimit() - leafBytes[i];
        }
        if (!fits) {
          break;
        }
      }
      // Consume a batch already sized for this file; combining never changes its row capacity.
      currentFile.takeNextBatchRows();
      int last = parts.size() - 1;
      if (last >= 0 && parts.get(last).getFile().sameOutput(currentFile)) {
        MetadataFileBatch previous = parts.get(last);
        parts.set(last, new MetadataFileBatch(previous.getFile(), previous.getRows() + nextRows));
      } else {
        parts.add(new MetadataFileBatch(currentFile, nextRows));
      }
      rows += nextRows;
      bytes += nextBytes;
      for (int i = 0; i < leafBytes.length; i++) {
        leafBytes[i] += leafSizes[i] * nextRows;
      }
      if (rows == context.getMaxRows() || bytes >= context.getCombineThresholdBytes()) {
        break;
      }
    }
    return materialize(parts.toArray(new MetadataFileBatch[0]));
  }

  private ColumnarBatch materialize(MetadataFileBatch[] batches) {
    if (batches.length == 1) {
      return batches[0].getFile().materializeRows(batches[0].getRows());
    }
    SpillableColumnarBatch[] parts = new SpillableColumnarBatch[batches.length];
    Throwable failure = null;
    try {
      for (int i = 0; i < batches.length; i++) {
        MetadataFileBatch batch = batches[i];
        ColumnarBatch materialized = batch.getFile().materializeRows(batch.getRows());
        parts[i] = ResourceUtil.closeOnExcept(materialized,
            cb -> SpillableColumnarBatch.apply(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY()));
      }
      return concatenate(parts);
    } catch (Throwable t) {
      failure = t;
      throw t;
    } finally {
      ResourceUtil.closeAll(failure, parts);
    }
  }

  @Override
  public void close() {
    closed = true;
    // A partially consumed file retains only metadata. Returned batches belong to the caller.
    currentFile = null;
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

  /**
   * Shares schemas and sizing limits across files to avoid repeated schema conversion.
   * Row capacity is estimated before materialization, including each variable-width leaf, so
   * a large file is emitted in bounded batches without waiting for an allocation failure.
   */
  private static final class MetadataReadContext {
    private final GpuIcebergParquetReaderConf conf;
    private final DataType[] sparkTypes;
    private final MessageType emptyParquetSchema;
    private final org.apache.parquet.schema.MessageType unshadedParquetSchema;
    private final int maxRows;
    private final long maxBytes;
    private final long columnLimit;
    private final long combineThresholdBytes;

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
      combineThresholdBytes = combineThreshold(conf, maxBytes);
    }

    private static long combineThreshold(GpuIcebergParquetReaderConf conf, long maxBytes) {
      // Match the data readers' file-boundary restrictions, even for metadata-only projections.
      if (conf.expectedSchema().findField(MetadataColumns.FILE_PATH.fieldId()) != null) {
        return 0;
      }
      if (conf.threadConf() instanceof MultiThread) {
        MultiThread multiThread = (MultiThread) conf.threadConf();
        if (!multiThread.disableCombining() && !multiThread.hasFilePathMetadata() &&
            !multiThread.hasRowPositionMetadata()) {
          return Math.max(0, Math.min(maxBytes,
              multiThread.combineConf().combineThresholdSize()));
        }
      } else if (conf.threadConf() instanceof MultiFile) {
        MultiFile multiFile = (MultiFile) conf.threadConf();
        if (!multiFile.hasFilePathMetadata() && !multiFile.hasRowPositionMetadata()) {
          return maxBytes;
        }
      }
      return 0;
    }

    public int estimateRowsPerBatch(long rowBytes, long[] leafSizes) {
      long rows = maxRows;
      if (rowBytes > 0) {
        // A single row may exceed the soft byte target, as in the data readers.
        rows = Math.min(rows, Math.max(1, maxBytes / rowBytes));
      }
      for (long size : leafSizes) {
        if (size > 0) {
          rows = Math.min(rows, columnLimit / size);
        }
      }
      if (rows == 0) {
        throw new IllegalArgumentException(
            "Iceberg metadata value exceeds the GPU column size limit");
      }
      return (int) rows;
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

    public long getCombineThresholdBytes() {
      return combineThresholdBytes;
    }
  }

  /**
   * Emits one file's constants in a lazy sequence of pre-sized batches.
   * Files may contain more rows than a GPU batch can represent, so only the remaining row count
   * is retained between calls. Materialization uses the post-processor's non-splitting retry.
   */
  private static final class MetadataFileReader extends AbstractIterator<ColumnarBatch> {
    private final FileMetadata file;
    private final long rowBytes;
    private final long[] leafSizes;
    private final MetadataReadContext context;
    private final Object[] values;
    private final int rowsPerBatch;
    private long remainingRows;
    private GpuParquetReaderPostProcessor processor;

    MetadataFileReader(FileMetadata file, MetadataReadContext context) {
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
      rowsPerBatch = context.estimateRowsPerBatch(rowBytes, leafSizes);
      remainingRows = file.getRowCount();
    }

    public long getRowBytes() {
      return rowBytes;
    }

    public long[] getLeafSizes() {
      return leafSizes;
    }

    public int getNextBatchRows() {
      return (int) Math.min(remainingRows, rowsPerBatch);
    }

    @Override
    public boolean hasNext() {
      return remainingRows > 0;
    }

    public Object[] getValues() {
      return values;
    }

    public boolean sameOutput(MetadataFileReader other) {
      return Arrays.deepEquals(values, other.getValues());
    }

    public int takeNextBatchRows() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more rows in Iceberg metadata file");
      }
      int rows = getNextBatchRows();
      remainingRows -= rows;
      return rows;
    }

    @Override
    public ColumnarBatch next() {
      return materializeRows(takeNextBatchRows());
    }

    public ColumnarBatch materializeRows(int rows) {
      if (rows <= 0 || rows > rowsPerBatch) {
        throw new IllegalArgumentException("Metadata batch exceeds its estimated row capacity");
      }
      GpuSemaphore$.MODULE$.acquireIfNecessary(TaskContext.get());
      ColumnarBatch batch = new ColumnarBatch(new ColumnVector[0], rows);
      if (context.getSparkTypes().length > 0) {
        batch = getProcessor().process(batch);
      }
      return batch;
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

  /** A bounded run of planned file batches whose projected constants can be materialized once. */
  private static final class MetadataFileBatch {
    private final MetadataFileReader file;
    private final int rows;

    MetadataFileBatch(MetadataFileReader file, int rows) {
      this.file = file;
      this.rows = rows;
    }

    public MetadataFileReader getFile() {
      return file;
    }

    public int getRows() {
      return rows;
    }
  }
}
