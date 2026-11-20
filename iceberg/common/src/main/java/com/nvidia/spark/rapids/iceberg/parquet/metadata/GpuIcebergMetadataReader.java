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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import scala.Option;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.collection.immutable.List$;

import com.nvidia.spark.rapids.DateTimeRebaseCorrected$;
import com.nvidia.spark.rapids.GpuNullBatchIterator;
import com.nvidia.spark.rapids.iceberg.ShimUtils;
import com.nvidia.spark.rapids.iceberg.parquet.GpuIcebergParquetReaderConf;
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetReaderPostProcessor;
import com.nvidia.spark.rapids.iceberg.parquet.MultiFile;
import com.nvidia.spark.rapids.iceberg.parquet.MultiThread;
import com.nvidia.spark.rapids.parquet.ParquetFileInfoWithBlockMeta;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Reads eligible Iceberg metadata-only scans without opening Parquet data or footer files.
 *
 * <p>Iceberg already supplies exact record counts and constants for complete, unfiltered files
 * without deletes. When those values cover the projection, reading Parquet adds unnecessary I/O.
 *
 * <p>The planner combines the row counts into one file descriptor when the reader allows it
 * and every file has the same projected constants. Otherwise it keeps each file's descriptor.
 * The scan planner estimates the rows per batch from the expected schema. Each task group
 * caps that estimate for GPU column limits before its reader streams the planned files.
 */
public final class GpuIcebergMetadataReader extends AbstractIterator<ColumnarBatch>
    implements AutoCloseable {
  private final GpuIcebergParquetReaderConf conf;
  private final int rowsPerBatch;
  private final java.util.Iterator<FileMetadata> files;
  private final StructType emptySparkSchema = new StructType();
  private final MessageType emptyParquetSchema =
      new MessageType("metadata", Collections.emptyList());
  private final org.apache.parquet.schema.MessageType unshadedParquetSchema =
      ShimUtils.unshadeParquetSchema(emptyParquetSchema);
  private Iterator<ColumnarBatch> batches;
  private boolean closed;

  public GpuIcebergMetadataReader(List<FileMetadata> files, int rowsPerBatch,
      GpuIcebergParquetReaderConf conf) {
    if (rowsPerBatch <= 0) {
      throw new IllegalArgumentException("rowsPerBatch must be positive");
    }
    this.files = files.iterator();
    this.rowsPerBatch = rowsPerBatch;
    this.conf = conf;
  }

  /** Estimate once on the driver for all partitions of the scan. */
  public static int estimateRowsPerBatch(Schema schema, int maxRows, long readBytes,
      long targetBytes) {
    long bytes = Math.min(Integer.MAX_VALUE, Math.min(readBytes, targetBytes));
    return GpuNullBatchIterator.maxRows(SparkSchemaUtil.convert(schema), maxRows, bytes);
  }

  /** Plan metadata-only reads for a task group before opening any of its files. */
  public static Optional<ReadPlan> plan(List<FileScanTask> tasks, GpuIcebergParquetReaderConf conf,
      int rowsPerBatch, Function<FileScanTask, Map<Integer, ?>> constants) {
    Schema schema = conf.expectedSchema();
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
    List<FileMetadata> plannedFiles = combineFiles(files, conf);
    // cuDF must be able to repeat any projected string or binary value at this batch size.
    // A wide value in a partition struct or file path can exceed the schema estimate.
    int widestValueBytes = 0;
    for (FileMetadata file : plannedFiles) {
      if (file.getRowCount() > 0) {
        widestValueBytes = Math.max(widestValueBytes, file.maxVariableWidthBytes(schema));
      }
    }
    if (widestValueBytes > 0) {
      rowsPerBatch = (int) Math.min(rowsPerBatch, conf.maxGpuColumnSizeBytes() / widestValueBytes);
      if (rowsPerBatch == 0) {
        throw new IllegalArgumentException(
            "Iceberg metadata value exceeds the GPU column size limit");
      }
    }
    return Optional.of(new ReadPlan(plannedFiles, rowsPerBatch));
  }

  private static List<FileMetadata> combineFiles(List<FileMetadata> files,
      GpuIcebergParquetReaderConf conf) {
    if (files.size() < 2 || !canCombineFiles(conf)) {
      return files;
    }
    FileMetadata first = files.get(0);
    long rows = first.getRowCount();
    for (int i = 1; i < files.size(); i++) {
      FileMetadata file = files.get(i);
      // One descriptor must represent both the total row count and every projected value.
      if (file.getRowCount() > Long.MAX_VALUE - rows ||
          !first.hasSameProjectedConstants(file, conf.expectedSchema())) {
        return files;
      }
      rows += file.getRowCount();
    }
    return Collections.singletonList(new FileMetadata(first.getPath(), rows, first.getConstants()));
  }

  private static boolean canCombineFiles(GpuIcebergParquetReaderConf conf) {
    if (conf.expectedSchema().findField(MetadataColumns.FILE_PATH.fieldId()) != null) {
      return false;
    }
    if (conf.threadConf() instanceof MultiThread) {
      MultiThread multiThread = (MultiThread) conf.threadConf();
      // A positive threshold enables combining. Only metadata is combined here, so the
      // whole group can share a descriptor even when its rows span many output batches.
      return !multiThread.disableCombining() &&
          multiThread.combineConf().combineThresholdSize() > 0 &&
          !multiThread.hasFilePathMetadata() && !multiThread.hasRowPositionMetadata();
    }
    if (conf.threadConf() instanceof MultiFile) {
      MultiFile multiFile = (MultiFile) conf.threadConf();
      return !multiFile.hasFilePathMetadata() && !multiFile.hasRowPositionMetadata();
    }
    return false;
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

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }
    while (batches == null || !batches.hasNext()) {
      if (!files.hasNext()) {
        return false;
      }
      FileMetadata file = files.next();
      if (file.getRowCount() > 0) {
        batches = batchesFor(file);
      }
    }
    return true;
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
    closed = true;
    batches = null;
  }

  private Iterator<ColumnarBatch> batchesFor(FileMetadata file) {
    Iterator<ColumnarBatch> result = GpuNullBatchIterator.apply(emptySparkSchema,
        file.getRowCount(), rowsPerBatch);
    if (conf.expectedSchema().columns().isEmpty()) {
      return result;
    }
    ParquetFileInfoWithBlockMeta info = new ParquetFileInfoWithBlockMeta(
        new Path(file.getPath()), List$.MODULE$.empty(), InternalRow.empty(),
        unshadedParquetSchema, emptySparkSchema,
        DateTimeRebaseCorrected$.MODULE$, DateTimeRebaseCorrected$.MODULE$, false,
        List$.MODULE$.empty(), Option.empty());
    GpuParquetReaderPostProcessor processor = new GpuParquetReaderPostProcessor(info,
        file.getConstants(), conf.expectedSchema(), emptyParquetSchema, conf.metrics());
    return result.map(processor::process);
  }

  /** The files and single batch row limit for an eligible Iceberg task group. */
  public static final class ReadPlan {
    private final List<FileMetadata> files;
    private final int rowsPerBatch;

    private ReadPlan(List<FileMetadata> files, int rowsPerBatch) {
      this.files = Collections.unmodifiableList(files);
      this.rowsPerBatch = rowsPerBatch;
    }

    public List<FileMetadata> getFiles() {
      return files;
    }

    public int getRowsPerBatch() {
      return rowsPerBatch;
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

    private int maxVariableWidthBytes(Schema schema) {
      int maxBytes = 0;
      for (Types.NestedField field : schema.columns()) {
        int id = field.fieldId();
        Object value = id == MetadataColumns.FILE_PATH.fieldId() && !constants.containsKey(id) ?
            UTF8String.fromString(new Path(path).toString()) : constants.get(id);
        maxBytes = Math.max(maxBytes, maxVariableWidthBytes(field.type(), value));
      }
      return maxBytes;
    }

    private static int maxVariableWidthBytes(Type type, Object value) {
      if (value instanceof UTF8String) {
        return ((UTF8String) value).numBytes();
      }
      if (value instanceof byte[]) {
        return ((byte[]) value).length;
      }
      int maxBytes = 0;
      if (value instanceof InternalRow) {
        List<Types.NestedField> fields = type.asStructType().fields();
        for (int i = 0; i < fields.size(); i++) {
          Type child = fields.get(i).type();
          Object childValue = ((InternalRow) value).get(i, SparkSchemaUtil.convert(child));
          maxBytes = Math.max(maxBytes, maxVariableWidthBytes(child, childValue));
        }
      }
      return maxBytes;
    }

    private boolean hasSameProjectedConstants(FileMetadata other, Schema schema) {
      for (Types.NestedField field : schema.columns()) {
        int id = field.fieldId();
        if (!sameConstant(field.type(), constants.get(id), other.getConstants().get(id))) {
          return false;
        }
      }
      return true;
    }

    private static boolean sameConstant(Type type, Object left, Object right) {
      if (left == right) {
        return true;
      }
      if (left == null || right == null) {
        return false;
      }
      // Compare struct children as stored. Spark row equality treats -0.0 and +0.0 as equal,
      // but substituting one partition value for the other is observable in the projection.
      if (type.isStructType()) {
        List<Types.NestedField> children = type.asStructType().fields();
        for (int i = 0; i < children.size(); i++) {
          Type child = children.get(i).type();
          DataType sparkType = SparkSchemaUtil.convert(child);
          if (!sameConstant(child, ((InternalRow) left).get(i, sparkType),
              ((InternalRow) right).get(i, sparkType))) {
            return false;
          }
        }
        return true;
      }
      if (left instanceof Float) {
        return Float.floatToRawIntBits((Float) left) == Float.floatToRawIntBits((Float) right);
      }
      if (left instanceof Double) {
        return Double.doubleToRawLongBits((Double) left) ==
            Double.doubleToRawLongBits((Double) right);
      }
      return Objects.deepEquals(left, right);
    }
  }
}
