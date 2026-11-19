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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
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
 * The reader streams each descriptor in batches sized from the expected schema and configured
 * limits, and uses its path and constants to materialize the projected columns.
 */
public final class GpuIcebergMetadataReader extends AbstractIterator<ColumnarBatch>
    implements AutoCloseable {
  private final MetadataReadContext context;
  private final java.util.Iterator<FileMetadata> files;
  private Iterator<ColumnarBatch> batches;
  private boolean closed;

  public GpuIcebergMetadataReader(List<FileMetadata> files, GpuIcebergParquetReaderConf conf) {
    context = new MetadataReadContext(conf);
    this.files = files.iterator();
  }

  /** Plan metadata-only reads for a task group before opening any of its files. */
  public static Optional<List<FileMetadata>> plan(List<FileScanTask> tasks,
      GpuIcebergParquetReaderConf conf, Function<FileScanTask, Map<Integer, ?>> constants) {
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
    return Optional.of(combineFiles(files, conf));
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
        batches = context.batchesFor(file);
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

  /** Shares the expected schema's row estimate and the empty Parquet schema across files. */
  private static final class MetadataReadContext {
    private final GpuIcebergParquetReaderConf conf;
    private final StructType sparkSchema;
    private final StructType emptySparkSchema = new StructType();
    private final MessageType emptyParquetSchema =
        new MessageType("metadata", Collections.emptyList());
    private final org.apache.parquet.schema.MessageType unshadedParquetSchema;
    private final long maxBytes;
    private final int maxRows;

    MetadataReadContext(GpuIcebergParquetReaderConf conf) {
      this.conf = conf;
      sparkSchema = SparkSchemaUtil.convert(conf.expectedSchema());
      unshadedParquetSchema = ShimUtils.unshadeParquetSchema(emptyParquetSchema);
      maxBytes = Math.max(1L, Math.min(Integer.MAX_VALUE,
          Math.min(conf.maxBatchSizeBytes(), conf.targetBatchSizeBytes())));
      maxRows = GpuNullBatchIterator.maxRows(sparkSchema, conf.maxBatchSizeRows(), maxBytes);
    }

    public Iterator<ColumnarBatch> batchesFor(FileMetadata file) {
      Iterator<ColumnarBatch> batches = GpuNullBatchIterator.apply(emptySparkSchema,
          file.getRowCount(), rowsPerBatch(file));
      if (sparkSchema.isEmpty()) {
        return batches;
      }
      ParquetFileInfoWithBlockMeta info = new ParquetFileInfoWithBlockMeta(
          new Path(file.getPath()), List$.MODULE$.empty(), InternalRow.empty(),
          unshadedParquetSchema, emptySparkSchema,
          DateTimeRebaseCorrected$.MODULE$, DateTimeRebaseCorrected$.MODULE$, false,
          List$.MODULE$.empty(), Option.empty());
      GpuParquetReaderPostProcessor processor = new GpuParquetReaderPostProcessor(info,
          file.getConstants(), conf.expectedSchema(), emptyParquetSchema, conf.metrics());
      return batches.map(processor::process);
    }

    // Variable-width constants can be larger than the schema estimate, including inside structs.
    private int rowsPerBatch(FileMetadata file) {
      List<Types.NestedField> fields = conf.expectedSchema().columns();
      List<Long> leaves = new ArrayList<>();
      long rowBytes = 0;
      for (int i = 0; i < fields.size(); i++) {
        int id = fields.get(i).fieldId();
        Object value = file.getConstants().containsKey(id) ? file.getConstants().get(id) :
            UTF8String.fromString(new Path(file.getPath()).toString());
        rowBytes += rowSize(value, sparkSchema.fields()[i].dataType(), leaves);
      }

      if (leaves.isEmpty()) {
        return maxRows;
      }
      long rows = Math.min(maxRows, Math.max(1L, maxBytes / rowBytes));
      long columnLimit = Math.min(conf.maxGpuColumnSizeBytes(), Integer.MAX_VALUE);
      for (long size : leaves) {
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
}
