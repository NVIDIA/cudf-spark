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

package com.nvidia.spark.rapids.iceberg.parquet.async;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

/**
 * Immutable unit of asynchronous I/O, finalization, and subsequent GPU decode.
 *
 * <p>The Spark task thread creates a subtask from ordered, compatible file slices. Construction
 * calculates the exact synthetic Parquet file: fragment slices follow the four-byte header, and
 * relocated row-group metadata is serialized into the final footer. The resulting total size is
 * therefore exact rather than an estimate.</p>
 *
 * <p>File slices retain stable input order and borrow their Iceberg footer state. Byte arrays are
 * defensively copied on access. {@code subtaskId} is unique only within one partition read plan;
 * row counts are rows and sizes and offsets are bytes.</p>
 */
public final class ReadSubtask {
  private static final byte[] PARQUET_MAGIC = new byte[] {'P', 'A', 'R', '1'};
  private static final int PARQUET_VERSION = 1;
  private static final String PARQUET_CREATOR = "RAPIDS Spark Plugin";

  private final long subtaskId;
  private final List<FileSlice> fileSlices;
  private final byte[] footerAndTrailerBytes;
  private final long dataSizeBytes;
  private final long totalSizeBytes;
  private final long rowCount;

  /**
   * Creates a subtask and its exact synthetic-Parquet layout.
   *
   * <p>Iceberg currently disables CPU decompression in its multithreaded reader, so every copied
   * column chunk retains its encoded byte length. If CPU-side decompression is introduced later,
   * this planning must change to account for decompressed sizes.</p>
   *
   * @param subtaskId non-negative identifier unique within the partition plan
   * @param fileSlices non-empty ordered, mutually compatible file slices
   */
  public ReadSubtask(
      long subtaskId,
      List<FileSlice> fileSlices) {
    if (subtaskId < 0) {
      throw new IllegalArgumentException("subtaskId must be non-negative");
    }
    this.subtaskId = subtaskId;
    this.fileSlices = immutableCopy(fileSlices);
    if (this.fileSlices.isEmpty()) {
      throw new IllegalArgumentException("a read subtask must contain at least one file slice");
    }

    long rows = 0L;
    long sourceDataBytes = 0L;
    for (FileSlice fileSlice : this.fileSlices) {
      for (BlockMetaData block : fileSlice.getBlocks()) {
        rows = Math.addExact(rows, block.getRowCount());
        for (ColumnChunkMetaData column : block.getColumns()) {
          sourceDataBytes = Math.addExact(sourceDataBytes, column.getTotalSize());
        }
      }
    }
    this.rowCount = rows;

    MessageType schema = this.fileSlices.get(0).getFooter().getClippedSchema();
    ArrayList<BlockMetaData> adjustedBlocks = new ArrayList<>();
    long outputOffset = PARQUET_MAGIC.length;

    for (FileSlice fileSlice : this.fileSlices) {
      if (!schema.equals(fileSlice.getFooter().getClippedSchema())) {
        throw new IllegalArgumentException(
            "all file slices in a synthetic Parquet file must use the same schema");
      }
      for (BlockMetaData sourceBlock : fileSlice.getBlocks()) {
        BlockMetaData adjustedBlock = new BlockMetaData();
        adjustedBlock.setRowCount(sourceBlock.getRowCount());
        adjustedBlock.setRowIndexOffset(sourceBlock.getRowIndexOffset());
        long uncompressedSize = 0L;

        for (ColumnChunkMetaData sourceColumn : sourceBlock.getColumns()) {
          long length = sourceColumn.getTotalSize();

          ColumnChunkMetaData adjustedColumn = relocateColumn(sourceColumn, outputOffset);
          adjustedBlock.addColumn(adjustedColumn);
          uncompressedSize = Math.addExact(
              uncompressedSize, adjustedColumn.getTotalUncompressedSize());
          outputOffset = Math.addExact(outputOffset, length);
        }
        adjustedBlock.setTotalByteSize(uncompressedSize);
        adjustedBlocks.add(adjustedBlock);
      }
    }

    this.footerAndTrailerBytes = serializeFooterAndTrailer(schema, adjustedBlocks);
    this.dataSizeBytes = Math.subtractExact(outputOffset, PARQUET_MAGIC.length);
    this.totalSizeBytes = Math.addExact(outputOffset, footerAndTrailerBytes.length);

    if (sourceDataBytes != dataSizeBytes) {
      throw new IllegalArgumentException(
          "planned data size does not match the file-slice column chunks");
    }
  }

  private static List<FileSlice> immutableCopy(List<FileSlice> values) {
    Objects.requireNonNull(values, "fileSlices");
    ArrayList<FileSlice> copy = new ArrayList<>(values);
    if (copy.contains(null)) {
      throw new IllegalArgumentException("fileSlices must not contain null values");
    }
    return Collections.unmodifiableList(copy);
  }

  /**
   * Relocates one column while preserving the distance between its dictionary and first data
   * pages. The output range starts at the earlier of those two original offsets.
   */
  @SuppressWarnings("deprecation")
  private static ColumnChunkMetaData relocateColumn(
      ColumnChunkMetaData source,
      long syntheticStartingOffset) {
    if (source.isEncrypted()) {
      throw new UnsupportedOperationException(
          "asynchronous synthetic Parquet does not support encrypted column metadata");
    }
    long adjustment = Math.subtractExact(
        syntheticStartingOffset, source.getStartingPos());
    long firstDataPageOffset = Math.addExact(
        source.getFirstDataPageOffset(), adjustment);
    long dictionaryPageOffset = source.getDictionaryPageOffset() > 0
        ? Math.addExact(source.getDictionaryPageOffset(), adjustment)
        : 0L;
    return ColumnChunkMetaData.get(
        source.getPath(),
        source.getPrimitiveType(),
        source.getCodec(),
        source.getEncodingStats(),
        source.getEncodings(),
        source.getStatistics(),
        firstDataPageOffset,
        dictionaryPageOffset,
        source.getValueCount(),
        source.getTotalSize(),
        source.getTotalUncompressedSize());
  }

  private static byte[] serializeFooterAndTrailer(
      MessageType schema,
      List<BlockMetaData> blocks) {
    FileMetaData fileMetadata = new FileMetaData(
        schema, Collections.<String, String>emptyMap(), PARQUET_CREATOR);
    ParquetMetadata parquetMetadata = new ParquetMetadata(fileMetadata, blocks);
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    org.apache.parquet.format.FileMetaData thriftMetadata =
        converter.toParquetMetadata(PARQUET_VERSION, parquetMetadata);

    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      Util.writeFileMetaData(thriftMetadata, output);
      int footerLength = output.size();
      writeLittleEndianInt(output, footerLength);
      output.write(PARQUET_MAGIC);
      return output.toByteArray();
    } catch (IOException e) {
      // ByteArrayOutputStream does not throw during normal writes, but the Parquet serializer
      // exposes IOException and a corrupt metadata graph should fail planning explicitly.
      throw new IllegalStateException("failed to serialize synthetic Parquet footer", e);
    }
  }

  private static void writeLittleEndianInt(ByteArrayOutputStream output, int value) {
    output.write(value & 0xff);
    output.write((value >>> 8) & 0xff);
    output.write((value >>> 16) & 0xff);
    output.write((value >>> 24) & 0xff);
  }

  public long getSubtaskId() {
    return subtaskId;
  }

  public List<FileSlice> getFileSlices() {
    return fileSlices;
  }

  public byte[] getHeaderBytes() {
    return PARQUET_MAGIC.clone();
  }

  public byte[] getFooterAndTrailerBytes() {
    return footerAndTrailerBytes.clone();
  }

  public long getDataSizeBytes() {
    return dataSizeBytes;
  }

  /** Returns the first synthetic byte offset occupied by the serialized footer. */
  public long getFooterOffset() {
    return PARQUET_MAGIC.length + dataSizeBytes;
  }

  public long getTotalSizeBytes() {
    return totalSizeBytes;
  }

  public long getRowCount() {
    return rowCount;
  }

  /**
   * Consecutive filtered row groups borrowed from one footer result.
   *
   * <p>Only indexes are stored: the footer already owns the immutable block list, so copying the
   * block references or caching the same row/byte totals would add state without adding
   * ownership. The task-thread planner constructs this slice before publishing the enclosing
   * subtask to workers.</p>
   */
  public static final class FileSlice {
    private final FooterResult footer;
    private final int firstBlock;
    private final int blockCount;

    public FileSlice(FooterResult footer, int firstBlock, int blockCount) {
      this.footer = Objects.requireNonNull(footer, "footer");
      if (firstBlock < 0 || blockCount <= 0 ||
          firstBlock > footer.getBlocks().size() - blockCount) {
        throw new IllegalArgumentException(
            "invalid row-group slice: first=" + firstBlock + ", count=" + blockCount);
      }
      this.firstBlock = firstBlock;
      this.blockCount = blockCount;
    }

    public FooterResult getFooter() {
      return footer;
    }

    public int getFirstBlock() {
      return firstBlock;
    }

    public int getBlockCount() {
      return blockCount;
    }

    public List<BlockMetaData> getBlocks() {
      return footer.getBlocks().subList(firstBlock, firstBlock + blockCount);
    }
  }
}
