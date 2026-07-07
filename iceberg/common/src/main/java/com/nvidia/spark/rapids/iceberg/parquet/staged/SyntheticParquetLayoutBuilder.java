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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

/**
 * Builds an exact synthetic-Parquet layout without reading column data.
 *
 * <p>The builder runs on the Spark task thread after footer filtering. Iceberg currently disables
 * CPU decompression in its multithreaded reader, so every copied column chunk retains its encoded
 * byte length. This lets the builder relocate all column metadata and serialize the final footer
 * before asynchronous I/O begins. If CPU-side decompression is introduced later, callers must not
 * use this builder until decompressed sizes can be planned exactly.</p>
 *
 * <p>The builder never mutates source metadata. It emits a new {@link BlockMetaData} and
 * {@link ColumnChunkMetaData} graph whose offsets address the synthetic file.</p>
 */
public final class SyntheticParquetLayoutBuilder {
  private static final byte[] PARQUET_MAGIC = new byte[] {'P', 'A', 'R', '1'};
  private static final int PARQUET_VERSION = 1;
  private static final String PARQUET_CREATOR = "RAPIDS Spark Plugin";

  /**
   * Builds the exact layout for ordered, mutually compatible segments.
   *
   * @param segments non-empty segments in synthetic-file order
   * @return exact immutable layout
   */
  public SyntheticParquetLayout build(List<ReadSegment> segments) {
    if (segments == null || segments.isEmpty()) {
      throw new IllegalArgumentException("segments must not be empty");
    }

    MessageType schema = segments.get(0).getFooter().getClippedSchema();
    ArrayList<PlannedReadRange> ranges = new ArrayList<>();
    ArrayList<BlockMetaData> adjustedBlocks = new ArrayList<>();
    long outputOffset = PARQUET_MAGIC.length;

    for (ReadSegment segment : segments) {
      if (!schema.equals(segment.getFooter().getClippedSchema())) {
        throw new IllegalArgumentException(
            "all segments in a synthetic Parquet file must use the same schema");
      }
      for (BlockMetaData sourceBlock : segment.getBlocks()) {
        BlockMetaData adjustedBlock = new BlockMetaData();
        adjustedBlock.setRowCount(sourceBlock.getRowCount());
        adjustedBlock.setRowIndexOffset(sourceBlock.getRowIndexOffset());
        long uncompressedSize = 0L;

        for (ColumnChunkMetaData sourceColumn : sourceBlock.getColumns()) {
          long sourceOffset = sourceColumn.getStartingPos();
          long length = sourceColumn.getTotalSize();
          ranges.add(new PlannedReadRange(
              segment.getFooter().getSource(), sourceOffset, length, outputOffset));

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

    byte[] footerAndTrailer = serializeFooterAndTrailer(schema, adjustedBlocks);
    long dataSize = outputOffset - PARQUET_MAGIC.length;
    long totalSize = Math.addExact(outputOffset, footerAndTrailer.length);
    return new SyntheticParquetLayout(
        ranges,
        PARQUET_MAGIC,
        footerAndTrailer,
        dataSize,
        totalSize);
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
          "staged synthetic Parquet does not support encrypted column metadata");
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
}
