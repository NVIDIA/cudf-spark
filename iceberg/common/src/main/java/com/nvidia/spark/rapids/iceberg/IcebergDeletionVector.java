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

package com.nvidia.spark.rapids.iceberg;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.zip.CRC32;

/**
 * An Iceberg deletion vector kept in its compressed Roaring-bitmap representation.
 *
 * <p>The serialized bytes use the portable 64-bit Roaring format expected by cuDF. This object
 * owns its host buffer and must be closed after all borrowed references have been released.
 */
public final class IcebergDeletionVector implements AutoCloseable {
    private static final int MAGIC_NUMBER = 0x6439D3D1;
    private static final int LENGTH_SIZE_BYTES = Integer.BYTES;
    private static final int MAGIC_NUMBER_SIZE_BYTES = Integer.BYTES;
    private static final int CRC_SIZE_BYTES = Integer.BYTES;
    private static final int BITMAP_OFFSET_BYTES = LENGTH_SIZE_BYTES + MAGIC_NUMBER_SIZE_BYTES;
    private static final int ENVELOPE_SIZE_BYTES = BITMAP_OFFSET_BYTES + CRC_SIZE_BYTES;
    private static final int MINIMUM_SIZE_BYTES = 20;

    private final HostMemoryBuffer serializedBitmap;
    private final long serializedSizeInBytes;
    private final long cardinality;

    IcebergDeletionVector(
            HostMemoryBuffer serializedBitmap,
            long serializedSizeInBytes,
            long cardinality) {
        this.serializedBitmap = serializedBitmap;
        this.serializedSizeInBytes = serializedSizeInBytes;
        this.cardinality = cardinality;
    }

    /**
     * Reads and validates an Iceberg deletion-vector byte range.
     *
     * <p>The range contains the bitmap-data length as a 4-byte big-endian integer, a 4-byte
     * little-endian magic number, the portable Roaring bitmap in little-endian order, and a
     * 4-byte big-endian CRC-32 of the magic number and bitmap. Only the bitmap is copied into the
     * returned host buffer because that is the representation expected by cuDF. The checksum is
     * validated only when {@code validateCrc} is true.
     */
    public static IcebergDeletionVector read(
            RapidsInputFile inputFile,
            Long offset,
            Long size,
            long cardinality,
            boolean validateCrc) throws IOException {
        if (offset == null || offset < 0) {
            throw new IllegalArgumentException("Invalid deletion vector offset: " + offset);
        }
        if (size == null || size < MINIMUM_SIZE_BYTES || size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid deletion vector size: " + size);
        }
        if (offset > Long.MAX_VALUE - size) {
            throw new IllegalArgumentException(
                    "Invalid deletion vector range: offset=" + offset + ", size=" + size);
        }

        try (HostMemoryBuffer envelope = HostMemoryBuffer.allocate(size)) {
            inputFile.readVectored(
                    envelope,
                    Collections.singletonList(new CopyRange(offset, size, 0)));
            ByteBuffer envelopeBuffer = envelope.asByteBuffer().order(ByteOrder.BIG_ENDIAN);
            int bitmapDataLength = envelopeBuffer.getInt();
            int expectedBitmapDataLength =
                    Math.toIntExact(size - LENGTH_SIZE_BYTES - CRC_SIZE_BYTES);
            if (bitmapDataLength != expectedBitmapDataLength) {
                throw new IOException("Invalid bitmap data length: " + bitmapDataLength
                        + ", expected " + expectedBitmapDataLength);
            }
            int magicNumber =
                    envelopeBuffer.order(ByteOrder.LITTLE_ENDIAN).getInt(LENGTH_SIZE_BYTES);
            if (magicNumber != MAGIC_NUMBER) {
                throw new IOException("Invalid magic number: " + magicNumber
                        + ", expected " + MAGIC_NUMBER);
            }

            int bitmapLength = Math.toIntExact(size - ENVELOPE_SIZE_BYTES);
            if (validateCrc) {
                CRC32 crc = new CRC32();
                crc.update(envelope.asByteBuffer(LENGTH_SIZE_BYTES, expectedBitmapDataLength));
                int expectedCrc = envelope.asByteBuffer(
                        size - CRC_SIZE_BYTES, CRC_SIZE_BYTES).order(ByteOrder.BIG_ENDIAN).getInt();
                int actualCrc = (int) crc.getValue();
                if (actualCrc != expectedCrc) {
                    throw new IOException("Invalid CRC: " + actualCrc
                            + ", expected " + expectedCrc);
                }
            }

            HostMemoryBuffer bitmap = envelope.slice(BITMAP_OFFSET_BYTES, bitmapLength);
            try {
                return new IcebergDeletionVector(bitmap, size, cardinality);
            } catch (RuntimeException | Error e) {
                bitmap.close();
                throw e;
            }
        }
    }

    /**
     * Returns the portable serialized 64-bit Roaring bitmap expected by cuDF.
     *
     * <p>The returned buffer is owned by this object. Callers that retain it must increment its
     * reference count.
     */
    public HostMemoryBuffer serializedBitmap() {
        return serializedBitmap;
    }

    /** Returns the full serialized deletion-vector size, including its header and checksum. */
    public long serializedSizeInBytes() {
        return serializedSizeInBytes;
    }

    /** Returns the number of positions in the deletion vector. */
    public long cardinality() {
        return cardinality;
    }

    @Override
    public void close() {
        serializedBitmap.close();
    }
}
