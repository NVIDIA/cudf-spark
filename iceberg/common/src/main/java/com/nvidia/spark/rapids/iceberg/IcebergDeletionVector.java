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
import com.nvidia.spark.rapids.jni.fileio.SeekableInputStream;
import org.apache.iceberg.io.IOUtil;

import java.io.IOException;

/**
 * An Iceberg deletion vector kept in its compressed Roaring-bitmap representation.
 *
 * <p>The serialized bytes use the portable 64-bit Roaring format expected by cuDF. This object
 * owns its host buffer and must be closed after all borrowed references have been released.
 */
public final class IcebergDeletionVector implements AutoCloseable {
    private final HostMemoryBuffer serializedBitmap;
    private final long serializedSizeInBytes;
    private final long cardinality;

    public IcebergDeletionVector(
            byte[] serializedIndex,
            int bitmapOffset,
            int bitmapLength,
            long cardinality) {
        HostMemoryBuffer bitmap = HostMemoryBuffer.allocate(bitmapLength);
        try {
            bitmap.setBytes(0, serializedIndex, bitmapOffset, bitmapLength);
        } catch (RuntimeException | Error e) {
            bitmap.close();
            throw e;
        }
        this.serializedBitmap = bitmap;
        this.serializedSizeInBytes = serializedIndex.length;
        this.cardinality = cardinality;
    }

    /** Reads an Iceberg deletion-vector byte range. */
    public static IcebergDeletionVector read(
            RapidsInputFile inputFile,
            Long offset,
            Long size,
            long cardinality) throws IOException {
        if (offset == null || offset < 0) {
            throw new IllegalArgumentException("Invalid deletion vector offset: " + offset);
        }
        if (size == null || size < 20 || size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid deletion vector size: " + size);
        }

        byte[] bytes = new byte[size.intValue()];
        try (SeekableInputStream stream = inputFile.open()) {
            stream.seek(offset);
            IOUtil.readFully(stream, bytes, 0, bytes.length);
        }

        return new IcebergDeletionVector(
                bytes, 8, bytes.length - 12, cardinality);
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
