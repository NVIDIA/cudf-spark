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

package com.nvidia.spark.rapids.delta;

import com.databricks.sql.transaction.tahoe.deletionvectors.NativeRoaringBitmapArraySerializationFormat;
import com.databricks.sql.transaction.tahoe.deletionvectors.PortableRoaringBitmapArraySerializationFormat;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.roaringbitmap.RoaringBitmap;

final class DeltaBitmapUtils {
  private static final int DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE = 4;

  private DeltaBitmapUtils() {
  }

  static int portableMagicNumber() {
    return PortableRoaringBitmapArraySerializationFormat.MAGIC_NUMBER();
  }

  static int nativeMagicNumber() {
    return NativeRoaringBitmapArraySerializationFormat.MAGIC_NUMBER();
  }

  static RoaringBitmap[] deserializeNative(ByteBuffer buffer) {
    return NativeRoaringBitmapArraySerializationFormat.deserialize(buffer);
  }

  static byte[] serializeAsDeltaPortable(RoaringBitmap[] bitmaps) {
    long serializedSize =
        PortableRoaringBitmapArraySerializationFormat.serializedSizeInBytes(bitmaps) +
            DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE;
    if (serializedSize > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Serialized deletion vector bitmap is too large: " + serializedSize + " bytes");
    }
    // Roaring bitmap serialization stores all words in little-endian order by
    // format contract, independent of the native CPU byte order.
    // See https://github.com/RoaringBitmap/RoaringFormatSpec#general-layout
    ByteBuffer buffer = ByteBuffer.allocate((int) serializedSize).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(PortableRoaringBitmapArraySerializationFormat.MAGIC_NUMBER());
    PortableRoaringBitmapArraySerializationFormat.serialize(bitmaps, buffer);
    return buffer.array();
  }
}
